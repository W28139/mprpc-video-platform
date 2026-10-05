#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <unistd.h>
#include "scheduler.pb.h"
#include "worker.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"
#include "video_platform/mysql_pool.h"
#include "video_platform/redis_client.h"
#include "video_platform/mq_client.h"
#include "video_platform/ffmpeg_executor.h"

using namespace video_platform;


static std::mutex g_schedule_job_mutex;

// job 是否已到达终态（SUCCESS/FAILED/CANCELED）
static bool IsJobTerminal(int32_t job_status)
{
    return job_status == static_cast<int32_t>(JobStatus::JOB_CANCELED)
        || job_status == static_cast<int32_t>(JobStatus::JOB_SUCCESS)
        || job_status == static_cast<int32_t>(JobStatus::JOB_FAILED);
}

// 若 job 已终态，把 shard 标记为 CANCELED（终态任务的残留执行不得被重新分配/重调度）。
static bool MarkShardCanceledIfJobTerminal(const std::string& shard_id,
                                           const std::string& job_id,
                                           ShardRecord& shard)
{
    // 看看Job存不存在
    auto j_opt = JobStore::GetInstance().Get(job_id);
    if (!j_opt.has_value()) return false;
    // 看看job有没有进入终态
    if (!IsJobTerminal(j_opt->status)) return false;

    // 进入终态，代表Job结束，分配已经完成，重置该分片，设置状态
    int from_status = shard.status; 
    shard.status = static_cast<int32_t>(ShardStatus::SHARD_CANCELED);
    shard.assigned_worker_id.clear();
    shard.attempt_id.clear();
    shard.updated_at = NowMs();
    ShardStore::GetInstance().UpdateIfStatus(shard_id, {from_status}, shard);
    return true;
}

// 检查 job 下所有 shard 是否都已进入终态；有"非 SUCCESS"的终态 shard（FAILED/CANCELED）
// 且没有进行中的 shard 时，标记 JOB_FAILED。
// FAILED = 业务失败；CANCELED = 卡死超时重试耗尽（系统放弃）——两者都无法产出成品，
// 任务一样合不出完整视频，故一并计入 any_failed。
static void CheckAndMarkJobFailed(const std::string& job_id)
{
    // job 已终态（尤其 JOB_CANCELED）→ 不覆盖，避免把用户取消改写成 FAILED
    auto job_opt = JobStore::GetInstance().Get(job_id);
    if (!job_opt.has_value()) return;
    if (IsJobTerminal(job_opt->status)) return;

    // 获取该 job 所有切片 shards
    auto shards = ShardStore::GetInstance().ListByJob(job_id);
    if (shards.empty()) return;

    bool all_terminal = true;
    bool any_failed = false;    // FAILED 或 CANCELED（非 SUCCESS 的终态）
    for (const auto& s : shards)
    {
        int st = s.status;
        int s_success  = static_cast<int32_t>(ShardStatus::SHARD_SUCCESS);
        int s_failed   = static_cast<int32_t>(ShardStatus::SHARD_FAILED);
        int s_canceled = static_cast<int32_t>(ShardStatus::SHARD_CANCELED);
        if (st != s_success && st != s_failed && st != s_canceled)
        {
            // 有进行中的shard，因此未进入终态
            all_terminal = false;
            break;
        }
        if (st == s_failed || st == s_canceled) any_failed = true;
    }

    if (!all_terminal || !any_failed) return;

    // 所有 shard 都在终态且至少有一个 FAILED/CANCELED → JOB_FAILED （任务失败，
    // 片是唯一的，坏一片成品永远合不成）
    auto job = job_opt.value();
    int from_status = job.status;
    job.status = static_cast<int32_t>(JobStatus::JOB_FAILED);
    job.updated_at = NowMs();
    JobStore::GetInstance().UpdateIfStatus(job_id, {from_status}, job);

    LOG_WARN("SchedulerService: job %s marked JOB_FAILED (all shards terminal, "
             "has FAILED/CANCELED)", job_id.c_str());
}


// 任何 shard 状态变为 WAITING 的路径都应调用，让 Scheduler 的 MQ 消费线程立即分配
static void NotifyShardWaiting(const std::string& shard_id)
{
    auto& mq = MqClient::GetInstance();         // 获取mq实例
    if (!mq.inited() || !mq.enabled()) return;  // 门卫：没启用就静默跳过

    // 发布 shard.waiting 事件：Push 即时分配, 失败退化为轮询兜底
    if (!mq.PublishShardWaiting(shard_id)) 
    {
        LOG_WARN("NotifyShardWaiting: publish %s failed, fallback to "
                 "SchedulingLoop polling", shard_id.c_str());
    }
}

class SchedulerServiceImpl : public SchedulerService {
public:
    //  对 job 进行切分并启动调度
    // 由 JobService.SubmitJob() 通过 ZK 发现的 SchedulerService_Stub 调用。
    // 本方法只做切分和写入，不做 Worker 分配。
    void ScheduleJob(::google::protobuf::RpcController* controller,
                     const ::ScheduleJobRequest* request,
                     ::ScheduleJobResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        // 必须作为原子单元，防止并发 ScheduleJob 混合分片计划。
        std::lock_guard<std::mutex> schedule_lock(g_schedule_job_mutex);

        const auto& jobInfo = request->job_info();
        const std::string& job_id = jobInfo.job_id();

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s, input=%s",
                 job_id.c_str(), jobInfo.input_path().c_str());

        
        // 获取Job实例,从数据库读取该job数据
        JobRecord local_job;
        bool job_exists = false;
        auto existing = JobStore::GetInstance().Get(job_id);
        // 读取成功
        if (existing.has_value())
        {
            // 拿到jobstore实例
            local_job = existing.value();
            job_exists = true;
        }
        // 数据库没有的话，就从request里直接拿，然后放入数据库中
        else
        {
            local_job.job_id             = jobInfo.job_id();
            local_job.user_id            = jobInfo.user_id();
            local_job.input_path         = jobInfo.input_path();
            local_job.output_path        = jobInfo.output_path();
            local_job.target_format      = jobInfo.target_format();
            local_job.target_resolution  = jobInfo.target_resolution();
            local_job.target_bitrate     = jobInfo.target_bitrate();
            local_job.priority           = jobInfo.priority();
            local_job.status             = static_cast<int32_t>(JobStatus::JOB_PENDING);
            local_job.shard_count        = 0;
            local_job.shard_duration_sec = jobInfo.shard_duration_sec(); 
            local_job.created_at         = NowMs();
            local_job.updated_at         = NowMs();
            JobStore::GetInstance().Insert(local_job);
            LOG_INFO("SchedulerService::ScheduleJob job_id=%s: created local copy from request",
                     job_id.c_str());
        }
        // 判断该jobstore是否已经切过片(比如之前调用过，只不过失败了需要重试)
        auto existing_shards = ShardStore::GetInstance().ListByJob(job_id);
        // 存在的话就直接传回去数据
        if (!existing_shards.empty())
        {
            LOG_INFO("SchedulerService::ScheduleJob job_id=%s: already has %zu shards, "
                     "returning existing plan (idempotent, fix #14)",
                     job_id.c_str(), existing_shards.size());

            response->set_error_code(0);
            response->set_error_msg("");
            response->set_accepted(true);
            response->set_shard_count(static_cast<int32_t>(existing_shards.size()));
            // 把分片信息传回去
            for (const auto& s : existing_shards)
            {
                auto* si = response->add_shards();
                si->set_shard_id(s.shard_id);
                si->set_job_id(s.job_id);
                si->set_shard_index(s.shard_index);
                si->set_start_ms(s.start_ms);
                si->set_duration_ms(s.duration_ms);
                si->set_status(static_cast<ShardStatus>(s.status));
                si->set_retry_count(s.retry_count);
                si->set_max_retry(s.max_retry);
                si->set_input_path(s.input_path);
                si->set_output_path(s.output_path);
                si->set_target_resolution(s.target_resolution);
                si->set_target_bitrate(s.target_bitrate);
                si->set_created_at(s.created_at);
                si->set_updated_at(s.updated_at);
            }
            done->Run();
            return;
        }

        // 确定切分参数
        auto& config = MprpcApplication::GetConfig();   // 拿到 mprpc 框架全局配置对象的引用
        int64_t job_duration_ms = 0;
        {
            auto video_info = FfmpegExecutor::Probe(local_job.input_path);
            
            // 调用Probe方法,探测视频真实时长
            if (video_info.valid && video_info.duration_ms > 0)
            {
                job_duration_ms = video_info.duration_ms;
                LOG_INFO("SchedulerService::ScheduleJob job_id=%s: probed video duration=%lldms "
                         "(%dx%d, codec=%s)",
                         job_id.c_str(), (long long)job_duration_ms,
                         video_info.width, video_info.height,
                         video_info.codec_name.c_str());
            }
            // 探测失败时回退到配置默认值
            else
            {
                int fallback_sec = config.LoadInt("job_duration_fallback_sec", 60, 1, 86400);
                job_duration_ms = static_cast<int64_t>(fallback_sec) * 1000;
                LOG_WARN("SchedulerService::ScheduleJob job_id=%s: ffprobe failed, "
                         "using fallback duration=%lldms",
                         job_id.c_str(), (long long)job_duration_ms);
            }
        }

        // 每个 shard 时长：优先用户指定 > 配置默认值 20s
        int shard_dur_sec = (local_job.shard_duration_sec > 0)
                            ? local_job.shard_duration_sec
                            : config.LoadInt("shard_duration_sec", 20, 1, 3600);
        int64_t shard_duration_ms = static_cast<int64_t>(shard_dur_sec) * 1000;

        // 按真实时长计算 shard 数量,切片小于1的就直接按1
        int shard_count = static_cast<int>((job_duration_ms + shard_duration_ms - 1)
                                           / shard_duration_ms);
        if (shard_count < 1) shard_count = 1;

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s: splitting into %d shards "
                 "(job_duration=%lldms, shard_duration=%lldms)",
                 job_id.c_str(), shard_count,
                 (long long)job_duration_ms, (long long)shard_duration_ms);

        // 回填真实时长并落库（切分完成后统一置 SCHEDULING）
        local_job.duration_sec = job_duration_ms / 1000;
        local_job.updated_at = NowMs();
        JobStore::GetInstance().Update(job_id, local_job);

        // 按时间切片创建 ShardRecord
        std::vector<ShardRecord> created_shards;
        for (int i = 0; i < shard_count; ++i)
        {
            int64_t shard_start = static_cast<int64_t>(i) * shard_duration_ms;
            // 最后一个 shard 时长取剩余部分（可能小于 shard_duration_ms）
            int64_t shard_dur = (i == shard_count - 1)
                                ? (job_duration_ms - shard_start)
                                : shard_duration_ms;
            if (shard_dur <= 0) shard_dur = shard_duration_ms;  // 防御
            
            // 创建该job对应的每个切片shard实例，每个切片初始化为wait状态
            ShardRecord shard;
            shard.shard_id    = job_id + "_shard_" + std::to_string(i);
            shard.job_id      = job_id;
            shard.shard_index = i;
            shard.start_ms    = shard_start;
            shard.duration_ms = shard_dur;
            shard.status      = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
            shard.assigned_worker_id.clear();
            shard.attempt_id.clear();
            shard.retry_count = 0;
            shard.max_retry   = 3;
            shard.input_path       = local_job.input_path;
            // 若 job 未指定 output_path，回退到 ffmpeg_work_dir
            std::string shard_out_dir = local_job.output_path;
            if (shard_out_dir.empty())
            {
                shard_out_dir = MprpcApplication::GetConfig().Load("ffmpeg_work_dir");
                if (shard_out_dir.empty()) shard_out_dir = "/tmp/transcode_worker";
            }
            shard.output_path      = shard_out_dir + "/" + shard.shard_id + ".mp4";
            shard.target_resolution = local_job.target_resolution;
            shard.target_bitrate    = local_job.target_bitrate;
            shard.created_at       = NowMs();
            shard.updated_at       = NowMs();

            // 把每个切片存入内存
            ShardStore::GetInstance().Insert(shard);
            created_shards.push_back(shard);
            LOG_INFO("SchedulerService: created shard %s [%lld-%lld ms]",
                     shard.shard_id.c_str(),
                     (long long)shard.start_ms,
                     (long long)(shard.start_ms + shard.duration_ms));
        }

        // 切完片及时更新该job状态，此时进入scheduling(正在为 shard 匹配 Worker)状态
        local_job.shard_count = shard_count;
        local_job.status = static_cast<int32_t>(JobStatus::JOB_SCHEDULING);
        local_job.updated_at = NowMs();
        JobStore::GetInstance().Update(job_id, local_job);

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s: %d shards created, status=SCHEDULING",
                 job_id.c_str(), shard_count);

        // 把每个切片shard，发布到消息队列
        for (const auto& s : created_shards)
        {
            NotifyShardWaiting(s.shard_id);
        }

        // 把shard切片信息传回去
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        response->set_job_id(job_id);
        response->set_shard_count(shard_count);  // 直接数据路径，避免侧信道依赖

        // 将 shard 列表填入 response（历史遗留，目前感觉保留意义不大）
        for (const auto& s : created_shards)
        {
            auto* si = response->add_shards();
            si->set_shard_id(s.shard_id);
            si->set_job_id(s.job_id);
            si->set_shard_index(s.shard_index);
            si->set_start_ms(s.start_ms);
            si->set_duration_ms(s.duration_ms);
            si->set_status(static_cast<ShardStatus>(s.status));
            si->set_input_path(s.input_path);
            si->set_output_path(s.output_path);
            si->set_retry_count(s.retry_count);
            si->set_max_retry(s.max_retry);
            si->set_target_resolution(s.target_resolution);
            si->set_target_bitrate(s.target_bitrate);
            si->set_created_at(s.created_at);
            si->set_updated_at(s.updated_at);
        }
        done->Run();
    }

    // 重新调度一个 shard（失败 / 超时后触发）
    void RescheduleShard(::google::protobuf::RpcController* controller,
                         const ::RescheduleShardRequest* request,
                         ::RescheduleShardResponse* response,
                         ::google::protobuf::Closure* done) override
    {
        const std::string& shard_id = request->shard_id();
        LOG_INFO("SchedulerService::RescheduleShard shard_id=%s, job_id=%s, reason=%s",
                 shard_id.c_str(),
                 request->job_id().c_str(),
                 request->reason().c_str());

        auto shard_opt = ShardStore::GetInstance().Get(shard_id);
        if (!shard_opt.has_value())
        {
            response->set_error_code(1);
            response->set_error_msg("shard not found: " + shard_id);
            response->set_accepted(false);
            done->Run();
            return;
        }
        // 通过shard_id 拿到对应的shard
        auto shard = shard_opt.value();

        if (!request->attempt_id().empty()
            && shard.attempt_id != request->attempt_id())
        {
            LOG_WARN("SchedulerService::RescheduleShard shard_id=%s attempt=%s "
                     "ignored (current attempt=%s, idempotent)",
                     shard_id.c_str(), request->attempt_id().c_str(),
                     shard.attempt_id.c_str());
            response->set_error_code(0);
            response->set_error_msg("duplicate reschedule for stale attempt, ignored");
            response->set_accepted(false);
            done->Run();
            return;
        }

        // 终态守卫：SUCCESS / CANCELED 不复活（CANCELED 是 proto 语义明确的终态，
        // 取消窗口内到达的 FAILED 上报不得把已取消 shard 拉回重跑）。
        // 注意 FAILED 不在拒绝集合内——RescheduleShard 的职责就是处理
        // "执行失败 → 重试"，对 FAILED shard 的重复调用由上方 attempt_id 幂等检查拦截。
        {
            int st = shard.status;
            int s_success  = static_cast<int32_t>(ShardStatus::SHARD_SUCCESS);
            int s_canceled = static_cast<int32_t>(ShardStatus::SHARD_CANCELED);
            if (st == s_success || st == s_canceled)
            {
                LOG_WARN("SchedulerService::RescheduleShard shard_id=%s already "
                         "terminal (status=%d), rejecting reschedule",
                         shard_id.c_str(), st);
                response->set_error_code(0);
                response->set_error_msg("shard already terminal, reschedule rejected");
                response->set_accepted(false);
                done->Run();
                return;
            }

            // 判断是否为job 已终态（SUCCESS/FAILED/CANCELED）的 shard
            // CANCELED，不重调度（与 NotifyWorkerOffline 同语义）
            auto j_opt = JobStore::GetInstance().Get(shard.job_id);
            if (j_opt.has_value())
            {
                int32_t j_status = j_opt->status;
                bool job_terminal =
                    j_status == static_cast<int32_t>(JobStatus::JOB_CANCELED)
                 || j_status == static_cast<int32_t>(JobStatus::JOB_SUCCESS)
                 || j_status == static_cast<int32_t>(JobStatus::JOB_FAILED);
                if (job_terminal)
                {
                    int from_status = shard.status;   // 快照前置状态
                    shard.status = s_canceled;       
                    shard.assigned_worker_id.clear();
                    shard.attempt_id.clear();
                    shard.updated_at = NowMs();
                    // 把shard更改为cancel
                    ShardStore::GetInstance().UpdateIfStatus(shard_id, {from_status}, shard);
                    LOG_WARN("SchedulerService::RescheduleShard shard_id=%s job %s "
                             "terminal (%d), marked CANCELED and rejected",
                             shard_id.c_str(), shard.job_id.c_str(), j_status);
                    response->set_error_code(0);
                    response->set_error_msg("job terminal, shard marked CANCELED");
                    response->set_accepted(false);
                    done->Run();
                    return;
                }
            }
        }

        // 检查是否已达最大重试次数
        if (shard.retry_count >= shard.max_retry)
        {
            LOG_WARN("SchedulerService::RescheduleShard shard_id=%s reached max retry "
                     "(%d/%d), marking SHARD_FAILED",
                     shard_id.c_str(), shard.retry_count, shard.max_retry);

            // 设置shard状态为出错，然后更新
            shard.status = static_cast<int32_t>(ShardStatus::SHARD_FAILED);
            ShardStore::GetInstance().UpdateIfStatus(
                shard_id, {static_cast<int32_t>(ShardStatus::SHARD_WAITING),
                           static_cast<int32_t>(ShardStatus::SHARD_RUNNING)},
                shard);

            // 检查该 job 是否需要标记为 JOB_FAILED
            CheckAndMarkJobFailed(shard.job_id);

            response->set_error_code(0);
            response->set_error_msg("max retry exceeded, shard marked FAILED");
            response->set_accepted(false);
            done->Run();
            return;
        }

        // 未超次：增加重试计数，直接重置为 WAITING（单条 CAS，无中间过渡态）
        int from_status = shard.status;   // 快照前置状态（条件更新防覆盖）
        shard.retry_count++;
        shard.status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
        // 保留 assigned_worker_id（= 刚失败的 Worker）：TryAssignShard 重试时据此降权，
        // 优先换节点重试（磁盘满/OOM 等节点本地故障，重试同一节点必然再失败）
        shard.attempt_id.clear();
        shard.updated_at = NowMs();
        ShardStore::GetInstance().UpdateIfStatus(shard_id, {from_status}, shard);

        LOG_INFO("SchedulerService::RescheduleShard shard_id=%s retry=%d/%d → WAITING",
                 shard_id.c_str(), shard.retry_count, shard.max_retry);

        // 重试的 shard 也发事件立即重分配（不等轮询）
        NotifyShardWaiting(shard_id);

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        done->Run();
    }

    // Worker 心跳超时通知 — 重调度该 Worker 上所有运行中的 shard
    void NotifyWorkerOffline(::google::protobuf::RpcController* controller,
                             const ::NotifyWorkerOfflineRequest* request,
                             ::NotifyWorkerOfflineResponse* response,
                             ::google::protobuf::Closure* done) override
    {
        const std::string& worker_id = request->worker_id();
        LOG_INFO("SchedulerService::NotifyWorkerOffline worker_id=%s, reason=%s",
                 worker_id.c_str(), request->reason().c_str());
        
        // 拿到该work里的所有片,置为wait,让其他空闲work调用
        auto shards = ShardStore::GetInstance().ListByWorker(worker_id);
        int rescheduled = 0;

        // 仅当 shard 仍是 RUNNING 时才重调度
        constexpr int32_t s_running  = static_cast<int32_t>(ShardStatus::SHARD_RUNNING);

        for (const auto& s : shards)
        {
            int st = s.status;
            if (st != s_running)
                continue;

            auto fresh_opt = ShardStore::GetInstance().Get(s.shard_id);
            if (!fresh_opt.has_value()) continue;
            auto shard = fresh_opt.value();

            // job 已终态（SUCCESS/FAILED/CANCELED）的 shard 不重调度
            if (MarkShardCanceledIfJobTerminal(s.shard_id, shard.job_id, shard))
            {
                LOG_INFO("SchedulerService::NotifyWorkerOffline: shard %s "
                         "→ CANCELED (job terminal)",
                         s.shard_id.c_str());
                continue;
            }

            if (shard.retry_count >= shard.max_retry)
            {
                // 已达最大重试次数 → 永久失败
                shard.status = static_cast<int32_t>(ShardStatus::SHARD_FAILED);
                shard.updated_at = NowMs();
                ShardStore::GetInstance().UpdateIfStatus(
                    s.shard_id, {s_running}, shard);
                LOG_WARN("SchedulerService::NotifyWorkerOffline: shard %s max retry "
                         "(%d/%d) → FAILED",
                         s.shard_id.c_str(), shard.retry_count, shard.max_retry);
                CheckAndMarkJobFailed(shard.job_id);
            }
            else
            {
                // 增加重试计数，重置为 WAITING
                shard.retry_count++;
                shard.status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
                shard.assigned_worker_id.clear();
                shard.attempt_id.clear();
                shard.updated_at = NowMs();
                ShardStore::GetInstance().UpdateIfStatus(
                    s.shard_id, {s_running}, shard);
                ++rescheduled;
                NotifyShardWaiting(s.shard_id);
                LOG_INFO("SchedulerService::NotifyWorkerOffline: shard %s → WAITING "
                         "(retry=%d/%d)",
                         s.shard_id.c_str(), shard.retry_count, shard.max_retry);
            }
        }

        LOG_INFO("SchedulerService::NotifyWorkerOffline worker_id=%s: %d shards "
                 "rescheduled (of %zu)",
                 worker_id.c_str(), rescheduled, shards.size());

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_rescheduled_count(rescheduled);
        done->Run();
    }

    // 取消 job 的所有 shard 并 best-effort 通知 Worker
    void CancelJobShards(::google::protobuf::RpcController* controller,
                          const ::CancelJobShardsRequest* request,
                          ::CancelJobShardsResponse* response,
                          ::google::protobuf::Closure* done) override
    {
        const std::string& job_id = request->job_id();
        const std::string& reason = request->reason().empty()
                                    ? "USER_CANCEL" : request->reason();
        LOG_INFO("SchedulerService::CancelJobShards job_id=%s, reason=%s",
                 job_id.c_str(), reason.c_str());

        // 1. 列出该 job 的所有 shard
        auto shards = ShardStore::GetInstance().ListByJob(job_id);
        int total = static_cast<int>(shards.size());
        int notified = 0;
        int skipped = 0;

        if (shards.empty())
        {
            LOG_INFO("SchedulerService::CancelJobShards job_id=%s: "
                     "no shards found in local store", job_id.c_str());
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_total_shards(0);
            response->set_notified_count(0);
            response->set_skipped_count(0);
            done->Run();
            return;
        }

        // 2. 收集需要通知的 shard (处于worker里的shard)
        // 先排除SHARD_SUCCESS与SHARD_FAILED
        std::vector<std::pair<std::string, std::string>> shards_to_notify;
        int32_t s_success  = static_cast<int32_t>(ShardStatus::SHARD_SUCCESS);
        int32_t s_failed   = static_cast<int32_t>(ShardStatus::SHARD_FAILED);

        // 拿分片以及对应的work_id
        for (const auto& s : shards)
        {
            if (s.status != s_success && s.status != s_failed
                && !s.assigned_worker_id.empty())
            {
                shards_to_notify.emplace_back(s.shard_id, s.assigned_worker_id);
            }
            else
            {
                ++skipped;
            }
        }

        if (shards_to_notify.empty())
        {
            LOG_INFO("SchedulerService::CancelJobShards job_id=%s: "
                     "no active shards to notify (total=%d, skipped=%d)",
                     job_id.c_str(), total, skipped);
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_total_shards(total);
            response->set_notified_count(0);
            response->set_skipped_count(skipped);
            done->Run();
            return;
        }

        // 3. 查询 WorkerManager 获取 worker ip:port 映射
        MprpcChannel wm_channel;
        WorkerManagerService_Stub wm_stub(&wm_channel);

        ListWorkersRequest lw_req;
        lw_req.set_filter_status(WorkerStatus::WORKER_STATUS_UNKNOWN);

        ListWorkersResponse lw_resp;
        MprpcController lw_ctrl;
        lw_ctrl.SetTimeoutMs(3000);

        wm_stub.ListWorkers(&lw_ctrl, &lw_req, &lw_resp, nullptr);

        if (lw_ctrl.Failed() || lw_resp.error_code() != 0)
        {
            LOG_WARN("SchedulerService::CancelJobShards job_id=%s: "
                     "ListWorkers RPC failed, skipping worker notification "
                     "for %zu shards: %s",
                     job_id.c_str(), shards_to_notify.size(),
                     lw_ctrl.Failed() ? lw_ctrl.ErrorText().c_str()
                                      : lw_resp.error_msg().c_str());
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_total_shards(total);
            response->set_notified_count(0);
            response->set_skipped_count(skipped + static_cast<int>(shards_to_notify.size()));
            done->Run();
            return;
        }

        // 4. 构建 worker_id → (ip, port) 映射
        std::unordered_map<std::string,
                           std::pair<std::string, uint16_t>> worker_map;
        for (const auto& w : lw_resp.workers())
        {
            worker_map[w.worker_id()] = {w.ip(),
                                         static_cast<uint16_t>(w.port())};
        }

        // 5. 直连每个 Worker 发送 CancelShard
        // 根据分片—分片所在work_id 与 work_id-对应ip_port信息，找到分片对应的具体哪个work
        for (const auto& kv : shards_to_notify)
        {
            const std::string& shard_id  = kv.first;
            const std::string& worker_id = kv.second;

            auto it = worker_map.find(worker_id);
            if (it == worker_map.end())
            {
                LOG_WARN("SchedulerService::CancelJobShards job_id=%s: "
                         "worker %s not found in ListWorkers result, "
                         "skipping shard %s",
                         job_id.c_str(), worker_id.c_str(), shard_id.c_str());
                ++skipped;
                continue;
            }

            const auto& ip   = it->second.first;
            uint16_t    port = it->second.second;

            MprpcChannel worker_channel(ip, port);
            WorkerService_Stub worker_stub(&worker_channel);

            CancelShardRequest cs_req;
            cs_req.set_shard_id(shard_id);
            cs_req.set_reason(reason);

            // 对每个有shard的worker通知，取消该shard
            CancelShardResponse cs_resp;
            MprpcController cs_ctrl;
            cs_ctrl.SetTimeoutMs(3000);

            worker_stub.CancelShard(&cs_ctrl, &cs_req, &cs_resp, nullptr);

            if (!cs_ctrl.Failed() && cs_resp.canceled())
            {
                ++notified;
                LOG_INFO("SchedulerService::CancelJobShards: notified "
                         "worker %s (%s:%d) to cancel shard %s",
                         worker_id.c_str(), ip.c_str(), port, shard_id.c_str());
            }
            else
            {
                ++skipped;
                LOG_WARN("SchedulerService::CancelJobShards: CancelShard "
                         "to worker %s (%s:%d) for shard %s failed: %s",
                         worker_id.c_str(), ip.c_str(), port, shard_id.c_str(),
                         cs_ctrl.Failed() ? cs_ctrl.ErrorText().c_str()
                                          : cs_resp.error_msg().c_str());
            }
        }

        LOG_INFO("SchedulerService::CancelJobShards job_id=%s: done "
                 "(total=%d, notified=%d, skipped=%d)",
                 job_id.c_str(), total, notified, skipped);

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_total_shards(total);
        response->set_notified_count(notified);
        response->set_skipped_count(skipped);
        done->Run();
    }

};


// 用于查询返回ONLINE Worker列表
static bool LoadOnlineWorkers(WorkerManagerService_Stub& wm_stub,
                              ListWorkersResponse& resp)
{
    // 优先查询：Redis 快照
    auto& redis = RedisClient::GetInstance();
    if (redis.inited() && redis.enabled())
    {
        std::vector<std::pair<std::string, std::string>> snapshots;
        if (redis.HGetAll("worker:load", snapshots) && !snapshots.empty())
        {
            constexpr int64_t kHeartbeatTimeoutMs = 20000;  // 与 WorkerManager 一致
            int64_t now = NowMs();
            bool any_live = false;
            for (const auto& [worker_id, payload] : snapshots)
            {
                // 解析快照串
                // 快照串格式 `ip|port|cpu_cores|memory_mb|max_running|current_running|cpu_usage|memory_usage|ts`）
                std::string::size_type pos = 0, next = 0;
                std::string fields[9];
                int idx = 0;
                while (idx < 9 && (next = payload.find('|', pos)) != std::string::npos)
                {
                    fields[idx++] = payload.substr(pos, next - pos);
                    pos = next + 1;
                }
                if (idx == 8)  // 最后一段（ts）后没有 '|'，单独收尾
                    fields[8] = payload.substr(pos);
                if (fields[8].empty()) continue;  // 格式不完整，跳过

                int64_t ts = std::atoll(fields[8].c_str());
                // 如果 当前时间 - 上一次心跳时间 > 最大时间 ==> 超时，视为offline，不加入返回列
                if (now - ts > kHeartbeatTimeoutMs) continue;  // 心跳超时 → OFFLINE

                auto* wi = resp.add_workers();
                wi->set_worker_id(worker_id);
                wi->set_ip(fields[0]);
                wi->set_port(std::atoi(fields[1].c_str()));
                wi->set_cpu_cores(std::atoi(fields[2].c_str()));
                wi->set_memory_mb(std::atoi(fields[3].c_str()));
                wi->set_max_running_shards(std::atoi(fields[4].c_str()));
                wi->set_current_running_shards(std::atoi(fields[5].c_str()));
                wi->set_cpu_usage(std::atoi(fields[6].c_str()));
                wi->set_memory_usage(std::atoi(fields[7].c_str()));
                wi->set_status(WorkerStatus::WORKER_ONLINE);
                wi->set_last_heartbeat(ts);
                any_live = true;
            }
            if (any_live)
            {
                LOG_INFO("LoadOnlineWorkers: %d ONLINE workers from redis snapshot",
                         resp.workers_size());
                return true;
            }
            // 快照存在但全部过期：Worker 可能真下线了，回退 RPC 确认
            LOG_WARN("LoadOnlineWorkers: redis snapshot all expired, fallback to "
                     "ListWorkers RPC");
        }
        else
        {
            LOG_WARN("LoadOnlineWorkers: redis snapshot unavailable, fallback to "
                     "ListWorkers RPC");
        }
    }

    // 回退调用：ListWorkers RPC （开销会大一些）
    // 这里拿到数据无需写回redis，因为redis会由WorkerManager心跳更新
    ListWorkersRequest req;
    req.set_filter_status(WorkerStatus::WORKER_ONLINE);
    ListWorkersResponse rpc_resp;
    MprpcController ctrl;
    ctrl.SetTimeoutMs(3000);
    wm_stub.ListWorkers(&ctrl, &req, &rpc_resp, nullptr);
    if (ctrl.Failed() || rpc_resp.error_code() != 0)
    {
        LOG_WARN("LoadOnlineWorkers: ListWorkers RPC failed: %s",
                 ctrl.Failed() ? ctrl.ErrorText().c_str()
                               : rpc_resp.error_msg().c_str());
        return false;
    }
    LOG_INFO("LoadOnlineWorkers: %d ONLINE workers from ListWorkers RPC (redis degraded)",
             rpc_resp.workers_size());
             
    // 把拿到的数据放入resp中
    resp.Swap(&rpc_resp);
    return true;
}

// 为单个 WAITING shard 分配 worker 并推进 RUNNING（轮询与 MQ 消费线程共用）
static bool TryAssignShard(const ShardRecord& shard,
                           const ListWorkersResponse& workers,
                           std::unordered_map<std::string, int>& round_assigned,
                           const char* caller)
{
    // 状态复核,重新查库——shard 现在还是不是 WAITING
    // 避免传进来的快照shard已经过期（毕竟并发，可能两个相同shard被同时调用,后调用的就会过期）
    {
        auto fresh_opt = ShardStore::GetInstance().Get(shard.shard_id);
        if (!fresh_opt.has_value())
        {
            LOG_INFO("%s: shard %s not found, skip", caller, shard.shard_id.c_str());
            return false;
        }
        if (fresh_opt->status != static_cast<int32_t>(ShardStatus::SHARD_WAITING))
        {
            LOG_INFO("%s: shard %s already status=%d, skip (double-path guard)",
                     caller, shard.shard_id.c_str(), fresh_opt->status);
            return false;
        }
    }

    // 终态检查：job 已终态的残留 shard 直接 CANCELED，不分配
    //（终态通知到达前被重置为 WAITING 的残留 shard，此处兜底拦截）
    ShardRecord terminal_check = shard;
    if (MarkShardCanceledIfJobTerminal(terminal_check.shard_id,
                                       terminal_check.job_id, terminal_check))
    {
        LOG_INFO("%s: shard %s marked CANCELED (job terminal)",
                 caller, shard.shard_id.c_str());
        return false;
    }

    // 加权评分选择 Worker
    // score = available_slots * 10 - cpu_usage * 0.5 - memory_usage * 0.2
    // 空闲槽位越多、负载越低的 Worker 得分越高
    const WorkerInfo* best_worker = nullptr;
    double best_score = -1e9;   // 低于任何降权后的分数，保证唯一可用候选仍能被选中

    for (const auto& w : workers.workers())
    {
        // round_assigned 本回合已分配计数（防同一轮对同一 Worker 超额分配；轮询来源按轮共享，MQ 来源每次调用独立）
        int already = round_assigned[w.worker_id()];
        int available = w.max_running_shards()
                      - w.current_running_shards()
                      - already;
        if (available <= 0) continue;  // 无空槽位，跳过

        double score = available * 10.0
                     - w.cpu_usage() * 0.5
                     - w.memory_usage() * 0.2;

        // 重试时降权上次执行的 Worker：优先换节点重试（节点本地故障如磁盘满/OOM
        // 重试同一节点必然再失败）；若它是唯一可用候选仍会被选中，不会卡死 shard
        if (shard.retry_count > 0 && !shard.assigned_worker_id.empty()
            && w.worker_id() == shard.assigned_worker_id)
        {
            score -= 1000.0;
        }

        if (score > best_score)
        {
            best_score = score;
            best_worker = &w;
        }
    }

    if (best_worker == nullptr)
    {
        LOG_WARN("%s: no worker with available slots for shard %s (workers=%d)",
                 caller, shard.shard_id.c_str(), workers.workers_size());
        return false; 
    }

    // 本地槽位计数 +1
    round_assigned[best_worker->worker_id()]++;

    // 分布式锁：SETNX 防多实例/重复投递重复分配
    // - 锁 key：shard:lock:{shard_id}，value：scheduler:{pid}，TTL 10s（TTL 防持锁进程崩溃后死锁）
    // - 释放时 GET 校验 value 相同才 DEL，防误删他人锁
    // - Redis 故障降级放行：MySQL 条件更新（WAITING→RUNNING）兜底
    auto& redis = RedisClient::GetInstance();
    bool lock_acquired = false;
    std::string lock_key = "shard:lock:" + shard.shard_id;
    std::string lock_value;
    if (redis.inited() && redis.enabled())
    {
        // 保证不同进程lock_valude不同
        lock_value = "scheduler:" + std::to_string(::getpid());

        // 抢该 shard 的分布式锁（SET NX EX 原子）：防多实例/双路径重复分配
        // 抢到 → 持锁执行下方分配
        if (redis.SetNxEx(lock_key, lock_value, 10))
        {
            lock_acquired = true;
        }
        // 没抢到 → 走 else 区分"被占用"与"Redis 故障"
        else
        {
            // 区分"锁被占用"与"Redis 故障"：
            std::string cur;
            bool found = false;
            if (redis.Get(lock_key, cur, found) && found)
            {
                // 锁被占用 → 其他实例正在分配，本实例跳过
                LOG_INFO("%s: shard %s locked by another instance (value=%s), skip",
                         caller, shard.shard_id.c_str(), cur.c_str());
                return false;
            }
            else
            {
                // Redis 故障 / 锁不存在 → 降级放行，MySQL 条件更新兜底
                LOG_WARN("%s: distributed lock unavailable for %s, degrade to MySQL "
                         "conditional update", caller, shard.shard_id.c_str());
            }
        }
    }
    // 写针对上锁释放方法，给下面调用：值校验 + DEL（Redis 故障时 GET 失败，锁自然 TTL 过期）
    auto release_lock = [&]() {
        if (!lock_acquired) return;
        std::string cur;
        bool found = false;
        if (redis.Get(lock_key, cur, found) && found && cur == lock_value)
            redis.Del(lock_key);
    };

    // 直连 Worker 的 AssignShard
    constexpr int64_t kAssignTimeoutMs = 5000;  // AssignShard RPC 超时
    MprpcChannel worker_channel(best_worker->ip(),
                                static_cast<uint16_t>(best_worker->port()));
    WorkerService_Stub worker_stub(&worker_channel);

    AssignShardRequest as_req;
    auto* si = as_req.mutable_shard();
    si->set_shard_id(shard.shard_id);
    si->set_job_id(shard.job_id);
    si->set_shard_index(shard.shard_index);
    si->set_start_ms(shard.start_ms);
    si->set_duration_ms(shard.duration_ms);
    si->set_status(static_cast<ShardStatus>(shard.status));
    si->set_input_path(shard.input_path);
    si->set_output_path(shard.output_path);
    si->set_created_at(shard.created_at);
    si->set_retry_count(shard.retry_count);  // 透传 retry_count 给 Worker
    si->set_target_resolution(shard.target_resolution);  // 透传转码参数
    si->set_target_bitrate(shard.target_bitrate);

    AssignShardResponse as_resp;
    MprpcController as_ctrl;
    as_ctrl.SetTimeoutMs(kAssignTimeoutMs);

    worker_stub.AssignShard(&as_ctrl, &as_req, &as_resp, nullptr);

    if (as_ctrl.Failed() || !as_resp.accepted())
    {
        LOG_WARN("%s: AssignShard %s to worker %s failed: %s",
                 caller, shard.shard_id.c_str(),
                 best_worker->worker_id().c_str(),
                 as_ctrl.Failed() ? as_ctrl.ErrorText().c_str()
                                  : as_resp.error_msg().c_str());
        release_lock();  // 分配失败立即释放，后续可重试
        return false;
    }

    // 更新 shard 状态 → RUNNING
    // Worker 同步接受（accepted=true 才走到这里），分配成功即开始执行。
    // 重新从 ShardStore 读取最新状态，避免用快照副本覆盖并发修改
    {
        auto fresh_opt = ShardStore::GetInstance().Get(shard.shard_id);
        if (!fresh_opt.has_value())  // 已被删除
        {
            release_lock();
            return false;
        }
        // 获取该切片，并进行修改状态以及其他信息
        auto fresh = fresh_opt.value();
        fresh.status = static_cast<int32_t>(ShardStatus::SHARD_RUNNING);
        fresh.assigned_worker_id = best_worker->worker_id();
        fresh.attempt_id = fresh.shard_id + "_attempt_" + std::to_string(fresh.retry_count);
        fresh.updated_at = NowMs();

        // 条件更新：仅当仍是 WAITING 才推进（防并发取消/终态覆盖/重复分配）
        ShardStore::GetInstance().UpdateIfStatus(
            fresh.shard_id,
            {static_cast<int32_t>(ShardStatus::SHARD_WAITING)}, fresh);
    }

    // 分配完成，释放分布式锁
    release_lock();

    // 更新 job 状态 → RUNNING（若是首次分配）
    auto job_opt = JobStore::GetInstance().Get(shard.job_id);
    if (job_opt.has_value() &&
        job_opt->status == static_cast<int32_t>(JobStatus::JOB_SCHEDULING))
    {
        auto job = job_opt.value();
        job.status = static_cast<int32_t>(JobStatus::JOB_RUNNING);
        job.updated_at = NowMs();
        // 条件更新：仅当仍是 SCHEDULING 才推进（防覆盖 RC 已写入的终态）
        JobStore::GetInstance().UpdateIfStatus(
            shard.job_id,
            {static_cast<int32_t>(JobStatus::JOB_SCHEDULING)}, job);
    }

    // 日志
    int remaining = best_worker->max_running_shards()
                  - best_worker->current_running_shards()
                  - round_assigned[best_worker->worker_id()];
    LOG_INFO("%s: assigned shard %s to worker %s (%s:%d), "
             "score=%.1f, cpu=%d%%, mem=%d%%, slots_left=%d",
             caller, shard.shard_id.c_str(),
             best_worker->worker_id().c_str(),
             best_worker->ip().c_str(),
             best_worker->port(),
             best_score,
             best_worker->cpu_usage(),
             best_worker->memory_usage(),
             remaining);
    return true;
}

// SchedulingLoop — 后台调度循环（Pull 模式）
// 在后台线程中运行调度循环，由 main() 在 Provider 启动前创建
static void SchedulingLoop(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kScheduleIntervalMs     = 2000;  // MQ 不在线时,Pull模式每 2 秒扫描一次
    constexpr int64_t kScheduleIntervalMsIdle = 5000;  // MQ 在线时, Pull模式每5秒扫描一次
    constexpr int64_t kTimeoutRescanIntervalMs = 30000; // 超时重扫周期（按时间，与间隔解耦）

    // 等待 Provider 启动完成
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 初步判断 mq 在线状态
    auto& mq = MqClient::GetInstance();
    bool mq_active = mq.inited() && mq.enabled() && mq.connected();
    int64_t interval_ms = mq_active ? kScheduleIntervalMsIdle
                                    : kScheduleIntervalMs;
    LOG_INFO("SchedulingLoop thread started, interval=%lldms (mq=%s)",
             (long long)interval_ms, mq_active ? "push" : "pull");

    // 启动恢复：扫描残留的 RUNNING shard 
    // 场景：Scheduler 进程崩溃（kill -9、OOM、段错误），然后被监控系统重启。
    // 崩溃前可能已经通过 AssignShard 将 shard 分配给了 Worker，shard 状态为 RUNNING。
    // 重启后 SchedulingLoop 只扫描 WAITING 状态，这些 shard 永远不会被重新调度——成为永久的"孤儿 shard"。
    {
        LOG_INFO("SchedulingLoop: running startup recovery scan...");
        constexpr int32_t s_running = static_cast<int32_t>(ShardStatus::SHARD_RUNNING);

        auto running_shards = ShardStore::GetInstance().ListByStatus(s_running);

        int recovered = 0;
        for (const auto& s : running_shards)
        {
            // 快照副本，条件更新保证不覆盖并发修改
            ShardRecord fresh = s;
            fresh.status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
            fresh.assigned_worker_id.clear();
            fresh.attempt_id.clear();
            fresh.updated_at = NowMs();
            if (ShardStore::GetInstance().UpdateIfStatus(
                    s.shard_id, {s_running}, fresh))
            {
                ++recovered;
                // 恢复的 shard 发事件立即重分配
                NotifyShardWaiting(s.shard_id);
                LOG_INFO("SchedulingLoop: startup recovery: reset %s RUNNING → WAITING",
                         s.shard_id.c_str());
            }
        }

        if (recovered > 0)
        {
            LOG_INFO("SchedulingLoop: startup recovery complete, %d shards reset to WAITING",
                     recovered);
        }
    }

    // WorkerManager 通过 ZK 发现
    WorkerManagerService_Stub wm_stub(new MprpcChannel());

    // Metrics 计数器 
    int64_t metrics_round    = 0;   // 循环次数
    int64_t shards_assigned  = 0;   // shards分配个数

    // 超时重扫时间戳（按时间驱动，见主循环）
    int64_t last_rescan_ms = 0;

    while (!stop_flag)
    {
        // 依据mq在线状态，确定循环频率
        bool mq_active = mq.inited() && mq.enabled() && mq.connected();
        interval_ms = mq_active ? kScheduleIntervalMsIdle
                                : kScheduleIntervalMs;
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        if (stop_flag) break;

        ++metrics_round;

        // RUNNING 超时重扫
        // Worker 接受 shard 后卡死（ffmpeg 挂死但心跳存活），导致 shard 永久停留在 RUNNING。
        // 超时后重置为 WAITING 让其他 Worker 接管。
        int64_t now_ms = NowMs();
        if (now_ms - last_rescan_ms >= kTimeoutRescanIntervalMs)
        {
            last_rescan_ms = now_ms;
            constexpr int64_t kRunningTimeoutMs  = 300000;  // RUNNING 5 分钟超时
            int64_t now = NowMs();

            // 查询 ONLINE Worker 地址表（Redis 快照优先，回退 RPC），
            // 超时重置前用它给原 Worker 发 CancelShard（best-effort）
            std::unordered_map<std::string,
                               std::pair<std::string, uint16_t>> worker_addr;
            {
                ListWorkersResponse lw_resp;
                if (!LoadOnlineWorkers(wm_stub, lw_resp))
                {
                    LOG_WARN("SchedulingLoop: LoadOnlineWorkers failed during "
                             "timeout rescan, skip rescan this round");
                }
                else
                {
                    for (const auto& w : lw_resp.workers())
                    {
                        worker_addr[w.worker_id()] = {
                            w.ip(), static_cast<uint16_t>(w.port())};
                    }
                }
            }

            auto running_shards = ShardStore::GetInstance().ListByStatus(
                static_cast<int32_t>(ShardStatus::SHARD_RUNNING));

            int timed_out = 0;
            // 处理 running_shards
            for (const auto& s : running_shards)
            {
                if (now - s.updated_at <= kRunningTimeoutMs) continue;
                auto fresh_opt = ShardStore::GetInstance().Get(s.shard_id);
                if (!fresh_opt.has_value()) continue;
                auto fresh = fresh_opt.value();
                if (fresh.status != static_cast<int32_t>(ShardStatus::SHARD_RUNNING))
                    continue;
                // job 已终态的残留 RUNNING shard 直接 CANCELED，
                // 避免重置为 WAITING 后被重复分配
                if (MarkShardCanceledIfJobTerminal(s.shard_id, fresh.job_id, fresh))
                {
                    ++timed_out;
                    LOG_WARN("SchedulingLoop: RUNNING timeout for shard %s "
                             "but job terminal, marked CANCELED",
                             s.shard_id.c_str());
                    continue;
                }
                // RUNNING 5 分钟超时本身已是很强的卡死信号（远超正常转码时长）
                // 无需 QueryShard 确认，直接：通知原 Worker 取消 + retry 语义修正。
                std::string stalled_worker = fresh.assigned_worker_id;
                std::string stalled_attempt = fresh.attempt_id;
                {
                    auto raddr_it = worker_addr.find(fresh.assigned_worker_id);
                    if (raddr_it != worker_addr.end())
                    {
                        MprpcChannel cancel_channel(raddr_it->second.first,
                                                    raddr_it->second.second);
                        WorkerService_Stub cancel_stub(&cancel_channel);
                        CancelShardRequest cs_req;
                        cs_req.set_shard_id(s.shard_id);
                        cs_req.set_reason("TIMEOUT");
                        cs_req.set_attempt_id(stalled_attempt);
                        CancelShardResponse cs_resp;
                        MprpcController cs_ctrl;
                        cs_ctrl.SetTimeoutMs(3000);
                        cancel_stub.CancelShard(&cs_ctrl, &cs_req, &cs_resp, nullptr);
                        if (cs_ctrl.Failed() || !cs_resp.canceled())
                        {
                            LOG_WARN("SchedulingLoop: CancelShard notify worker %s "
                                     "for shard %s failed: %s",
                                     stalled_worker.c_str(), s.shard_id.c_str(),
                                     cs_ctrl.Failed() ? cs_ctrl.ErrorText().c_str()
                                                      : cs_resp.error_msg().c_str());
                        }
                    }
                }

                if (fresh.retry_count >= fresh.max_retry)
                {
                    // 重试预算已耗尽 → 终态 CANCELED，不再分配
                    fresh.status = static_cast<int32_t>(ShardStatus::SHARD_CANCELED);
                    fresh.updated_at = NowMs();
                    ShardStore::GetInstance().UpdateIfStatus(
                        s.shard_id,
                        {static_cast<int32_t>(ShardStatus::SHARD_RUNNING)}, fresh);
                    ++timed_out;
                    LOG_WARN("SchedulingLoop: RUNNING timeout for shard %s (worker=%s) "
                             "and retry exhausted (%d/%d), marked CANCELED",
                             s.shard_id.c_str(), stalled_worker.c_str(),
                             fresh.retry_count, fresh.max_retry);
                    // 与"执行失败"路径一致：立即结算 job（CANCELED 计入 any_failed），
                    // 不等 RC 的 TerminalSweepLoop 兜底
                    CheckAndMarkJobFailed(fresh.job_id);
                    continue;
                }

                fresh.retry_count++;
                fresh.status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
                fresh.assigned_worker_id.clear();
                fresh.attempt_id.clear();
                fresh.updated_at = NowMs();
                ShardStore::GetInstance().UpdateIfStatus(
                    s.shard_id,
                    {static_cast<int32_t>(ShardStatus::SHARD_RUNNING)}, fresh);
                ++timed_out;
                // 重置为 WAITING 的 shard 发事件立即重分配
                NotifyShardWaiting(s.shard_id);
                LOG_WARN("SchedulingLoop: RUNNING timeout for shard %s (worker=%s, "
                         "stale=%lldms), canceled and reset to WAITING (retry=%d/%d)",
                         s.shard_id.c_str(), stalled_worker.c_str(),
                         (long long)(now - s.updated_at),
                         fresh.retry_count, fresh.max_retry);
            }
            if (timed_out > 0)
            {
                LOG_INFO("SchedulingLoop: timeout rescan complete, %d shards reset to WAITING",
                         timed_out);
            }
        }

        // 开始扫描需要提交的 WAITING shard
        auto waiting_shards = ShardStore::GetInstance().ListByStatus(
            static_cast<int32_t>(ShardStatus::SHARD_WAITING));

        if (waiting_shards.empty())
            continue;  // 没有待分配 shard，下一轮

        LOG_INFO("SchedulingLoop: found %zu WAITING shards", waiting_shards.size());

        // 查询 ONLINE Worker
        ListWorkersResponse lw_resp;
        if (!LoadOnlineWorkers(wm_stub, lw_resp))
        {
            LOG_WARN("SchedulingLoop: LoadOnlineWorkers failed, skip this round");
            continue;
        }

        if (lw_resp.workers_size() == 0)
        {
            LOG_WARN("SchedulingLoop: no ONLINE workers available");
            continue;
        }

        LOG_INFO("SchedulingLoop: %d ONLINE workers", lw_resp.workers_size());

        // 按优先级排序 WAITING shard
        // priority 越大越优先，同优先级按创建时间 FIFO
        // 批量查询 job 优先级并缓存，避免 N 次 JobStore::Get
        std::unordered_map<std::string, int32_t> job_priority_cache;
        for (const auto& s : waiting_shards)
        {
            // 给本轮所有 shard 批量查出它们所属 job 的优先级，并缓存起来
            if (job_priority_cache.find(s.job_id) == job_priority_cache.end())
            {
                auto job_opt = JobStore::GetInstance().Get(s.job_id);
                job_priority_cache[s.job_id] = job_opt.has_value()
                    ? job_opt->priority : 0;
            }
        }
        std::sort(waiting_shards.begin(), waiting_shards.end(),
                  [&job_priority_cache](const ShardRecord& a, const ShardRecord& b) {
                      int pa = job_priority_cache[a.job_id];
                      int pb = job_priority_cache[b.job_id];
                      if (pa != pb)
                          return pa > pb;  // 高优先级在前
                      return a.created_at < b.created_at;  // 同优先级 FIFO
                  });

        // 对每个 WAITING shard 分配 Worker 
        // 本地槽位计数：防止同一轮内对同一 Worker 超额分配
        // key = worker_id, value = 本回合已分配的 shard 数（从 0 累加）
        std::unordered_map<std::string, int> round_assigned;

        for (auto& shard : waiting_shards)
        {
            // 分配逻辑抽到 TryAssignShard，保证打分/锁/推进行为一致；轮询是 MQ 掉线或消息丢失时的兜底路径
            if (TryAssignShard(shard, lw_resp, round_assigned, "SchedulingLoop"))
                ++shards_assigned;
        }

        // 打印日志
        if (metrics_round % 5 == 0)
        {
            // 聚合所有 ONLINE Worker 的运行中 shard 总数
            int total_running = 0;
            int total_max     = 0;
            for (const auto& w : lw_resp.workers())
            {
                total_running += w.current_running_shards();
                total_max     += w.max_running_shards();
            }
            LOG_INFO("[SchedulerMetrics] round=%lld | queue=%zu | running=%d/%d | "
                     "workers=%d | assigned=%lld",
                     (long long)metrics_round,
                     waiting_shards.size(),
                     total_running, total_max,
                     lw_resp.workers_size(),
                     (long long)shards_assigned);
        }
    }

    LOG_INFO("SchedulingLoop thread stopped");
}

// MqConsumeLoop — MQ 消费线程（Push 调度）
// - 本线程：即时分配（消息到达 → <100ms 内 AssignShard）
// - SchedulingLoop：兜底（消息丢失/发布失败时 5s 内扫到）+超时重扫/启动恢复等可靠性职责
static void MqConsumeLoop(std::atomic<bool>& stop_flag)
{
    auto& mq = MqClient::GetInstance();
    LOG_INFO("MqConsumeLoop thread started (consume shard.waiting, push mode)");

    // 本线程独立 stub（LoadOnlineWorkers 回退 RPC 时使用）
    WorkerManagerService_Stub wm_stub(new MprpcChannel());

    while (!stop_flag)
    {
        // 确保连接可用（MQ 故障时在此等待重连，每 2s 重试一次）
        if (!mq.connected())
        {
            if (!mq.Reconnect())
            {
                LOG_WARN("MqConsumeLoop: MQ unavailable, retry in 2s "
                         "(SchedulingLoop polling is the fallback)");
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }
            LOG_INFO("MqConsumeLoop: MQ reconnected, resume push scheduling");
        }

        // 阻塞消费（2s 超时：可感知 stop_flag 与连接状态变化）
        std::string body;
        int64_t delivery_tag = 0;
        if (!mq.ConsumeBlocking("shard.waiting", body, delivery_tag, 2000))
        {
            if (stop_flag) break;
            continue;  // 超时或连接断开（断开时 connected()==false → 顶部重连）
        }

        // 处理消息：尝试分配该 shard
        const std::string& shard_id = body;
        {
            auto shard_opt = ShardStore::GetInstance().Get(shard_id);
            if (!shard_opt.has_value())
            {
                // 陈旧消息（shard 已被删除）：ack 丢弃
                LOG_WARN("MqConsumeLoop: shard %s not found, ack & drop (stale msg)",
                         shard_id.c_str());
            }
            else if (shard_opt->status !=
                     static_cast<int32_t>(ShardStatus::SHARD_WAITING))
            {
                // 重复投递/轮询已分配：幂等跳过
                LOG_INFO("MqConsumeLoop: shard %s already status=%d, skip (idempotent)",
                         shard_id.c_str(), shard_opt->status);
            }
            else
            {
                ListWorkersResponse workers;
                std::unordered_map<std::string, int> round_assigned;
                if (LoadOnlineWorkers(wm_stub, workers))
                {
                    TryAssignShard(shard_opt.value(), workers, round_assigned,
                                   "MqConsume");
                }
            }
        }
        // 确认消息（处理完成才 ack；ack 失败只 WARN——Broker 会重新投递，
        //    幂等检查兜底，不会重复执行）
        if (!mq.Ack(delivery_tag))
        {
            LOG_WARN("MqConsumeLoop: ack failed for %s, broker may redeliver",
                     shard_id.c_str());
        }
    }

    LOG_INFO("MqConsumeLoop thread stopped");
}

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief SchedulerService 启动流程
///   1. 加载配置 + 初始化日志
///   2. 启动后台调度循环线程
///   3. 主线程启动 RpcProvider 接收 ScheduleJob / RescheduleShard
int main(int argc, char** argv)
{
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    if (!MprpcApplication::Init(argc, argv))
    {
        LOG_ERROR("MprpcApplication init failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    if (!MysqlPool::GetInstance().Init())
    {
        LOG_ERROR("MysqlPool init failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // 阶段 10：Redis/MQ 是可降级组件，Init 失败只 WARN 不拒绝启动
    RedisClient::GetInstance().Init();
    MqClient::GetInstance().Init();

    // 启动后台调度循环线程 + MQ 消费线程（阶段 10：Push 调度）
    std::atomic<bool> scheduling_stopped{false};
    std::thread scheduling_thread(SchedulingLoop, std::ref(scheduling_stopped));
    std::thread mq_thread(MqConsumeLoop, std::ref(scheduling_stopped));

    RpcProvider provider;
    provider.NotifyService(new SchedulerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("SchedulerService start failed");
        scheduling_stopped = true;
        if (scheduling_thread.joinable()) scheduling_thread.join();
        if (mq_thread.joinable()) mq_thread.join();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // Provider 退出后清理
    scheduling_stopped = true;
    if (scheduling_thread.joinable()) scheduling_thread.join();
    if (mq_thread.joinable()) mq_thread.join();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}