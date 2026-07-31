#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include "scheduler.pb.h"
#include "worker.pb.h"
#include "job.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"
#include "video_platform/ffmpeg_executor.h"

using namespace video_platform;

// ============================================================================
// SchedulerService — 任务切分、调度与故障恢复（阶段 5：完整重试链路）
// ============================================================================
//
// Scheduler 是系统中最核心的调度节点。它负责：
// 1. ScheduleJob：接收 JobService 提交的 job，按时间切片拆分为 N 个 shard
// 2. RescheduleShard：接收来自 ResultCollector（Worker 执行失败）或
//    NotifyWorkerOffline（Worker 心跳超时）的重调度请求
// 3. NotifyWorkerOffline：Worker 心跳超时后，批量重调度该 Worker 的 RUNNING shard
// 4. SchedulingLoop（后台线程）：Pull 模式循环扫描 WAITING shard，
//    从 WorkerManager 拉取 ONLINE Worker，贪心匹配并 AssignShard
//
// 阶段 5 核心增强：
// - RescheduleShard 完整实现：
//   retry_count >= max_retry → 永久 SHARD_FAILED → CheckAndMarkJobFailed → 可能 JOB_FAILED
//   retry_count <  max_retry → retry_count++ → SHARD_RETRYING → SHARD_WAITING → 下一轮重分配
// - NotifyWorkerOffline：遍历该 Worker 的所有 ASSIGNED/RUNNING shard，逐个执行重调度
// - 启动恢复：SchedulingLoop 首次运行时扫描残留的 ASSIGNED/RUNNING shard，
//   重置为 WAITING（不增加 retry_count，因为是 Scheduler 崩溃，不是 Worker 故障）
// - CheckAndMarkJobFailed：当 RescheduleShard 标记 SHARD_FAILED 后，
//   检查该 job 所有 shard 是否都进入终态，若是且有 FAILED → JOB_FAILED
// - NotifyJobServiceStatus：提取为独立方法，Scheduler 侧统一通过
//   JobService.UpdateJobStatus RPC 推送状态变更
//
// ⚠️ 跨进程存储：JobStore/ShardStore 是进程内单例，Scheduler 进程的数据
// 与其他服务进程完全隔离。状态同步通过 RPC（UpdateJobStatus/NotifyJobServiceStatus）完成。

/// @brief SchedulerService RPC 实现
///
/// 阶段 5 实现：
/// - ScheduleJob：将 job 按时间切片拆分为 N 个 shard，写入 ShardStore
/// - RescheduleShard：完整重试逻辑（retry_count vs max_retry → FAILED 或 WAITING）
/// - NotifyWorkerOffline：Worker 心跳超时后，重调度该 Worker 的 RUNNING shard
///
/// 后台线程 SchedulingLoop 周期扫描 WAITING shard，匹配 ONLINE Worker
/// 并通过直连调用 WorkerService.AssignShard 分配。
///
/// 启动恢复：SchedulingLoop 首次运行时扫描残留的 ASSIGNED/RUNNING shard，
/// 重置为 WAITING（不增加 retry_count），应对 Scheduler 自身崩溃。
///
/// ⚠️ 跨进程存储：JobStore/ShardStore 是进程内单例。
class SchedulerServiceImpl : public SchedulerService {
public:
    /// @brief 对 job 进行切分并启动调度
    ///
    /// 由 JobService.SubmitJob() 通过 ZK 发现的 SchedulerService_Stub 调用。
    ///
    /// 步骤：
    /// 1. 从请求的 JobInfo 构造/获取本地 JobRecord 副本（首次调用时本地不存在）
    /// 2. ffprobe 探测真实视频时长，失败则回退到 job_duration_fallback_sec
    /// 3. 计算 shard_count = job_duration / shard_duration，最少 1 个
    /// 4. 创建 ShardRecord 写入 ShardStore（status=WAITING，由 SchedulingLoop 异步分配）
    /// 5. 更新 Job 状态 → SCHEDULING，回填 shard_count
    ///
    /// @note 本方法只做切分和写入，不做 Worker 分配。
    ///       Worker 分配由后台 SchedulingLoop 异步完成，"Pull 模式"。
    /// 3. 计算 shard 数量并按时间切片创建 ShardRecord
    /// 4. 写入 ShardStore，更新 JobStore
    void ScheduleJob(::google::protobuf::RpcController* controller,
                     const ::ScheduleJobRequest* request,
                     ::ScheduleJobResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        const auto& jobInfo = request->job_info();
        const std::string& job_id = jobInfo.job_id();

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s, input=%s",
                 job_id.c_str(), jobInfo.input_path().c_str());

        // 1. 从请求的 JobInfo 创建本地 JobRecord 副本
        //    （JobStore 是进程内单例，Scheduler 进程需要自己的本地副本，
        //     JobService 写入的数据在另一个进程的 JobStore 中，不可见）
        JobRecord local_job;
        bool job_exists = false;
        // 这里验证，该job_id是否在JobStore的哈希表里已经存在，如果存在了直接使用，不需要新建jobRecord并放入哈希表中
        auto existing = JobStore::GetInstance().Get(job_id);
        if (existing.has_value())
        {
            // 已有本地副本：可能是 JobService 重试导致 ScheduleJob 被调用多次
            local_job = existing.value();
            job_exists = true;
        }
        else
        {
            // 首次调度：从 RPC 请求的 proto JobInfo 构造本地 JobRecord
            // proto JobInfo 不含 shard_duration_sec，该字段使用配置默认值
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
            local_job.shard_duration_sec = jobInfo.shard_duration_sec();  // 从 JobInfo proto 读取用户指定值
            local_job.created_at         = NowMs();
            local_job.updated_at         = NowMs();
            JobStore::GetInstance().Insert(local_job);
            LOG_INFO("SchedulerService::ScheduleJob job_id=%s: created local copy from request",
                     job_id.c_str());
        }

        // 2. 探测视频真实时长，确定切分参数
        auto& config = MprpcApplication::GetConfig();

        // 用 ffprobe 探测输入视频的真实时长（阶段 6）
        int64_t job_duration_ms = 0;
        {
            auto video_info = FfmpegExecutor::Probe(local_job.input_path);
            if (video_info.valid && video_info.duration_ms > 0)
            {
                job_duration_ms = video_info.duration_ms;
                LOG_INFO("SchedulerService::ScheduleJob job_id=%s: probed video duration=%lldms "
                         "(%dx%d, codec=%s)",
                         job_id.c_str(), (long long)job_duration_ms,
                         video_info.width, video_info.height,
                         video_info.codec_name.c_str());
            }
            else
            {
                // 探测失败时回退到配置默认值（兼容非视频输入）
                int fallback_sec = config.LoadInt("job_duration_fallback_sec", 60, 1, 86400);
                job_duration_ms = static_cast<int64_t>(fallback_sec) * 1000;
                LOG_WARN("SchedulerService::ScheduleJob job_id=%s: ffprobe failed, "
                         "using fallback duration=%lldms",
                         job_id.c_str(), (long long)job_duration_ms);
            }
        }

        // shard 时长：优先用户指定 > 配置默认值 20s
        int shard_dur_sec = (local_job.shard_duration_sec > 0)
                            ? local_job.shard_duration_sec
                            : config.LoadInt("shard_duration_sec", 20, 1, 3600);
        int64_t shard_duration_ms = static_cast<int64_t>(shard_dur_sec) * 1000;

        // 按真实时长计算 shard 数量
        int shard_count = static_cast<int>((job_duration_ms + shard_duration_ms - 1)
                                           / shard_duration_ms);
        if (shard_count < 1) shard_count = 1;

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s: splitting into %d shards "
                 "(job_duration=%lldms, shard_duration=%lldms)",
                 job_id.c_str(), shard_count,
                 (long long)job_duration_ms, (long long)shard_duration_ms);

        // 3. 更新 Job 状态 → SPLITTING，回填真实时长
        local_job.status = static_cast<int32_t>(JobStatus::JOB_SPLITTING);
        local_job.duration_sec = job_duration_ms / 1000;
        local_job.updated_at = NowMs();
        // JobStore类中，是存放所有JobRecord,用哈希表存放，提供多种更新方法
        JobStore::GetInstance().Update(job_id, local_job);

        // 4. 按时间切片创建 ShardRecord
        std::vector<ShardRecord> created_shards;
        for (int i = 0; i < shard_count; ++i)
        {
            int64_t shard_start = static_cast<int64_t>(i) * shard_duration_ms;
            // 最后一个 shard 时长取剩余部分（可能小于 shard_duration_ms）
            int64_t shard_dur = (i == shard_count - 1)
                                ? (job_duration_ms - shard_start)
                                : shard_duration_ms;
            if (shard_dur <= 0) shard_dur = shard_duration_ms;  // 防御

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

            ShardStore::GetInstance().Insert(shard);
            created_shards.push_back(shard);
            LOG_INFO("SchedulerService: created shard %s [%lld-%lld ms]",
                     shard.shard_id.c_str(),
                     (long long)shard.start_ms,
                     (long long)(shard.start_ms + shard.duration_ms));
        }

        // 5. 更新 Job 状态 → SCHEDULING，回填 shard_count
        local_job.shard_count = shard_count;
        local_job.status = static_cast<int32_t>(JobStatus::JOB_SCHEDULING);
        local_job.updated_at = NowMs();
        JobStore::GetInstance().Update(job_id, local_job);

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s: %d shards created, status=SCHEDULING",
                 job_id.c_str(), shard_count);

        // ── 反向通知 JobService 更新 shard_count 和状态 ──────────────────
        // JobStore 是进程内存储，Scheduler 的切分结果需要同步回 JobService，
        // 让 QueryJob 能返回正确的 shard_count。
        {
            // 这里调用channel的目的，不是为了调用对方函数获取结果，仅仅是为了把参数传递到对方进程所在的变量里，巧妙，非常巧妙
            MprpcChannel js_channel;
            JobService_Stub js_stub(&js_channel);

            UpdateJobStatusRequest update_req;
            update_req.set_job_id(job_id);
            update_req.set_status(JobStatus::JOB_SCHEDULING);
            update_req.set_shard_count(shard_count);

            // 同步 shard 列表到 JobService，让 QueryJob 能返回 shard 详情
            for (const auto& s : created_shards)
            {
                auto* si = update_req.add_shards();
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

            // 调用对方函数，把信息同步过去
            UpdateJobStatusResponse update_resp;
            MprpcController update_ctrl;
            update_ctrl.SetTimeoutMs(3000);

            js_stub.UpdateJobStatus(&update_ctrl, &update_req, &update_resp, nullptr);

            if (!update_ctrl.Failed() && update_resp.error_code() == 0)
            {
                LOG_INFO("SchedulerService: notified JobService job=%s shard_count=%d",
                         job_id.c_str(), shard_count);
            }
            else
            {
                LOG_WARN("SchedulerService: failed to notify JobService for job=%s: %s",
                         job_id.c_str(),
                         update_ctrl.Failed() ? update_ctrl.ErrorText().c_str()
                                              : update_resp.error_msg().c_str());
            }
        }
        // 把shard切片信息传回去
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        response->set_job_id(job_id);
        response->set_shard_count(shard_count);  // 直接数据路径，避免侧信道依赖

        // 将 shard 列表填入 response，让 JobService 可构造本地 ShardStore 副本
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

    /// @brief 重新调度一个 shard（失败 / 超时后触发 — 阶段 5 完整实现）
    ///
    /// 调用方：
    /// - ResultCollector::ReportShardResult（Worker 返回执行失败）
    /// - WorkerManager::HeartbeatTimeoutCheck（通过 NotifyWorkerOffline）
    ///
    /// 逻辑：
    /// 1. retry_count >= max_retry → 标记 SHARD_FAILED（终态），检查 job 是否 JOB_FAILED
    /// 2. retry_count < max_retry → retry_count++，重置为 WAITING，由 SchedulingLoop 重新分配
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

        // ── 检查是否已达最大重试次数 ────────────────────────────
        if (shard.retry_count >= shard.max_retry)
        {
            LOG_WARN("SchedulerService::RescheduleShard shard_id=%s reached max retry "
                     "(%d/%d), marking SHARD_FAILED",
                     shard_id.c_str(), shard.retry_count, shard.max_retry);

            shard.status = static_cast<int32_t>(ShardStatus::SHARD_FAILED);
            shard.updated_at = NowMs();
            ShardStore::GetInstance().Update(shard_id, shard);

            // 检查该 job 是否需要标记为 JOB_FAILED
            CheckAndMarkJobFailed(shard.job_id);

            response->set_error_code(0);
            response->set_error_msg("max retry exceeded, shard marked FAILED");
            response->set_accepted(false);
            done->Run();
            return;
        }

        // ── 未超次：增加重试计数，重置为 WAITING ──────────────
        shard.retry_count++;
        shard.status = static_cast<int32_t>(ShardStatus::SHARD_RETRYING);
        shard.assigned_worker_id.clear();
        shard.attempt_id.clear();
        shard.updated_at = NowMs();
        ShardStore::GetInstance().Update(shard_id, shard);

        LOG_INFO("SchedulerService::RescheduleShard shard_id=%s retry=%d/%d → RETRYING",
                 shard_id.c_str(), shard.retry_count, shard.max_retry);

        // 立即转为 WAITING，让 SchedulingLoop 下一轮扫描时分配
        shard.status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
        ShardStore::GetInstance().Update(shard_id, shard);

        LOG_INFO("SchedulerService::RescheduleShard shard_id=%s → WAITING (retry=%d/%d)",
                 shard_id.c_str(), shard.retry_count, shard.max_retry);

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        done->Run();
    }

    /// @brief Worker 心跳超时通知 — 重调度该 Worker 上所有运行中的 shard
    ///
    /// 由 WorkerManager 在检测到 Worker 心跳超时后调用。
    /// 在本地 ShardStore 中查找该 Worker 的 ASSIGNED/RUNNING shard，
    /// 逐个执行重调度逻辑（内联，避免递归 RPC）。
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

        for (const auto& s : shards)
        {
            int st = s.status;
            if (st != static_cast<int32_t>(ShardStatus::SHARD_ASSIGNED)
                && st != static_cast<int32_t>(ShardStatus::SHARD_RUNNING))
                continue;

            auto fresh_opt = ShardStore::GetInstance().Get(s.shard_id);
            if (!fresh_opt.has_value()) continue;
            auto shard = fresh_opt.value();

            if (shard.retry_count >= shard.max_retry)
            {
                // 已达最大重试次数 → 永久失败
                shard.status = static_cast<int32_t>(ShardStatus::SHARD_FAILED);
                shard.updated_at = NowMs();
                ShardStore::GetInstance().Update(s.shard_id, shard);
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
                ShardStore::GetInstance().Update(s.shard_id, shard);
                ++rescheduled;
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

    /// @brief 取消 job 的所有 shard 并 best-effort 通知 Worker
    ///
    /// 由 JobService.CancelJob 调用。Scheduler 在本地 ShardStore 中查找
    /// 该 job 的 shard，对 RUNNING/ASSIGNED 状态的 shard 直连 Worker
    /// 发送 CancelShard RPC。
    ///
    /// 最佳努力语义：
    /// - Worker 通知失败只打 WARN 日志，不返回错误
    /// - WorkerManager.ListWorkers RPC 失败则跳过通知阶段
    /// - 不修改本地 shard 状态（由 JobService 负责标记 CANCELED）
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

        // 2. 收集需要通知的 shard（RUNNING/ASSIGNED 且有分配 worker）
        std::vector<std::pair<std::string, std::string>> shards_to_notify;
        int32_t s_running  = static_cast<int32_t>(ShardStatus::SHARD_RUNNING);
        int32_t s_assigned = static_cast<int32_t>(ShardStatus::SHARD_ASSIGNED);

        // 拿到所有分片以及对应的work_id
        for (const auto& s : shards)
        {
            if ((s.status == s_running || s.status == s_assigned)
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

        // 5. 直连每个 Worker 发送 CancelShard（best-effort）
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

private:
    /// @brief 检查 job 下所有 shard 是否都已进入终态，
    ///        若有 FAILED 且无进行中的 shard，标记 JOB_FAILED
    ///
    /// 在 RescheduleShard 标记 SHARD_FAILED 后调用。
    void CheckAndMarkJobFailed(const std::string& job_id)
    {
        auto shards = ShardStore::GetInstance().ListByJob(job_id);
        if (shards.empty()) return;

        bool all_terminal = true;
        bool any_failed = false;
        for (const auto& s : shards)
        {
            int st = s.status;
            int s_success  = static_cast<int32_t>(ShardStatus::SHARD_SUCCESS);
            int s_failed   = static_cast<int32_t>(ShardStatus::SHARD_FAILED);
            int s_canceled = static_cast<int32_t>(ShardStatus::SHARD_CANCELED);
            if (st != s_success && st != s_failed && st != s_canceled)
            {
                all_terminal = false;
                break;
            }
            if (st == s_failed) any_failed = true;
        }

        if (!all_terminal || !any_failed) return;

        // 所有 shard 都在终态且至少有一个 FAILED → JOB_FAILED
        auto job_opt = JobStore::GetInstance().Get(job_id);
        if (job_opt.has_value())
        {
            auto job = job_opt.value();
            job.status = static_cast<int32_t>(JobStatus::JOB_FAILED);
            job.updated_at = NowMs();
            JobStore::GetInstance().Update(job_id, job);
        }

        LOG_WARN("SchedulerService: job %s marked JOB_FAILED (all shards terminal, "
                 "has FAILED)", job_id.c_str());

        // ── 反向通知 JobService ──────────────────────────────────
        NotifyJobServiceStatus(job_id, JobStatus::JOB_FAILED);
    }

    /// @brief 通过 RPC 通知 JobService 更新 job 状态（跨进程同步）
    ///
    /// 复用 UpdateJobStatus RPC，将 Scheduler 进程的 job 状态变更
    /// 推送到 JobService 进程。
    void NotifyJobServiceStatus(const std::string& job_id, JobStatus status)
    {
        MprpcChannel js_channel;
        JobService_Stub js_stub(&js_channel);

        UpdateJobStatusRequest update_req;
        update_req.set_job_id(job_id);
        update_req.set_status(status);

        // 携带 shard 状态快照
        auto shards = ShardStore::GetInstance().ListByJob(job_id);
        for (const auto& s : shards)
        {
            auto* si = update_req.add_shards();
            si->set_shard_id(s.shard_id);
            si->set_job_id(s.job_id);
            si->set_shard_index(s.shard_index);
            si->set_status(static_cast<ShardStatus>(s.status));
            si->set_assigned_worker_id(s.assigned_worker_id);
            si->set_attempt_id(s.attempt_id);
            si->set_retry_count(s.retry_count);
            si->set_output_path(s.output_path);
            si->set_target_resolution(s.target_resolution);
            si->set_target_bitrate(s.target_bitrate);
        }

        UpdateJobStatusResponse update_resp;
        MprpcController update_ctrl;
        update_ctrl.SetTimeoutMs(3000);

        js_stub.UpdateJobStatus(&update_ctrl, &update_req, &update_resp, nullptr);

        if (!update_ctrl.Failed() && update_resp.error_code() == 0)
        {
            LOG_INFO("SchedulerService: notified JobService job=%s → %d",
                     job_id.c_str(), static_cast<int>(status));
        }
        else
        {
            LOG_WARN("SchedulerService: failed to notify JobService for job=%s: %s",
                     job_id.c_str(),
                     update_ctrl.Failed() ? update_ctrl.ErrorText().c_str()
                                          : update_resp.error_msg().c_str());
        }
    }
};

// ============================================================================
// SchedulingLoop — 后台调度循环（Pull 模式）
// ============================================================================

/// @brief 在后台线程中运行调度循环，由 main() 在 Provider 启动前创建
///
/// 设计：Pull 模式 —— 主动扫描 WAITING shard（等待被操作的片） 并拉取 ONLINE Worker(就绪的工作者) 列表，
/// 而非在 ScheduleJob 中同步分配。优点：
/// - ScheduleJob 不会被慢 Worker 阻塞（Worker 可能过载或网络慢）
/// - Worker 列表变化（上线/离线）时分配逻辑自动感知
/// - Worker 暂时不可用时（无可用槽位），shard 留在队列中等待下一轮
///
/// Worker 选择策略（当前：贪心最优槽位）：
///   available_slots = max_running_shards - current_running_shards
///   选 available_slots 最大的 Worker
///   阶段 7 将扩展为资源感知评分：score = slots*10 - cpu*0.5 - mem*0.2
///
/// RPC 通信方式：
/// - WorkerManager.ListWorkers → ZK 发现（只需要查到哪个 Worker 在线即可）
/// - WorkerService.AssignShard → 直连 MprpcChannel(ip, port)
///   因为必须调用特定 Worker，不能走 ZK 随机轮询
///
/// 线程安全：本函数运行在独立线程中，所有 Store 操作通过读写锁保护。
///
/// @param stop_flag 主线程设置此标志为 true 时，循环退出
static void SchedulingLoop(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kScheduleIntervalMs = 2000;  // 每 2 秒扫描一次
    constexpr int64_t kAssignTimeoutMs    = 5000;  // AssignShard RPC 超时

    LOG_INFO("SchedulingLoop thread started, interval=%lldms",
             (long long)kScheduleIntervalMs);

    // 等待 Provider 启动完成
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // ── 启动恢复：扫描残留的 ASSIGNED/RUNNING shard ─────────────────
    // 场景：Scheduler 进程崩溃（kill -9、OOM、段错误），然后被监控系统重启。
    // 崩溃前可能已经通过 AssignShard 将 shard 分配给了 Worker，shard 状态为
    // ASSIGNED 或 RUNNING。重启后 SchedulingLoop 只扫描 WAITING 状态，
    // 这些 shard 永远不会被重新调度——成为永久的"孤儿 shard"。
    //
    // 恢复策略：启动时将残留的 ASSIGNED/RUNNING shard 重置为 WAITING。
    // - 不增加 retry_count（这是 Scheduler 崩溃，不是 Worker 执行失败）
    // - 清除 assigned_worker_id 和 attempt_id（旧 Worker 可能已不存在）
    // - reset 后 SchedulingLoop 下一轮扫描时会自动重新分配
    // - 若旧 Worker 仍在运行且正在执行该 shard，它会正常上报结果；
    //   ResultCollector 的 attempt_id 校验会处理新旧 attempt 的冲突
    //
    // 注意：两个 ListByStatus 返回的是快照副本，所以每次更新前重新 Get() 一次。
    // 这是阶段 4 #S4 bug 的防御措施——防止快照覆盖并发修改。
    {
        LOG_INFO("SchedulingLoop: running startup recovery scan...");
        auto assigned_shards = ShardStore::GetInstance().ListByStatus(
            static_cast<int32_t>(ShardStatus::SHARD_ASSIGNED));
        auto running_shards  = ShardStore::GetInstance().ListByStatus(
            static_cast<int32_t>(ShardStatus::SHARD_RUNNING));

        int recovered = 0;
        for (const auto& s : assigned_shards)
        {
            auto fresh_opt = ShardStore::GetInstance().Get(s.shard_id);
            if (!fresh_opt.has_value()) continue;
            auto fresh = fresh_opt.value();
            fresh.status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
            fresh.assigned_worker_id.clear();
            fresh.attempt_id.clear();
            fresh.updated_at = NowMs();
            ShardStore::GetInstance().Update(s.shard_id, fresh);
            ++recovered;
            LOG_INFO("SchedulingLoop: startup recovery: reset %s ASSIGNED → WAITING",
                     s.shard_id.c_str());
        }
        for (const auto& s : running_shards)
        {
            auto fresh_opt = ShardStore::GetInstance().Get(s.shard_id);
            if (!fresh_opt.has_value()) continue;
            auto fresh = fresh_opt.value();
            fresh.status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
            fresh.assigned_worker_id.clear();
            fresh.attempt_id.clear();
            fresh.updated_at = NowMs();
            ShardStore::GetInstance().Update(s.shard_id, fresh);
            ++recovered;
            LOG_INFO("SchedulingLoop: startup recovery: reset %s RUNNING → WAITING",
                     s.shard_id.c_str());
        }

        if (recovered > 0)
        {
            LOG_INFO("SchedulingLoop: startup recovery complete, %d shards reset to WAITING",
                     recovered);
        }
    }

    // WorkerManager 通过 ZK 发现
    WorkerManagerService_Stub wm_stub(new MprpcChannel());

    // ── 阶段 7：Metrics 计数器 ────────────────────────────────────────
    int64_t metrics_round    = 0;
    int64_t shards_assigned  = 0;

    while (!stop_flag)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kScheduleIntervalMs));
        if (stop_flag) break;

        ++metrics_round;

        // ── 1. 扫描 WAITING shard ─────────────────────────────────────
        auto waiting_shards = ShardStore::GetInstance().ListByStatus(
            static_cast<int32_t>(ShardStatus::SHARD_WAITING));

        if (waiting_shards.empty())
            continue;  // 没有待分配 shard，下一轮

        LOG_INFO("SchedulingLoop: found %zu WAITING shards", waiting_shards.size());

        // ── 2. 查询 ONLINE Worker ─────────────────────────────────────
        ListWorkersRequest lw_req;
        lw_req.set_filter_status(WorkerStatus::WORKER_ONLINE);

        ListWorkersResponse lw_resp;
        MprpcController lw_ctrl;
        wm_stub.ListWorkers(&lw_ctrl, &lw_req, &lw_resp, nullptr);

        if (lw_ctrl.Failed() || lw_resp.error_code() != 0)
        {
            LOG_WARN("SchedulingLoop: ListWorkers failed: %s",
                     lw_ctrl.Failed() ? lw_ctrl.ErrorText().c_str()
                                      : lw_resp.error_msg().c_str());
            continue;
        }

        if (lw_resp.workers_size() == 0)
        {
            LOG_WARN("SchedulingLoop: no ONLINE workers available");
            continue;
        }

        LOG_INFO("SchedulingLoop: %d ONLINE workers", lw_resp.workers_size());

        // ── 3. 按优先级排序 WAITING shard（阶段 7：优先级队列） ──────
        // priority 越大越优先，同优先级按创建时间 FIFO
        // 批量查询 job 优先级并缓存，避免 N 次 JobStore::Get
        std::unordered_map<std::string, int32_t> job_priority_cache;
        for (const auto& s : waiting_shards)
        {
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

        // ── 4. 对每个 WAITING shard 分配 Worker ─────────────────────
        // 本地槽位计数：防止同一轮内对同一 Worker 超额分配
        // key = worker_id, value = 本回合已分配的 shard 数（从 0 累加）
        std::unordered_map<std::string, int> round_assigned;

        for (auto& shard : waiting_shards)
        {
            // 阶段 7：加权评分选择 Worker
            // score = available_slots * 10 - cpu_usage * 0.5 - memory_usage * 0.2
            // 空闲槽位越多、负载越低的 Worker 得分越高
            const WorkerInfo* best_worker = nullptr;
            double best_score = -999.0;

            for (const auto& w : lw_resp.workers())
            {
                int already = round_assigned[w.worker_id()];  // 本回合已分配
                int available = w.max_running_shards()
                              - w.current_running_shards()
                              - already;
                if (available <= 0) continue;  // 无空槽位，跳过

                double score = available * 10.0
                             - w.cpu_usage() * 0.5
                             - w.memory_usage() * 0.2;

                if (score > best_score)
                {
                    best_score = score;
                    best_worker = &w;
                }
            }

            if (best_worker == nullptr)
            {
                LOG_WARN("SchedulingLoop: no worker with available slots for shard %s "
                         "(queue=%zu)",
                         shard.shard_id.c_str(), waiting_shards.size());
                continue;  // 下一轮再试
            }

            // 本地槽位计数 +1
            round_assigned[best_worker->worker_id()]++;

            // ── 4. 直连 Worker 的 AssignShard ─────────────────────────
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
                LOG_WARN("SchedulingLoop: AssignShard %s to worker %s failed: %s",
                         shard.shard_id.c_str(),
                         best_worker->worker_id().c_str(),
                         as_ctrl.Failed() ? as_ctrl.ErrorText().c_str()
                                          : as_resp.error_msg().c_str());
                continue;  // 下一轮重试分配
            }

            // ── 5. 更新 shard 状态 → ASSIGNED ───────────────────────
            // 重新从 ShardStore 读取最新状态，避免用快照副本覆盖并发修改
            {
                auto fresh_opt = ShardStore::GetInstance().Get(shard.shard_id);
                if (!fresh_opt.has_value()) continue;  // 已被删除
                auto fresh = fresh_opt.value();
                fresh.status = static_cast<int32_t>(ShardStatus::SHARD_ASSIGNED);
                fresh.assigned_worker_id = best_worker->worker_id();
                fresh.attempt_id = fresh.shard_id + "_attempt_" + std::to_string(fresh.retry_count);
                fresh.updated_at = NowMs();
                ShardStore::GetInstance().Update(fresh.shard_id, fresh);
            }

            // ── 6. 本地槽位已在 round_assigned map 中追踪，无需额外递减 ──
            ++shards_assigned;

            // ── 7. 更新 job 状态 → RUNNING（若是首次分配） ─────────────
            auto job_opt = JobStore::GetInstance().Get(shard.job_id);
            if (job_opt.has_value() &&
                job_opt->status == static_cast<int32_t>(JobStatus::JOB_SCHEDULING))
            {
                auto job = job_opt.value();
                job.status = static_cast<int32_t>(JobStatus::JOB_RUNNING);
                job.updated_at = NowMs();
                JobStore::GetInstance().Update(shard.job_id, job);

                // 反向通知 JobService 状态变更（跨进程同步）
                {
                    MprpcChannel js_channel;
                    JobService_Stub js_stub(&js_channel);
                    UpdateJobStatusRequest update_req;
                    update_req.set_job_id(shard.job_id);
                    update_req.set_status(JobStatus::JOB_RUNNING);
                    UpdateJobStatusResponse update_resp;
                    MprpcController update_ctrl;
                    update_ctrl.SetTimeoutMs(3000);
                    js_stub.UpdateJobStatus(&update_ctrl, &update_req, &update_resp, nullptr);

                    if (update_ctrl.Failed() || update_resp.error_code() != 0)
                    {
                        LOG_WARN("SchedulingLoop: UpdateJobStatus(RUNNING) failed for job=%s: %s",
                                 shard.job_id.c_str(),
                                 update_ctrl.Failed() ? update_ctrl.ErrorText().c_str()
                                                      : update_resp.error_msg().c_str());
                    }
                }
            }

            int remaining = best_worker->max_running_shards()
                          - best_worker->current_running_shards()
                          - round_assigned[best_worker->worker_id()];
            LOG_INFO("SchedulingLoop: assigned shard %s to worker %s (%s:%d), "
                     "score=%.1f, cpu=%d%%, mem=%d%%, slots_left=%d",
                     shard.shard_id.c_str(),
                     best_worker->worker_id().c_str(),
                     best_worker->ip().c_str(),
                     best_worker->port(),
                     best_score,
                     best_worker->cpu_usage(),
                     best_worker->memory_usage(),
                     remaining);
        }

        // ── 阶段 7：Metrics 输出（每 5 轮 ≈ 10 秒） ──────────────────
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

    // 启动后台调度循环线程
    std::atomic<bool> scheduling_stopped{false};
    std::thread scheduling_thread(SchedulingLoop, std::ref(scheduling_stopped));

    RpcProvider provider;
    provider.NotifyService(new SchedulerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("SchedulerService start failed");
        scheduling_stopped = true;
        if (scheduling_thread.joinable()) scheduling_thread.join();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // Provider 退出后清理
    scheduling_stopped = true;
    if (scheduling_thread.joinable()) scheduling_thread.join();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
