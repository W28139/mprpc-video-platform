#include <string>
#include <cstdlib>
#include <thread>
#include <chrono>
#include "job.pb.h"
#include "scheduler.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"

using namespace video_platform;

// ============================================================================
// JobService — 接收用户提交任务、查询任务状态、取消任务
// ============================================================================

/// @brief JobService RPC 实现
///
/// 由 job_client（CLI 工具）通过 ZK 发现的 JobService_Stub 调用。
/// 是用户操作整个视频处理平台的唯一入口。
///
/// 阶段 4 改进：
/// - SubmitJob：创建 JobRecord 后立即 RPC 调用 Scheduler.ScheduleJob
///   触发异步切分和调度（不等待 Scheduler 完成才返回）
/// - 将 shard_duration_sec 写入 JobRecord，传递给 Scheduler
/// - 新增 input_path 必填校验，防无效提交
///
/// 错误处理策略：
/// SubmitJob 的 Scheduler.ScheduleJob 调用失败不回滚 job 创建 ——
/// job 已持久化到 JobStore，Scheduler 的后台 SchedulingLoop 可通过重新
/// 扫描发现未切分的 job（后续增强），或支持手动触发调度。
class JobServiceImpl : public JobService {
public:
    /// @brief 提交视频处理任务
    ///
    /// 流程：校验参数 → 创建 JobRecord → 写入 JobStore →
    ///       调用 Scheduler.ScheduleJob 触发切分和调度
    void SubmitJob(::google::protobuf::RpcController* controller,
                   const ::SubmitJobRequest* request,
                   ::SubmitJobResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::SubmitJob user=%s, input=%s, target_res=%s, shard_dur=%ds",
                 request->user_id().c_str(),
                 request->input_path().c_str(),
                 request->target_resolution().c_str(),
                 request->shard_duration_sec());

        // 参数校验
        if (request->input_path().empty())
        {
            response->set_error_code(1);
            response->set_error_msg("input_path is required");
            done->Run();
            return;
        }

        // 用 GenerateId 生成唯一 job_id
        std::string job_id = GenerateId("job");
        JobRecord job;
        job.job_id             = job_id;
        job.user_id            = request->user_id();
        job.input_path         = request->input_path();
        job.output_path        = request->output_path();
        job.target_format      = request->target_format();
        job.target_resolution  = request->target_resolution();
        job.target_bitrate     = request->target_bitrate();
        job.priority           = request->priority();
        job.status             = static_cast<int32_t>(JobStatus::JOB_PENDING);
        job.shard_count        = 0;
        job.shard_duration_sec = request->shard_duration_sec();
        job.created_at         = NowMs();
        job.updated_at         = NowMs();

        // 写入内存存储
        JobStore::GetInstance().Insert(job);
        LOG_INFO("JobService::SubmitJob created job_id=%s", job_id.c_str());

        // 构造响应
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_job_id(job_id);

        auto* jobInfo = response->mutable_job_info();
        jobInfo->set_job_id(job.job_id);
        jobInfo->set_user_id(job.user_id);
        jobInfo->set_input_path(job.input_path);
        jobInfo->set_output_path(job.output_path);
        jobInfo->set_target_format(job.target_format);
        jobInfo->set_target_resolution(job.target_resolution);
        jobInfo->set_target_bitrate(job.target_bitrate);
        jobInfo->set_priority(job.priority);
        jobInfo->set_status(JobStatus::JOB_PENDING);
        jobInfo->set_shard_count(0);
        jobInfo->set_shard_duration_sec(job.shard_duration_sec);
        jobInfo->set_created_at(job.created_at);
        jobInfo->set_updated_at(job.updated_at);

        // ── 调用 Scheduler.ScheduleJob 触发切分和调度 ─────────────────
        // 最多重试 3 次，防止单次网络抖动导致 job 永久卡死在 PENDING
        bool schedule_ok = false;
        int32_t shard_count_from_scheduler = 0;
        std::vector<ShardInfo> shard_list_from_scheduler;  // 收集 shard 用于跨进程同步
        for (int attempt = 1; attempt <= 3; ++attempt)
        {
            MprpcChannel sched_channel;
            SchedulerService_Stub sched_stub(&sched_channel);

            ScheduleJobRequest sched_req;
            *sched_req.mutable_job_info() = *jobInfo;
            sched_req.mutable_job_info()->set_duration_sec(job.duration_sec);

            ScheduleJobResponse sched_resp;
            MprpcController sched_ctrl;

            sched_stub.ScheduleJob(&sched_ctrl, &sched_req, &sched_resp, nullptr);
            // sched_resp.accepted() 通常请求已被受理
            if (!sched_ctrl.Failed() && sched_resp.accepted())
            {
                LOG_INFO("JobService::SubmitJob job_id=%s: Scheduler accepted (attempt %d)",
                         job_id.c_str(), attempt);
                // 直接从 ScheduleJobResponse 获取 shard_count（直接数据路径，避免侧信道）
                shard_count_from_scheduler = sched_resp.shard_count();
                // 捕获 shard 列表用于跨进程同步
                shard_list_from_scheduler.clear();
                for (const auto& si : sched_resp.shards())
                    shard_list_from_scheduler.push_back(si);
                schedule_ok = true;
                break;
            }

            LOG_WARN("JobService::SubmitJob job_id=%s: Scheduler.ScheduleJob failed "
                     "(attempt %d/3): %s",
                     job_id.c_str(), attempt,
                     sched_ctrl.Failed() ? sched_ctrl.ErrorText().c_str()
                                         : sched_resp.error_msg().c_str());

            if (attempt < 3)
                std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (schedule_ok)
        {
            // 使用 ScheduleJobResponse 返回的 shard_count 更新 JobStore里job_id该JobRecord的信息
            // refreshed_opt的类型为JobRecord，更新状态信息、分片数信息、最新更新时间信息等信息
            auto refreshed_opt = JobStore::GetInstance().Get(job_id);
            if (refreshed_opt.has_value())
            {
                auto refreshed = refreshed_opt.value();
                refreshed.status = static_cast<int32_t>(JobStatus::JOB_SCHEDULING);
                refreshed.shard_count = shard_count_from_scheduler;
                refreshed.updated_at = NowMs();
                JobStore::GetInstance().Update(job_id, refreshed);
                // 同步更新响应中的 job_info, jobInfo 是指向 RPC 响应 的指针response,它最终会被序列化发给客户端
                jobInfo->set_status(JobStatus::JOB_SCHEDULING);
                jobInfo->set_shard_count(shard_count_from_scheduler);
                jobInfo->set_updated_at(refreshed.updated_at);
            }

            // 将 Scheduler 响应中的 shard 写入本地 ShardStore。
            // shard_list_from_scheduler 是栈上临时对象（从 RPC response 提取），
            // 函数返回后即析构，必须在消失前将数据搬入进程生命周期的单例。
            // JobService 和 Scheduler 是不同进程，各自持有独立的 ShardStore 实例，
            // 不写入则本进程 QueryJob 查不到 shard 列表。
            for (const auto& si : shard_list_from_scheduler)
            {
                ShardRecord s;
                s.shard_id      = si.shard_id();
                s.job_id        = si.job_id();
                s.shard_index   = si.shard_index();
                s.start_ms      = si.start_ms();
                s.duration_ms   = si.duration_ms();
                s.status        = static_cast<int32_t>(si.status());
                s.retry_count   = si.retry_count();
                s.max_retry     = si.max_retry();
                s.input_path    = si.input_path();
                s.output_path   = si.output_path();
                s.created_at    = si.created_at();
                s.updated_at    = si.updated_at();
                ShardStore::GetInstance().Insert(s);
            }
            LOG_INFO("JobService::SubmitJob job_id=%s: populated %zu shards in local ShardStore",
                     job_id.c_str(), shard_list_from_scheduler.size());
        }
        else
        {
            LOG_ERROR("JobService::SubmitJob job_id=%s: Scheduler.ScheduleJob failed "
                      "after 3 retries, job stuck in PENDING – will be retried by "
                      "background SchedulingLoop",
                      job_id.c_str());
            // Job 留在 PENDING 状态，SchedulingLoop 可通过扫描 PENDING job 兜底
        }

        done->Run();
    }

    /// @brief 查询任务状态和进度
    void QueryJob(::google::protobuf::RpcController* controller,
                  const ::QueryJobRequest* request,
                  ::QueryJobResponse* response,
                  ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::QueryJob job_id=%s", request->job_id().c_str());

        auto job_opt = JobStore::GetInstance().Get(request->job_id());
        if (!job_opt.has_value())
        {
            response->set_error_code(1);
            response->set_error_msg("job not found: " + request->job_id());
            done->Run();
            return;
        }

        response->set_error_code(0);
        response->set_error_msg("");

        // 填充 job 信息
        auto* jobInfo = response->mutable_job_info();
        jobInfo->set_job_id(job_opt->job_id);
        jobInfo->set_user_id(job_opt->user_id);
        jobInfo->set_input_path(job_opt->input_path);
        jobInfo->set_output_path(job_opt->output_path);
        jobInfo->set_target_format(job_opt->target_format);
        jobInfo->set_target_resolution(job_opt->target_resolution);
        jobInfo->set_target_bitrate(job_opt->target_bitrate);
        jobInfo->set_priority(job_opt->priority);
        jobInfo->set_status(static_cast<JobStatus>(job_opt->status));
        jobInfo->set_shard_count(job_opt->shard_count);
        jobInfo->set_created_at(job_opt->created_at);
        jobInfo->set_updated_at(job_opt->updated_at);

        // 填充关联的 shard 列表，前面填入的信息，在这里被获取查询
        auto shards = ShardStore::GetInstance().ListByJob(request->job_id());
        for (const auto& s : shards)
        {
            auto* si = response->add_shards();
            si->set_shard_id(s.shard_id);
            si->set_job_id(s.job_id);
            si->set_shard_index(s.shard_index);
            si->set_start_ms(s.start_ms);
            si->set_duration_ms(s.duration_ms);
            si->set_status(static_cast<ShardStatus>(s.status));
            si->set_assigned_worker_id(s.assigned_worker_id);
            si->set_attempt_id(s.attempt_id);
            si->set_retry_count(s.retry_count);
            si->set_max_retry(s.max_retry);
            si->set_input_path(s.input_path);
            si->set_output_path(s.output_path);
            si->set_created_at(s.created_at);
            si->set_updated_at(s.updated_at);
        }

        done->Run();
    }

    /// @brief 取消任务（含分布式传播）
    ///
    /// 阶段 4 改进：除标记 JobStore 外，同步标记所有 shard 为 CANCELED，把未切分和已切分的任务都标记为取消
    /// 防止 SchedulingLoop 继续扫描分配给已取消 job 的 WAITING shard。
    /// 对已分配的 shard 尝试通知 Worker 取消执行（best-effort）。
    void CancelJob(::google::protobuf::RpcController* controller,
                   const ::CancelJobRequest* request,
                   ::CancelJobResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::CancelJob job_id=%s, reason=%s",
                 request->job_id().c_str(), request->reason().c_str());

        auto job_opt = JobStore::GetInstance().Get(request->job_id());
        if (!job_opt.has_value())
        {
            response->set_error_code(1);
            response->set_error_msg("job not found: " + request->job_id());
            done->Run();
            return;
        }

        auto job = job_opt.value();
        job.status = static_cast<int32_t>(JobStatus::JOB_CANCELED);
        job.updated_at = NowMs();
        JobStore::GetInstance().Update(request->job_id(), job);

        // ── 传播取消到所有 shard ─────────────────────────────────────
        // 将所有 shard 标记为 CANCELED，使 SchedulingLoop 不再扫描它们。
        // 对已分配的 shard，best-effort 通知 Worker 取消执行。
        auto shards = ShardStore::GetInstance().ListByJob(request->job_id());
        int canceled_count = 0;
        for (auto& s : shards)
        {
            s.status = static_cast<int32_t>(ShardStatus::SHARD_CANCELED);
            s.updated_at = NowMs();
            ShardStore::GetInstance().Update(s.shard_id, s);
            ++canceled_count;
        }
        LOG_INFO("JobService::CancelJob job_id=%s: canceled %d shards",
                 request->job_id().c_str(), canceled_count);

        // ── Best-effort 委托 Scheduler 向 Worker 传播取消 ───────────
        // 本地 ShardStore 不持有 assigned_worker_id（数据在 Scheduler 进程），
        // 因此通过 Scheduler.CancelJobShards RPC 让 Scheduler 直连 Worker送 CancelShard。
        // 无论成功与否都不影响 CancelJob 本身的返回结果。
        {
            MprpcChannel sched_channel;
            SchedulerService_Stub sched_stub(&sched_channel);

            CancelJobShardsRequest cancel_req;
            cancel_req.set_job_id(request->job_id());
            cancel_req.set_reason(request->reason());

            CancelJobShardsResponse cancel_resp;
            MprpcController cancel_ctrl;
            cancel_ctrl.SetTimeoutMs(5000);

            sched_stub.CancelJobShards(&cancel_ctrl, &cancel_req,
                                        &cancel_resp, nullptr);

            if (!cancel_ctrl.Failed() && cancel_resp.error_code() == 0)
            {
                LOG_INFO("JobService::CancelJob job_id=%s: Scheduler "
                         "propagated, total=%d notified=%d skipped=%d",
                         request->job_id().c_str(),
                         cancel_resp.total_shards(),
                         cancel_resp.notified_count(),
                         cancel_resp.skipped_count());
            }
            else
            {
                LOG_WARN("JobService::CancelJob job_id=%s: "
                         "Scheduler.CancelJobShards failed (%s), "
                         "workers not notified (best-effort)",
                         request->job_id().c_str(),
                         cancel_ctrl.Failed()
                             ? cancel_ctrl.ErrorText().c_str()
                             : cancel_resp.error_msg().c_str());
            }
        }

        response->set_error_code(0);
        response->set_error_msg("");
        done->Run();
    }

    /// @brief 跨进程状态同步 — 由 Scheduler / ResultCollector 反向调用
    ///
    /// 由于 JobStore 是进程内存储，Scheduler 和 ResultCollector 的状态变更
    /// 不会自动反映到 JobService。此 RPC 允许其他服务主动推送状态更新。
    ///
    /// 调用方：
    /// - Scheduler.ScheduleJob（切分完成后更新 shard_count + status→SCHEDULING）
    /// - ResultCollector.CheckJobDone（全部 shard 完成后更新 status→SUCCESS）‘
    // 调用方调用这个函数，通过request把参数传回job_server所在进程，更新数据（巧妙设计）
    void UpdateJobStatus(::google::protobuf::RpcController* controller,
                         const ::UpdateJobStatusRequest* request,
                         ::UpdateJobStatusResponse* response,
                         ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::UpdateJobStatus job_id=%s, status=%d, shard_count=%d",
                 request->job_id().c_str(),
                 static_cast<int>(request->status()),
                 request->shard_count());

        auto job_opt = JobStore::GetInstance().Get(request->job_id());
        if (!job_opt.has_value())
        {
            response->set_error_code(1);
            response->set_error_msg("job not found: " + request->job_id());
            done->Run();
            return;
        }

        auto job = job_opt.value();

        // 状态单调性检查：只接受状态升级，拒绝降级
        // 例如 JOB_SUCCESS(6) 不可被 JOB_RUNNING(4) 覆盖
        if (request->status() != JobStatus::JOB_STATUS_UNKNOWN)
        {
            int32_t new_status = static_cast<int32_t>(request->status());
            if (new_status > job.status)
            {
                job.status = new_status;
            }
            else if (new_status != job.status)
            {
                LOG_INFO("JobService::UpdateJobStatus job_id=%s: ignoring status "
                         "downgrade %d → %d",
                         request->job_id().c_str(), job.status, new_status);
            }
        }
        if (request->shard_count() > job.shard_count)
        {
            job.shard_count = request->shard_count();
        }
        job.updated_at = NowMs();
        JobStore::GetInstance().Update(request->job_id(), job);

        // ── 同步 shard 状态更新 ─────────────────────────────────────
        // 其他服务（Scheduler/ResultCollector）通过此 RPC 推送 shard 状态变更。
        // 首次创建 shard 副本（Scheduler.ScheduleJob 时）或更新已有 shard 状态。
        for (const auto& si : request->shards())
        {
            ShardRecord s;
            s.shard_id      = si.shard_id();
            s.job_id        = si.job_id();
            s.shard_index   = si.shard_index();
            s.start_ms      = si.start_ms();
            s.duration_ms   = si.duration_ms();
            s.status        = static_cast<int32_t>(si.status());
            s.assigned_worker_id = si.assigned_worker_id();
            s.attempt_id    = si.attempt_id();
            s.retry_count   = si.retry_count();
            s.max_retry     = si.max_retry();
            s.input_path    = si.input_path();
            s.output_path   = si.output_path();
            s.created_at    = si.created_at();
            s.updated_at    = si.updated_at();
            // 幂等：已存在则覆盖更新
            ShardStore::GetInstance().InsertOrUpdate(s);
        }

        LOG_INFO("JobService::UpdateJobStatus job_id=%s updated → status=%d, shard_count=%d",
                 request->job_id().c_str(), job.status, job.shard_count);

        response->set_error_code(0);
        response->set_error_msg("");
        done->Run();
    }
};

// ============================================================================
// main — 服务入口
// ============================================================================

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

    RpcProvider provider;
    provider.NotifyService(new JobServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("JobService start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
