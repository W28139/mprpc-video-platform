#include <string>
#include <cstdlib>
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
        jobInfo->set_created_at(job.created_at);
        jobInfo->set_updated_at(job.updated_at);

        // ── 调用 Scheduler.ScheduleJob 触发切分和调度 ─────────────────
        MprpcChannel sched_channel;
        SchedulerService_Stub sched_stub(&sched_channel);

        ScheduleJobRequest sched_req;
        *sched_req.mutable_job_info() = *jobInfo;
        sched_req.mutable_job_info()->set_duration_sec(job.duration_sec);

        ScheduleJobResponse sched_resp;
        MprpcController sched_ctrl;

        sched_stub.ScheduleJob(&sched_ctrl, &sched_req, &sched_resp, nullptr);

        if (!sched_ctrl.Failed() && sched_resp.accepted())
        {
            LOG_INFO("JobService::SubmitJob job_id=%s: Scheduler accepted", job_id.c_str());
            // ⚠️ 必须从 JobStore 重新读取 —— Scheduler.ScheduleJob 内部通过
            // UpdateJobStatus RPC 已经写入了 shard_count，直接用本地 job 变量
            // 会覆盖掉 RPC 写入的值（本地 job.shard_count 还是 0）
            JobRecord* refreshed = JobStore::GetInstance().Get(job_id);
            if (refreshed != nullptr)
            {
                refreshed->status = static_cast<int32_t>(JobStatus::JOB_SCHEDULING);
                refreshed->updated_at = NowMs();
                JobStore::GetInstance().Update(job_id, *refreshed);
                // 同步更新响应中的 job_info
                jobInfo->set_status(JobStatus::JOB_SCHEDULING);
                jobInfo->set_shard_count(refreshed->shard_count);
            }
        }
        else
        {
            LOG_WARN("JobService::SubmitJob job_id=%s: Scheduler.ScheduleJob failed: %s",
                     job_id.c_str(),
                     sched_ctrl.Failed() ? sched_ctrl.ErrorText().c_str()
                                         : sched_resp.error_msg().c_str());
            // 不阻塞 SubmitJob 响应 -- job 已创建，Scheduler 后续可以通过
            // 后台循环重新处理（或手动触发）
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

        JobRecord* job = JobStore::GetInstance().Get(request->job_id());
        if (job == nullptr)
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
        jobInfo->set_job_id(job->job_id);
        jobInfo->set_user_id(job->user_id);
        jobInfo->set_input_path(job->input_path);
        jobInfo->set_output_path(job->output_path);
        jobInfo->set_target_format(job->target_format);
        jobInfo->set_target_resolution(job->target_resolution);
        jobInfo->set_target_bitrate(job->target_bitrate);
        jobInfo->set_priority(job->priority);
        jobInfo->set_status(static_cast<JobStatus>(job->status));
        jobInfo->set_shard_count(job->shard_count);
        jobInfo->set_created_at(job->created_at);
        jobInfo->set_updated_at(job->updated_at);

        // 填充关联的 shard 列表
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

    /// @brief 取消任务
    void CancelJob(::google::protobuf::RpcController* controller,
                   const ::CancelJobRequest* request,
                   ::CancelJobResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::CancelJob job_id=%s, reason=%s",
                 request->job_id().c_str(), request->reason().c_str());

        JobRecord* job = JobStore::GetInstance().Get(request->job_id());
        if (job == nullptr)
        {
            response->set_error_code(1);
            response->set_error_msg("job not found: " + request->job_id());
            done->Run();
            return;
        }

        job->status = static_cast<int32_t>(JobStatus::JOB_CANCELED);
        job->updated_at = NowMs();
        JobStore::GetInstance().Update(request->job_id(), *job);

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
    /// - ResultCollector.CheckJobDone（全部 shard 完成后更新 status→SUCCESS）
    void UpdateJobStatus(::google::protobuf::RpcController* controller,
                         const ::UpdateJobStatusRequest* request,
                         ::UpdateJobStatusResponse* response,
                         ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::UpdateJobStatus job_id=%s, status=%d, shard_count=%d",
                 request->job_id().c_str(),
                 static_cast<int>(request->status()),
                 request->shard_count());

        JobRecord* job = JobStore::GetInstance().Get(request->job_id());
        if (job == nullptr)
        {
            response->set_error_code(1);
            response->set_error_msg("job not found: " + request->job_id());
            done->Run();
            return;
        }

        // 只更新状态（不降级：如果已经是 SUCCESS 不会被改回 RUNNING）
        if (request->status() != JobStatus::JOB_STATUS_UNKNOWN)
        {
            job->status = static_cast<int32_t>(request->status());
        }
        if (request->shard_count() > job->shard_count)
        {
            job->shard_count = request->shard_count();
        }
        job->updated_at = NowMs();
        JobStore::GetInstance().Update(request->job_id(), *job);

        LOG_INFO("JobService::UpdateJobStatus job_id=%s updated → status=%d, shard_count=%d",
                 request->job_id().c_str(), job->status, job->shard_count);

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
