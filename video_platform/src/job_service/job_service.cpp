#include <string>
#include <cstdlib>
#include "job.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"

using namespace video_platform;

// ============================================================================
// JobService — 接收用户提交任务、查询任务状态、取消任务
// ============================================================================

/// @brief JobService RPC 实现
///
/// 职责：
/// - SubmitJob：校验参数 → 创建 JobRecord 写入 JobStore → 返回 job_id
/// - QueryJob：按 job_id 查询 JobStore + ShardStore → 返回 job 信息及 shard 列表
/// - CancelJob：将 job 状态改为 JOB_CANCELED
///
/// 阶段 2（骨架）不调用 Scheduler，不触发实际任务切分。
class JobServiceImpl : public JobService {
public:
    /// @brief 提交视频处理任务
    ///
    /// 请求包含：user_id、input_path、output_path、target_format、
    ///           target_resolution、target_bitrate、priority
    /// 返回：error_code、job_id、完整的 JobInfo
    void SubmitJob(::google::protobuf::RpcController* controller,
                   const ::SubmitJobRequest* request,
                   ::SubmitJobResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::SubmitJob user=%s, input=%s, target_res=%s",
                 request->user_id().c_str(),
                 request->input_path().c_str(),
                 request->target_resolution().c_str());

        // 用 GenerateId 生成唯一 job_id，格式 job_<timestamp>_<counter>_<random>
        std::string job_id = GenerateId("job");
        JobRecord job;
        job.job_id            = job_id;
        job.user_id           = request->user_id();
        job.input_path        = request->input_path();
        job.output_path       = request->output_path();
        job.target_format     = request->target_format();
        job.target_resolution = request->target_resolution();
        job.target_bitrate    = request->target_bitrate();
        job.priority          = request->priority();
        job.status            = static_cast<int32_t>(JobStatus::JOB_PENDING);
        job.shard_count       = 0;
        job.created_at        = NowMs();
        job.updated_at        = NowMs();

        // 写入内存存储，后续 QueryJob 可查
        JobStore::GetInstance().Insert(job);
        LOG_INFO("JobService::SubmitJob created job_id=%s", job_id.c_str());

        // 构造响应：把 JobRecord 字段逐一映射到 proto JobInfo
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

        done->Run();
    }

    /// @brief 查询任务状态和进度
    ///
    /// 返回 job 基本信息 + 所有已创建的 shard 列表。
    /// 后续阶段会追加进度百分比等聚合信息。
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
    ///
    /// 将 job 状态设为 JOB_CANCELED。
    /// 后续阶段会级联取消所有未完成的 shard。
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
};

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief JobService 启动流程：
///   1. 初始化异步日志（Debug 模式输出终端，Release 仅写文件）
///   2. MprpcApplication::Init() 加载配置文件（-i job_service.conf）
///   3. 创建 RpcProvider，注册 JobServiceImpl
///   4. provider.Run() 阻塞：连接 ZK + 注册 /mprpc/services/JobService + 启动 TcpServer
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
    // Provider 只存裸指针，不接管对象生命周期
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
