#include <string>
#include <cstdlib>
#include "scheduler.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"

using namespace video_platform;

// ============================================================================
// SchedulerService — 任务切分与分配
// ============================================================================

/// @brief SchedulerService RPC 实现（骨架版本）
///
/// 阶段 2（骨架）行为：
/// - ScheduleJob：只打日志返回 accepted=true，不切分不调度
/// - RescheduleShard：只打日志返回 accepted=true，不重新分配
///
/// 阶段 4（mock 闭环）将实现完整逻辑：
/// - 按时间切片策略将 job 拆成 shard
/// - 从 WorkerManager 查询 ONLINE Worker
/// - 按评分策略分配 shard 给 Worker
/// - 处理超时 shard 的重调度
class SchedulerServiceImpl : public SchedulerService {
public:
    /// @brief 对 job 进行切分并启动调度
    ///
    /// 未来完整实现步骤：
    /// 1. 读取 job_info.duration_sec（如为 0 则先探测）
    /// 2. 按 shard_duration_sec 拆分，创建 ShardRecord 列表
    /// 3. 将 shard 状态设为 SHARD_WAITING，推入待调度队列
    /// 4. 触发现有调度循环
    /// @param request 包含完整 JobInfo，job_id / input_path / target_resolution 等
    void ScheduleJob(::google::protobuf::RpcController* controller,
                     const ::ScheduleJobRequest* request,
                     ::ScheduleJobResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        const auto& jobInfo = request->job_info();
        LOG_INFO("SchedulerService::ScheduleJob job_id=%s, input=%s",
                 jobInfo.job_id().c_str(),
                 jobInfo.input_path().c_str());

        // Stage 2 骨架：只记录请求，不做实际切分
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        response->set_job_id(jobInfo.job_id());
        done->Run();
    }

    /// @brief 重新调度一个 shard
    ///
    /// 触发场景：
    /// - Worker 执行失败（ffmpeg 返回非零）
    /// - Worker 心跳超时（WorkerManager 检测到后调用）
    /// - 手动重调度
    /// @param request.reason 触发原因：TIMEOUT / WORKER_FAILED / MANUAL
    void RescheduleShard(::google::protobuf::RpcController* controller,
                         const ::RescheduleShardRequest* request,
                         ::RescheduleShardResponse* response,
                         ::google::protobuf::Closure* done) override
    {
        LOG_INFO("SchedulerService::RescheduleShard shard_id=%s, job_id=%s, reason=%s",
                 request->shard_id().c_str(),
                 request->job_id().c_str(),
                 request->reason().c_str());

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        done->Run();
    }
};

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief SchedulerService 启动流程（与 JobService 相同模式）
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
    provider.NotifyService(new SchedulerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("SchedulerService start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
