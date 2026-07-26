#include <string>
#include <cstdlib>
#include "worker.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"

using namespace video_platform;

// ============================================================================
// WorkerService — TranscodeWorker 端 RPC 接口
// ============================================================================

/// @brief WorkerService RPC 实现（骨架版本）
///
/// TranscodeWorker 有双重角色：
/// 1. 作为 RPC Provider，接收 Scheduler 下发的调度命令
///    - AssignShard：接收一个 shard，放入内部执行队列
///    - CancelShard：取消一个正在执行的 shard
///    - QueryShard：查询 shard 的执行进度
/// 2. 作为 RPC Consumer，主动调用其他服务（阶段 3+ 实现）
///    - 调用 WorkerManager.Heartbeat 周期上报负载
///    - 调用 ResultCollector.ReportShardProgress 上报进度
///    - 调用 ResultCollector.ReportShardResult 上报最终结果
///
/// 阶段 2（骨架）只搭好 Provider 侧的 RPC 接口，不启动 Consumer 侧的调用。
class WorkerServiceImpl : public WorkerService {
public:
    /// @brief 接收 Scheduler 分配的 shard
    ///
    /// 未来完整实现步骤：
    /// 1. 写入 ShardStore 或内部执行队列
    /// 2. 启动执行线程（mock sleep 或 ffmpeg 子进程）
    /// 3. 周期性通过 Consumer 端调用 ReportShardProgress
    /// 4. 执行完成后调用 ReportShardResult
    /// @param request.shard 包含完整的 ShardInfo（shard_id / job_id / start_ms / duration_ms 等）
    void AssignShard(::google::protobuf::RpcController* controller,
                     const ::AssignShardRequest* request,
                     ::AssignShardResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        const auto& shard = request->shard();
        LOG_INFO("WorkerService::AssignShard shard_id=%s, job_id=%s, shard_index=%d, start=%lldms, dur=%lldms",
                 shard.shard_id().c_str(),
                 shard.job_id().c_str(),
                 shard.shard_index(),
                 (long long)shard.start_ms(),
                 (long long)shard.duration_ms());

        // Stage 2 骨架：只记录，不做实际执行
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        done->Run();
    }

    /// @brief 取消正在执行的 shard
    ///
    /// Scheduler 或用户主动取消时调用。
    /// 实现时需要终止对应的子进程或取消对应的执行线程。
    void CancelShard(::google::protobuf::RpcController* controller,
                     const ::CancelShardRequest* request,
                     ::CancelShardResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        LOG_INFO("WorkerService::CancelShard shard_id=%s, reason=%s",
                 request->shard_id().c_str(),
                 request->reason().c_str());

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_canceled(true);
        done->Run();
    }

    /// @brief 查询 shard 执行进度
    ///
    /// 返回 progress 0-100 百分比。
    /// 阶段 2 骨架始终返回 0。
    void QueryShard(::google::protobuf::RpcController* controller,
                    const ::QueryShardRequest* request,
                    ::QueryShardResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        LOG_INFO("WorkerService::QueryShard shard_id=%s", request->shard_id().c_str());

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_progress(0);  // 骨架阶段无实际执行，进度恒为 0
        done->Run();
    }
};

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief TranscodeWorker 启动流程（与 JobService 相同模式）
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
    provider.NotifyService(new WorkerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("WorkerService start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
