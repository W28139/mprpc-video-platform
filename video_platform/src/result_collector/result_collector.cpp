#include <string>
#include <cstdlib>
#include "result.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"

using namespace video_platform;

// ============================================================================
// ResultCollectorService — 结果收集与聚合
// ============================================================================

/// @brief ResultCollectorService RPC 实现（骨架版本）
///
/// 负责接收 Worker 上报的进度和结果：
/// - ReportShardProgress：Worker 执行中周期性上报（每 1 秒或每 10%）
/// - ReportShardResult：Worker 执行完毕上报最终成功或失败
///
/// 阶段 2（骨架）只打日志重播记录。
/// 阶段 4（mock 闭环）将实现：
/// - 判断 shard 是否已完成（幂等：已 SUCCESS 的再次上报直接返回 accepted=true）
/// - 通过 attempt_id 校验防止旧 attempt 覆盖新 attempt
/// - 判断 job 是否全部 shard 完成（最后一个 shard 成功时 job_done=true）
/// - 失败 shard 触发 Scheduler.RescheduleShard 重试
class ResultCollectorServiceImpl : public ResultCollectorService {
public:
    /// @brief Worker 上报 shard 执行进度
    ///
    /// Worker 在 mock/ffmpeg 执行过程中周期性调用。
    /// @param request.progress 0-100 的整数百分比
    /// @param request.message 可选附加信息（如 ffmpeg 当前帧号）
    void ReportShardProgress(::google::protobuf::RpcController* controller,
                             const ::ReportShardProgressRequest* request,
                             ::ReportShardProgressResponse* response,
                             ::google::protobuf::Closure* done) override
    {
        LOG_INFO("ResultCollectorService::ReportShardProgress job_id=%s, shard_id=%s, worker=%s, progress=%d%%",
                 request->job_id().c_str(),
                 request->shard_id().c_str(),
                 request->worker_id().c_str(),
                 request->progress());

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_recorded(true);
        done->Run();
    }

    /// @brief Worker 上报 shard 最终执行结果
    ///
    /// 无论成功还是失败，Worker 在 shard 执行结束后必须调用。
    /// @param request.is_success 执行是否成功
    /// @param request.exit_code ffmpeg 进程退出码（成功时为 0）
    /// @param request.error_msg 失败时的 stderr 或错误摘要
    /// @param request.output_path 输出文件路径
    /// @param request.elapsed_ms 执行耗时
    void ReportShardResult(::google::protobuf::RpcController* controller,
                           const ::ReportShardResultRequest* request,
                           ::ReportShardResultResponse* response,
                           ::google::protobuf::Closure* done) override
    {
        LOG_INFO("ResultCollectorService::ReportShardResult job_id=%s, shard_id=%s, worker=%s, attempt=%s, success=%d, exit_code=%d, elapsed=%lldms",
                 request->job_id().c_str(),
                 request->shard_id().c_str(),
                 request->worker_id().c_str(),
                 request->attempt_id().c_str(),
                 request->is_success(),
                 request->exit_code(),
                 (long long)request->elapsed_ms());

        response->set_error_code(0);
        response->set_error_msg("");

        // 幂等处理：无论是否重复上报，都返回 accepted=true
        // 阶段 4 将根据 attempt_id 做真正的幂等校验
        response->set_accepted(true);

        // 当前不判断 job 是否完成，留给阶段 4 聚合逻辑
        response->set_job_done(false);
        done->Run();
    }
};

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief ResultCollectorService 启动流程（与 JobService 相同模式）
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
    provider.NotifyService(new ResultCollectorServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("ResultCollectorService start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
