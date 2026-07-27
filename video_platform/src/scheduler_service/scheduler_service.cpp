#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include "scheduler.pb.h"
#include "worker.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
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
// PeriodicListWorkers — 周期查询 Worker 列表后台线程
// ============================================================================

/// @brief 在后台线程中周期性调用 WorkerManager.ListWorkers，查询并打印 ONLINE Worker 列表
///
/// 阶段 3 用途：验证 Scheduler 能通过 RPC 获取 Worker 列表。
/// 阶段 4+ 会扩展为实际的调度循环，根据 Worker 资源状态分配 shard。
///
/// @param stop_flag 主线程设置的停止标志
static void PeriodicListWorkers(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kQueryIntervalMs = 5000;  // 每 5 秒查询一次

    LOG_INFO("PeriodicListWorkers thread started, interval=%lldms",
             (long long)kQueryIntervalMs);

    // 等待 Provider 启动完成
    std::this_thread::sleep_for(std::chrono::seconds(1));

    WorkerManagerService_Stub stub(new MprpcChannel());

    while (!stop_flag)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kQueryIntervalMs));
        if (stop_flag) break;

        ListWorkersRequest req;
        // filter_status=WORKER_STATUS_UNKNOWN(0) 表示不过滤，返回全部
        req.set_filter_status(WorkerStatus::WORKER_STATUS_UNKNOWN);

        ListWorkersResponse resp;
        MprpcController controller;

        stub.ListWorkers(&controller, &req, &resp, nullptr);

        if (!controller.Failed() && resp.error_code() == 0)
        {
            int online_count = 0;
            for (const auto& w : resp.workers())
            {
                if (w.status() == WorkerStatus::WORKER_ONLINE)
                    ++online_count;
            }
            LOG_INFO("PeriodicListWorkers: total=%d, online=%d",
                     resp.workers_size(), online_count);
        }
        else
        {
            LOG_WARN("PeriodicListWorkers: ListWorkers RPC failed: %s",
                     controller.Failed() ? controller.ErrorText().c_str()
                                         : resp.error_msg().c_str());
        }
    }

    LOG_INFO("PeriodicListWorkers thread stopped");
}

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

    // 启动后台线程：周期性查询 WorkerManager 的 ONLINE Worker 列表
    std::atomic<bool> list_workers_stopped{false};
    std::thread list_workers_thread(PeriodicListWorkers, std::ref(list_workers_stopped));

    RpcProvider provider;
    provider.NotifyService(new SchedulerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("SchedulerService start failed");
        list_workers_stopped = true;
        if (list_workers_thread.joinable()) list_workers_thread.join();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // Provider 退出后清理
    list_workers_stopped = true;
    if (list_workers_thread.joinable()) list_workers_thread.join();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
