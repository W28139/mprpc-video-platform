#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include "worker.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"

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
// RunHeartbeatLoop — Worker 注册与心跳后台线程
// ============================================================================

/// @brief 在后台线程中执行 RegisterWorker → Heartbeat 循环
///
/// 使用 MprpcChannel 通过 ZK 发现 WorkerManagerService，先注册再周期心跳。
/// 注册失败时最多重试 3 次，全部失败则线程退出（Provider 仍运行，但 Worker
/// 会在 WorkerManager 端因心跳超时而被标 OFFLINE）。
///
/// @param worker_id          配置中的 Worker 唯一标识
/// @param cpu_cores          CPU 核心数（上报用）
/// @param memory_mb          总内存 MB（上报用）
/// @param max_running_shards 最大并发 shard 数（上报用）
/// @param stop_flag          主线程设置的停止标志
static void RunHeartbeatLoop(std::string worker_id,
                              int cpu_cores,
                              int memory_mb,
                              int max_running_shards,
                              std::atomic<bool>& stop_flag)
{
    auto& config = MprpcApplication::GetConfig();

    // 读自己的监听地址（注册时上报给 WorkerManager）
    std::string ip = config.Load("rpcserverip");
    int port = config.LoadInt("rpcserverport", -1, 1, 65535);
    if (ip.empty() || port == -1)
    {
        LOG_ERROR("RunHeartbeatLoop: missing rpcserverip or rpcserverport in config");
        return;
    }

    LOG_INFO("RunHeartbeatLoop: starting for worker_id=%s, ip=%s:%d, cores=%d, mem=%dMB, max_shards=%d",
             worker_id.c_str(), ip.c_str(), port, cpu_cores, memory_mb, max_running_shards);

    // 创建 WorkerManagerService 的 RPC stub（通过 ZK 发现服务）
    MprpcChannel channel;
    WorkerManagerService_Stub stub(&channel);

    // ── 1. RegisterWorker（最多重试 3 次） ──────────────────────────────
    bool registered = false;
    for (int attempt = 1; attempt <= 3 && !stop_flag; ++attempt)
    {
        RegisterWorkerRequest reg_req;
        reg_req.set_worker_id(worker_id);
        reg_req.set_ip(ip);
        reg_req.set_port(port);
        reg_req.set_cpu_cores(cpu_cores);
        reg_req.set_memory_mb(memory_mb);
        reg_req.set_max_running_shards(max_running_shards);

        RegisterWorkerResponse reg_resp;
        MprpcController controller;

        stub.RegisterWorker(&controller, &reg_req, &reg_resp, nullptr);

        if (!controller.Failed() && reg_resp.error_code() == 0)
        {
            LOG_INFO("RunHeartbeatLoop: RegisterWorker success, worker_id=%s (attempt %d)",
                     worker_id.c_str(), attempt);
            registered = true;
            break;
        }

        LOG_WARN("RunHeartbeatLoop: RegisterWorker failed (attempt %d/%d): %s",
                 attempt, 3,
                 controller.Failed() ? controller.ErrorText().c_str()
                                     : reg_resp.error_msg().c_str());

        if (attempt < 3 && !stop_flag)
            std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (!registered)
    {
        LOG_ERROR("RunHeartbeatLoop: RegisterWorker failed after 3 attempts, heartbeat thread exiting");
        return;
    }

    // ── 2. Heartbeat 循环（每 3 秒） ────────────────────────────────────
    while (!stop_flag)
    {
        std::this_thread::sleep_for(std::chrono::seconds(3));
        if (stop_flag) break;

        HeartbeatRequest hb_req;
        auto* load = hb_req.mutable_load();
        load->set_worker_id(worker_id);
        load->set_cpu_usage(0);          // 阶段 3 暂为骨架，后续接入真实采集
        load->set_memory_usage(0);
        load->set_running_shards(0);
        load->set_finished_shards(0);
        load->set_failed_shards(0);
        load->set_timestamp(NowMs());

        HeartbeatResponse hb_resp;
        MprpcController controller;

        stub.Heartbeat(&controller, &hb_req, &hb_resp, nullptr);

        if (!controller.Failed() && hb_resp.error_code() == 0)
        {
            LOG_DEBUG("RunHeartbeatLoop: Heartbeat success, worker_id=%s", worker_id.c_str());

            if (!hb_resp.alive())
            {
                LOG_WARN("RunHeartbeatLoop: WorkerManager reports worker_id=%s is NOT alive (possibly de-registered)",
                         worker_id.c_str());
            }
        }
        else
        {
            LOG_WARN("RunHeartbeatLoop: Heartbeat failed: %s",
                     controller.Failed() ? controller.ErrorText().c_str()
                                         : hb_resp.error_msg().c_str());
        }
    }

    LOG_INFO("RunHeartbeatLoop: heartbeat thread stopped for worker_id=%s", worker_id.c_str());
}

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief TranscodeWorker 启动流程（双角色：Provider + Consumer）
///
/// 1. 读取 Worker 专有配置（worker_id / cpu_cores / memory_mb / max_running_shards）
/// 2. 启动后台心跳线程（RegisterWorker → Heartbeat 循环）
/// 3. 主线程启动 RpcProvider 接收 Scheduler 下发的 AssignShard 等命令
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

    // ── 读取 Worker 专有配置 ──────────────────────────────────────────
    auto& config = MprpcApplication::GetConfig();

    std::string worker_id, error;
    if (!config.LoadRequired("worker_id", worker_id, error))
    {
        LOG_ERROR("%s", error.c_str());
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    int cpu_cores = config.LoadInt("cpu_cores", -1, 1, 1024);
    if (cpu_cores == -1)
    {
        LOG_ERROR("required config key invalid: cpu_cores");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    int memory_mb = config.LoadInt("memory_mb", -1, 128, 1048576);
    if (memory_mb == -1)
    {
        LOG_ERROR("required config key invalid: memory_mb");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    int max_running_shards = config.LoadInt("max_running_shards", -1, 1, 1024);
    if (max_running_shards == -1)
    {
        LOG_ERROR("required config key invalid: max_running_shards");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    LOG_INFO("TranscodeWorker config: worker_id=%s, cores=%d, mem=%dMB, max_shards=%d",
             worker_id.c_str(), cpu_cores, memory_mb, max_running_shards);

    // ── 启动心跳线程（Provider 启动前先注册） ──────────────────────────
    std::atomic<bool> heartbeat_stopped{false};
    std::thread heartbeat_thread(RunHeartbeatLoop, worker_id,
                                  cpu_cores, memory_mb, max_running_shards,
                                  std::ref(heartbeat_stopped));

    // ── 启动 RPC Provider（阻塞主线程） ─────────────────────────────────
    RpcProvider provider;
    provider.NotifyService(new WorkerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("WorkerService start failed");
        heartbeat_stopped = true;
        if (heartbeat_thread.joinable()) heartbeat_thread.join();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // Provider 退出后清理
    heartbeat_stopped = true;
    if (heartbeat_thread.joinable()) heartbeat_thread.join();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
