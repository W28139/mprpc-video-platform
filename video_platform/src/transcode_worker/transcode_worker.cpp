#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <memory>
#include "worker.pb.h"
#include "result.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"

using namespace video_platform;

// ============================================================================
// WorkerService — TranscodeWorker 端 RPC 接口（阶段 4：mock 执行器）
// ============================================================================

/// @brief 运行中的 shard 追踪结构
///
/// 每个通过 AssignShard 接收到的 shard 都会创建一个 RunningShard 对象，
/// 生命周期由 shared_ptr 管理（AssignShard 和 MockExecute 线程共享所有权）。
///
/// progress 和 cancelled 是 std::atomic，允许 MockExecute 线程写入、
/// QueryShard RPC（在 IO 线程）和心跳线程同时无锁读取。
struct RunningShard {
    ShardInfo   info;                      ///< shard 元信息（proto 拷贝）
    std::string attempt_id;               ///< 执行尝试 ID，格式 "{shard_id}_attempt_{N}"
    std::atomic<int> progress{0};        ///< 执行进度 0-100，原子变量无锁读取
    std::atomic<bool> cancelled{false};  ///< 取消标志，CancelShard 设置、MockExecute 检测
    std::thread executor_thread;         ///< mock 执行线程句柄

    RunningShard(const ShardInfo& s, const std::string& aid)
        : info(s), attempt_id(aid) {}
};

/// @brief WorkerService RPC 实现
///
/// TranscodeWorker 双重角色：
/// 1. Provider — 接收 Scheduler 下发的调度命令：
///    - AssignShard：接收 shard → 校验无重复 → 启动 mock 线程
///    - CancelShard：设置 cancelled 标志，mock 线程在下个 sleep 周期检测并退出
///    - QueryShard：返回 shard 实时 progress（0-100 或 -1=不存在）
///
/// 2. Consumer — 主动调用其他服务（不在本类中，由独立函数实现）：
///    - RunHeartbeatLoop：周期调用 WorkerManager.RegisterWorker / Heartbeat
///    - MockExecute：shard 执行线程内调用 ResultCollector.ReportProgress / ReportResult
///
/// 线程模型：
///   running_shards_ map 由 mutex_ 保护，
///   AssignShard / CancelShard / QueryShard 在 IO 线程（或 work 线程）执行，
///   MockExecute 在独立 std::thread 中执行，
///   心跳线程 RunHeartbeatLoop 通过 RunningShardCount() 只读访问（持锁遍历）。
class WorkerServiceImpl : public WorkerService {
public:
    /// @brief 获取当前运行中 shard 数量（供心跳线程使用）
    int RunningShardCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        int count = 0;
        for (const auto& kv : running_shards_) {
            if (!kv.second->cancelled) ++count;
        }
        return count;
    }

    /// @brief 接收 Scheduler 分配的 shard，启动 mock 执行线程
    void AssignShard(::google::protobuf::RpcController* controller,
                     const ::AssignShardRequest* request,
                     ::AssignShardResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        const auto& shard = request->shard();
        LOG_INFO("WorkerService::AssignShard shard_id=%s, job_id=%s, shard_index=%d, "
                 "start=%lldms, dur=%lldms",
                 shard.shard_id().c_str(),
                 shard.job_id().c_str(),
                 shard.shard_index(),
                 (long long)shard.start_ms(),
                 (long long)shard.duration_ms());

        // 检查是否为新 shard（非重试的分配）
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = running_shards_.find(shard.shard_id());
            if (it != running_shards_.end() && !it->second->cancelled)
            {
                LOG_WARN("WorkerService::AssignShard shard %s already running, rejecting",
                         shard.shard_id().c_str());
                response->set_error_code(1);
                response->set_error_msg("shard already running: " + shard.shard_id());
                response->set_accepted(false);
                done->Run();
                return;
            }
        }

        // 生成 attempt_id
        std::string attempt_id = shard.shard_id() + "_attempt_"
            + std::to_string(shard.retry_count());

        // 创建 RunningShard 追踪对象
        auto rs = std::make_shared<RunningShard>(shard, attempt_id);

        // 读取 mock 执行时长配置
        int mock_execution_ms = MprpcApplication::GetConfig().LoadInt(
            "mock_execution_time_ms", 5000, 500, 120000);

        // 启动 mock 执行线程
        rs->executor_thread = std::thread(&WorkerServiceImpl::MockExecute,
                                          this, rs, mock_execution_ms);

        // 加入运行中 map
        {
            std::lock_guard<std::mutex> lock(mutex_);
            running_shards_[shard.shard_id()] = rs;
        }

        LOG_INFO("WorkerService::AssignShard shard=%s attempt=%s started (mock_exec=%dms)",
                 shard.shard_id().c_str(), attempt_id.c_str(), mock_execution_ms);

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        done->Run();
    }

    /// @brief 取消正在执行的 shard
    void CancelShard(::google::protobuf::RpcController* controller,
                     const ::CancelShardRequest* request,
                     ::CancelShardResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        LOG_INFO("WorkerService::CancelShard shard_id=%s, reason=%s",
                 request->shard_id().c_str(),
                 request->reason().c_str());

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = running_shards_.find(request->shard_id());
        if (it != running_shards_.end())
        {
            it->second->cancelled = true;
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_canceled(true);
            LOG_INFO("WorkerService::CancelShard shard=%s cancelled", request->shard_id().c_str());
        }
        else
        {
            response->set_error_code(1);
            response->set_error_msg("shard not found: " + request->shard_id());
            response->set_canceled(false);
        }
        done->Run();
    }

    /// @brief 查询 shard 执行进度
    void QueryShard(::google::protobuf::RpcController* controller,
                    const ::QueryShardRequest* request,
                    ::QueryShardResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        LOG_DEBUG("WorkerService::QueryShard shard_id=%s", request->shard_id().c_str());

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = running_shards_.find(request->shard_id());
        if (it != running_shards_.end())
        {
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_progress(it->second->progress);
            auto* si = response->mutable_shard_info();
            const auto& s = it->second->info;
            si->set_shard_id(s.shard_id());
            si->set_job_id(s.job_id());
            si->set_shard_index(s.shard_index());
            si->set_status(ShardStatus::SHARD_RUNNING);
            si->set_assigned_worker_id(s.assigned_worker_id());
            si->set_attempt_id(it->second->attempt_id);
            si->set_retry_count(s.retry_count());
        }
        else
        {
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_progress(-1);  // shard 不在本 Worker
        }
        done->Run();
    }

private:
    /// @brief mock 执行器：sleep 模拟视频转码，每秒上报进度，完成后上报结果
    ///
    /// 在 AssignShard 创建的独立 std::thread 中运行，不阻塞 RPC 响应。
    ///
    /// 执行流程：
    ///   FOR step = 1 .. total_steps:
    ///     sleep(1s)
    ///     IF cancelled → CleanupShard → RETURN
    ///     progress = step * 100 / total_steps
    ///     RPC: ResultCollector.ReportShardProgress(progress%)
    ///   RPC: ResultCollector.ReportShardResult(is_success=true)
    ///   CleanupShard
    ///
    /// 错误处理策略：
    /// - 进度上报失败：只打 WARN 日志，不中断执行（下一轮继续重试）
    /// - 结果上报失败：打 ERROR 日志，shard 实际已执行完成；
    ///   阶段 5 的重试机制可以通过 RescheduleShard 兜底
    ///
    /// @param rs  RunningShard 的 shared_ptr，保证本线程和 Provider 线程共享所有权
    /// @param mock_execution_ms  mock 执行总时长（毫秒），从配置文件读取
    void MockExecute(std::shared_ptr<RunningShard> rs, int mock_execution_ms)
    {
        const std::string& shard_id = rs->info.shard_id();
        const std::string& job_id   = rs->info.job_id();
        std::string worker_id = MprpcApplication::GetConfig().Load("worker_id");

        LOG_INFO("MockExecute: shard=%s started, mock_duration=%dms",
                 shard_id.c_str(), mock_execution_ms);

        // ResultCollector stub（通过 ZK 发现）
        MprpcChannel rc_channel;
        ResultCollectorService_Stub rc_stub(&rc_channel);

        int report_interval_ms = 1000;                // 每秒上报一次
        int total_steps = mock_execution_ms / report_interval_ms;
        if (total_steps < 1) total_steps = 1;

        for (int step = 1; step <= total_steps; ++step)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(report_interval_ms));

            if (rs->cancelled)
            {
                LOG_INFO("MockExecute: shard=%s cancelled at progress=%d%%",
                         shard_id.c_str(), rs->progress.load());
                CleanupShard(shard_id);
                return;
            }

            int progress = step * 100 / total_steps;
            rs->progress = progress;

            // 上报进度给 ResultCollector
            ReportShardProgressRequest prog_req;
            prog_req.set_shard_id(shard_id);
            prog_req.set_job_id(job_id);
            prog_req.set_worker_id(worker_id);
            prog_req.set_attempt_id(rs->attempt_id);
            prog_req.set_progress(progress);

            ReportShardProgressResponse prog_resp;
            MprpcController ctrl;

            rc_stub.ReportShardProgress(&ctrl, &prog_req, &prog_resp, nullptr);

            if (!ctrl.Failed() && prog_resp.recorded())
            {
                LOG_DEBUG("MockExecute: shard=%s progress=%d%% reported", shard_id.c_str(), progress);
            }
            else
            {
                LOG_WARN("MockExecute: shard=%s progress report failed: %s",
                         shard_id.c_str(),
                         ctrl.Failed() ? ctrl.ErrorText().c_str() : prog_resp.error_msg().c_str());
            }
        }

        // 执行完成，上报最终结果
        rs->progress = 100;

        ReportShardResultRequest result_req;
        result_req.set_shard_id(shard_id);
        result_req.set_job_id(job_id);
        result_req.set_worker_id(worker_id);
        result_req.set_attempt_id(rs->attempt_id);
        result_req.set_is_success(true);
        result_req.set_exit_code(0);
        result_req.set_error_msg("");
        result_req.set_output_path(rs->info.output_path());
        result_req.set_elapsed_ms(mock_execution_ms);

        ReportShardResultResponse result_resp;
        MprpcController ctrl;

        rc_stub.ReportShardResult(&ctrl, &result_req, &result_resp, nullptr);

        if (!ctrl.Failed() && result_resp.accepted())
        {
            LOG_INFO("MockExecute: shard=%s SUCCESS, job_done=%d",
                     shard_id.c_str(), result_resp.job_done());
        }
        else
        {
            LOG_ERROR("MockExecute: shard=%s result report failed: %s",
                      shard_id.c_str(),
                      ctrl.Failed() ? ctrl.ErrorText().c_str() : result_resp.error_msg().c_str());
        }

        CleanupShard(shard_id);
    }

    /// @brief 从 running_shards_ map 中移除已完成的 shard
    ///
    /// ⚠️ 线程安全要点：
    /// 1. 持锁期间只做 map::erase（O(1)），不 join 线程
    /// 2. join/detach 在锁外执行，避免死锁：
    ///    若在锁内 join，而 executor_thread 正在执行 CancelShard→lock(mutex_)，
    ///    会形成 AB-BA 死锁
    /// 3. 使用 detach() 而非 join()：mock 线程即将结束，detach 让 OS 回收资源
    void CleanupShard(const std::string& shard_id)
    {
        std::shared_ptr<RunningShard> rs;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = running_shards_.find(shard_id);
            if (it != running_shards_.end())
            {
                rs = it->second;
                running_shards_.erase(it);  // 锁内仅做 map 操作
            }
        }
        // 锁外处理线程资源，避免与 MockExecute / CancelShard 死锁
        if (rs && rs->executor_thread.joinable())
        {
            rs->executor_thread.detach();
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<RunningShard>> running_shards_;
};

// ============================================================================
// RunHeartbeatLoop — Worker 注册与心跳后台线程
// ============================================================================

/// @brief 在后台线程中执行 RegisterWorker → Heartbeat 循环
///
/// 阶段 4 改进：心跳上报真实的 current_running_shards（从 WorkerServiceImpl 获取），
/// 不再硬编码为 0。
static void RunHeartbeatLoop(std::string worker_id,
                              int cpu_cores,
                              int memory_mb,
                              int max_running_shards,
                              WorkerServiceImpl* worker_service,
                              std::atomic<bool>& stop_flag)
{
    auto& config = MprpcApplication::GetConfig();

    std::string ip = config.Load("rpcserverip");
    int port = config.LoadInt("rpcserverport", -1, 1, 65535);
    if (ip.empty() || port == -1)
    {
        LOG_ERROR("RunHeartbeatLoop: missing rpcserverip or rpcserverport in config");
        return;
    }

    LOG_INFO("RunHeartbeatLoop: starting for worker_id=%s, ip=%s:%d, cores=%d, mem=%dMB, max_shards=%d",
             worker_id.c_str(), ip.c_str(), port, cpu_cores, memory_mb, max_running_shards);

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

        int running = worker_service ? worker_service->RunningShardCount() : 0;

        HeartbeatRequest hb_req;
        auto* load = hb_req.mutable_load();
        load->set_worker_id(worker_id);
        load->set_cpu_usage(0);          // 阶段 4 暂未接入真实 CPU 采集
        load->set_memory_usage(0);
        load->set_running_shards(running);
        load->set_finished_shards(0);
        load->set_failed_shards(0);
        load->set_timestamp(NowMs());

        HeartbeatResponse hb_resp;
        MprpcController controller;

        stub.Heartbeat(&controller, &hb_req, &hb_resp, nullptr);

        if (!controller.Failed() && hb_resp.error_code() == 0)
        {
            LOG_DEBUG("RunHeartbeatLoop: Heartbeat success, worker_id=%s, running=%d",
                      worker_id.c_str(), running);

            if (!hb_resp.alive())
            {
                LOG_WARN("RunHeartbeatLoop: WorkerManager reports worker_id=%s is NOT alive",
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
/// 1. 读取 Worker 专有配置
/// 2. 创建 WorkerServiceImpl（含 mock 执行器）
/// 3. 启动后台心跳线程（RegisterWorker → Heartbeat 循环）
/// 4. 主线程启动 RpcProvider 接收 AssignShard 等命令
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

    // ── 创建 WorkerServiceImpl ──────────────────────────────────────
    auto* worker_service = new WorkerServiceImpl();

    // ── 启动心跳线程（传入 worker_service 指针以获取真实的 running count） ─
    std::atomic<bool> heartbeat_stopped{false};
    std::thread heartbeat_thread(RunHeartbeatLoop, worker_id,
                                  cpu_cores, memory_mb, max_running_shards,
                                  worker_service,
                                  std::ref(heartbeat_stopped));

    // ── 启动 RPC Provider（阻塞主线程） ─────────────────────────────────
    RpcProvider provider;
    provider.NotifyService(worker_service);

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
