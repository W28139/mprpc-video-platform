#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <mutex>
#include <unordered_set>
#include <chrono>
#include "worker.pb.h"
#include "scheduler.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "mprpcmetrics.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"
#include "video_platform/mysql_pool.h"
#include "video_platform/redis_client.h"

using namespace video_platform;

// ============================================================================
// WorkerManagerService — Worker 注册、心跳、查询
// ============================================================================

/// @brief WorkerManagerService RPC 实现
///
/// 负责维护所有 Worker 节点的生命周期：
/// - RegisterWorker：新 Worker 启动时调用，写入 WorkerStore
/// - Heartbeat：Worker 周期性上报负载，更新 heartbeat 时间戳和运行状态
/// - ListWorkers：让 Scheduler 查询可用的 ONLINE Worker
///
/// 阶段 5 增强：心跳超时后通知 Scheduler 重调度该 Worker 上的 RUNNING shard
/// （通过 SchedulerService.NotifyWorkerOffline RPC）。
class WorkerManagerServiceImpl : public WorkerManagerService {
public:
    /// @brief Worker 注册
    ///
    /// 请求包含 Worker 的硬件信息（cores/mem/gpu）和能力上限（max_running_shards）。
    /// 注册后状态为 WORKER_ONLINE，Scheduler 即可发现此节点。
    /// worker_id 已存在时 Insert 返回 false（幂等）。
    void RegisterWorker(::google::protobuf::RpcController* controller,
                        const ::RegisterWorkerRequest* request,
                        ::RegisterWorkerResponse* response,
                        ::google::protobuf::Closure* done) override
    {
        LOG_INFO("WorkerManagerService::RegisterWorker worker_id=%s, ip=%s:%d, cores=%d, mem=%dMB",
                 request->worker_id().c_str(),
                 request->ip().c_str(),
                 request->port(),
                 request->cpu_cores(),
                 request->memory_mb());

        WorkerRecord worker;
        worker.worker_id              = request->worker_id();
        worker.ip                     = request->ip();
        worker.port                   = request->port();
        worker.cpu_cores              = request->cpu_cores();
        worker.memory_mb              = request->memory_mb();
        worker.gpu_count              = request->gpu_count();
        worker.max_running_shards     = request->max_running_shards();
        worker.current_running_shards = 0;
        worker.status                 = static_cast<int32_t>(WorkerStatus::WORKER_ONLINE);
        worker.last_heartbeat         = NowMs();

        bool is_new = WorkerStore::GetInstance().InsertOrUpdate(worker);
        LOG_INFO("WorkerManagerService::RegisterWorker worker_id=%s %s",
                 request->worker_id().c_str(),
                 is_new ? "registered" : "re-registered (overwritten existing record)");

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_registered(true);
        done->Run();
    }

    /// @brief Worker 心跳上报
    ///
    /// Worker 每 3 秒调用一次
    // 1. 保活（存活检测） Worker 每 3 秒调一次，WorkerManager 收到后更新 last_heartbeat 时间戳(心跳断了 20 秒，Worker 就被判定死亡)
    // 2. 负载上报,请求携带 WorkerLoad。UpdateHeartbeat 把这些值写入 WorkerRecord 的负载快照，Scheduler 的 ListWorkers拿到后做加权评分选 worker
    // 3. 反向告知存活状态 — 响应里的 alive （Worker 收到 alive=false（说明 manager 已把它标记死亡））
    void Heartbeat(::google::protobuf::RpcController* controller,
                   const ::HeartbeatRequest* request,
                   ::HeartbeatResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        const auto& load = request->load();
        LOG_INFO("WorkerManagerService::Heartbeat worker_id=%s, cpu=%d%%, mem=%d%%, running=%d",
                 load.worker_id().c_str(),
                 load.cpu_usage(),
                 load.memory_usage(),
                 load.running_shards());

        // 原子更新：在 unique_lock 内完成 查找→更新 heartbeat/status/running_shards/cpu/mem
        bool alive = WorkerStore::GetInstance().UpdateHeartbeat(
            load.worker_id(), load.running_shards(),
            load.cpu_usage(), load.memory_usage());

        // 阶段 10：心跳双写 Redis 负载快照（HSET worker:load {worker_id} → 快照串）。
        // Scheduler 直接从 Redis 读快照，省掉 ListWorkers RPC 往返。
        // 快照串格式：ip|port|cpu_cores|memory_mb|max_running|current_running|cpu_usage|memory_usage|ts
        // - 从 MySQL 读全量记录拼快照：心跳请求只带 WorkerLoad（cpu/mem/running），
        //   ip/port/max_running 是注册时写入的，MySQL 是唯一真相源
        // - Redis 写入失败只 WARN：Scheduler 回退 ListWorkers RPC，核心链路不受影响
        // - alive=false 表示 Worker 已被标记死亡，不写快照（避免"幽灵快照"误导调度）
        if (alive)
        {
            auto& redis = RedisClient::GetInstance();
            if (redis.inited() && redis.enabled())
            {
                auto w_opt = WorkerStore::GetInstance().Get(load.worker_id());
                if (w_opt.has_value())
                {
                    const auto& w = w_opt.value();
                    std::string snapshot =
                        w.ip + "|" + std::to_string(w.port) + "|" +
                        std::to_string(w.cpu_cores) + "|" + std::to_string(w.memory_mb) + "|" +
                        std::to_string(w.max_running_shards) + "|" + std::to_string(w.current_running_shards) + "|" +
                        std::to_string(w.cpu_usage) + "|" + std::to_string(w.memory_usage) + "|" +
                        std::to_string(NowMs());
                    if (!redis.HSet("worker:load", w.worker_id, snapshot))
                    {
                        LOG_WARN("WorkerManagerService: write load snapshot to redis "
                                 "failed for worker %s", w.worker_id.c_str());
                    }
                }
            }
        }

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_alive(alive);
        done->Run();
    }

    /// @brief 列出 Worker 列表
    ///
    /// Scheduler 在分配 shard 前调用，获取所有 ONLINE 节点。
    /// @param request.filter_status 按状态过滤，WORKER_STATUS_UNKNOWN（0）表示全部
    void ListWorkers(::google::protobuf::RpcController* controller,
                     const ::ListWorkersRequest* request,
                     ::ListWorkersResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        LOG_INFO("WorkerManagerService::ListWorkers filter_status=%d",
                 static_cast<int>(request->filter_status()));

        // proto 默认值 0 表示不过滤
        int32_t statusFilter = request->filter_status() == WorkerStatus::WORKER_STATUS_UNKNOWN
                               ? -1    // -1 传给 Store 表示 ListByStatus 返回全部
                               : static_cast<int32_t>(request->filter_status());

        auto workers = WorkerStore::GetInstance().ListByStatus(statusFilter);

        response->set_error_code(0);
        response->set_error_msg("");
        for (const auto& w : workers)
        {
            auto* wi = response->add_workers();
            wi->set_worker_id(w.worker_id);
            wi->set_ip(w.ip);
            wi->set_port(w.port);
            wi->set_cpu_cores(w.cpu_cores);
            wi->set_memory_mb(w.memory_mb);
            wi->set_gpu_count(w.gpu_count);
            wi->set_current_running_shards(w.current_running_shards);
            wi->set_max_running_shards(w.max_running_shards);
            wi->set_status(static_cast<WorkerStatus>(w.status));
            wi->set_last_heartbeat(w.last_heartbeat);
            wi->set_cpu_usage(w.cpu_usage);
            wi->set_memory_usage(w.memory_usage);
        }

        done->Run();
    }
};

// ============================================================================
// HeartbeatTimeoutCheck — 心跳超时检测线程（阶段 5：自动故障恢复）
// ============================================================================
//
// 本线程周期扫描所有 ONLINE Worker 的 last_heartbeat，检测超时并触发恢复。
//
// 阶段 5 端到端恢复流程（核心链路）：
//   1. HeartbeatTimeoutCheck 检测到 Worker 超时（10 秒无心跳）
//   2. WorkerStore::MarkOfflineIfTimeout 原子标记 OFFLINE
//      （与 Heartbeat RPC 的 UpdateHeartbeat 互斥，消除 TOCTOU）
//   3. RPC → SchedulerService::NotifyWorkerOffline(worker_id, "TIMEOUT")
//   4. Scheduler 在本地 ShardStore 中 ListByWorker(worker_id)，
//      找到所有 ASSIGNED/RUNNING 的 shard
//   5. 逐个 shard 执行重调度：
//      - retry_count >= max_retry → SHARD_FAILED → 可能 JOB_FAILED
//      - retry_count <  max_retry → retry_count++ → SHARD_WAITING
//   6. SchedulingLoop 在后续扫描中将 WAITING shard 分配给其他 ONLINE Worker
//
// 设计要点：
// - WorkerManager 不直接操作 ShardStore（那是 Scheduler 进程的数据），
//   而是通过 RPC 通知 Scheduler 自主处理。保持各服务的 Store 边界清晰。
// - NotifyWorkerOffline RPC 失败时不重试——心跳超时检查每 2 秒运行一次，
//   如果 Scheduler 暂时不可用，下一轮会再次检测并重试通知。
// - Worker 已经 OFFLINE 的情况下再次触发 MarkOfflineIfTimeout 为 no-op，
//   不会重复通知 Scheduler。

static void HeartbeatTimeoutCheck(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kHeartbeatTimeoutMs = 20000;  // 阶段 8 #12：20 秒（≈2 个心跳周期）无心跳 → OFFLINE
    constexpr int64_t kCheckIntervalMs    = 2000;   // 每 2 秒扫描一次

    // 阶段 8 #11：NotifyWorkerOffline RPC 失败的 worker 需要持续重试
    static std::unordered_set<std::string> pending_notify;
    static std::mutex notify_mutex;

    LOG_INFO("HeartbeatTimeoutCheck thread started, timeout=%lldms, interval=%lldms",
             (long long)kHeartbeatTimeoutMs, (long long)kCheckIntervalMs);

    while (!stop_flag)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kCheckIntervalMs));
        if (stop_flag) break;

        int64_t now = NowMs();
        auto workers = WorkerStore::GetInstance().ListAll();

        // ── 1. 检查 ONLINE Worker 是否超时 ──────────────────────────
        for (const auto& w : workers)
        {
            if (w.status != static_cast<int32_t>(WorkerStatus::WORKER_ONLINE))
                continue;

            if (now - w.last_heartbeat <= kHeartbeatTimeoutMs)
                continue;

            if (WorkerStore::GetInstance().MarkOfflineIfTimeout(
                    w.worker_id, now, kHeartbeatTimeoutMs))
            {
                LOG_WARN("WorkerManager: worker_id=%s marked OFFLINE (heartbeat timeout, "
                         "last=%lld, now=%lld, gap=%lldms)",
                         w.worker_id.c_str(),
                         (long long)w.last_heartbeat,
                         (long long)now,
                         (long long)(now - w.last_heartbeat));

                // ── 通知 Scheduler ──────────────────────────────
                {
                    MprpcChannel sched_channel;
                    SchedulerService_Stub sched_stub(&sched_channel);

                    NotifyWorkerOfflineRequest nw_req;
                    nw_req.set_worker_id(w.worker_id);
                    nw_req.set_reason("TIMEOUT");

                    NotifyWorkerOfflineResponse nw_resp;
                    MprpcController nw_ctrl;
                    nw_ctrl.SetTimeoutMs(5000);

                    sched_stub.NotifyWorkerOffline(&nw_ctrl, &nw_req, &nw_resp, nullptr);

                    if (!nw_ctrl.Failed() && nw_resp.error_code() == 0)
                    {
                        LOG_INFO("WorkerManager: NotifyWorkerOffline for %s: %d shards rescheduled",
                                 w.worker_id.c_str(), nw_resp.rescheduled_count());
                    }
                    else
                    {
                        LOG_WARN("WorkerManager: NotifyWorkerOffline RPC failed for %s: %s "
                                 "(will retry, fix #11)",
                                 w.worker_id.c_str(),
                                 nw_ctrl.Failed() ? nw_ctrl.ErrorText().c_str()
                                                  : nw_resp.error_msg().c_str());
                        // 阶段 8 #11：RPC 失败时加入待重试集合
                        std::lock_guard<std::mutex> lock(notify_mutex);
                        pending_notify.insert(w.worker_id);
                    }
                }
            }
        }

        // ── 2. 阶段 8 #11：重试之前失败的 NotifyWorkerOffline ─────
        {
            std::vector<std::string> to_retry;
            {
                std::lock_guard<std::mutex> lock(notify_mutex);
                to_retry.assign(pending_notify.begin(), pending_notify.end());
            }

            for (const auto& worker_id : to_retry)
            {
                // #5 修复：重试前复查 worker 存活状态。
                // 场景：网络分区 20s 被标 OFFLINE → 通知失败入集合 →
                // worker 心跳恢复（UpdateHeartbeat 置回 ONLINE）→ 若仍重发
                // NotifyWorkerOffline，Scheduler 会把存活 worker 正在执行的
                // shard 重置 WAITING 重新分配 → 双份转码。
                auto w_opt = WorkerStore::GetInstance().Get(worker_id);
                if (w_opt.has_value()
                    && w_opt->status == static_cast<int32_t>(WorkerStatus::WORKER_ONLINE))
                {
                    LOG_INFO("WorkerManager: worker %s back ONLINE, drop pending "
                             "NotifyWorkerOffline (fix #5)",
                             worker_id.c_str());
                    std::lock_guard<std::mutex> lock(notify_mutex);
                    pending_notify.erase(worker_id);
                    continue;
                }

                MprpcChannel sched_channel;
                SchedulerService_Stub sched_stub(&sched_channel);

                NotifyWorkerOfflineRequest nw_req;
                nw_req.set_worker_id(worker_id);
                nw_req.set_reason("TIMEOUT");

                NotifyWorkerOfflineResponse nw_resp;
                MprpcController nw_ctrl;
                nw_ctrl.SetTimeoutMs(5000);

                sched_stub.NotifyWorkerOffline(&nw_ctrl, &nw_req, &nw_resp, nullptr);

                if (!nw_ctrl.Failed() && nw_resp.error_code() == 0)
                {
                    LOG_INFO("WorkerManager: retry NotifyWorkerOffline SUCCESS for %s",
                             worker_id.c_str());
                    std::lock_guard<std::mutex> lock(notify_mutex);
                    pending_notify.erase(worker_id);
                }
                else
                {
                    LOG_WARN("WorkerManager: retry NotifyWorkerOffline still failed for %s: %s",
                             worker_id.c_str(),
                             nw_ctrl.Failed() ? nw_ctrl.ErrorText().c_str()
                                              : nw_resp.error_msg().c_str());
                }
            }
        }
    }

    LOG_INFO("HeartbeatTimeoutCheck thread stopped");
}

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief WorkerManagerService 启动流程（与 JobService 相同模式）
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

    if (!MysqlPool::GetInstance().Init())
    {
        LOG_ERROR("MysqlPool init failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // 阶段 10：Redis 是可降级组件，Init 失败只 WARN 不拒绝启动
    RedisClient::GetInstance().Init();

    // ── 阶段 11：可观测性（metrics_port<=0 时不启用，可降级组件） ──
    int metrics_port = MprpcApplication::GetConfig().LoadInt("metrics_port", 0, 0, 65535);
    mprpc::MetricsHttpServer metrics_server;
    metrics_server.Init(metrics_port);
    auto& metrics_reg = mprpc::MetricsRegistry::GetInstance();
    // 内置日志告警兜底：
    // 1. 本进程出站 RPC P99 延迟 > 1000ms
    // 2. 在线 Worker 数为 0（平台无法执行任何转码）
    metrics_reg.RegisterAlertRule(
        {"rpc_latency_p99_high", "WARN",
         []() {
             return mprpc::MetricsRegistry::GetInstance()
                 .HistogramQuantile("rpc_latency_ms", 0.99);
         },
         1000, true, 0, "本进程 RPC P99 延迟超过 1000ms"});
    metrics_reg.RegisterAlertRule(
        {"worker_offline", "ERROR",
         []() {
             return mprpc::MetricsRegistry::GetInstance()
                 .Gauge("worker_online", "在线 Worker 数").Value();
         },
         1, false, 0, "没有在线转码 Worker"});
    // Gauge 采样器：每 5s 扫描 WorkerStore 刷新在线数与各 Worker 负载
    metrics_reg.RegisterSampler([](mprpc::MetricsRegistry& r) {
        auto workers = WorkerStore::GetInstance().ListAll();
        if (workers.empty() && WorkerStore::GetInstance().Count() == 0)
        {
            // 查询失败：跳过本轮，保持旧值（避免 0 值尖刺误告警）
            return;
        }
        int online = 0;
        for (const auto& w : workers)
        {
            std::vector<mprpc::MetricLabel> lbl{{"worker_id", w.worker_id}};
            r.Gauge("worker_cpu_usage", "Worker CPU 使用率（%）", lbl)
                .Set(static_cast<double>(w.cpu_usage));
            r.Gauge("worker_memory_usage", "Worker 内存使用率（%）", lbl)
                .Set(static_cast<double>(w.memory_usage));
            r.Gauge("worker_running_shards", "Worker 正在执行的 shard 数", lbl)
                .Set(static_cast<double>(w.current_running_shards));
            if (w.status == static_cast<int32_t>(WorkerStatus::WORKER_ONLINE))
                ++online;
        }
        r.Gauge("worker_online", "在线 Worker 数").Set(static_cast<double>(online));
    }, 5000);
    metrics_server.Start();

    // 启动心跳超时检测后台线程
    std::atomic<bool> timeout_stopped{false};
    std::thread timeout_thread(HeartbeatTimeoutCheck, std::ref(timeout_stopped));

    RpcProvider provider;
    provider.NotifyService(new WorkerManagerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("WorkerManagerService start failed");
        timeout_stopped = true;
        if (timeout_thread.joinable()) timeout_thread.join();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    timeout_stopped = true;
    if (timeout_thread.joinable()) timeout_thread.join();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
