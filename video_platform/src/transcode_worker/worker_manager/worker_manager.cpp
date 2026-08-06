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

// WorkerManagerService — Worker 注册、心跳、查询

// 负责维护所有 Worker 节点的生命周期：
// - RegisterWorker：新 Worker 启动时调用，写入 WorkerStore
// - Heartbeat：Worker 周期性上报负载，更新 heartbeat 时间戳和运行状态
// - ListWorkers：让 Scheduler 查询可用的 ONLINE Worker

class WorkerManagerServiceImpl : public WorkerManagerService {
public:
    // Worker 注册上报
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

    // 每个Worker循环调用，作为心跳上报给manager
    // 1. 存活检测  Worker 每 3 秒调一次，WorkerManager 收到后更新 last_heartbeat 时间戳(心跳断了 20 秒，Worker 就被判定死亡)
    // 2. 负载上报,把work信息写入redis
    // 3. 通过response里的alive,清楚本worker在workermanager里的状态
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

        // 依据心跳，填入work最新信息，如果work死亡，那就返回false
        bool alive = WorkerStore::GetInstance().UpdateHeartbeat(
            load.worker_id(), load.running_shards(),
            load.cpu_usage(), load.memory_usage());

        // 心跳双写 Redis 负载快照
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

    // 列出 Worker 列表
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


// HeartbeatTimeoutCheck — 心跳超时检测线程
// 本线程周期扫描所有 ONLINE Worker 的 last_heartbeat，检测超时并触发恢复。
static void HeartbeatTimeoutCheck(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kHeartbeatTimeoutMs = 20000;  // 20 秒无心跳 → OFFLINE
    constexpr int64_t kCheckIntervalMs    = 2000;   // 每 2 秒扫描一次

    // NotifyWorkerOffline RPC 失败的 worker 需要持续重试
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

        // 检查 ONLINE Worker 是否超时
        for (const auto& w : workers)
        {
            if (w.status != static_cast<int32_t>(WorkerStatus::WORKER_ONLINE))
                continue;

            if (now - w.last_heartbeat <= kHeartbeatTimeoutMs)
                continue;

            // 外层快照判断超时，这里直接查数据库进行验证，判断是否真超时
            if (WorkerStore::GetInstance().MarkOfflineIfTimeout(
                    w.worker_id, now, kHeartbeatTimeoutMs))
            {
                LOG_WARN("WorkerManager: worker_id=%s marked OFFLINE (heartbeat timeout, "
                         "last=%lld, now=%lld, gap=%lldms)",
                         w.worker_id.c_str(),
                         (long long)w.last_heartbeat,
                         (long long)now,
                         (long long)(now - w.last_heartbeat));

                // 通知 Scheduler，该worker已死亡，需要重新处理该worker上的shard
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
                        // RPC 失败时加入待重试集合
                        std::lock_guard<std::mutex> lock(notify_mutex);
                        pending_notify.insert(w.worker_id);
                    }
                }
            }
        }

        // 重试之前失败的 NotifyWorkerOffline（增加代码健壮性）
        {
            std::vector<std::string> to_retry;
            {
                std::lock_guard<std::mutex> lock(notify_mutex);
                to_retry.assign(pending_notify.begin(), pending_notify.end());
            }

            for (const auto& worker_id : to_retry)
            {
                // 重试前复查 worker 存活状态
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
                // 这次是真死了，而且RPC还失败了，那就再试一次
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
                // 还是失败,那就继续在pending_notify里等待下层次RPC调用，直到成功
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
