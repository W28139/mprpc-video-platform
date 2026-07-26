#include <string>
#include <cstdlib>
#include "worker.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"

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
/// 阶段 3 将增加心跳超时检测（定时扫描 last_heartbeat，超时标 OFFLINE）。
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
        worker.current_running_shards = 0;    // 刚注册时没有正在执行的任务
        worker.status                 = static_cast<int32_t>(WorkerStatus::WORKER_ONLINE);
        worker.last_heartbeat         = NowMs();

        WorkerStore::GetInstance().Insert(worker);

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_registered(true);
        done->Run();
    }

    /// @brief Worker 心跳上报
    ///
    /// Worker 每 3 秒调用一次，携带 WorkerLoad（cpu_usage / memory_usage /
    /// running_shards / finished_shards / failed_shards）。
    /// 更新 WorkerRecord 的 last_heartbeat 和负载快照。
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

        WorkerRecord* worker = WorkerStore::GetInstance().Get(load.worker_id());
        if (worker != nullptr)
        {
            // 更新实时负载与心跳时间
            worker->current_running_shards = load.running_shards();
            worker->last_heartbeat         = NowMs();
            worker->status                 = static_cast<int32_t>(WorkerStatus::WORKER_ONLINE);
            WorkerStore::GetInstance().Update(load.worker_id(), *worker);
        }
        // 若 worker 不存在（未注册或已下线），仍返回 alive=false 语义留后续处理
        // 当前阶段不因心跳失败而影响客户端

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_alive(worker != nullptr);
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
        }

        done->Run();
    }
};

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

    RpcProvider provider;
    provider.NotifyService(new WorkerManagerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("WorkerManagerService start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
