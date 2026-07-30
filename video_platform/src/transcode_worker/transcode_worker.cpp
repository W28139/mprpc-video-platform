#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <memory>
#include <random>
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
// WorkerService — TranscodeWorker 端 RPC 接口（阶段 5：故障注入 + 重试验证）
// ============================================================================
//
// TranscodeWorker 是整个调度链路中实际"干活"的节点。它同时作为 RPC Provider
// （接收 Scheduler 的 AssignShard/CancelShard 命令）和 RPC Consumer
// （向 WorkerManager 注册/心跳、向 ResultCollector 上报进度和结果）。
//
// 阶段 5 新增能力：
// 1. mock_fail_ratio 配置项：通过随机数模拟 shard 执行失败，用于测试重试机制
//    和故障恢复流程。0=全部成功，100=全部失败，中间值=按百分比随机失败。
// 2. 失败时携带 exit_code 和 error_msg 上报，让 ResultCollector 能区分
//    真实失败和网络故障，并触发 RescheduleShard 重试。
//
// 线程安全设计：
// - running_shards_ map 由 mutex_ 保护（std::mutex，非读写锁）
// - progress 和 cancelled 是 std::atomic，允许 MockExecute 线程写入、
//   QueryShard RPC 和心跳线程同时无锁读取
// - 检查+插入合并在同一锁区间内，消除阶段 4 发现的 TOCTOU 竞态窗口
// - 线程启动在锁外执行，避免与 CancelShard/CleanupShard 形成 AB-BA 死锁

/// @brief 运行中的 shard 追踪结构
///
/// 每个通过 AssignShard 接收到的 shard 都会创建一个 RunningShard 对象。
/// 生命周期由 shared_ptr 管理——AssignShard（IO 线程）和 MockExecute
///（执行线程）共享所有权。当双方都不再引用时，对象自动析构。
///
/// 字段说明：
/// - info: proto ShardInfo 的拷贝，包含 shard_id/job_id/shard_index/时间范围等
/// - attempt_id: 格式 "{shard_id}_attempt_{N}"，N 从 AssignShard 请求中的
///   retry_count 获取。Worker 用它来标识本次执行是第几次尝试，ResultCollector
///   用它来做新旧 attempt 的幂等校验
/// - progress: 0-100，MockExecute 每秒更新一次，QueryShard 和心跳线程无锁读取
/// - cancelled: CancelShard RPC 设置为 true，MockExecute 在下个 sleep 周期检测
/// - executor_thread: 持有 MockExecute 线程的句柄，CleanupShard 时 detach
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
///    - AssignShard：接收 shard → 校验无重复（同一锁内检查+插入）→ 锁外启动 mock 线程
///    - CancelShard：设置 cancelled 标志，mock 线程在下个 sleep 周期检测并退出
///    - QueryShard：返回 shard 实时 progress（0-100 或 -1=不存在）
///
/// 2. Consumer — 主动调用其他服务（不在本类中，由独立函数实现）：
///    - RunHeartbeatLoop：启动时 RegisterWorker + 周期 3s Heartbeat
///    - MockExecute：shard 执行线程内周期 ReportProgress + 最终 ReportResult
///
/// 线程模型（5 类线程同时运行）：
///   - main 线程：RpcProvider::Run() 阻塞等待连接
///   - IO 线程池（io_threads 个）：处理 AssignShard / CancelShard / QueryShard
///   - Work 线程池（work_threads 个）：处理 RPC 业务逻辑
///   - MockExecute 线程（每个 shard 一个）：sleep 模拟执行
///   - 心跳线程（1 个）：RunHeartbeatLoop
///   running_shards_ map 由 mutex_ 保护，所有跨线程访问都经过锁。
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
    ///
    /// 这是 Worker 被调度分配的入口。由 Scheduler 的 SchedulingLoop 后台线程
    /// 通过直连 RPC（MprpcChannel(ip, port)）调用，不经过 ZK 服务发现。
    ///
    /// 1. 从请求中提取 ShardInfo 和 retry_count，生成 attempt_id
    /// 2. 加锁检查 running_shards_ 中是否已存在同一 shard_id 且未被取消：
    ///    - 若存在 → 拒绝分配（返回 accepted=false），防止同一 shard 双倍执行
    ///    - 若不存在 → 创建 RunningShard 对象并插入 map（检查+插入在同一锁内）
    /// 3. 解锁，异步启动 MockExecute 线程（锁外启动，避免与 CancelShard 死锁）
    /// 4. 返回 accepted=true，Scheduler 将 shard 状态置为 ASSIGNED
    ///
    /// @note 本方法只负责接收和创建追踪，不阻塞等待执行完成。
    ///       实际转码（mock）在 MockExecute 线程中进行。
    ///
    /// @see MockExecute  🌐 异步 mock 执行线程
    /// @see CancelShard  🛑 取消正在执行的 shard
    /// @see CleanupShard 🧹 执行完成后清理追踪状态
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

        // 生成 attempt_id
        std::string attempt_id = shard.shard_id() + "_attempt_"
            + std::to_string(shard.retry_count());

        // 读取 mock 执行时长配置
        int mock_execution_ms = MprpcApplication::GetConfig().LoadInt(
            "mock_execution_time_ms", 5000, 500, 120000);

        std::shared_ptr<RunningShard> rs;

        // 检查 + 插入合并在同一锁内，消除 TOCTOU 窗口
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

            // 创建 RunningShard 追踪对象并插入 map（在同一临界区内）
            rs = std::make_shared<RunningShard>(shard, attempt_id);
            running_shards_[shard.shard_id()] = rs;
        }

        // 锁外启动 mock 执行线程，避免与 CancelShard/CleanupShard 死锁
        rs->executor_thread = std::thread(&WorkerServiceImpl::MockExecute,
                                          this, rs, mock_execution_ms);

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
    /// 阶段 5 增强：mock 随机失败注入
    /// - 通过配置文件 mock_fail_ratio (0-100) 控制失败概率
    /// - 使用 thread_local std::mt19937 保证线程安全且每次执行独立随机
    /// - 失败时设置 is_success=false, exit_code=-1，携带错误描述
    /// - ResultCollector 收到 FAILED 后会调用 Scheduler.RescheduleShard 触发重试
    ///
    /// 执行流程（阶段 4/5）：
    ///   1. 读取 mock 配置（执行时长 + 故障率）
    ///   2. FOR step = 1 .. total_steps:
    ///        sleep(1s)
    ///        IF cancelled → CleanupShard → RETURN
    ///        progress = step * 100 / total_steps
    ///        RPC: ResultCollector.ReportShardProgress(progress%, attempt_id)
    ///   3. 随机决定 mock_success（阶段 5 新增）
    ///   4. RPC: ResultCollector.ReportShardResult(is_success, exit_code, error_msg)
    ///      （带 3 次重试，防止网络抖动导致结果永久丢失）
    ///   5. 若 3 次上报均失败 → 保留在 running_shards_ 中，等待心跳恢复
    ///   6. CleanupShard
    ///
    /// 错误处理策略：
    /// - 进度上报失败：只打 WARN，不中断执行（下轮继续重试）
    /// - 结果上报成功但 is_success=false：由 ResultCollector → Scheduler 走重试链路
    /// - 结果上报 RPC 失败（3 次）：保留在 running_shards_，心跳线程可发现并重试
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
            // 检查 cancel 标志是否为 true(检测这个shark有没有被取消)
            if (rs->cancelled)
            {
                LOG_INFO("MockExecute: shard=%s cancelled at progress=%d%%",
                         shard_id.c_str(), rs->progress.load());
                CleanupShard(shard_id);     // 从 running_shards_ 中移除自己
                return;                     // 线程直接结束，不再继续执行
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

        // 执行完成，上报最终结果（带重试，防止网络抖动导致结果永久丢失）
        rs->progress = 100;

        // ── mock 随机失败注入（阶段 5） ──────────────────────────────
        // 设计思路：在 mock 执行完成（sleep 结束）后、上报 ResultCollector 之前，
        // 根据配置的概率随机决定本次执行是成功还是失败。这样在不引入真实 FFmpeg
        // 的前提下，就能完整测试 RescheduleShard → retry → max_retry → FAILED
        // → JOB_FAILED 的整个重试恢复链路。
        //
        // 配置：mock_fail_ratio=0（默认）表示全部成功，100 表示全部失败。
        // 测试建议：
        //   - 30~50：验证部分 shard 重试后最终成功
        //   - 100：验证超过 max_retry(3) 后 shard 变为 FAILED，job 变为 JOB_FAILED
        //
        // 线程安全：使用 thread_local mt19937，每个 MockExecute 线程有独立生成器。
        // 用 std::random_device 做种子（每次线程创建时初始化一次），避免伪随机序列
        // 在不同线程间共享导致可预测的失败模式。
        int mock_fail_ratio = MprpcApplication::GetConfig().LoadInt("mock_fail_ratio", 0, 0, 100);
        bool mock_success = true;
        std::string mock_error_msg;
        if (mock_fail_ratio > 0)
        {
            thread_local std::mt19937 rng(std::random_device{}());
            int roll = std::uniform_int_distribution<int>(0, 99)(rng);
            mock_success = (roll >= mock_fail_ratio);  // roll=0~99, fail_ratio=40 → roll<40 失败
            if (!mock_success)
            {
                mock_error_msg = "mock random failure (ratio="
                    + std::to_string(mock_fail_ratio) + "%, roll="
                    + std::to_string(roll) + ")";
            }
        }

        // ── 上报最终结果（带 3 次重试） ──────────────────────────
        // 网络抖动可能导致 ReportShardResult RPC 失败。若 3 次重试都失败，
        // 保留 shard 在 running_shards_ 中——心跳线程会持续上报 running_shards，
        // WorkerManager 能看到这个 shard，阶段 5 的心跳恢复机制可兜底。
        // 若 RPC 成功但 is_success=false，由 ResultCollector 走 RescheduleShard 链路。
        bool result_reported = false;
        for (int retry = 0; retry < 3 && !result_reported; ++retry)
        {
            if (retry > 0)
                std::this_thread::sleep_for(std::chrono::seconds(1));

            ReportShardResultRequest result_req;
            result_req.set_shard_id(shard_id);
            result_req.set_job_id(job_id);
            result_req.set_worker_id(worker_id);
            result_req.set_attempt_id(rs->attempt_id);
            result_req.set_is_success(mock_success);
            result_req.set_exit_code(mock_success ? 0 : -1);
            result_req.set_error_msg(mock_success ? "" : mock_error_msg);
            result_req.set_output_path(rs->info.output_path());
            result_req.set_elapsed_ms(mock_execution_ms);
            result_req.set_shard_index(rs->info.shard_index());

            ReportShardResultResponse result_resp;
            MprpcController ctrl;

            rc_stub.ReportShardResult(&ctrl, &result_req, &result_resp, nullptr);

            if (!ctrl.Failed() && result_resp.accepted())
            {
                LOG_INFO("MockExecute: shard=%s %s, job_done=%d (attempt %d)",
                         shard_id.c_str(),
                         mock_success ? "SUCCESS" : "FAILED (mock)",
                         result_resp.job_done(), retry + 1);
                result_reported = true;
            }
            else
            {
                LOG_ERROR("MockExecute: shard=%s result report failed (attempt %d/3): %s",
                          shard_id.c_str(), retry + 1,
                          ctrl.Failed() ? ctrl.ErrorText().c_str()
                                        : result_resp.error_msg().c_str());
            }
        }

        if (!result_reported)
        {
            LOG_ERROR("MockExecute: shard=%s result report FAILED after 3 retries, "
                      "keeping in running_shards for heartbeat-based recovery",
                      shard_id.c_str());
            // 不调用 CleanupShard — 保留在 running_shards_ 中，
            // 由心跳线程检测并重试上报（阶段 5 完善）
            return;
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

/// @brief Worker 注册与心跳后台线程（RegisterWorker + Heartbeat 循环）
///
/// Worker 作为 Consumer 主动与 WorkerManager 通信，维持自己的在线状态。
/// WorkerManager 通过心跳超时判断 Worker 离线，触发故障恢复链路。
///
/// === 流程 ===
/// 阶段一 — RegisterWorker（最多重试 3 次）：
///   读取本地 ip:port → 调用 WorkerManager.RegisterWorker(资源信息)
///   成功 → 进入阶段二；3 次全失败 → 线程退出（Worker 无法上线）
///
/// 阶段二 — Heartbeat 循环（每 3 秒）：
///   从 WorkerServiceImpl 获取真实 running_shards 数 →
///   填充负载信息 → 调用 WorkerManager.Heartbeat
///   alive=false 时打 WARN（WorkerManager 标记已死亡）
///
/// === 关键设计 ===
/// - RegisterWorker 带重试：解决 WorkerManager 未就绪的启动时序问题
/// - 上报真实 running_shards（非硬编码 0）：阶段 7 资源感知调度的基础
/// - 心跳是故障检测信号：心跳超时 → NotifyWorkerOffline → RescheduleShard
///
/// @param worker_id         Worker 唯一标识
/// @param cpu_cores,memory_mb,max_running_shards  注册时上报的资源信息
/// @param worker_service    用于获取真实 running_shards 数
/// @param stop_flag         主线程设置的停止标志
///
/// @see RunningShardCount  🔢 获取当前运行中的 shard 数量
/// @see RegisterWorker     📝 Worker 上线注册 RPC
/// @see Heartbeat          💓 Worker 心跳保活 RPC
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
