#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <memory>
#include <random>
#include <fstream>
#include <sstream>
#include <set>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include "worker.pb.h"
#include "result.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "mprpcmetrics.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"
#include "video_platform/ffmpeg_executor.h"
#include "video_platform/mq_client.h"

using namespace video_platform;

// 前向声明（阶段 7：CPU/内存采集函数，定义在 RunHeartbeatLoop 之前）
static int CollectCpuUsage();
static int CollectMemUsage();

/// @brief 尝试通过 MQ 上报 shard 执行结果（阶段 10：MQ 优先，回退直连 RPC）。
///
/// publish result.pending（消息体 = 序列化的 ReportShardResultRequest），
/// ResultCollector 的 MQ 消费线程接收后走与 RPC 完全相同的聚合逻辑。
/// 失败（MQ 不可用/掉线/序列化异常）返回 false，调用方回退原 RPC 路径。
/// 每次上报独立判断：MQ 恢复后自动切回，无需状态机。
static bool TryPublishResultToMq(const ::ReportShardResultRequest& result_req)
{
    auto& mq = MqClient::GetInstance();
    if (!mq.inited() || !mq.enabled()) return false;
    std::string payload;
    if (!result_req.SerializeToString(&payload)) return false;
    if (mq.PublishResult(payload)) return true;
    LOG_WARN("TryPublishResultToMq: publish failed, fallback to direct RPC");
    return false;
}

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

/// @brief 待重试上报的结果（修复 #6：上报失败后心跳兜底）
struct PendingReport {
    std::string shard_id;
    std::string job_id;
    std::string worker_id;
    std::string attempt_id;
    bool        is_success;
    int         exit_code;
    std::string error_msg;
    std::string output_path;
    int64_t     elapsed_ms;
    int32_t     shard_index;
    std::string screenshot_path;
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

        // 读取 mock 执行时长配置（mock 模式用）
        int mock_execution_ms = MprpcApplication::GetConfig().LoadInt(
            "mock_execution_time_ms", 5000, 500, 120000);

        // ── 按 executor_mode 选择执行器 ──────────────────────────
        // "ffmpeg" = 真实 ffmpeg 转码（阶段 6）
        // "mock" 或其他值 = sleep 模拟（阶段 4/5）
        std::string executor_mode = MprpcApplication::GetConfig().Load("executor_mode");

        // ── 阶段 7：Worker 过载保护 ────────────────────────────────
        // Worker 负载过高时拒绝新 shard，由 Scheduler 在下一轮分配给其他 Worker
        int current_cpu = CollectCpuUsage();
        {
            std::lock_guard<std::mutex> lock(mutex_);
            int running = 0;
            for (const auto& kv : running_shards_) {
                if (!kv.second->cancelled) ++running;
            }
            int max_shards = MprpcApplication::GetConfig().LoadInt(
                "max_running_shards", 2, 1, 1024);
            if (running >= max_shards || current_cpu > 90)
            {
                LOG_WARN("WorkerService::AssignShard shard %s rejected: overloaded "
                         "(running=%d/%d, cpu=%d%%)",
                         shard.shard_id().c_str(), running, max_shards, current_cpu);
                response->set_error_code(2);
                response->set_error_msg("worker overloaded");
                response->set_accepted(false);
                done->Run();
                return;
            }
        }

        std::shared_ptr<RunningShard> rs;

        // 检查 + 插入合并在同一锁内，消除 TOCTOU 窗口
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = running_shards_.find(shard.shard_id());
            // #1 修复：只要 map 中还存在该 shard 的条目（无论是否已取消）就拒绝。
            // 此前「已取消也放行」允许同一 worker 同时存在新旧 attempt 的两个
            // 执行线程：旧线程 CleanupShard 会按 shard_id 移除 map 中的新条目
            // 并 detach 新执行线程，造成执行线程生命周期混乱甚至进程崩溃。
            if (it != running_shards_.end())
            {
                LOG_WARN("WorkerService::AssignShard shard %s already has an active "
                         "execution (attempt=%s, cancelled=%d), rejecting",
                         shard.shard_id().c_str(), it->second->attempt_id.c_str(),
                         (int)it->second->cancelled.load());
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

        // 锁外启动执行线程，避免与 CancelShard/CleanupShard 死锁
        if (executor_mode == "ffmpeg")
        {
            rs->executor_thread = std::thread(&WorkerServiceImpl::FfmpegExecute,
                                              this, rs);
            LOG_INFO("WorkerService::AssignShard shard=%s attempt=%s started (ffmpeg mode)",
                     shard.shard_id().c_str(), attempt_id.c_str());
        }
        else
        {
            rs->executor_thread = std::thread(&WorkerServiceImpl::MockExecute,
                                              this, rs, mock_execution_ms);
            LOG_INFO("WorkerService::AssignShard shard=%s attempt=%s started (mock_exec=%dms)",
                     shard.shard_id().c_str(), attempt_id.c_str(), mock_execution_ms);
        }

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
            // #1 修复：attempt_id 精确匹配。重扫取消通知可能与该 shard 的
            // 重新分配竞态——shard 已被重新分配为新 attempt 时（旧执行线程
            // 尚未清理、map 条目已被新 RunningShard 替换），旧 attempt 的
            // CancelShard 不得误伤新执行。请求带 attempt_id 时仅匹配当前
            // 执行的 attempt；不匹配则视为已进入新 attempt，拒绝取消。
            if (!request->attempt_id().empty()
                && it->second->attempt_id != request->attempt_id())
            {
                response->set_error_code(0);
                response->set_error_msg("shard moved to new attempt, cancel ignored");
                response->set_canceled(false);
                LOG_INFO("WorkerService::CancelShard shard=%s attempt=%s ignored "
                         "(current attempt=%s)",
                         request->shard_id().c_str(),
                         request->attempt_id().c_str(),
                         it->second->attempt_id.c_str());
                done->Run();
                return;
            }
            it->second->cancelled = true;
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_canceled(true);
            LOG_INFO("WorkerService::CancelShard shard=%s attempt=%s cancelled",
                     request->shard_id().c_str(), it->second->attempt_id.c_str());
        }
        else
        {
            response->set_error_code(1);
            response->set_error_msg("shard not found: " + request->shard_id());
            response->set_canceled(false);
        }
        done->Run();
    }

    /// @brief 查询 shard 执行进度(分片级：1 个 shard 的实时进度),progress 0-100 实时百分比,毫秒级实时
    // QueryJob（JobService :9001): 任务级：1 个 job 全貌 + 所有 shard 的状态列表
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
            // 修复 #29：不用 std::random_device（WSL/容器熵不足时阻塞），
            // 改用 steady_clock + thread_id hash 播种（与本仓库 common_store.cpp 准则一致）
            thread_local std::mt19937 rng(
                static_cast<uint64_t>(
                    std::chrono::steady_clock::now().time_since_epoch().count()) +
                std::hash<std::thread::id>{}(std::this_thread::get_id()));
            int roll = std::uniform_int_distribution<int>(0, 99)(rng);
            mock_success = (roll >= mock_fail_ratio);  // roll=0~99, fail_ratio=40 → roll<40 失败
            if (!mock_success)
            {
                mock_error_msg = "mock random failure (ratio="
                    + std::to_string(mock_fail_ratio) + "%, roll="
                    + std::to_string(roll) + ")";
            }
        }

        // ── 上报最终结果（MQ 优先，回退直连 RPC 带 3 次重试） ──────
        // 网络抖动可能导致 ReportShardResult RPC 失败。若 3 次重试都失败，
        // 保留 shard 在 running_shards_ 中——心跳线程会持续上报 running_shards，
        // WorkerManager 能看到这个 shard，阶段 5 的心跳恢复机制可兜底。
        // 若上报成功但 is_success=false，由 ResultCollector 走 RescheduleShard 链路。
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

        bool result_reported = false;
        // 阶段 10：MQ 优先（publish 成功即已投递，RC 消费后走同一聚合逻辑）
        if (TryPublishResultToMq(result_req))
        {
            LOG_INFO("MockExecute: shard=%s %s published to MQ (result.pending)",
                     shard_id.c_str(), mock_success ? "SUCCESS" : "FAILED (mock)");
            result_reported = true;
        }
        for (int retry = 0; retry < 3 && !result_reported; ++retry)
        {
            if (retry > 0)
                std::this_thread::sleep_for(std::chrono::seconds(1));

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
                      "queuing for heartbeat retry (fix #6)",
                      shard_id.c_str());
            // 阶段 8 修复 #6：结果暂存到待重试队列，心跳线程定期重试上报
            PendingReport pr;
            pr.shard_id    = shard_id;
            pr.job_id      = job_id;
            pr.worker_id   = worker_id;
            pr.attempt_id  = rs->attempt_id;
            pr.is_success  = mock_success;
            pr.exit_code   = mock_success ? 0 : -1;
            pr.error_msg   = mock_success ? "" : mock_error_msg;
            pr.output_path = rs->info.output_path();
            pr.elapsed_ms  = mock_execution_ms;
            pr.shard_index = rs->info.shard_index();
            QueuePendingReport(pr);
        }

        CleanupShard(shard_id);
    }

    // ── FfmpegExecute：真实 FFmpeg 执行器（阶段 6） ──────────────────────

    /// @brief FFmpeg 执行器：调用真实 ffmpeg 命令执行视频切片+转码
    ///
    /// 与 MockExecute 共用相同的进度上报/结果上报/cancel 检测架构。
    /// 区别在于执行环节：不再 sleep，而是启动 ffmpeg 子进程，
    /// 通过解析 ffmpeg stderr 的 time= 输出获取真实转码进度。
    ///
    /// 工作目录结构：
    ///   {ffmpeg_work_dir}/{shard_id}/
    ///     ├── slice_input.mp4    ← 切片结果（如果有切片步骤）
    ///     └── transcode_out.mp4  ← 转码输出
    ///
    /// 子进程管理：
    ///   使用 fork() + execvp() + pipe() 启动 ffmpeg，记录子进程 PID。
    ///   进度回调中检测 rs->cancelled → kill(pid, SIGTERM) 终止子进程。
    ///   父进程在 waitpid() 后获取退出码。
    ///
    /// @see FfmpegExecutor    封装 ffmpeg 命令行调用
    /// @see MockExecute       对应的 mock 实现
    /// @see CleanupShard      执行完成后清理追踪状态
    void FfmpegExecute(std::shared_ptr<RunningShard> rs)
    {
        const std::string& shard_id = rs->info.shard_id();
        const std::string& job_id   = rs->info.job_id();
        std::string worker_id = MprpcApplication::GetConfig().Load("worker_id");

        // 读取 FFmpeg 相关配置
        std::string work_dir = MprpcApplication::GetConfig().Load("ffmpeg_work_dir");
        if (work_dir.empty()) work_dir = "/tmp/transcode_worker";

        LOG_INFO("FfmpegExecute: shard=%s started, work_dir=%s",
                 shard_id.c_str(), work_dir.c_str());

        // ── 1. 创建工作目录 ──────────────────────────────────────────
        std::string shard_work_dir = work_dir + "/" + shard_id;
        mkdir(shard_work_dir.c_str(), 0755);  // 忽略 EEXIST（目录已存在）

        // 确定输入和输出路径
        std::string input_path = rs->info.input_path();
        std::string output_path = rs->info.output_path();
        if (output_path.empty())
        {
            output_path = shard_work_dir + "/output.mp4";
        }

        int64_t start_ms = rs->info.start_ms();
        int64_t duration_ms = rs->info.duration_ms();

        LOG_INFO("FfmpegExecute: shard=%s input=%s, output=%s, start=%lldms, dur=%lldms",
                 shard_id.c_str(), input_path.c_str(), output_path.c_str(),
                 (long long)start_ms, (long long)duration_ms);

        // ── 2. 转码（切片+转码合一，-ss/-t 直接传给 ffmpeg） ──────
        // 转码参数从 ShardInfo 透传，不再读本地配置
        std::string target_resolution = rs->info.target_resolution();
        int target_bitrate = rs->info.target_bitrate();

        // ResultCollector stub（用于进度上报）
        MprpcChannel rc_channel;
        ResultCollectorService_Stub rc_stub(&rc_channel);

        // 进度回调：更新 rs->progress 并上报 ResultCollector
        auto progress_cb = [&](int progress) {
            if (progress < 0) return;
            if (progress > 100) progress = 100;

            rs->progress = progress;

            if (rs->cancelled)
            {
                LOG_INFO("FfmpegExecute: shard=%s cancelled at progress=%d%%",
                         shard_id.c_str(), progress);
                return;
            }

            ReportShardProgressRequest prog_req;
            prog_req.set_shard_id(shard_id);
            prog_req.set_job_id(job_id);
            prog_req.set_worker_id(worker_id);
            prog_req.set_attempt_id(rs->attempt_id);
            prog_req.set_progress(progress);

            ReportShardProgressResponse prog_resp;
            MprpcController ctrl;
            rc_stub.ReportShardProgress(&ctrl, &prog_req, &prog_resp, nullptr);

            if (ctrl.Failed() || !prog_resp.recorded())
            {
                LOG_WARN("FfmpegExecute: shard=%s progress=%d%% report failed: %s",
                         shard_id.c_str(), progress,
                         ctrl.Failed() ? ctrl.ErrorText().c_str() : prog_resp.error_msg().c_str());
            }
        };

        LOG_INFO("FfmpegExecute: transcoding shard=%s, input=%s, output=%s, "
                 "start=%lldms, dur=%lldms, resolution=%s, bitrate=%d",
                 shard_id.c_str(), input_path.c_str(), output_path.c_str(),
                 (long long)start_ms, (long long)duration_ms,
                 target_resolution.c_str(), target_bitrate);

        auto ts_start = std::chrono::steady_clock::now();
        // cancel 检查回调：检测 rs->cancelled 标志，触发时 fork+exec 子进程被 kill
        auto cancel_cb = [&rs]() -> bool { return rs->cancelled.load(); };

        auto transcode_result = FfmpegExecutor::Transcode(
            input_path, output_path,
            target_resolution, target_bitrate,
            start_ms, duration_ms,
            progress_cb, cancel_cb);
        auto ts_end = std::chrono::steady_clock::now();
        int64_t transcode_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            ts_end - ts_start).count();

        // 阶段 11：转码耗时观测（cancel 的耗时同样有效，计入直方图）
        mprpc::MetricsRegistry::GetInstance()
            .Histogram("transcode_duration_ms", "单 shard 转码耗时（毫秒）",
                       std::vector<double>{100, 500, 1000, 2000, 5000, 10000,
                                           30000, 60000, 120000, 300000})
            .Observe(static_cast<double>(transcode_elapsed));

        // ── 4. 处理 cancel 情况 ──────────────────────────────────────
        if (rs->cancelled)
        {
            LOG_INFO("FfmpegExecute: shard=%s cancelled during transcode", shard_id.c_str());
            CleanupShard(shard_id);
            return;
        }

        // ── 4.5 截图：转码成功后截取视频中点帧（阶段 6 遗留项 5） ──
        std::string screenshot_path;
        if (transcode_result.success)
        {
            int64_t midpoint_ms = start_ms + duration_ms / 2;
            screenshot_path = shard_work_dir + "/screenshot.jpg";
            auto ss_result = FfmpegExecutor::Screenshot(input_path, midpoint_ms,
                                                         screenshot_path);
            if (ss_result.success)
            {
                LOG_INFO("FfmpegExecute: shard=%s screenshot saved to %s",
                         shard_id.c_str(), screenshot_path.c_str());
            }
            else
            {
                LOG_WARN("FfmpegExecute: shard=%s screenshot failed: %s",
                         shard_id.c_str(), ss_result.error_msg.c_str());
                screenshot_path.clear();  // 失败时清空，不上报无效路径
            }
        }

        // ── 4.6 再次检查 cancel：截图/重试睡眠期间可能收到取消（修复 #19） ──
        if (rs->cancelled)
        {
            LOG_INFO("FfmpegExecute: shard=%s cancelled after transcode/screenshot", shard_id.c_str());
            CleanupShard(shard_id);
            return;
        }

        // ── 5. 上报最终结果（带重试，与 MockExecute 相同逻辑） ────────
        bool is_success = transcode_result.success;
        int exit_code = transcode_result.exit_code;
        std::string error_msg = transcode_result.error_msg;

        rs->progress = 100;
        int64_t total_elapsed = transcode_elapsed;

        // 构造结果请求（MQ 与 RPC 共用，阶段 10）
        ReportShardResultRequest result_req;
        result_req.set_shard_id(shard_id);
        result_req.set_job_id(job_id);
        result_req.set_worker_id(worker_id);
        result_req.set_attempt_id(rs->attempt_id);
        result_req.set_is_success(is_success);
        result_req.set_exit_code(exit_code);
        result_req.set_error_msg(error_msg);
        result_req.set_output_path(output_path);
        result_req.set_elapsed_ms(total_elapsed);
        result_req.set_shard_index(rs->info.shard_index());
        result_req.set_screenshot_path(screenshot_path);

        bool result_reported = false;
        // 阶段 10：MQ 优先（publish 成功即已投递，RC 消费后走同一聚合逻辑）
        if (TryPublishResultToMq(result_req))
        {
            LOG_INFO("FfmpegExecute: shard=%s %s published to MQ (result.pending)",
                     shard_id.c_str(), is_success ? "SUCCESS" : "FAILED");
            result_reported = true;
        }
        for (int retry = 0; retry < 3 && !result_reported; ++retry)
        {
            if (retry > 0)
                std::this_thread::sleep_for(std::chrono::seconds(1));

            ReportShardResultResponse result_resp;
            MprpcController ctrl;
            rc_stub.ReportShardResult(&ctrl, &result_req, &result_resp, nullptr);

            if (!ctrl.Failed() && result_resp.accepted())
            {
                LOG_INFO("FfmpegExecute: shard=%s %s (exit=%d, elapsed=%lldms, attempt %d)",
                         shard_id.c_str(),
                         is_success ? "SUCCESS" : "FAILED",
                         exit_code,
                         (long long)total_elapsed,
                         retry + 1);
                result_reported = true;
            }
            else
            {
                LOG_ERROR("FfmpegExecute: shard=%s result report failed (attempt %d/3): %s",
                          shard_id.c_str(), retry + 1,
                          ctrl.Failed() ? ctrl.ErrorText().c_str()
                                        : result_resp.error_msg().c_str());
            }
        }

        if (!result_reported)
        {
            LOG_ERROR("FfmpegExecute: shard=%s result report FAILED after 3 retries, "
                      "queuing for heartbeat retry (fix #6)",
                      shard_id.c_str());
            // 阶段 8 修复 #6：将结果暂存到待重试队列，心跳线程定期重试上报
            // CleanupShard 照常执行——释放 running_shards_ 槽位，避免楔死 Worker
            PendingReport pr;
            pr.shard_id        = shard_id;
            pr.job_id          = job_id;
            pr.worker_id       = worker_id;
            pr.attempt_id      = rs->attempt_id;
            pr.is_success      = is_success;
            pr.exit_code       = exit_code;
            pr.error_msg       = error_msg;
            pr.output_path     = output_path;
            pr.elapsed_ms      = total_elapsed;
            pr.shard_index     = rs->info.shard_index();
            pr.screenshot_path = screenshot_path;
            QueuePendingReport(pr);
        }

        CleanupShard(shard_id);
    }

    /// @brief 辅助方法：上报 shard 执行结果
    ///
    /// 封装 ReportShardResult RPC 调用，供 FfmpegExecute 在切片失败时直接上报。
    /// 单次调用，不带重试（调用方负责重试逻辑）。
    void ReportShardResult(const std::string& shard_id,
                           const std::string& job_id,
                           const std::string& worker_id,
                           const std::string& attempt_id,
                           bool is_success,
                           int exit_code,
                           const std::string& error_msg,
                           const std::string& output_path,
                           int64_t elapsed_ms,
                           int32_t shard_index)
    {
        MprpcChannel rc_channel;
        ResultCollectorService_Stub rc_stub(&rc_channel);

        ReportShardResultRequest result_req;
        result_req.set_shard_id(shard_id);
        result_req.set_job_id(job_id);
        result_req.set_worker_id(worker_id);
        result_req.set_attempt_id(attempt_id);
        result_req.set_is_success(is_success);
        result_req.set_exit_code(exit_code);
        result_req.set_error_msg(error_msg);
        result_req.set_output_path(output_path);
        result_req.set_elapsed_ms(elapsed_ms);
        result_req.set_shard_index(shard_index);

        // 阶段 10：MQ 优先（publish 成功即已投递；失败回退直连 RPC）
        if (TryPublishResultToMq(result_req))
        {
            LOG_INFO("ReportShardResult: shard=%s %s published to MQ",
                     shard_id.c_str(), is_success ? "SUCCESS" : "FAILED");
            return;
        }

        ReportShardResultResponse result_resp;
        MprpcController ctrl;
        rc_stub.ReportShardResult(&ctrl, &result_req, &result_resp, nullptr);

        if (!ctrl.Failed() && result_resp.accepted())
        {
            LOG_INFO("ReportShardResult: shard=%s %s",
                     shard_id.c_str(), is_success ? "SUCCESS" : "FAILED");
        }
        else
        {
            LOG_ERROR("ReportShardResult: shard=%s report FAILED: %s",
                      shard_id.c_str(),
                      ctrl.Failed() ? ctrl.ErrorText().c_str() : result_resp.error_msg().c_str());
        }
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

    // 阶段 8 修复 #6：上报失败的结果暂存队列，由心跳线程定期重试
    mutable std::mutex pending_mutex_;
    std::vector<PendingReport> pending_reports_;

public:
    /// @brief 把上报失败的结果压入待重试队列（#18 收敛：两处入队统一，
    ///        避免后续新增字段时漏拷——历史上 max_retry 丢字段正是此类）
    void QueuePendingReport(const PendingReport& pr)
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_reports_.push_back(pr);
    }

    /// @brief 心跳线程调用：重试所有待上报的 shard 结果（修复 #6）
    /// @return 成功上报并从队列中移除的数量
    int RetryPendingReports()
    {
        std::vector<PendingReport> batch;
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            batch.swap(pending_reports_);  // 取出全部，减少锁持有时间
        }

        if (batch.empty()) return 0;

        MprpcChannel rc_channel;
        ResultCollectorService_Stub rc_stub(&rc_channel);
        int succeeded = 0;

        for (auto& pr : batch)
        {
            ReportShardResultRequest req;
            req.set_shard_id(pr.shard_id);
            req.set_job_id(pr.job_id);
            req.set_worker_id(pr.worker_id);
            req.set_attempt_id(pr.attempt_id);
            req.set_is_success(pr.is_success);
            req.set_exit_code(pr.exit_code);
            req.set_error_msg(pr.error_msg);
            req.set_output_path(pr.output_path);
            req.set_elapsed_ms(pr.elapsed_ms);
            req.set_shard_index(pr.shard_index);
            req.set_screenshot_path(pr.screenshot_path);

            ReportShardResultResponse resp;
            MprpcController ctrl;
            ctrl.SetTimeoutMs(5000);  // #14：显式超时，避免无限阻塞心跳线程
            rc_stub.ReportShardResult(&ctrl, &req, &resp, nullptr);

            if (!ctrl.Failed() && resp.accepted())
            {
                ++succeeded;
                LOG_INFO("RetryPendingReports: shard=%s report SUCCESS via heartbeat retry",
                         pr.shard_id.c_str());
            }
            else if (!ctrl.Failed())
            {
                // #14 修复：确定性拒绝（accepted=false，如 stale attempt——
                // shard 已被重调度到更高 attempt，旧结果永远不可能被接受）
                // 直接丢弃，此前无差别重入队导致 3s 间隔无限重试 + WARN 刷屏。
                LOG_WARN("RetryPendingReports: shard=%s deterministically rejected, "
                         "dropping entry: %s", pr.shard_id.c_str(),
                         resp.error_msg().c_str());
            }
            else
            {
                // 仅网络失败才放回队列等待下次心跳
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_reports_.push_back(pr);
                LOG_WARN("RetryPendingReports: shard=%s still failed (network): %s",
                         pr.shard_id.c_str(), ctrl.ErrorText().c_str());
            }
        }

        if (succeeded > 0)
            LOG_INFO("RetryPendingReports: %d reports succeeded via heartbeat, %zu still pending",
                     succeeded, pending_reports_.size());
        return succeeded;
    }
};

// ============================================================================
// 系统资源采集（阶段 7：资源感知调度）
// ============================================================================

/// @brief 聚合「本进程 + 所有存活直接子进程」的 CPU ticks（#15 修复）
///
/// 原实现读 /proc/self/stat 的 cutime/cstime——它们只统计已被 waitpid
/// 收割的已终止子进程，运行中的 ffmpeg 子进程不计入 → 转码进行中心跳
/// 上报 cpu_usage≈0，Worker 过载门与 Scheduler 加权评分同时失效。
/// 改为直接聚合：自身 utime+stime + /proc/self/task/*/children 列出的
/// 存活子进程的 utime+stime（ffmpeg 基本不再 fork，一层足够）。
static uint64_t CollectTreeCpuTicks()
{
    uint64_t ticks = 0;

    // 1. 自身 utime/stime（字段 14/15）
    {
        std::ifstream stat_file("/proc/self/stat");
        if (!stat_file.is_open()) return 0;
        std::string line;
        std::getline(stat_file, line);
        size_t comm_end = line.rfind(')');
        if (comm_end == std::string::npos) return 0;
        std::istringstream iss(line.substr(comm_end + 2));
        std::string field;
        for (int i = 3; i <= 15; ++i)
        {
            if (!(iss >> field)) break;
            if (i == 14) ticks += std::stoull(field);
            if (i == 15) ticks += std::stoull(field);
        }
    }

    // 2. 存活直接子进程（所有线程的 children 并集去重）
    std::set<pid_t> children;
    DIR* task_dir = opendir("/proc/self/task");
    if (task_dir)
    {
        struct dirent* ent;
        while ((ent = readdir(task_dir)) != nullptr)
        {
            if (ent->d_name[0] == '.') continue;
            std::string children_path = std::string("/proc/self/task/")
                                      + ent->d_name + "/children";
            std::ifstream cf(children_path);
            if (!cf.is_open()) continue;
            pid_t pid;
            while (cf >> pid) children.insert(pid);
        }
        closedir(task_dir);
    }

    for (pid_t pid : children)
    {
        std::ifstream stat_file("/proc/" + std::to_string(pid) + "/stat");
        if (!stat_file.is_open()) continue;  // 子进程已退出
        std::string line;
        std::getline(stat_file, line);
        size_t comm_end = line.rfind(')');
        if (comm_end == std::string::npos) continue;
        std::istringstream iss(line.substr(comm_end + 2));
        std::string field;
        for (int i = 3; i <= 15; ++i)
        {
            if (!(iss >> field)) break;
            if (i == 14) ticks += std::stoull(field);
            if (i == 15) ticks += std::stoull(field);
        }
    }
    return ticks;
}

/// @brief 从 /proc/self/stat 读取本进程 CPU 时间并计算使用率（修复 #15）
///
/// 阶段 8 修复：原实现读 /proc/stat 第一行（主机全核 CPU），在单机多 Worker
/// 部署下邻居 Worker 的繁忙会使空闲 Worker 也报高 CPU → 过载门反向工作。
/// 改为 /proc/self/stat（进程 CPU），正确反映本 Worker 进程的 CPU 占用。
///
/// 线程安全：mutex 保护采样状态（替代 thread_local，消除首次返回 0 的窗口）。
///
/// @return CPU 使用率 0-100
static int CollectCpuUsage()
{
    static std::mutex cpu_mutex;
    static uint64_t prev_ticks = 0;
    static int64_t  prev_time_ms = 0;
    static bool     first_call = true;

    std::lock_guard<std::mutex> lock(cpu_mutex);

    // #15 修复：聚合自身 + 存活子进程（cutime/cstime 只计已收割子进程，
    // 转码中恒 ~0）
    uint64_t total_ticks = CollectTreeCpuTicks();
    int64_t now_ms = NowMs();

    int usage = 0;
    if (!first_call && prev_time_ms > 0)
    {
        int64_t elapsed_ms = now_ms - prev_time_ms;
        // ticks 是 USER_HZ (通常 100)，elapsed_ms 是毫秒
        // CPU% = (delta_ticks * 1000 / HZ) * 100 / elapsed_ms
        //      = delta_ticks * 100000 / HZ / elapsed_ms
        if (elapsed_ms > 0)
        {
            uint64_t delta_ticks = total_ticks - prev_ticks;
            long sys_hz = sysconf(_SC_CLK_TCK);
            if (sys_hz <= 0) sys_hz = 100;
            usage = static_cast<int>(delta_ticks * 100000 / static_cast<uint64_t>(sys_hz)
                                     / static_cast<uint64_t>(elapsed_ms));
            // 在多核机器上可能 >100（进程用满一核以上），clamp 到 [0,100] 用于过载判断
            if (usage > 100) usage = 100;
            if (usage < 0)   usage = 0;
        }
    }

    prev_ticks  = total_ticks;
    prev_time_ms = now_ms;
    first_call   = false;
    return usage;
}

/// @brief 从 /proc/self/status 读取本进程内存使用率（修复 #15）
///
/// 原实现读 /proc/meminfo（主机级内存），改为读本进程 VmRSS，
/// 与主机总内存对比得到进程维度的内存使用率。
static int CollectMemUsage()
{
    static std::once_flag memtotal_init;
    static uint64_t mem_total_kb = 0;

    // 延迟初始化：读取一次主机总内存（所有 Worker 共享，只需一次）
    std::call_once(memtotal_init, []() {
        std::ifstream mt("/proc/meminfo");
        if (mt.is_open())
        {
            std::string line;
            while (std::getline(mt, line))
            {
                if (line.find("MemTotal:") == 0)
                {
                    std::istringstream iss(line);
                    std::string label;
                    iss >> label >> mem_total_kb;
                    break;
                }
            }
        }
    });

    // 读本进程 VmRSS
    std::ifstream status_file("/proc/self/status");
    if (!status_file.is_open()) return 0;

    uint64_t vm_rss_kb = 0;
    std::string line;
    while (std::getline(status_file, line))
    {
        if (line.find("VmRSS:") == 0)
        {
            std::istringstream iss(line);
            std::string label;
            iss >> label >> vm_rss_kb;
            break;
        }
    }

    if (mem_total_kb == 0 || vm_rss_kb == 0) return 0;
    return static_cast<int>(vm_rss_kb * 100 / mem_total_kb);
}

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
        int cpu    = CollectCpuUsage();
        int mem    = CollectMemUsage();

        HeartbeatRequest hb_req;
        auto* load = hb_req.mutable_load();
        load->set_worker_id(worker_id);
        load->set_cpu_usage(cpu);        // 阶段 7：真实 CPU 采集
        load->set_memory_usage(mem);     // 阶段 7：真实内存采集
        load->set_running_shards(running);
        load->set_finished_shards(0);
        load->set_failed_shards(0);
        load->set_timestamp(NowMs());

        HeartbeatResponse hb_resp;
        MprpcController controller;

        // 向workmanager发送心跳
        stub.Heartbeat(&controller, &hb_req, &hb_resp, nullptr);
        // 心跳成功
        if (!controller.Failed() && hb_resp.error_code() == 0)
        {
            LOG_DEBUG("RunHeartbeatLoop: Heartbeat success, worker_id=%s, running=%d",
                      worker_id.c_str(), running);

            // 检查队列有没有未上报的结果，有的话就上报给ResultCollector
            // 队列里存在未上报结果的原因通常是 ResultCollector 宕机/重启，
            // 这里借心跳循环（每 3 秒）作为结果上报失败后的兜底重试
            if (worker_service)
            {
                int retried = worker_service->RetryPendingReports();
                if (retried > 0)
                    LOG_INFO("RunHeartbeatLoop: %d pending reports retried successfully", retried);
            }

            // alive=false：WM 的注册表里查无此 worker（典型场景：WM 重启后内存 WorkerStore 丢失）
            // 要重新注册恢复记录，下一轮心跳即恢复
            if (!hb_resp.alive())
            {
                LOG_WARN("RunHeartbeatLoop: WorkerManager reports worker_id=%s is NOT alive, "
                         "re-registering...",
                         worker_id.c_str());

                // WM 重启后 WorkerStore 丢失，需要重新注册。
                // InsertOrUpdate 是幂等的（已存在则覆盖），不会产生重复记录。
                RegisterWorkerRequest reg_req;
                reg_req.set_worker_id(worker_id);
                reg_req.set_ip(ip);
                reg_req.set_port(port);
                reg_req.set_cpu_cores(cpu_cores);
                reg_req.set_memory_mb(memory_mb);
                reg_req.set_max_running_shards(max_running_shards);

                RegisterWorkerResponse reg_resp;
                MprpcController reg_ctrl;

                stub.RegisterWorker(&reg_ctrl, &reg_req, &reg_resp, nullptr);

                if (!reg_ctrl.Failed() && reg_resp.error_code() == 0)
                {
                    LOG_INFO("RunHeartbeatLoop: re-registered worker_id=%s successfully",
                             worker_id.c_str());
                }
                else
                {
                    LOG_WARN("RunHeartbeatLoop: re-registration failed for worker_id=%s: %s",
                             worker_id.c_str(),
                             reg_ctrl.Failed() ? reg_ctrl.ErrorText().c_str()
                                               : reg_resp.error_msg().c_str());
                }
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

    // 阶段 10：MQ 是可降级组件，Init 失败只 WARN（结果上报回退直连 RPC）
    MqClient::GetInstance().Init();

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

    // ── FFmpeg 启动检查（阶段 6 遗留项 4） ─────────────────────────
    std::string executor_mode = config.Load("executor_mode");
    if (executor_mode == "ffmpeg" && !FfmpegExecutor::CheckAvailable())
    {
        LOG_ERROR("executor_mode=ffmpeg but ffmpeg/ffprobe not found in PATH, "
                  "refusing to start. Install ffmpeg or set executor_mode=mock.");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // ── 阶段 11：可观测性（metrics_port<=0 时不启用，可降级组件） ──
    // 放在 Worker 配置校验 + FFmpeg 检查之后：前面的失败路径不涉及
    // metrics 线程的启动/清理，保持每个失败分支只做日志清理
    int metrics_port = config.LoadInt("metrics_port", 0, 0, 65535);
    mprpc::MetricsHttpServer metrics_server;
    metrics_server.Init(metrics_port);
    // 内置日志告警兜底：本进程出站 RPC P99 延迟 > 1000ms
    mprpc::MetricsRegistry::GetInstance().RegisterAlertRule(
        {"rpc_latency_p99_high", "WARN",
         []() {
             return mprpc::MetricsRegistry::GetInstance()
                 .HistogramQuantile("rpc_latency_ms", 0.99);
         },
         1000, true, 0, "本进程 RPC P99 延迟超过 1000ms"});
    metrics_server.Start();

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
        metrics_server.Stop();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // Provider 退出后清理
    heartbeat_stopped = true;
    if (heartbeat_thread.joinable()) heartbeat_thread.join();
    metrics_server.Stop();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
