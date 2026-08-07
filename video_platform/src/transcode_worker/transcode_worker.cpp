#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <memory>
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
#include "mprpcutil.h"

using namespace video_platform;

static int CollectCpuUsage();
static int CollectMemUsage();
static std::string ResolveWorkerId();

// 解析 Worker 唯一标识,保证同一进程不同线程的id一致
// 避免所有副本注册成同一个 worker_id 互相覆盖
static std::string ResolveWorkerId()
{
    const char* env = getenv("WORKER_ID");
    if (env && *env) return env;

    std::string id = MprpcApplication::GetConfig().Load("worker_id");
    if (!id.empty()) return id;

    char hostname[256] = {0};
    if (gethostname(hostname, sizeof(hostname) - 1) == 0 && hostname[0])
        return hostname;
    return "unknown_worker";
}

// 尝试通过 MQ 上报 shard 执行结果。
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


struct RunningShard {
    ShardInfo   info;                      ///< shard 元信息（proto 拷贝）
    std::string attempt_id;               ///< 执行尝试 ID，格式 "{shard_id}_attempt_{N}"
    std::atomic<int> progress{0};        ///< 执行进度 0-100，原子变量无锁读取
    std::atomic<bool> cancelled{false};  ///< 取消标志，CancelShard 设置、执行线程检测
    std::thread executor_thread;         ///< 执行线程句柄

    RunningShard(const ShardInfo& s, const std::string& aid)
        : info(s), attempt_id(aid) {}
};

/// @brief 待重试上报的结果
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
};

/// @brief WorkerService RPC 实现
///
/// TranscodeWorker 双重角色：
/// 1. Provider — 接收 Scheduler 下发的调度命令：
///    - AssignShard：接收 shard → 校验无重复（同一锁内检查+插入）→ 锁外启动执行线程
///    - CancelShard：设置 cancelled 标志，执行线程在转码过程中检测并退出
///    - QueryShard：返回 shard 实时 progress（0-100 或 -1=不存在）
///
/// 2. Consumer — 主动调用其他服务（不在本类中，由独立函数实现）：
///    - RunHeartbeatLoop：启动时 RegisterWorker + 周期 3s Heartbeat
///
/// 线程模型（5 类线程同时运行）：
///   - main 线程：RpcProvider::Run() 阻塞等待连接
///   - IO 线程池（io_threads 个）：处理 AssignShard / CancelShard / QueryShard
///   - Work 线程池（work_threads 个）：处理 RPC 业务逻辑
///   - 执行线程（每个 shard 一个）：ffmpeg 转码执行
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

    // 接收 Scheduler 分配的 shard，启动执行线程
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

        // Worker 过载保护
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
            // 负载过高，不进行分配
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

        std::shared_ptr<RunningShard> running_shard;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = running_shards_.find(shard.shard_id());
            // 只要 map 中还存在该 shard 的条目就拒绝
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

            // 创建 RunningShard 追踪对象并插入 map
            running_shard = std::make_shared<RunningShard>(shard, attempt_id);
            running_shards_[shard.shard_id()] = running_shard;
        }

        // 锁外启动执行线程，执行任务
        running_shard->executor_thread = std::thread(&WorkerServiceImpl::FfmpegExecute,
                                          this, running_shard);
        LOG_INFO("WorkerService::AssignShard shard=%s attempt=%s started (ffmpeg mode)",
                 shard.shard_id().c_str(), attempt_id.c_str());

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        done->Run();
    }

    // 取消正在执行的 shard
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
        // 先判断该shard在不在本worker
        if (it != running_shards_.end())
        {
            // attempt 精确匹配的校验，请求要取消的是哪次执行，Worker 上现在跑的是哪次执行，对得上才杀
            if (!request->attempt_id().empty()
                && it->second->attempt_id != request->attempt_id())
            {
                // 不符合取消逻辑，忽略取消该shard
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
            // 满足条件，标记shard为取消
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

    // 查询 shard 执行进度(分片级：1 个 shard 的实时进度),progress 0-100 实时百分比,毫秒级实时
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
    // 真实 FFmpeg 执行器,会阻塞，等待完成
    void FfmpegExecute(std::shared_ptr<RunningShard> running_shard)
    {
        const std::string& shard_id = running_shard->info.shard_id();
        const std::string& job_id   = running_shard->info.job_id();
        // 拿到该进程唯一ID
        std::string worker_id = ResolveWorkerId();

        // 读取 FFmpeg 相关配置
        std::string work_dir = MprpcApplication::GetConfig().Load("ffmpeg_work_dir");
        if (work_dir.empty()) work_dir = "/tmp/transcode_worker";

        LOG_INFO("FfmpegExecute: shard=%s started, work_dir=%s",
                 shard_id.c_str(), work_dir.c_str());

        // 创建工作目录
        std::string shard_work_dir = work_dir + "/" + shard_id;
        mkdir(shard_work_dir.c_str(), 0755);  

        // 确定输入和输出路径
        std::string input_path = running_shard->info.input_path();
        std::string output_path = running_shard->info.output_path();
        if (output_path.empty())
        {
            output_path = shard_work_dir + "/output.mp4";
        }

        int64_t start_ms = running_shard->info.start_ms();
        int64_t duration_ms = running_shard->info.duration_ms();

        LOG_INFO("FfmpegExecute: shard=%s input=%s, output=%s, start=%lldms, dur=%lldms",
                 shard_id.c_str(), input_path.c_str(), output_path.c_str(),
                 (long long)start_ms, (long long)duration_ms);

        // 转码（切片+转码合一，-ss/-t 直接传给 ffmpeg） 
        // 转码参数从 ShardInfo 透传，不再读本地配置
        std::string target_resolution = running_shard->info.target_resolution();
        int target_bitrate = running_shard->info.target_bitrate();

        // ResultCollector stub（用于结果上报）
        MprpcChannel rc_channel;
        ResultCollectorService_Stub rc_stub(&rc_channel);

        // 进度回调：更新 running_shard->progress（Scheduler 直连 QueryShard 读取实时进度）
        auto progress_cb = [&](int progress) {
            if (progress < 0) return;
            if (progress > 100) progress = 100;

            running_shard->progress = progress;

            if (running_shard->cancelled)
            {
                LOG_INFO("FfmpegExecute: shard=%s cancelled at progress=%d%%",
                         shard_id.c_str(), progress);
                return;
            }
        };

        LOG_INFO("FfmpegExecute: transcoding shard=%s, input=%s, output=%s, "
                 "start=%lldms, dur=%lldms, resolution=%s, bitrate=%d",
                 shard_id.c_str(), input_path.c_str(), output_path.c_str(),
                 (long long)start_ms, (long long)duration_ms,
                 target_resolution.c_str(), target_bitrate);

        auto ts_start = std::chrono::steady_clock::now();
        // cancel 检查回调：检测 running_shard->cancelled 标志，触发时 fork+exec 子进程被 kill
        auto cancel_cb = [&running_shard]() -> bool { return running_shard->cancelled.load(); };

        auto transcode_result = FfmpegExecutor::Transcode(
            input_path, output_path,
            target_resolution, target_bitrate,
            start_ms, duration_ms,
            progress_cb, cancel_cb);
        auto ts_end = std::chrono::steady_clock::now();
        int64_t transcode_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            ts_end - ts_start).count();

        // 转码耗时观测
        mprpc::MetricsRegistry::GetInstance()
            .Histogram("transcode_duration_ms", "单 shard 转码耗时（毫秒）",
                       std::vector<double>{100, 500, 1000, 2000, 5000, 10000,
                                           30000, 60000, 120000, 300000})
            .Observe(static_cast<double>(transcode_elapsed));

        // 处理 cancel 情况
        if (running_shard->cancelled)
        {
            LOG_INFO("FfmpegExecute: shard=%s cancelled during transcode", shard_id.c_str());
            CleanupShard(shard_id);
            return;
        }

        // 上报最终结果（带重试） 
        bool is_success = transcode_result.success;
        int exit_code = transcode_result.exit_code;
        std::string error_msg = transcode_result.error_msg;

        running_shard->progress = 100;
        int64_t total_elapsed = transcode_elapsed;

        // 构造结果请求（MQ 与 RPC 共用）
        ReportShardResultRequest result_req;
        result_req.set_shard_id(shard_id);
        result_req.set_job_id(job_id);
        result_req.set_worker_id(worker_id);
        result_req.set_attempt_id(running_shard->attempt_id);
        result_req.set_is_success(is_success);
        result_req.set_exit_code(exit_code);
        result_req.set_error_msg(error_msg);
        result_req.set_output_path(output_path);
        result_req.set_elapsed_ms(total_elapsed);
        result_req.set_shard_index(running_shard->info.shard_index());

        bool result_reported = false;
        //MQ 优先（publish 成功即已投递，RC 消费后走同一聚合逻辑）
        if (TryPublishResultToMq(result_req))
        {
            LOG_INFO("FfmpegExecute: shard=%s %s published to MQ (result.pending)",
                     shard_id.c_str(), is_success ? "SUCCESS" : "FAILED");
            result_reported = true;
        }
        // MQ行不通，那就RPC重复3次
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

        // 最终还没成功汇报该片结果，那就将结果暂存到待重试队列，心跳线程定期重试上报
        if (!result_reported)
        {
            LOG_ERROR("FfmpegExecute: shard=%s result report FAILED after 3 retries, "
                      "queuing for heartbeat retry (fix #6)",
                      shard_id.c_str());
            PendingReport pr;
            pr.shard_id        = shard_id;
            pr.job_id          = job_id;
            pr.worker_id       = worker_id;
            pr.attempt_id      = running_shard->attempt_id;
            pr.is_success      = is_success;
            pr.exit_code       = exit_code;
            pr.error_msg       = error_msg;
            pr.output_path     = output_path;
            pr.elapsed_ms      = total_elapsed;
            pr.shard_index     = running_shard->info.shard_index();
            QueuePendingReport(pr);
        }
        // 任务彻底完成，清理
        CleanupShard(shard_id);
    }

    //从 running_shards_ map 中移除已完成的 shard
    void CleanupShard(const std::string& shard_id)
    {
        std::shared_ptr<RunningShard> running_shard;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = running_shards_.find(shard_id);
            if (it != running_shards_.end())
            {
                running_shard = it->second;
                running_shards_.erase(it);  // 锁内仅做 map 操作
            }
        }
        // 锁外处理线程资源，避免与执行线程 / CancelShard 死锁
        if (running_shard && running_shard->executor_thread.joinable())
        {
            running_shard->executor_thread.detach();
        }
    }
    
    mutable std::mutex mutex_;
    // 该worker上正在运行的shards
    std::unordered_map<std::string, std::shared_ptr<RunningShard>> running_shards_;

    // 上报失败的结果暂存队列，由心跳线程定期重试
    mutable std::mutex pending_mutex_;
    std::vector<PendingReport> pending_reports_;

public:
    // 把上报失败的结果压入待重试队列
    void QueuePendingReport(const PendingReport& pr)
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_reports_.push_back(pr);
    }

    // 心跳线程调用,重试所有待上报的 shard 结果
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

            ReportShardResultResponse resp;
            MprpcController ctrl;
            ctrl.SetTimeoutMs(5000);  // 显式超时，避免无限阻塞心跳线程
            rc_stub.ReportShardResult(&ctrl, &req, &resp, nullptr);

            if (!ctrl.Failed() && resp.accepted())
            {
                ++succeeded;
                LOG_INFO("RetryPendingReports: shard=%s report SUCCESS via heartbeat retry",
                         pr.shard_id.c_str());
            }
            else if (!ctrl.Failed())
            {
                // 确定性拒绝（accepted=false，如 stale attempt——shard 已被重调度到更高 attempt，旧结果永远不可能被接受）
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

// 读取本进程 CPU 时间并计算使用率
static int CollectCpuUsage()
{
    static std::mutex cpu_mutex;
    static uint64_t prev_ticks = 0;
    static int64_t  prev_time_ms = 0;
    static bool     first_call = true;

    std::lock_guard<std::mutex> lock(cpu_mutex);

    auto collect_tree_ticks = []() -> uint64_t {
        uint64_t ticks = 0;

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
            if (!stat_file.is_open()) continue;
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
    };

    uint64_t total_ticks = collect_tree_ticks();
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

// 读取本进程内存使用率

static int CollectMemUsage()
{
    static std::once_flag memtotal_init;
    static uint64_t mem_total_kb = 0;

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



// Worker 注册与心跳后台线程（RegisterWorker + Heartbeat 循环）
// Worker 作为 Consumer 主动与 WorkerManager 通信，维持自己的在线状态。
// WorkerManager 通过心跳超时判断 Worker 离线，触发故障恢复链路。
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
    // Docker 部署 rpcserverip=0.0.0.0（全接口监听），注册上报
    // 必须用本机实际 IP，否则 WM 拿到 0.0.0.0 无法回连 Worker
    if (ip == "0.0.0.0" || ip.empty())
    {
        ip = mprpc::GetLocalIp();
        LOG_INFO("RunHeartbeatLoop: rpcserverip is 0.0.0.0, advertise ip=%s", ip.c_str());
    }
    if (ip.empty() || port == -1)
    {
        LOG_ERROR("RunHeartbeatLoop: missing rpcserverip or rpcserverport in config");
        return;
    }

    LOG_INFO("RunHeartbeatLoop: starting for worker_id=%s, ip=%s:%d, cores=%d, mem=%dMB, max_shards=%d",
             worker_id.c_str(), ip.c_str(), port, cpu_cores, memory_mb, max_running_shards);

    MprpcChannel channel;
    WorkerManagerService_Stub stub(&channel);

    // RegisterWorker
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

    // Heartbeat 循环（每 3 秒）
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
/// 2. 创建 WorkerServiceImpl
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

    // 阶段 13：worker_id 不再必填——ResolveWorkerId() 兜底 hostname，
    // 支持 Docker --scale 多副本（各容器 hostname 天然唯一）
    std::string worker_id = ResolveWorkerId();
    if (worker_id == "unknown_worker")
    {
        LOG_ERROR("failed to resolve worker_id (no WORKER_ID env / worker_id config / hostname)");
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
    if (!FfmpegExecutor::CheckAvailable())
    {
        LOG_ERROR("ffmpeg/ffprobe not found in PATH, refusing to start. "
                  "Install ffmpeg.");
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
