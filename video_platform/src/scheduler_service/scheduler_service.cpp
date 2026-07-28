#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include "scheduler.pb.h"
#include "worker.pb.h"
#include "job.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"

using namespace video_platform;

// ============================================================================
// SchedulerService — 任务切分与分配（阶段 4：mock 调度闭环）
// ============================================================================

/// @brief SchedulerService RPC 实现
///
/// 阶段 4 实现：
/// - ScheduleJob：将 job 按时间切片拆分为 N 个 shard，写入 ShardStore，
///   更新 Job 状态 → JOB_SPLITTING → JOB_SCHEDULING
/// - RescheduleShard：将 shard 重新标记为 WAITING（阶段 5 完善重试逻辑）
///
/// 后台线程 SchedulingLoop 周期扫描 WAITING shard，匹配 ONLINE Worker
/// 并通过直连调用 WorkerService.AssignShard 分配。
///
/// ⚠️ 跨进程存储备注：
/// JobStore / ShardStore 是进程内单例，Scheduler 需要从 RPC 请求参数
/// 构造本地副本。详见业务日志第 3 篇「踩坑记录」节。
class SchedulerServiceImpl : public SchedulerService {
public:
    /// @brief 对 job 进行切分并启动调度
    ///
    /// 由 JobService.SubmitJob() 通过 ZK 发现的 SchedulerService_Stub 调用。
    ///
    /// 步骤：
    /// 1. 从请求的 JobInfo 构造/获取本地 JobRecord 副本（首次调用时本地不存在）
    /// 2. 读取 mock 配置（mock_job_duration_sec / mock_shard_duration_sec）
    /// 3. 计算 shard_count = job_duration / shard_duration，最少 1 个
    /// 4. 创建 ShardRecord 写入 ShardStore（status=WAITING，由 SchedulingLoop 异步分配）
    /// 5. 更新 Job 状态 → SCHEDULING，回填 shard_count
    ///
    /// @note 本方法只做切分和写入，不做 Worker 分配。
    ///       Worker 分配由后台 SchedulingLoop 异步完成，"Pull 模式"。
    /// 3. 计算 shard 数量并按时间切片创建 ShardRecord
    /// 4. 写入 ShardStore，更新 JobStore
    void ScheduleJob(::google::protobuf::RpcController* controller,
                     const ::ScheduleJobRequest* request,
                     ::ScheduleJobResponse* response,
                     ::google::protobuf::Closure* done) override
    {
        const auto& jobInfo = request->job_info();
        const std::string& job_id = jobInfo.job_id();

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s, input=%s",
                 job_id.c_str(), jobInfo.input_path().c_str());

        // 1. 从请求的 JobInfo 创建本地 JobRecord 副本
        //    （JobStore 是进程内单例，Scheduler 进程需要自己的本地副本，
        //     JobService 写入的数据在另一个进程的 JobStore 中，不可见）
        JobRecord local_job;
        bool job_exists = false;
        // 这里验证，该job_id是否在JobStore的哈希表里已经存在，如果存在了直接使用，不需要新建jobRecord并放入哈希表中
        JobRecord* existing = JobStore::GetInstance().Get(job_id);
        if (existing != nullptr)
        {
            // 已有本地副本：可能是 JobService 重试导致 ScheduleJob 被调用多次
            local_job = *existing;
            job_exists = true;
        }
        else
        {
            // 首次调度：从 RPC 请求的 proto JobInfo 构造本地 JobRecord
            // proto JobInfo 不含 shard_duration_sec，该字段使用配置默认值
            local_job.job_id             = jobInfo.job_id();
            local_job.user_id            = jobInfo.user_id();
            local_job.input_path         = jobInfo.input_path();
            local_job.output_path        = jobInfo.output_path();
            local_job.target_format      = jobInfo.target_format();
            local_job.target_resolution  = jobInfo.target_resolution();
            local_job.target_bitrate     = jobInfo.target_bitrate();
            local_job.priority           = jobInfo.priority();
            local_job.status             = static_cast<int32_t>(JobStatus::JOB_PENDING);
            local_job.shard_count        = 0;
            local_job.shard_duration_sec = 0;  // proto JobInfo 不含此字段，使用配置默认值
            local_job.created_at         = NowMs();
            local_job.updated_at         = NowMs();
            JobStore::GetInstance().Insert(local_job);
            LOG_INFO("SchedulerService::ScheduleJob job_id=%s: created local copy from request",
                     job_id.c_str());
        }

        // 2. 读取 mock 配置
        auto& config = MprpcApplication::GetConfig();
        int mock_job_duration_sec = config.LoadInt("mock_job_duration_sec", 60, 1, 86400);
        int mock_shard_duration_sec = config.LoadInt("mock_shard_duration_sec", 20, 1, 3600);

        // 使用本地副本中记录的 shard_duration_sec，否则用配置默认值
        int shard_dur = (local_job.shard_duration_sec > 0)
                        ? local_job.shard_duration_sec : mock_shard_duration_sec;

        int shard_count = mock_job_duration_sec / shard_dur;
        if (shard_count < 1) shard_count = 1;

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s: splitting into %d shards "
                 "(job_duration=%ds, shard_duration=%ds)",
                 job_id.c_str(), shard_count, mock_job_duration_sec, shard_dur);

        // 3. 更新 Job 状态 → SPLITTING(切分状态)
        local_job.status = static_cast<int32_t>(JobStatus::JOB_SPLITTING);
        local_job.updated_at = NowMs();
        // JobStore类中，是存放所有JobRecord,用哈希表存放，提供多种更新方法
        JobStore::GetInstance().Update(job_id, local_job);

        // 4. 按时间切片创建 ShardRecord
        int64_t shard_duration_ms = static_cast<int64_t>(shard_dur) * 1000;
        for (int i = 0; i < shard_count; ++i)
        {
            ShardRecord shard;
            shard.shard_id    = job_id + "_shard_" + std::to_string(i);
            shard.job_id      = job_id;
            shard.shard_index = i;
            shard.start_ms    = static_cast<int64_t>(i) * shard_duration_ms;
            shard.duration_ms = shard_duration_ms;
            shard.status      = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
            shard.assigned_worker_id.clear();
            shard.attempt_id.clear();
            shard.retry_count = 0;
            shard.max_retry   = 3;
            shard.input_path  = local_job.input_path;  // mock: 复用原始输入路径
            shard.output_path = local_job.output_path + "/" + shard.shard_id + ".mp4";
            shard.created_at  = NowMs();
            shard.updated_at  = NowMs();

            // 类似JobStore 有存放ShardRecord的哈希，提供一些操作的方法
            ShardStore::GetInstance().Insert(shard);
            LOG_INFO("SchedulerService: created shard %s [%lld-%lld ms]",
                     shard.shard_id.c_str(),
                     (long long)shard.start_ms,
                     (long long)(shard.start_ms + shard.duration_ms));
        }

        // 5. 更新 Job 状态 → SCHEDULING，回填 shard_count
        local_job.shard_count = shard_count;
        local_job.status = static_cast<int32_t>(JobStatus::JOB_SCHEDULING);
        local_job.updated_at = NowMs();
        JobStore::GetInstance().Update(job_id, local_job);

        LOG_INFO("SchedulerService::ScheduleJob job_id=%s: %d shards created, status=SCHEDULING",
                 job_id.c_str(), shard_count);

        // ── 反向通知 JobService 更新 shard_count 和状态 ──────────────────
        // JobStore 是进程内存储，Scheduler 的切分结果需要同步回 JobService，
        // 让 QueryJob 能返回正确的 shard_count。
        {
            // 这里调用channel的目的，不是为了调用对方函数获取结果，仅仅是为了把参数传递到对方进程所在的变量里，巧妙，非常巧妙
            MprpcChannel js_channel;
            JobService_Stub js_stub(&js_channel);

            UpdateJobStatusRequest update_req;
            update_req.set_job_id(job_id);
            update_req.set_status(JobStatus::JOB_SCHEDULING);
            update_req.set_shard_count(shard_count);

            UpdateJobStatusResponse update_resp;
            MprpcController update_ctrl;
            update_ctrl.SetTimeoutMs(3000);

            js_stub.UpdateJobStatus(&update_ctrl, &update_req, &update_resp, nullptr);

            if (!update_ctrl.Failed() && update_resp.error_code() == 0)
            {
                LOG_INFO("SchedulerService: notified JobService job=%s shard_count=%d",
                         job_id.c_str(), shard_count);
            }
            else
            {
                LOG_WARN("SchedulerService: failed to notify JobService for job=%s: %s",
                         job_id.c_str(),
                         update_ctrl.Failed() ? update_ctrl.ErrorText().c_str()
                                              : update_resp.error_msg().c_str());
            }
        }

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        response->set_job_id(job_id);
        done->Run();
    }

    /// @brief 重新调度一个 shard（失败 / 超时后触发）
    ///
    /// 当前调用方：阶段 5 将由 ResultCollector（shard 执行失败）或
    /// WorkerManager（心跳超时）调用。
    ///
    /// 阶段 4 行为：将 shard 重置为 WAITING，retry_count++，
    /// 由后台 SchedulingLoop 下一轮扫描时自动重新分配。
    /// 阶段 5 将增加：retry_count > max_retry 时直接标记 SHARD_FAILED。
    void RescheduleShard(::google::protobuf::RpcController* controller,
                         const ::RescheduleShardRequest* request,
                         ::RescheduleShardResponse* response,
                         ::google::protobuf::Closure* done) override
    {
        const std::string& shard_id = request->shard_id();
        LOG_INFO("SchedulerService::RescheduleShard shard_id=%s, job_id=%s, reason=%s",
                 shard_id.c_str(),
                 request->job_id().c_str(),
                 request->reason().c_str());

        ShardRecord* shard = ShardStore::GetInstance().Get(shard_id);
        if (shard == nullptr)
        {
            response->set_error_code(1);
            response->set_error_msg("shard not found: " + shard_id);
            response->set_accepted(false);
            done->Run();
            return;
        }

        // 阶段 4：简单重置为 WAITING，由调度循环重新分配
        // 阶段 5 将在此处检查 retry_count 是否超过 max_retry
        shard->status = static_cast<int32_t>(ShardStatus::SHARD_WAITING);
        shard->assigned_worker_id.clear();
        shard->attempt_id.clear();
        shard->retry_count++;
        shard->updated_at = NowMs();
        ShardStore::GetInstance().Update(shard_id, *shard);

        LOG_INFO("SchedulerService::RescheduleShard shard_id=%s reset to WAITING (retry=%d/%d)",
                 shard_id.c_str(), shard->retry_count, shard->max_retry);

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        done->Run();
    }
};

// ============================================================================
// SchedulingLoop — 后台调度循环（Pull 模式）
// ============================================================================

/// @brief 在后台线程中运行调度循环，由 main() 在 Provider 启动前创建
///
/// 设计：Pull 模式 —— 主动扫描 WAITING shard（等待被操作的片） 并拉取 ONLINE Worker(就绪的工作者) 列表，
/// 而非在 ScheduleJob 中同步分配。优点：
/// - ScheduleJob 不会被慢 Worker 阻塞（Worker 可能过载或网络慢）
/// - Worker 列表变化（上线/离线）时分配逻辑自动感知
/// - Worker 暂时不可用时（无可用槽位），shard 留在队列中等待下一轮
///
/// Worker 选择策略（当前：贪心最优槽位）：
///   available_slots = max_running_shards - current_running_shards
///   选 available_slots 最大的 Worker
///   阶段 7 将扩展为资源感知评分：score = slots*10 - cpu*0.5 - mem*0.2
///
/// RPC 通信方式：
/// - WorkerManager.ListWorkers → ZK 发现（只需要查到哪个 Worker 在线即可）
/// - WorkerService.AssignShard → 直连 MprpcChannel(ip, port)
///   因为必须调用特定 Worker，不能走 ZK 随机轮询
///
/// 线程安全：本函数运行在独立线程中，所有 Store 操作通过读写锁保护。
///
/// @param stop_flag 主线程设置此标志为 true 时，循环退出
static void SchedulingLoop(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kScheduleIntervalMs = 2000;  // 每 2 秒扫描一次
    constexpr int64_t kAssignTimeoutMs    = 5000;  // AssignShard RPC 超时

    LOG_INFO("SchedulingLoop thread started, interval=%lldms",
             (long long)kScheduleIntervalMs);

    // 等待 Provider 启动完成
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // WorkerManager 通过 ZK 发现
    WorkerManagerService_Stub wm_stub(new MprpcChannel());

    while (!stop_flag)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kScheduleIntervalMs));
        if (stop_flag) break;

        // ── 1. 扫描 WAITING shard ─────────────────────────────────────
        auto waiting_shards = ShardStore::GetInstance().ListByStatus(
            static_cast<int32_t>(ShardStatus::SHARD_WAITING));

        if (waiting_shards.empty())
            continue;  // 没有待分配 shard，下一轮

        LOG_INFO("SchedulingLoop: found %zu WAITING shards", waiting_shards.size());

        // ── 2. 查询 ONLINE Worker ─────────────────────────────────────
        ListWorkersRequest lw_req;
        lw_req.set_filter_status(WorkerStatus::WORKER_ONLINE);

        ListWorkersResponse lw_resp;
        MprpcController lw_ctrl;
        wm_stub.ListWorkers(&lw_ctrl, &lw_req, &lw_resp, nullptr);

        if (lw_ctrl.Failed() || lw_resp.error_code() != 0)
        {
            LOG_WARN("SchedulingLoop: ListWorkers failed: %s",
                     lw_ctrl.Failed() ? lw_ctrl.ErrorText().c_str()
                                      : lw_resp.error_msg().c_str());
            continue;
        }

        if (lw_resp.workers_size() == 0)
        {
            LOG_WARN("SchedulingLoop: no ONLINE workers available");
            continue;
        }

        LOG_INFO("SchedulingLoop: %d ONLINE workers", lw_resp.workers_size());

        // ── 3. 对每个 WAITING shard 分配 Worker ─────────────────────
        for (auto& shard : waiting_shards)
        {
            // 选择有可用槽位的 Worker
            const WorkerInfo* best_worker = nullptr;
            int best_slots = -1;

            for (const auto& w : lw_resp.workers())
            {
                int available = w.max_running_shards() - w.current_running_shards();
                if (available > best_slots)
                {
                    best_slots = available;
                    best_worker = &w;
                }
            }

            if (best_worker == nullptr || best_slots <= 0)
            {
                LOG_WARN("SchedulingLoop: no worker with available slots for shard %s",
                         shard.shard_id.c_str());
                continue;  // 下一轮再试
            }

            // ── 4. 直连 Worker 的 AssignShard ─────────────────────────
            MprpcChannel worker_channel(best_worker->ip(),
                                        static_cast<uint16_t>(best_worker->port()));
            WorkerService_Stub worker_stub(&worker_channel);

            AssignShardRequest as_req;
            auto* si = as_req.mutable_shard();
            si->set_shard_id(shard.shard_id);
            si->set_job_id(shard.job_id);
            si->set_shard_index(shard.shard_index);
            si->set_start_ms(shard.start_ms);
            si->set_duration_ms(shard.duration_ms);
            si->set_status(static_cast<ShardStatus>(shard.status));
            si->set_input_path(shard.input_path);
            si->set_output_path(shard.output_path);
            si->set_created_at(shard.created_at);

            AssignShardResponse as_resp;
            MprpcController as_ctrl;
            as_ctrl.SetTimeoutMs(kAssignTimeoutMs);

            worker_stub.AssignShard(&as_ctrl, &as_req, &as_resp, nullptr);

            if (as_ctrl.Failed() || !as_resp.accepted())
            {
                LOG_WARN("SchedulingLoop: AssignShard %s to worker %s failed: %s",
                         shard.shard_id.c_str(),
                         best_worker->worker_id().c_str(),
                         as_ctrl.Failed() ? as_ctrl.ErrorText().c_str()
                                          : as_resp.error_msg().c_str());
                continue;  // 下一轮重试分配
            }

            // ── 5. 更新 shard 状态 → ASSIGNED ───────────────────────
            shard.status = static_cast<int32_t>(ShardStatus::SHARD_ASSIGNED);
            shard.assigned_worker_id = best_worker->worker_id();
            shard.attempt_id = shard.shard_id + "_attempt_" + std::to_string(shard.retry_count);
            shard.updated_at = NowMs();
            ShardStore::GetInstance().Update(shard.shard_id, shard);

            // ── 6. 更新 job 状态 → RUNNING（若是首次分配） ─────────────
            JobRecord* job = JobStore::GetInstance().Get(shard.job_id);
            if (job != nullptr &&
                job->status == static_cast<int32_t>(JobStatus::JOB_SCHEDULING))
            {
                job->status = static_cast<int32_t>(JobStatus::JOB_RUNNING);
                job->updated_at = NowMs();
                JobStore::GetInstance().Update(shard.job_id, *job);

                // 反向通知 JobService 状态变更（跨进程同步）
                {
                    MprpcChannel js_channel;
                    JobService_Stub js_stub(&js_channel);
                    UpdateJobStatusRequest update_req;
                    update_req.set_job_id(shard.job_id);
                    update_req.set_status(JobStatus::JOB_RUNNING);
                    UpdateJobStatusResponse update_resp;
                    MprpcController update_ctrl;
                    update_ctrl.SetTimeoutMs(3000);
                    js_stub.UpdateJobStatus(&update_ctrl, &update_req, &update_resp, nullptr);
                }
            }

            LOG_INFO("SchedulingLoop: assigned shard %s to worker %s (%s:%d), slots_left=%d",
                     shard.shard_id.c_str(),
                     best_worker->worker_id().c_str(),
                     best_worker->ip().c_str(),
                     best_worker->port(),
                     best_slots - 1);
        }
    }

    LOG_INFO("SchedulingLoop thread stopped");
}

// ============================================================================
// main — 服务入口
// ============================================================================

/// @brief SchedulerService 启动流程
///   1. 加载配置 + 初始化日志
///   2. 启动后台调度循环线程
///   3. 主线程启动 RpcProvider 接收 ScheduleJob / RescheduleShard
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

    // 启动后台调度循环线程
    std::atomic<bool> scheduling_stopped{false};
    std::thread scheduling_thread(SchedulingLoop, std::ref(scheduling_stopped));

    RpcProvider provider;
    provider.NotifyService(new SchedulerServiceImpl());

    if (!provider.Run())
    {
        LOG_ERROR("SchedulerService start failed");
        scheduling_stopped = true;
        if (scheduling_thread.joinable()) scheduling_thread.join();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // Provider 退出后清理
    scheduling_stopped = true;
    if (scheduling_thread.joinable()) scheduling_thread.join();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
