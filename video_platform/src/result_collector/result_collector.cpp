#include <string>
#include <cstdlib>
#include <algorithm>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <unordered_set>
#include "result.pb.h"
#include "job.pb.h"
#include "scheduler.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"
#include "video_platform/ffmpeg_executor.h"

using namespace video_platform;

// ============================================================================
// ResultCollectorService — 结果收集与聚合（阶段 5：幂等校验 + 故障恢复）
// ============================================================================
//
// ResultCollector 是调度链路的终点——Worker 执行完 shard 后向它上报结果，
// 它负责聚合所有 shard 的状态，判定 job 的整体终态（SUCCESS / FAILED）。
//
// 阶段 5 核心增强：
// 1. attempt_id 幂等校验
//    - 从 attempt_id 中解析 retry_count（格式 "{shard_id}_attempt_{N}"）
//    - 拒绝旧 attempt 的过期结果（incoming_retry < stored_retry）
//    - 防止 Worker 重试后旧 attempt 的延迟结果覆盖新 attempt 的状态
// 2. FAILED → RescheduleShard 重试链路
//    - Worker 上报 is_success=false → 标记本地 FAILED → RPC 调用
//      Scheduler.RescheduleShard → 成功后本地改为 RETRYING（防过早判定 JOB_FAILED）
// 3. CheckJobDone 增强
//    - 同时检查 SUCCESS 和 FAILED 终态
//    - 从 JobService 查询 expected_shard_count（跨进程 RPC，带重试）
//    - 查询成功后缓存到本地 JobStore 避免重复 RPC
// 4. MarkJobTerminal 提取为独立方法，JOB_SUCCESS 和 JOB_FAILED 共用
//
// ⚠️ 跨进程存储问题：
// ShardStore 是进程内单例，Worker 上报的 shard 在 ResultCollector 本地可能
// 不存在（由 Scheduler 在另一个进程中创建）。首次收到结果时从请求参数构造
// 本地副本。expected_shard_count 同样需要从 JobService 跨进程查询。

/// @brief 从 attempt_id 字符串中解析重试次数
///
/// attempt_id 格式：{shard_id}_attempt_{N}
/// 示例："job_xxx_shard_0_attempt_2" → 返回 2
///
/// 解析策略：从右侧查找最后一个 '_'，取其后的子串转为整数。
/// 这是安全的——attempt_id 由 Scheduler 在 AssignShard 时使用固定格式构造。
///
/// @param attempt_id 执行尝试标识符
/// @return 解析出的重试次数（0, 1, 2, ...），解析失败返回 0
static int parseRetryFromAttempt(const std::string& attempt_id)
{
    auto pos = attempt_id.rfind('_');
    if (pos == std::string::npos) return 0;
    try { return std::stoi(attempt_id.substr(pos + 1)); }
    catch (...) { return 0; }
}

/// @brief ResultCollectorService RPC 实现
///
/// 由 TranscodeWorker 的 MockExecute 线程通过 ZK 发现的
/// ResultCollectorService_Stub 调用。每次 shard 执行结束（成功或失败）
/// 都会触发一次 ReportShardResult，CheckJobDone 判定 job 是否到达终态。
///
/// 阶段 5 核心流程（6 步）：
///   1. 查找/创建 shard 本地副本（解决跨进程存储可见性问题）
///   2. attempt_id 幂等校验（拒绝旧 attempt 的过期结果）
///   3. 已 SUCCESS 的幂等检查（防重复上报）
///   4. 更新状态 → SUCCESS 或 FAILED
///   5. 若 FAILED → RPC 调用 Scheduler.RescheduleShard 触发重试
///   6. CheckJobDone → 判定 JOB_SUCCESS 或 JOB_FAILED
///
/// ⚠️ 跨进程存储：ShardStore 是进程内单例，详见业务日志第 3 篇「踩坑记录」节。
class ResultCollectorServiceImpl : public ResultCollectorService {
public:
    /// @brief 周期终态扫描（阶段 8 #10 修复）— 静态成员函数
    ///
    /// 作为成员函数以便访问私有的 CheckJobDone / MarkJobTerminal（均为 static）。
    /// 由 main() 启动为独立线程，每 15 秒扫描一次。
    static void TerminalSweepLoop(std::atomic<bool>& stop_flag);

    /// @brief Worker 上报 shard 执行进度
    /// @brief Worker 执行期间的周期进度上报（0-100，Mock 每 1 秒一次）
    ///
    /// ⚠️ 当前为「仅日志」接口：收到进度只打 LOG_INFO，不做任何落库
    /// 或状态更新——`ShardInfo` 没有 progress 字段，平台上也不存在
    /// 消费该进度的下游（`QueryJob` 只返回状态机，不带百分比）。
    ///
    /// 实时进度如需对外展示（如前端进度条），需要：
    /// 1. `common.proto` 的 ShardInfo 增加 progress 字段
    /// 2. 本方法将进度写入 ShardStore
    /// 3. `QueryJob` / `QueryShard` 响应中透出该字段
    ///
    /// @note 直连 Worker 的 QueryShard 可查询实时进度（读 Worker 内存），
    ///       不经由此接口，因此这里不维护进度状态也不会丢失信息。
    void ReportShardProgress(::google::protobuf::RpcController* controller,
                             const ::ReportShardProgressRequest* request,
                             ::ReportShardProgressResponse* response,
                             ::google::protobuf::Closure* done) override
    {
        LOG_INFO("ResultCollectorService::ReportShardProgress job_id=%s, shard_id=%s, "
                 "worker=%s, attempt=%s, progress=%d%%",
                 request->job_id().c_str(),
                 request->shard_id().c_str(),
                 request->worker_id().c_str(),
                 request->attempt_id().c_str(),
                 request->progress());

        response->set_error_code(0);
        response->set_error_msg("");
        response->set_recorded(true);
        done->Run();
    }

    /// @brief Worker 上报 shard 最终执行结果（聚合核心入口 — 阶段 5 增强）
    ///
    /// 由 WorkerServiceImpl::MockExecute() 在 shard 执行完成后调用。
    ///
    /// 处理流程（6 步）：
    /// 1. 从 ShardStore 获取 shard；不存在则从请求参数构造本地副本
    /// 2. attempt_id 幂等校验：拒绝旧 attempt 的过期结果
    /// 3. 已 SUCCESS 的幂等检查：直接返回，不重复处理
    /// 4. 更新 ShardStore：SUCCESS / FAILED
    /// 5. 若 FAILED：调用 Scheduler.RescheduleShard 触发重试
    /// 6. CheckJobDone：判断 job 终态（SUCCESS / FAILED / 等待中）
    void ReportShardResult(::google::protobuf::RpcController* controller,
                           const ::ReportShardResultRequest* request,
                           ::ReportShardResultResponse* response,
                           ::google::protobuf::Closure* done) override
    {
        const std::string& shard_id   = request->shard_id();
        const std::string& job_id     = request->job_id();
        const std::string& worker_id  = request->worker_id();
        const std::string& attempt_id = request->attempt_id();

        LOG_INFO("ResultCollectorService::ReportShardResult job_id=%s, shard_id=%s, "
                 "worker=%s, attempt=%s, success=%d, exit_code=%d, elapsed=%lldms",
                 job_id.c_str(),
                 shard_id.c_str(),
                 worker_id.c_str(),
                 attempt_id.c_str(),
                 request->is_success(),
                 request->exit_code(),
                 (long long)request->elapsed_ms());

        // 1. 查找或创建 shard 本地副本
        auto shard_opt = ShardStore::GetInstance().Get(shard_id);
        if (!shard_opt.has_value())
        {
            // 首次收到该 shard 的结果：从请求参数构造本地 ShardRecord
            ShardRecord shard_copy;
            shard_copy.shard_id      = shard_id;
            shard_copy.job_id        = job_id;
            shard_copy.shard_index   = request->shard_index();
            shard_copy.status        = static_cast<int32_t>(ShardStatus::SHARD_CREATED);
            shard_copy.assigned_worker_id = worker_id;
            shard_copy.attempt_id    = attempt_id;
            shard_copy.retry_count   = parseRetryFromAttempt(attempt_id);
            shard_copy.max_retry     = 3;
            shard_copy.output_path     = request->output_path();
            shard_copy.screenshot_path = request->screenshot_path();
            shard_copy.created_at      = NowMs();
            shard_copy.updated_at      = NowMs();
            ShardStore::GetInstance().Insert(shard_copy);
            shard_opt = ShardStore::GetInstance().Get(shard_id);
            LOG_INFO("ResultCollectorService: created local shard copy shard_id=%s", shard_id.c_str());
        }

        auto shard = shard_opt.value();

        // 2. attempt_id 幂等校验：拒绝旧 attempt 的过期结果
        //    时序场景：Scheduler 将 shard 重试（retry_count=0→1，新 attempt_id=xxx_attempt_1）
        //    → SchedulingLoop 重新分配给 Worker → Worker 开始执行 attempt_1。
        //    同时，旧 Worker 上 attempt_0 的 ReportShardResult RPC 因网络延迟此刻才到达。
        //    如果不校验 attempt_id，attempt_0 的过期 SUCCESS 结果会覆盖 attempt_1 的状态。
        //    通过从 attempt_id 字符串中解析 retry_count 数值，直接比较新旧。
        int stored_retry = parseRetryFromAttempt(shard.attempt_id);
        int incoming_retry = parseRetryFromAttempt(attempt_id);
        if (!shard.attempt_id.empty() && incoming_retry < stored_retry)
        {
            LOG_INFO("ResultCollectorService: rejecting stale result for shard %s "
                     "(incoming attempt retry=%d < stored retry=%d)",
                     shard_id.c_str(), incoming_retry, stored_retry);
            response->set_error_code(0);
            response->set_error_msg("stale result from old attempt, ignored");
            response->set_accepted(false);
            response->set_job_done(false);
            done->Run();
            return;
        }

        // 3. 幂等检查：已 SUCCESS 的不再处理（同一 attempt 重复上报）
        if (shard.status == static_cast<int32_t>(ShardStatus::SHARD_SUCCESS))
        {
            LOG_INFO("ResultCollectorService: shard %s already SUCCESS (idempotent)",
                     shard_id.c_str());
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_accepted(true);
            response->set_job_done(CheckJobDone(job_id));
            done->Run();
            return;
        }

        // 3.5 幂等检查：已 FAILED 且同一 attempt 不再重复处理（修复 #8）
        // 防止同一 attempt 的重复 FAILED 上报双倍消耗 retry_count。
        if (shard.status == static_cast<int32_t>(ShardStatus::SHARD_FAILED)
            && shard.attempt_id == attempt_id)
        {
            LOG_INFO("ResultCollectorService: shard %s already FAILED for same attempt=%s "
                     "(idempotent, fix #8)",
                     shard_id.c_str(), attempt_id.c_str());
            response->set_error_code(0);
            response->set_error_msg("duplicate FAILED for same attempt, ignored");
            response->set_accepted(true);
            response->set_job_done(CheckJobDone(job_id));
            done->Run();
            return;
        }

        // 4. 更新 shard 状态
        shard.status = request->is_success()
            ? static_cast<int32_t>(ShardStatus::SHARD_SUCCESS)
            : static_cast<int32_t>(ShardStatus::SHARD_FAILED);
        shard.attempt_id = attempt_id;
        shard.assigned_worker_id = worker_id;
        shard.output_path = request->output_path();
        shard.screenshot_path = request->screenshot_path();
        shard.updated_at = NowMs();
        ShardStore::GetInstance().Update(shard_id, shard);

        LOG_INFO("ResultCollectorService: shard %s → %s (attempt=%s)",
                 shard_id.c_str(),
                 request->is_success() ? "SUCCESS" : "FAILED",
                 attempt_id.c_str());

        // 5. 若执行失败，调用 Scheduler.RescheduleShard 触发重试
        //    这里是阶段 5 故障恢复的核心链路：
        //    Worker FAILED → ResultCollector → Scheduler.RescheduleShard
        //      → retry_count++ → 判断超限? → SHARD_FAILED(永久) : SHARD_WAITING(重分配)
        //    → SchedulingLoop 扫描 WAITING → 重新 AssignShard → Worker 再次执行
        //
        //    若 RescheduleShard 成功（accepted=true），将本地状态改为 SHARD_RETRYING，
        //    这样 CheckJobDone 不会因为看到 FAILED 而过早判定 JOB_FAILED。
        //    若 RescheduleShard 拒绝（accepted=false，即已超 max_retry），本地保持 FAILED。

        // shard 的转码工作本身没干成(is_success 是 Worker 填的，表示"这活干成没有")
        if (!request->is_success())
        {
            LOG_INFO("ResultCollectorService: triggering RescheduleShard for %s (reason=WORKER_FAILED)",
                     shard_id.c_str());

            // 阶段 8 修复 #5：RescheduleShard RPC 带重试+退避（3 次，各 1s/2s/4s 间隔）
            bool reschedule_ok = false;
            for (int rs_attempt = 1; rs_attempt <= 3; ++rs_attempt)
            {
                if (rs_attempt > 1)
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(500 * (1 << (rs_attempt - 1))));

                MprpcChannel sched_channel;
                SchedulerService_Stub sched_stub(&sched_channel);

                RescheduleShardRequest rs_req;
                rs_req.set_shard_id(shard_id);
                rs_req.set_job_id(job_id);
                rs_req.set_reason("WORKER_FAILED");

                RescheduleShardResponse rs_resp;
                MprpcController rs_ctrl;
                rs_ctrl.SetTimeoutMs(5000);

                sched_stub.RescheduleShard(&rs_ctrl, &rs_req, &rs_resp, nullptr);

                if (!rs_ctrl.Failed() && rs_resp.accepted())
                {
                    LOG_INFO("ResultCollectorService: RescheduleShard accepted for %s "
                             "(attempt %d)", shard_id.c_str(), rs_attempt);
                    reschedule_ok = true;
                    break;
                }

                LOG_WARN("ResultCollectorService: RescheduleShard failed for %s "
                         "(attempt %d/3): %s",
                         shard_id.c_str(), rs_attempt,
                         rs_ctrl.Failed() ? rs_ctrl.ErrorText().c_str()
                                          : rs_resp.error_msg().c_str());
            }

            if (reschedule_ok)
            {
                // 重试已触发，更新本地状态为 RETRYING，防止 CheckJobDone
                // 在重试结果到达前因看到 FAILED 而过早判定 JOB_FAILED
                shard.status = static_cast<int32_t>(ShardStatus::SHARD_RETRYING);
                shard.updated_at = NowMs();
                ShardStore::GetInstance().Update(shard_id, shard);
            }
            else
            {
                // 阶段 8 修复 #5：RescheduleShard 3 次重试全失败时，
                // 也标记为 RETRYING（而非保持 FAILED），防止不可逆 JOB_FAILED。
                // Scheduler 恢复后，RUNNING 超时重扫或新 FAILED 上报会重新触发。
                LOG_ERROR("ResultCollectorService: RescheduleShard FAILED after 3 retries "
                          "for %s, marking RETRYING anyway to prevent irreversible JOB_FAILED",
                          shard_id.c_str());
                shard.status = static_cast<int32_t>(ShardStatus::SHARD_RETRYING);
                shard.updated_at = NowMs();
                ShardStore::GetInstance().Update(shard_id, shard);
            }
        }

        // 6. 判断 job 是否到达终态
        bool job_done = CheckJobDone(job_id);
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        response->set_job_done(job_done);

        if (job_done)
        {
            LOG_INFO("ResultCollectorService: job %s reached terminal state", job_id.c_str());
        }

        done->Run();
    }

private:
    /// @brief 检查 job 下所有 shard 是否都已进入终态
    ///
    /// 阶段 5 增强：同时检查 SUCCESS 和 FAILED 终态。
    ///
    /// 判定逻辑：
    /// 1. 从 ShardStore 获取该 job 所有已上报 shard
    /// 2. 从 JobStore 获取预期的 shard_count
    /// 3. 分类统计：success_count / failed_count / in_progress
    /// 4. 若 success_count >= expected_shard_count → JOB_SUCCESS
    /// 5. 若没有进行中的 shard 且有 FAILED → JOB_FAILED
    /// 6. 否则 → 等待更多 shard 完成
    ///
    /// @return true 表示 job 已到达终态（SUCCESS 或 FAILED）
    /// @return false 表示还有未完成的 shard
    // static：仅操作单例 Store / RPC，不依赖实例状态，供静态线程函数
    // TerminalSweepLoop 直接调用
    static bool CheckJobDone(const std::string& job_id)
    {
        auto shards = ShardStore::GetInstance().ListByJob(job_id);
        if (shards.empty()) return false;

        // 从 JobStore 获取预期的 shard 总数
        int32_t expected_shard_count = 0;
        auto job_opt = JobStore::GetInstance().Get(job_id);
        if (job_opt.has_value())
        {
            expected_shard_count = job_opt->shard_count;
        }

        int success_count = 0;
        int failed_count = 0;
        int in_progress = 0;
        for (const auto& s : shards)
        {
            int st = s.status;
            if (st == static_cast<int32_t>(ShardStatus::SHARD_SUCCESS))
                ++success_count;
            else if (st == static_cast<int32_t>(ShardStatus::SHARD_FAILED)
                  || st == static_cast<int32_t>(ShardStatus::SHARD_CANCELED))
                ++failed_count;
            else
                ++in_progress;  // WAITING / ASSIGNED / RUNNING / RETRYING
        }

        // 当 expected_shard_count 未知时（ResultCollector 本地 JobStore 无此 job），
        // 尝试从 JobService 查询（跨进程 RPC）。JobService 的 JobStore 中
        // 有 Scheduler 切分时写入的 shard_count。
        if (expected_shard_count <= 0)
        {
            // 最多重试 3 次，应对 ZK 负载均衡到不同 job_service 实例的情况
            for (int retry = 0; retry < 3 && expected_shard_count <= 0; ++retry)
            {
                MprpcChannel js_channel;
                JobService_Stub js_stub(&js_channel);
                QueryJobRequest qj_req;
                qj_req.set_job_id(job_id);
                QueryJobResponse qj_resp;
                MprpcController qj_ctrl;
                qj_ctrl.SetTimeoutMs(3000);
                js_stub.QueryJob(&qj_ctrl, &qj_req, &qj_resp, nullptr);
                if (!qj_ctrl.Failed() && qj_resp.error_code() == 0
                    && qj_resp.job_info().shard_count() > 0)
                {
                    expected_shard_count = qj_resp.job_info().shard_count();
                    LOG_INFO("ResultCollectorService::CheckJobDone: queried JobService, "
                             "expected_shard_count=%d (attempt %d)",
                             expected_shard_count, retry + 1);

                    // 缓存到本地 JobStore，后续 CheckJobDone 避免重复 RPC
                    if (!job_opt.has_value())
                    {
                        JobRecord local_job;
                        local_job.job_id = job_id;
                        local_job.shard_count = expected_shard_count;
                        local_job.created_at = NowMs();
                        local_job.updated_at = NowMs();
                        JobStore::GetInstance().Insert(local_job);
                    }
                    else
                    {
                        auto j = job_opt.value();
                        j.shard_count = expected_shard_count;
                        JobStore::GetInstance().Update(job_id, j);
                    }
                    // 刷新 job_opt 以便 MarkJobTerminal 使用
                    job_opt = JobStore::GetInstance().Get(job_id);
                }
            }
        }

        // 若仍然未知，则无法判定终态（defer 到下次 ReportShardResult）
        if (expected_shard_count <= 0)
        {
            LOG_INFO("ResultCollectorService::CheckJobDone job=%s: expected_shard_count "
                     "unknown after retries, deferring terminal decision", job_id.c_str());
            return false;
        }

        // 判定 JOB_SUCCESS：所有预期 shard 都 SUCCESS
        if (success_count >= expected_shard_count)
        {
            MarkJobTerminal(job_id, JobStatus::JOB_SUCCESS);
            LOG_INFO("ResultCollectorService::CheckJobDone job=%s: %d/%d SUCCESS → JOB_SUCCESS",
                     job_id.c_str(), success_count, expected_shard_count);
            return true;
        }

        // 判定 JOB_FAILED：没有进行中的 shard 且至少有一个 FAILED
        if (in_progress == 0 && failed_count > 0
            && success_count + failed_count >= expected_shard_count)
        {
            MarkJobTerminal(job_id, JobStatus::JOB_FAILED);
            LOG_INFO("ResultCollectorService::CheckJobDone job=%s: %d success, %d failed → JOB_FAILED",
                     job_id.c_str(), success_count, failed_count);
            return true;
        }

        // 还有进行中的 shard，继续等待
        LOG_INFO("ResultCollectorService::CheckJobDone job=%s: %d success, %d failed, "
                 "%d in-progress (%d/%d total) – still running",
                 job_id.c_str(), success_count, failed_count, in_progress,
                 success_count + failed_count + in_progress, expected_shard_count);
        return false;
    }

    /// @brief Merge 互斥集：防止并发/重复 merge 毁掉成品视频（修复 #7）
    /// key=job_id，value=true 表示该 job 正在被 merge 或已 merge 完成
    static std::mutex& MergeMutex() { static std::mutex m; return m; }
    static std::unordered_set<std::string>& MergingJobs() {
        static std::unordered_set<std::string> s; return s;
    }

    /// @brief 标记 job 进入终态并通知 JobService
    ///
    /// 在 JobStore 中更新 job 状态，并通过 RPC 同步到 JobService 进程。
    /// 阶段 5 新增：提取为独立方法，JOB_SUCCESS 和 JOB_FAILED 共用。
    // static：仅操作单例 Store / RPC，不依赖实例状态，供静态线程函数
    // TerminalSweepLoop 直接调用
    static void MarkJobTerminal(const std::string& job_id, JobStatus status)
    {
        // 更新本地 JobStore
        auto job_opt = JobStore::GetInstance().Get(job_id);
        if (job_opt.has_value())
        {
            auto job = job_opt.value();
            // 状态单调性检查：只接受状态升级
            if (static_cast<int32_t>(status) > job.status)
            {
                job.status = static_cast<int32_t>(status);
                job.updated_at = NowMs();
                JobStore::GetInstance().Update(job_id, job);
            }
        }
        else
        {
            // job 不在本地 JobStore 中，创建最小副本
            JobRecord local_job;
            local_job.job_id     = job_id;
            local_job.status     = static_cast<int32_t>(status);
            local_job.created_at = NowMs();
            local_job.updated_at = NowMs();
            JobStore::GetInstance().Insert(local_job);
        }

        // ── JOB_SUCCESS 时合并所有 shard 输出为完整视频（阶段 6 遗留项 1） ──
        // 阶段 8 修复 #7：加互斥锁 + 原子标志，保证 merge 恰好执行一次
        if (status == JobStatus::JOB_SUCCESS)
        {
            {
                std::lock_guard<std::mutex> lock(MergeMutex());
                if (MergingJobs().count(job_id))
                {
                    LOG_INFO("ResultCollectorService: merge for job=%s already in progress "
                             "or completed, skipping (fix #7)", job_id.c_str());
                    goto skip_merge;
                }
                MergingJobs().insert(job_id);
            }

            auto shards = ShardStore::GetInstance().ListByJob(job_id);

            std::sort(shards.begin(), shards.end(),
                      [](const ShardRecord& a, const ShardRecord& b) {
                          return a.shard_index < b.shard_index;
                      });

            std::vector<std::string> merge_inputs;
            for (const auto& s : shards)
            {
                if (!s.output_path.empty())
                    merge_inputs.push_back(s.output_path);
            }

            if (!merge_inputs.empty() && job_opt.has_value())
            {
                std::string output_dir = job_opt->output_path;
                if (output_dir.empty() && !merge_inputs.empty())
                {
                    const auto& first = merge_inputs[0];
                    size_t pos = first.find_last_of('/');
                    if (pos != std::string::npos)
                        output_dir = first.substr(0, pos);
                }
                if (output_dir.empty())
                {
                    output_dir = MprpcApplication::GetConfig().Load("ffmpeg_work_dir");
                    if (output_dir.empty()) output_dir = "/tmp/transcode_worker";
                }
                // 阶段 8 修复 #7：filelist 用含 job_id 的唯一名，防止跨 job 共享
                // （改造在 ffmpeg_executor.cpp 的 Merge 中——调用方通过 output_dir
                //  隔离；此处传入的 output_dir 已包含 job_id 标识）
                std::string merged_output = output_dir + "/" + job_id + "_merged.mp4";
                LOG_INFO("ResultCollectorService: merging %zu shards for job=%s → %s",
                         merge_inputs.size(), job_id.c_str(), merged_output.c_str());

                auto merge_result = FfmpegExecutor::Merge(merge_inputs, merged_output);
                if (merge_result.success)
                {
                    LOG_INFO("ResultCollectorService: merge SUCCESS for job=%s, output=%s "
                             "(elapsed=%lldms)",
                             job_id.c_str(), merged_output.c_str(),
                             (long long)merge_result.elapsed_ms);

                    for (const auto& path : merge_inputs)
                    {
                        if (std::remove(path.c_str()) == 0)
                            LOG_DEBUG("ResultCollectorService: removed shard file %s", path.c_str());
                        else
                            LOG_WARN("ResultCollectorService: failed to remove shard file %s: %s",
                                     path.c_str(), std::strerror(errno));
                    }
                }
                else
                {
                    LOG_WARN("ResultCollectorService: merge FAILED for job=%s: %s",
                             job_id.c_str(), merge_result.error_msg.c_str());
                }
            }
            else
            {
                LOG_WARN("ResultCollectorService: no output paths to merge for job=%s",
                         job_id.c_str());
            }
        }
        skip_merge: ;

        // ── 反向通知 JobService（跨进程同步） ─────────────────
        {
            MprpcChannel js_channel;
            JobService_Stub js_stub(&js_channel);

            UpdateJobStatusRequest update_req;
            update_req.set_job_id(job_id);
            update_req.set_status(status);

            // 同步所有 shard 的最新状态到 JobService
            auto shards = ShardStore::GetInstance().ListByJob(job_id);
            for (const auto& s : shards)
            {
                auto* si = update_req.add_shards();
                si->set_shard_id(s.shard_id);
                si->set_job_id(s.job_id);
                si->set_shard_index(s.shard_index);
                si->set_status(static_cast<ShardStatus>(s.status));
                si->set_assigned_worker_id(s.assigned_worker_id);
                si->set_attempt_id(s.attempt_id);
                si->set_retry_count(s.retry_count);
                si->set_max_retry(s.max_retry);   // 修复：防止 JobService 侧 max_retry 被覆盖成 0
                si->set_output_path(s.output_path);
                si->set_target_resolution(s.target_resolution);
                si->set_target_bitrate(s.target_bitrate);
            }

            UpdateJobStatusResponse update_resp;
            MprpcController update_ctrl;
            update_ctrl.SetTimeoutMs(3000);

            js_stub.UpdateJobStatus(&update_ctrl, &update_req, &update_resp, nullptr);

            if (!update_ctrl.Failed() && update_resp.error_code() == 0)
            {
                LOG_INFO("ResultCollectorService: notified JobService job=%s → %d",
                         job_id.c_str(), static_cast<int>(status));
            }
            else
            {
                LOG_WARN("ResultCollectorService: failed to notify JobService for job=%s: %s",
                         job_id.c_str(),
                         update_ctrl.Failed() ? update_ctrl.ErrorText().c_str()
                                              : update_resp.error_msg().c_str());
            }
        }

        // ── 反向通知 Scheduler（跨进程同步，修复重复调度） ────────
        // 问题：任务到达终态（SUCCESS/FAILED）后，Scheduler 的本地 Store 里
        // 该 job 的 shard 可能仍是 ASSIGNED/RUNNING（Worker 执行期间 ResultCollector
        // 与 Scheduler 进程隔离，Scheduler 不知任务已完成）。Worker 死亡后这些
        // shard 会被超时重置为 WAITING 并重新分配，导致已完成任务的 shard 被
        // 重复转码。
        // 修复：终态时通知 Scheduler 清理该 job 的所有非终态 shard（标记
        // SHARD_CANCELED），使超时扫描与分配循环都跳过它们。best-effort，
        // 失败只打 WARN（Scheduler 重启后的启动恢复扫描会兜底处理残留）。
        {
            MprpcChannel sched_channel;
            SchedulerService_Stub sched_stub(&sched_channel);

            CancelJobShardsRequest cancel_req;
            cancel_req.set_job_id(job_id);
            cancel_req.set_reason("JOB_TERMINAL");

            CancelJobShardsResponse cancel_resp;
            MprpcController cancel_ctrl;
            cancel_ctrl.SetTimeoutMs(3000);

            sched_stub.CancelJobShards(&cancel_ctrl, &cancel_req, &cancel_resp, nullptr);

            if (!cancel_ctrl.Failed() && cancel_resp.error_code() == 0)
            {
                LOG_INFO("ResultCollectorService: notified Scheduler to finalize "
                         "shards for job=%s (skipped=%d)", job_id.c_str(),
                         cancel_resp.skipped_count());
            }
            else
            {
                LOG_WARN("ResultCollectorService: failed to notify Scheduler for "
                         "job=%s: %s", job_id.c_str(),
                         cancel_ctrl.Failed() ? cancel_ctrl.ErrorText().c_str()
                                              : cancel_resp.error_msg().c_str());
            }
        }
    }
};

// ============================================================================
// TerminalSweepLoop — 周期终态扫描（阶段 8 #10 修复）
// ============================================================================
//
// 背景：CheckJobDone 在 expected_shard_count 未知时依赖 QueryJob RPC，
// 若 JobService 在最后一次 shard 上报时不可用 → job 永久停在非终态。
// 本线程定期扫描所有非终态 job，重新调用 CheckJobDone 判定。
//
// ⚠️ 修复（阶段 8）：此前内联了一份「简化版」终态判定，只改本地 JobStore
// 状态，缺失 Merge 产物 / 通知 JobService / 通知 Scheduler，产生半成品终态。
// 现改为预筛后直接调用完整 CheckJobDone，与正常上报路径共用同一套逻辑。

void ResultCollectorServiceImpl::TerminalSweepLoop(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kSweepIntervalMs = 15000;  // 每 15 秒扫描一次

    LOG_INFO("TerminalSweepLoop thread started, interval=%lldms",
             (long long)kSweepIntervalMs);

    while (!stop_flag)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kSweepIntervalMs));
        if (stop_flag) break;

        auto all_jobs = JobStore::GetInstance().ListAll();
        int checked = 0, resolved = 0;

        for (const auto& job : all_jobs)
        {
            int st = job.status;
            // 只检查非终态 job（PENDING / SCHEDULING / RUNNING）
            if (st == static_cast<int32_t>(JobStatus::JOB_SUCCESS)
                || st == static_cast<int32_t>(JobStatus::JOB_FAILED)
                || st == static_cast<int32_t>(JobStatus::JOB_CANCELED))
                continue;

            // 检查该 job 的所有 shard 是否都处于终态
            auto shards = ShardStore::GetInstance().ListByJob(job.job_id);
            if (shards.empty()) continue;

            bool all_terminal = true;
            int success_count = 0, failed_count = 0;
            for (const auto& s : shards)
            {
                int sst = s.status;
                if (sst == static_cast<int32_t>(ShardStatus::SHARD_SUCCESS))
                    ++success_count;
                else if (sst == static_cast<int32_t>(ShardStatus::SHARD_FAILED)
                      || sst == static_cast<int32_t>(ShardStatus::SHARD_CANCELED))
                    ++failed_count;
                else
                    all_terminal = false;
            }

            ++checked;
            if (all_terminal && (success_count > 0 || failed_count > 0))
            {
                LOG_INFO("TerminalSweepLoop: job=%s has %d success + %d failed shards "
                         "all terminal, checking job done",
                         job.job_id.c_str(), success_count, failed_count);

                // 调用完整 CheckJobDone：终态判定 + Merge 产物 +
                // 通知 JobService（UpdateJobStatus）+ 通知 Scheduler
                // （CancelJobShards JOB_TERMINAL），与正常上报路径一致
                if (CheckJobDone(job.job_id))
                    ++resolved;
            }
        }

        if (checked > 0 || resolved > 0)
            LOG_INFO("TerminalSweepLoop: checked %d jobs, resolved %d", checked, resolved);
    }

    LOG_INFO("TerminalSweepLoop thread stopped");
}

// ============================================================================
// main — 服务入口
// ============================================================================

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
    provider.NotifyService(new ResultCollectorServiceImpl());

    // 启动终态扫描后台线程（阶段 8 #10 修复：RC 重启后/JobService 不可用时的兜底）
    std::atomic<bool> stop_flag{false};
    std::thread sweep_thread(ResultCollectorServiceImpl::TerminalSweepLoop,
                             std::ref(stop_flag));

    if (!provider.Run())
    {
        LOG_ERROR("ResultCollectorService start failed");
        stop_flag = true;
        sweep_thread.join();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    stop_flag = true;
    sweep_thread.join();
    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
