#include <string>
#include <cstdlib>
#include <algorithm>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include <unordered_set>
#include "result.pb.h"
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
#include "video_platform/mq_client.h"
#include "video_platform/ffmpeg_executor.h"

using namespace video_platform;


/// @brief 从 attempt_id 字符串中解析重试次数
// attempt_id 格式：{shard_id}_attempt_{N}
static int parseRetryFromAttempt(const std::string& attempt_id)
{
    auto pos = attempt_id.rfind('_');
    if (pos == std::string::npos) return 0;
    try { return std::stoi(attempt_id.substr(pos + 1)); }
    catch (...) { return 0; }
}

class ResultCollectorServiceImpl : public ResultCollectorService {
public:
    static void TerminalSweepLoop(std::atomic<bool>& stop_flag);

    // 由 WorkerServiceImpl::MockExecute() 在 shard 执行完成后调用
    void ReportShardResult(::google::protobuf::RpcController* controller,
                           const ::ReportShardResultRequest* request,
                           ::ReportShardResultResponse* response,
                           ::google::protobuf::Closure* done) override
    {
        HandleReportShardResult(request, response);
        done->Run();
    }

    // HandleReportShardResult — 接收并聚合 Worker 上报的 shard 执行结果
    static void HandleReportShardResult(const ::ReportShardResultRequest* request,
                                        ::ReportShardResultResponse* response)
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

        // 1. 从 MySQL 获取 shard
        auto shard_opt = ShardStore::GetInstance().Get(shard_id);
        if (!shard_opt.has_value())
        {
            LOG_ERROR("ResultCollectorService::ReportShardResult shard_id=%s not found "
                      "in MySQL (job=%s), rejecting result", shard_id.c_str(), job_id.c_str());
            response->set_error_code(1);
            response->set_error_msg("shard not found: " + shard_id);
            response->set_accepted(false);
            response->set_job_done(false);
            return;
        }
        auto shard = shard_opt.value();

        // 2. attempt_id 幂等校验：拒绝旧 attempt 的过期结果
        int stored_retry = parseRetryFromAttempt(shard.attempt_id);     // 最新进度
        int incoming_retry = parseRetryFromAttempt(attempt_id);         // 本次进度
        if (!shard.attempt_id.empty() && incoming_retry < stored_retry)
        {
            LOG_INFO("ResultCollectorService: rejecting stale result for shard %s "
                     "(incoming attempt retry=%d < stored retry=%d)",
                     shard_id.c_str(), incoming_retry, stored_retry);
            response->set_error_code(0);
            response->set_error_msg("stale result from old attempt, ignored");
            response->set_accepted(false);
            response->set_job_done(false);
            return;
        }

        // 3. 幂等检查1：已 SUCCESS 的不再处理
        // shard.attempt_id：MySQL 里 shard 记录的 attempt_id
        // attempt_id：Worker 上报请求里带的 attempt_id（这次结果对应的是第几次执行）
        if (shard.status == static_cast<int32_t>(ShardStatus::SHARD_SUCCESS))
        {
            LOG_INFO("ResultCollectorService: shard %s already SUCCESS (idempotent)",
                     shard_id.c_str());
            response->set_error_code(0);
            response->set_error_msg("");
            response->set_accepted(true);
            response->set_job_done(CheckJobDone(job_id));
            return;
        }

        // 3. 幂等检查2：忽略同一 attempt 的重复上报,执行重试逻辑
        if ((shard.status == static_cast<int32_t>(ShardStatus::SHARD_FAILED)
          || shard.status == static_cast<int32_t>(ShardStatus::SHARD_RETRYING))
            && shard.attempt_id == attempt_id)
        {
            LOG_INFO("ResultCollectorService: shard %s already processed for same "
                     "attempt=%s (status=%d, idempotent, fix #8/#3)",
                     shard_id.c_str(), attempt_id.c_str(), shard.status);
            response->set_error_code(0);
            response->set_error_msg("duplicate result for same attempt, ignored");
            response->set_accepted(true);
            response->set_job_done(CheckJobDone(job_id));
            return;
        }

        // 4. 更新 shard 状态，记录最新的shard
        {
            int from_status = shard.status;
            shard.status = request->is_success()
                ? static_cast<int32_t>(ShardStatus::SHARD_SUCCESS)
                : static_cast<int32_t>(ShardStatus::SHARD_FAILED);
            shard.attempt_id = attempt_id;
            shard.assigned_worker_id = worker_id;
            shard.output_path = request->output_path();
            shard.updated_at = NowMs();
            ShardStore::GetInstance().UpdateIfStatus(shard_id, {from_status}, shard);
        }

        LOG_INFO("ResultCollectorService: shard %s → %s (attempt=%s)",
                 shard_id.c_str(),
                 request->is_success() ? "SUCCESS" : "FAILED",
                 attempt_id.c_str());

        // shard 结果落定后失效进度缓存，保证查到的shard为最新状态
        // 这里不更新redis是考虑redis经典设计
        {
            auto& redis = RedisClient::GetInstance();
            if (redis.inited() && redis.enabled())
                redis.Del("job:progress:" + job_id);
        }

        // 5. 若执行失败，调用 Scheduler.RescheduleShard 触发重试
        if (!request->is_success())
        {
            LOG_INFO("ResultCollectorService: triggering RescheduleShard for %s (reason=WORKER_FAILED)",
                     shard_id.c_str());

            // RescheduleShard RPC 带重试+退避（3 次，各 1s/2s/4s 间隔）
            bool reschedule_ok = false;     // 区分RPC是否调用成功
            bool last_was_network = false;  // 区分网络失败与确定性拒绝
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
                // #3 修复：携带本次失败上报的 attempt_id，
                // Scheduler 按 attempt 幂等，重复触发不双倍消耗 retry_count
                rs_req.set_attempt_id(attempt_id);

                RescheduleShardResponse rs_resp;
                MprpcController rs_ctrl;
                rs_ctrl.SetTimeoutMs(5000);

                sched_stub.RescheduleShard(&rs_ctrl, &rs_req, &rs_resp, nullptr);

                // 重试成功，break循环
                if (!rs_ctrl.Failed() && rs_resp.accepted())
                {
                    LOG_INFO("ResultCollectorService: RescheduleShard accepted for %s "
                             "(attempt %d)", shard_id.c_str(), rs_attempt);
                    reschedule_ok = true;
                    break;
                }
                // 失败分两类：
                // 网络/框架层失败（Failed()）——不知道远端是否已处理
                // 业务层确定性拒绝（!Failed() && !accepted()）——远端明确答复（如 max_retry 耗尽），保持 FAILED
                else if (rs_ctrl.Failed())
                {
                    last_was_network = true;
                    LOG_WARN("ResultCollectorService: RescheduleShard network failed "
                             "for %s (attempt %d/3): %s",
                             shard_id.c_str(), rs_attempt,
                             rs_ctrl.ErrorText().c_str());
                }
                else
                {
                    last_was_network = false;
                    LOG_WARN("ResultCollectorService: RescheduleShard rejected for %s "
                             "(attempt %d/3): %s",
                             shard_id.c_str(), rs_attempt,
                             rs_resp.error_msg().c_str());
                }
            }

            // 调用重试RPC成功，更新shard信息
            if (reschedule_ok)
            {
                
                shard.status = static_cast<int32_t>(ShardStatus::SHARD_RETRYING);
                shard.updated_at = NowMs();
                ShardStore::GetInstance().UpdateIfStatus(
                    shard_id, {static_cast<int32_t>(ShardStatus::SHARD_FAILED)}, shard);
            }
            // 三次调用都失败了，但是这是网络错误
            else if (last_was_network)
            {
                LOG_ERROR("ResultCollectorService: RescheduleShard FAILED after 3 "
                          "network retries for %s, marking RETRYING",
                          shard_id.c_str());
                shard.status = static_cast<int32_t>(ShardStatus::SHARD_RETRYING);
                shard.updated_at = NowMs();
                ShardStore::GetInstance().UpdateIfStatus(
                    shard_id, {static_cast<int32_t>(ShardStatus::SHARD_FAILED)}, shard);
            }
            // 确定性拒绝：保持 FAILED
            else 
            {
                LOG_WARN("ResultCollectorService: RescheduleShard deterministically "
                         "rejected for %s (e.g. max retry), keeping FAILED ",
                         shard_id.c_str());
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
    }

private:
    //检查 job 下所有 shard 是否都已进入终态,如果是，进入MarkJobTerminal
    static bool CheckJobDone(const std::string& job_id)
    {
        auto shards = ShardStore::GetInstance().ListByJob(job_id);
        if (shards.empty()) return false;

        // 从 JobStore（MySQL）获取预期的 shard 总数。=
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
                ++in_progress;  
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

    // Merge 互斥集：防止并发/重复 merge 毁掉成品视频
    static std::mutex& MergeMutex() { static std::mutex m; return m; }
    static std::unordered_set<std::string>& MergingJobs() 
    {
        static std::unordered_set<std::string> s; 
        return s;
    }

    // MarkJobTerminal — job 进入终态后的统一处理
    static void MarkJobTerminal(const std::string& job_id, JobStatus status)
    {
        auto& metrics_reg = mprpc::MetricsRegistry::GetInstance();
        if (status == JobStatus::JOB_SUCCESS)
        {
            metrics_reg.Counter("job_success_total", "转码成功的任务总数").Inc();
        }
        else if (status == JobStatus::JOB_FAILED)
        {
            metrics_reg.Counter("job_failed_total", "转码失败的任务总数").Inc();
        }

        // 更新 JobStore（MySQL）
        auto job_opt = JobStore::GetInstance().Get(job_id);
        if (job_opt.has_value())
        {
            auto job = job_opt.value();
            // 状态单调性检查：只接受状态升级
            if (static_cast<int32_t>(status) > job.status)
            {
                int from_status = job.status;   // 快照前置状态
                job.status = static_cast<int32_t>(status);
                job.updated_at = NowMs();
                // 条件更新：仅当仍是快照状态时推进（防 Scheduler 并发覆盖）
                JobStore::GetInstance().UpdateIfStatus(job_id, {from_status}, job);
            }
        }
        else
        {
            // job 记录缺失（异常）：创建最小副本，避免终态判定失效
            JobRecord local_job;
            local_job.job_id     = job_id;
            local_job.status     = static_cast<int32_t>(status);
            local_job.created_at = NowMs();
            local_job.updated_at = NowMs();
            JobStore::GetInstance().Insert(local_job);
        }

        if (status == JobStatus::JOB_SUCCESS)
        {
            std::lock_guard<std::mutex> lock(MergeMutex());
            if (MergingJobs().count(job_id))
            {
                LOG_INFO("ResultCollectorService: merge for job=%s already in progress "
                         "or completed, skipping (fix #7)", job_id.c_str());
                goto skip_merge;
            }
            MergingJobs().insert(job_id);

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
                    MergingJobs().erase(job_id);
                    LOG_ERROR("ResultCollectorService: merge FAILED for job=%s: %s "
                              "(will retry via TerminalSweepLoop)",
                              job_id.c_str(), merge_result.error_msg.c_str());
                    return; 
                }
            }
            else
            {
                LOG_WARN("ResultCollectorService: no output paths to merge for job=%s",
                         job_id.c_str());
            }
        }
        skip_merge: ;
    }
};

// TerminalSweepLoop — 周期终态扫描
// 兜底轮询所有job,判断是否job任务可能完成
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

                // 调用完整 CheckJobDone：终态判定 + Merge 产物
                if (CheckJobDone(job.job_id))
                    ++resolved;
            }
        }

        if (checked > 0 || resolved > 0)
            LOG_INFO("TerminalSweepLoop: checked %d jobs, resolved %d", checked, resolved);
    }

    LOG_INFO("TerminalSweepLoop thread stopped");
}


// 消费 Worker 上报的执行结果消息，复用公共聚合逻辑
static void MqResultConsumeLoop(std::atomic<bool>& stop_flag)
{
    auto& mq = MqClient::GetInstance();
    LOG_INFO("MqResultConsumeLoop thread started (consume result.pending)");

    while (!stop_flag)
    {
        // 1. 确保连接可用（MQ 故障时等待重连；期间 Worker 走直连 RPC）
        if (!mq.connected())
        {
            if (!mq.Reconnect())
            {
                LOG_WARN("MqResultConsumeLoop: MQ unavailable, retry in 2s "
                         "(workers fall back to direct RPC)");
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }
            LOG_INFO("MqResultConsumeLoop: MQ reconnected, resume consuming");
        }

        // 2. 阻塞消费（2s 超时：可感知 stop_flag 与连接状态变化）
        std::string body;
        int64_t delivery_tag = 0;
        if (!mq.ConsumeBlocking("result.pending", body, delivery_tag, 2000))
        {
            if (stop_flag) break;
            continue;
        }

        // 3. 解析并处理
        ::ReportShardResultRequest req;
        ::ReportShardResultResponse resp;
        if (req.ParseFromString(body))
        {
            ResultCollectorServiceImpl::HandleReportShardResult(&req, &resp);
        }
        else
        {
            LOG_WARN("MqResultConsumeLoop: failed to parse result message "
                     "(len=%zu), ack & drop", body.size());
        }

        // 4. 确认消息（失败 → Broker 重投 → 幂等兜底）
        if (!mq.Ack(delivery_tag))
        {
            LOG_WARN("MqResultConsumeLoop: ack failed, broker may redeliver");
        }
    }

    LOG_INFO("MqResultConsumeLoop thread stopped");
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

    if (!MysqlPool::GetInstance().Init())
    {
        LOG_ERROR("MysqlPool init failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // 阶段 10：Redis/MQ 是可降级组件，Init 失败只 WARN 不拒绝启动
    RedisClient::GetInstance().Init();
    MqClient::GetInstance().Init();

    // ── 阶段 11：可观测性（metrics_port<=0 时不启用，可降级组件） ──
    int metrics_port = MprpcApplication::GetConfig().LoadInt("metrics_port", 0, 0, 65535);
    mprpc::MetricsHttpServer metrics_server;
    metrics_server.Init(metrics_port);
    auto& metrics_reg = mprpc::MetricsRegistry::GetInstance();
    // 内置日志告警兜底：
    // 1. 本进程出站 RPC P99 延迟 > 1000ms
    // 2. 任务失败率（5 分钟窗口，RateEstimator 增量估算）。
    //    内置口径：失败数 / 终态数（本进程同时维护 success/failed 两个
    //    counter，无需跨进程读 job_submitted_total）；
    //    Prometheus 侧口径为 failed/submitted（跨实例聚合，见 alerts.yml）
    metrics_reg.RegisterAlertRule(
        {"rpc_latency_p99_high", "WARN",
         []() {
             return mprpc::MetricsRegistry::GetInstance()
                 .HistogramQuantile("rpc_latency_ms", 0.99);
         },
         1000, true, 0, "本进程 RPC P99 延迟超过 1000ms"});
    mprpc::RateEstimator failed_estimator(5 * 60 * 1000);    // 失败计数增量速率
    mprpc::RateEstimator terminal_estimator(5 * 60 * 1000);  // 终态计数增量速率
    metrics_reg.RegisterAlertRule(
        {"job_failed_rate_high", "WARN",
         [&]() {
             double rf = failed_estimator.Observe(
                 metrics_reg.Counter("job_failed_total", "转码失败的任务总数").Value());
             double rt = terminal_estimator.Observe(
                 metrics_reg.Counter("job_failed_total", "转码失败的任务总数").Value() +
                 metrics_reg.Counter("job_success_total", "转码成功的任务总数").Value());
             return rt > 0 ? rf / std::max(rt, 0.001) : 0.0;
         },
         0.3, true, 0, "5 分钟窗口任务失败率超过 30%"});
    metrics_server.Start();

    RpcProvider provider;
    provider.NotifyService(new ResultCollectorServiceImpl());

    // 启动终态扫描后台线程（阶段 8 #10 修复：RC 重启后/JobService 不可用时的兜底）
    std::atomic<bool> stop_flag{false};
    std::thread sweep_thread(ResultCollectorServiceImpl::TerminalSweepLoop,
                             std::ref(stop_flag));
    // 阶段 10：MQ 消费线程（result.pending，Push 聚合）
    std::thread mq_thread(MqResultConsumeLoop, std::ref(stop_flag));

    if (!provider.Run())
    {
        LOG_ERROR("ResultCollectorService start failed");
        stop_flag = true;
        if (sweep_thread.joinable()) sweep_thread.join();
        if (mq_thread.joinable()) mq_thread.join();
        metrics_server.Stop();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    stop_flag = true;
    if (sweep_thread.joinable()) sweep_thread.join();
    if (mq_thread.joinable()) mq_thread.join();
    metrics_server.Stop();
    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
