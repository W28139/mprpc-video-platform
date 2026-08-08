#include <string>
#include <cstdlib>
#include <thread>
#include <atomic>
#include <chrono>
#include "job.pb.h"
#include "scheduler.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "mprpcmetrics.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"
#include "video_platform/mysql_pool.h"

using namespace video_platform;

// ============================================================================
// JobService — 接收用户提交任务、查询任务状态、取消任务
// ============================================================================

class JobServiceImpl : public JobService {
public:
    /// @brief 提交视频处理任务
    
    // 对外接口，用于提交任务
    // 内部是把任务交给schedule做切分，把切分好的放入数据库，把结果返回过来，最后返给调用方
    // job进入pending(已提交，等待系统处理) -> schedule
    void SubmitJob(::google::protobuf::RpcController* controller,
                   const ::SubmitJobRequest* request,
                   ::SubmitJobResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::SubmitJob user=%s, input=%s, target_res=%s, shard_dur=%ds",
                 request->user_id().c_str(),
                 request->input_path().c_str(),
                 request->target_resolution().c_str(),
                 request->shard_duration_sec());

        // 参数校验
        if (request->input_path().empty())
        {
            response->set_error_code(1);
            response->set_error_msg("input_path is required");
            done->Run();
            return;
        }

        // 用 GenerateId 生成唯一 job_id
        // 把job的信息整合到 =JobRecord里
        std::string job_id = GenerateId("job");
        JobRecord job;
        job.job_id             = job_id;
        job.user_id            = request->user_id();
        job.input_path         = request->input_path();
        job.output_path        = request->output_path();
        job.target_format      = request->target_format();
        job.target_resolution  = request->target_resolution();
        job.target_bitrate     = request->target_bitrate();
        job.priority           = request->priority();
        job.status             = static_cast<int32_t>(JobStatus::JOB_PENDING);
        job.shard_count        = 0;
        job.shard_duration_sec = request->shard_duration_sec();
        job.created_at         = NowMs();
        job.updated_at         = NowMs();

        // 任务提交观测（参数校验通过、平台实际受理后才计数，保证 job_failed_rate 的分母 = 平台受理数）
        mprpc::MetricsRegistry::GetInstance()
            .Counter("job_submitted_total", "提交的任务总数").Inc();

        // 写入MySQL内存存储
        JobStore::GetInstance().Insert(job);
        LOG_INFO("JobService::SubmitJob created job_id=%s", job_id.c_str());

        // 构造响应
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_job_id(job_id);

        // 初步构造，不改变 Job 状态，不写入分片数
        auto* jobInfo = response->mutable_job_info();
        jobInfo->set_job_id(job.job_id);
        jobInfo->set_user_id(job.user_id);
        jobInfo->set_input_path(job.input_path);
        jobInfo->set_output_path(job.output_path);
        jobInfo->set_target_format(job.target_format);
        jobInfo->set_target_resolution(job.target_resolution);
        jobInfo->set_target_bitrate(job.target_bitrate);
        jobInfo->set_priority(job.priority);
        jobInfo->set_status(JobStatus::JOB_PENDING);
        jobInfo->set_shard_count(0);
        jobInfo->set_shard_duration_sec(job.shard_duration_sec);
        jobInfo->set_created_at(job.created_at);
        jobInfo->set_updated_at(job.updated_at);

        // 调用 Scheduler.ScheduleJob 触发切分和调度
        // 最多重试 3 次，防止单次网络抖动导致 job 永久卡死在 PENDING
        bool schedule_ok = false;
        int32_t shard_count_from_scheduler = 0;
        for (int attempt = 1; attempt <= 3; ++attempt)
        {
            // 利用 rpc 调用切片函数 ScheduleJob
            MprpcChannel sched_channel;
            SchedulerService_Stub sched_stub(&sched_channel);

            ScheduleJobRequest sched_req;
            *sched_req.mutable_job_info() = *jobInfo;
            sched_req.mutable_job_info()->set_duration_sec(job.duration_sec);

            ScheduleJobResponse sched_resp;
            MprpcController sched_ctrl;

            sched_stub.ScheduleJob(&sched_ctrl, &sched_req, &sched_resp, nullptr);
            // sched_resp.accepted() 通常请求已被受理 成功的话就进入，收集，退出
            if (!sched_ctrl.Failed() && sched_resp.accepted())
            {
                LOG_INFO("JobService::SubmitJob job_id=%s: Scheduler accepted (attempt %d)",
                         job_id.c_str(), attempt);
                // 直接从 ScheduleJobResponse 获取 shard_count（直接数据路径，避免侧信道）
                // shard 列表不再收集——数据已由 Scheduler 写入 MySQL 共享
                shard_count_from_scheduler = sched_resp.shard_count();
                schedule_ok = true;
                break;
            }
            // 第 atteempt 次没成功
            LOG_WARN("JobService::SubmitJob job_id=%s: Scheduler.ScheduleJob failed "
                     "(attempt %d/3): %s",
                     job_id.c_str(), attempt,
                     sched_ctrl.Failed() ? sched_ctrl.ErrorText().c_str()
                                         : sched_resp.error_msg().c_str());

            if (attempt < 3)
                std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // 切片成功后的逻辑
        if (schedule_ok)
        {
            // latest_job_opt的类型为JobRecord，用于更新状态信息、分片数信息、最新更新时间信息等信息
            auto latest_job_opt = JobStore::GetInstance().Get(job_id);
            // 查询数据库里有没有job_id这个任务
            if (latest_job_opt.has_value())
            {
                // 有的话，获取该 JobRecord 实例 (此时已经被schedule切片逻辑更新过了)
                auto latest_job = latest_job_opt.value(); 
                int from_status = latest_job.status;   // 快照前置状态（在修改最新状态前，先把状态保存下来）
                // 设置job的最新状态为 JOB_SCHEDULING
                // 更新数据库里该JobStore的信息
                latest_job.status = static_cast<int32_t>(JobStatus::JOB_SCHEDULING);
                latest_job.shard_count = shard_count_from_scheduler;
                latest_job.updated_at = NowMs();
                JobStore::GetInstance().UpdateIfStatus(job_id, {from_status}, latest_job);

                // 更新response信息
                jobInfo->set_status(JobStatus::JOB_SCHEDULING);
                jobInfo->set_shard_count(shard_count_from_scheduler);
                jobInfo->set_updated_at(latest_job.updated_at);
            }
 
            LOG_INFO("JobService::SubmitJob job_id=%s: %d shards created "
                     "(shared via MySQL)",
                     job_id.c_str(), shard_count_from_scheduler);
        }
        // 切片失败后的逻辑
        else
        {
            LOG_ERROR("JobService::SubmitJob job_id=%s: Scheduler.ScheduleJob failed "
                      "after 3 retries, job queued as PENDING – will be retried by "
                      "background PendingScanLoop",
                      job_id.c_str());
            // 告知客户端当前 Scheduler 不可用，任务已排队等待后台重试
            response->set_error_code(2);
            response->set_error_msg("Scheduler unavailable after 3 retries, "
                                    "job queued as PENDING and will be retried automatically");
        }

        done->Run();
    }



    // 查询任务状态和进度
    void QueryJob(::google::protobuf::RpcController* controller,
                  const ::QueryJobRequest* request,
                  ::QueryJobResponse* response,
                  ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::QueryJob job_id=%s", request->job_id().c_str());

        // 直查 MySQL（单条 PK + 一次 ListByJob 共 <0.5ms，无需缓存）
        auto job_opt = JobStore::GetInstance().Get(request->job_id());
        if (!job_opt.has_value())
        {
            response->set_error_code(1);
            response->set_error_msg("job not found: " + request->job_id());
            done->Run();
            return;
        }

        response->set_error_code(0);
        response->set_error_msg("");

        // 填充 job 信息
        auto* jobInfo = response->mutable_job_info();
        jobInfo->set_job_id(job_opt->job_id);
        jobInfo->set_user_id(job_opt->user_id);
        jobInfo->set_input_path(job_opt->input_path);
        jobInfo->set_output_path(job_opt->output_path);
        jobInfo->set_target_format(job_opt->target_format);
        jobInfo->set_target_resolution(job_opt->target_resolution);
        jobInfo->set_target_bitrate(job_opt->target_bitrate);
        jobInfo->set_priority(job_opt->priority);
        jobInfo->set_status(static_cast<JobStatus>(job_opt->status));
        jobInfo->set_shard_count(job_opt->shard_count);
        jobInfo->set_created_at(job_opt->created_at);
        jobInfo->set_updated_at(job_opt->updated_at);

        // 填充关联的 shard 列表，前面填入的信息，在这里被获取查询
        auto shards = ShardStore::GetInstance().ListByJob(request->job_id());
        for (const auto& s : shards)
        {
            auto* si = response->add_shards();
            si->set_shard_id(s.shard_id);
            si->set_job_id(s.job_id);
            si->set_shard_index(s.shard_index);
            si->set_start_ms(s.start_ms);
            si->set_duration_ms(s.duration_ms);
            si->set_status(static_cast<ShardStatus>(s.status));
            si->set_assigned_worker_id(s.assigned_worker_id);
            si->set_attempt_id(s.attempt_id);
            si->set_retry_count(s.retry_count);
            si->set_max_retry(s.max_retry);
            si->set_input_path(s.input_path);
            si->set_output_path(s.output_path);
            si->set_created_at(s.created_at);
            si->set_updated_at(s.updated_at);
        }

        done->Run();
    }

    // 取消任务
    void CancelJob(::google::protobuf::RpcController* controller,
                   const ::CancelJobRequest* request,
                   ::CancelJobResponse* response,
                   ::google::protobuf::Closure* done) override
    {
        LOG_INFO("JobService::CancelJob job_id=%s, reason=%s",
                 request->job_id().c_str(), request->reason().c_str());

        // 找不到该任务，直接取消
        auto job_opt = JobStore::GetInstance().Get(request->job_id());
        if (!job_opt.has_value())
        {
            response->set_error_code(1);
            response->set_error_msg("job not found: " + request->job_id());
            done->Run();
            return;
        }

        {
            auto job = job_opt.value();
            int from_status = job.status;
            job.status = static_cast<int32_t>(JobStatus::JOB_CANCELED);
            job.updated_at = NowMs();
            JobStore::GetInstance().UpdateIfStatus(request->job_id(), {from_status}, job);
        }

        // 传播取消到所有 shard
        // 将所有非终态 shard 标记为 CANCELED，使 SchedulingLoop 不再扫描它们。
        // 对已分配的 shard，best-effort 通知 Worker 取消执行。
        auto shards = ShardStore::GetInstance().ListByJob(request->job_id());
        int canceled_count = 0;
        for (auto& s : shards)
        {
            int st = s.status;
            int s_success  = static_cast<int32_t>(ShardStatus::SHARD_SUCCESS);
            int s_failed   = static_cast<int32_t>(ShardStatus::SHARD_FAILED);
            int s_canceled = static_cast<int32_t>(ShardStatus::SHARD_CANCELED);
            if (st == s_success || st == s_failed || st == s_canceled)
                continue; 
            int from_status = st;   // 快照前置状态
            s.status = s_canceled;
            s.updated_at = NowMs();
            ShardStore::GetInstance().UpdateIfStatus(s.shard_id, {from_status}, s);
            ++canceled_count;
        }
        LOG_INFO("JobService::CancelJob job_id=%s: canceled %d shards",
                 request->job_id().c_str(), canceled_count);

        // 委托 Scheduler 向 Worker 传播取消
        {
            MprpcChannel sched_channel;
            SchedulerService_Stub sched_stub(&sched_channel);

            CancelJobShardsRequest cancel_req;
            cancel_req.set_job_id(request->job_id());
            cancel_req.set_reason(request->reason());

            CancelJobShardsResponse cancel_resp;
            MprpcController cancel_ctrl;
            cancel_ctrl.SetTimeoutMs(5000);

            sched_stub.CancelJobShards(&cancel_ctrl, &cancel_req,
                                        &cancel_resp, nullptr);

            if (!cancel_ctrl.Failed() && cancel_resp.error_code() == 0)
            {
                LOG_INFO("JobService::CancelJob job_id=%s: Scheduler "
                         "propagated, total=%d notified=%d skipped=%d",
                         request->job_id().c_str(),
                         cancel_resp.total_shards(),
                         cancel_resp.notified_count(),
                         cancel_resp.skipped_count());
            }
            else
            {
                LOG_WARN("JobService::CancelJob job_id=%s: "
                         "Scheduler.CancelJobShards failed (%s), "
                         "workers not notified (best-effort)",
                         request->job_id().c_str(),
                         cancel_ctrl.Failed()
                             ? cancel_ctrl.ErrorText().c_str()
                             : cancel_resp.error_msg().c_str());
            }
        }

        response->set_error_code(0);
        response->set_error_msg("");
        done->Run();
    }

};


// 本线程定期扫描 PENDING job 并重试 ScheduleJob，确保：
// 1. Scheduler 重启后，之前 PENDING 的 job 能被重新切分调度
// 2. SubmitJob 重试耗尽后，job 不会被永久遗忘
static void PendingScanLoop(std::atomic<bool>& stop_flag)
{
    constexpr int64_t kScanIntervalMs = 10000;  // 每 10 秒扫描一次

    LOG_INFO("PendingScanLoop thread started, interval=%lldms",
             (long long)kScanIntervalMs);

    while (!stop_flag)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(kScanIntervalMs));
        if (stop_flag) break;

        // 扫描所有当前处于 PENDING 状态的 job
        auto all_jobs = JobStore::GetInstance().ListAll();
        std::vector<JobRecord> pending_jobs;
        for (const auto& j : all_jobs)
        {
            if (j.status == static_cast<int32_t>(JobStatus::JOB_PENDING))
                pending_jobs.push_back(j);
        }

        if (pending_jobs.empty()) continue;

        LOG_INFO("PendingScanLoop: found %zu PENDING jobs, retrying ScheduleJob...",
                 pending_jobs.size());

        for (const auto& job : pending_jobs)
        {
            // 调用 ScheduleJob 前先 Get 最新状态，已取消/已完成的任务直接跳过，不再触发无谓的切分 RPC
            auto latest_job_opt = JobStore::GetInstance().Get(job.job_id);
            if (!latest_job_opt.has_value()) continue;  // 任务已被删除（异常情况）
            auto latest_job = latest_job_opt.value();
            if (latest_job.status != static_cast<int32_t>(JobStatus::JOB_PENDING))
            {
                LOG_WARN("PendingScanLoop: job_id=%s status=%d no longer PENDING, "
                         "skip ScheduleJob (fix #7)",
                         job.job_id.c_str(), latest_job.status);
                continue;
            }

            // 构造 JobInfo
            JobInfo job_info;
            job_info.set_job_id(job.job_id);
            job_info.set_user_id(job.user_id);
            job_info.set_input_path(job.input_path);
            job_info.set_output_path(job.output_path);
            job_info.set_target_format(job.target_format);
            job_info.set_target_resolution(job.target_resolution);
            job_info.set_target_bitrate(job.target_bitrate);
            job_info.set_priority(job.priority);
            job_info.set_status(JobStatus::JOB_PENDING);
            job_info.set_shard_count(job.shard_count);
            job_info.set_shard_duration_sec(job.shard_duration_sec);
            job_info.set_duration_sec(job.duration_sec);

            // 调用 Scheduler.ScheduleJob
            MprpcChannel sched_channel;
            SchedulerService_Stub sched_stub(&sched_channel);

            ScheduleJobRequest sched_req;
            *sched_req.mutable_job_info() = job_info;

            ScheduleJobResponse sched_resp;
            MprpcController sched_ctrl;
            sched_ctrl.SetTimeoutMs(5000);

            // 重新调用
            sched_stub.ScheduleJob(&sched_ctrl, &sched_req, &sched_resp, nullptr);

            // 如果调用成功
            if (!sched_ctrl.Failed() && sched_resp.accepted())
            {
                // 更新 JobStore 中的状态和 shard_count
                // UpdateIfStatus 条件 {PENDING} 不满足则拒绝覆盖（已取消的任务不复活）
                latest_job.status = static_cast<int32_t>(JobStatus::JOB_SCHEDULING);
                latest_job.shard_count = sched_resp.shard_count();
                latest_job.updated_at = NowMs();
                if (!JobStore::GetInstance().UpdateIfStatus(
                        job.job_id,
                        {static_cast<int32_t>(JobStatus::JOB_PENDING)}, latest_job))
                {
                    LOG_WARN("PendingScanLoop: job_id=%s status changed during "
                             "ScheduleJob, skip status overwrite (fix #7)",
                             job.job_id.c_str());
                }

                // 不再同步 shard 列表（MySQL 共享，QueryJob 直接读取）
                LOG_INFO("PendingScanLoop: job_id=%s rescheduled successfully, "
                         "%d shards created (shared via MySQL)",
                         job.job_id.c_str(), sched_resp.shard_count());
            }
            // 失败
            else
            {
                LOG_WARN("PendingScanLoop: ScheduleJob still failed for job_id=%s: %s",
                         job.job_id.c_str(),
                         sched_ctrl.Failed() ? sched_ctrl.ErrorText().c_str()
                                             : sched_resp.error_msg().c_str());
            }
        }
    }

    LOG_INFO("PendingScanLoop thread stopped");
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

    // ── 阶段 11：可观测性（metrics_port<=0 时不启用，可降级组件） ──
    int metrics_port = MprpcApplication::GetConfig().LoadInt("metrics_port", 0, 0, 65535);
    mprpc::MetricsHttpServer metrics_server;
    metrics_server.Init(metrics_port);
    // 内置日志告警兜底（Prometheus 未部署时仍能发现异常）：
    // 本进程出站 RPC P99 延迟 > 1000ms
    mprpc::MetricsRegistry::GetInstance().RegisterAlertRule(
        {"rpc_latency_p99_high", "WARN",
         []() {
             return mprpc::MetricsRegistry::GetInstance()
                 .HistogramQuantile("rpc_latency_ms", 0.99);
         },
         1000, true, 0, "本进程 RPC P99 延迟超过 1000ms"});
    metrics_server.Start();

    RpcProvider provider;
    provider.NotifyService(new JobServiceImpl());

    // 启动 PENDING 扫描后台线程（阶段 8 修复：Scheduler 重启后自动重试 PENDING job）
    std::atomic<bool> stop_flag{false};
    std::thread pending_thread(PendingScanLoop, std::ref(stop_flag));

    if (!provider.Run())
    {
        LOG_ERROR("JobService start failed");
        stop_flag = true;
        pending_thread.join();
        metrics_server.Stop();
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    stop_flag = true;
    pending_thread.join();
    metrics_server.Stop();
    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
