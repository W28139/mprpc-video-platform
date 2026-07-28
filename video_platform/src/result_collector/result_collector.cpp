#include <string>
#include <cstdlib>
#include "result.pb.h"
#include "job.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include "video_platform/common_store.h"

using namespace video_platform;

// ============================================================================
// ResultCollectorService — 结果收集与聚合（阶段 4：mock 闭环）
// ============================================================================

/// @brief ResultCollectorService RPC 实现
///
/// 由 TranscodeWorker 的 MockExecute 线程通过 ZK 发现的
/// ResultCollectorService_Stub 调用。
///
/// 阶段 4 实现：
/// - ReportShardProgress：记录进度日志（ShardRecord 无 progress 字段，短期不做存储）
/// - ReportShardResult：聚合核心逻辑（见下方详细注释）
///
/// ⚠️ 跨进程存储备注：
/// ShardStore 是进程内单例，Worker 上报的 shard 在 ResultCollector 进程中
/// 可能不存在（由 Scheduler 在另一个进程中创建）。ReportShardResult 在
/// shard 不存在时会从请求参数构造本地副本。详见业务日志第 3 篇「踩坑记录」节。
///
/// 幂等设计（阶段 4）：
/// 已 SHARD_SUCCESS 的 shard 再次上报直接返回 accepted=true，不重复计数。
/// 阶段 5 将增加 attempt_id 校验：只有与当前 attempt_id 匹配的结果才被接受，
/// 旧 attempt 的结果自动忽略，防止 Worker 重试后旧结果覆盖新结果。
class ResultCollectorServiceImpl : public ResultCollectorService {
public:
    /// @brief Worker 上报 shard 执行进度
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

    /// @brief Worker 上报 shard 最终执行结果（聚合核心入口）
    ///
    /// 由 WorkerServiceImpl::MockExecute() 在 shard 执行完成后调用。
    /// 是阶段 4 闭环的最后一个 RPC 环节，决定 job 是否达到终态。
    ///
    /// 处理流程（4 步）：
    /// 1. 从 ShardStore 获取 shard；不存在则从请求参数构造本地副本（跨进程存储）
    /// 2. 幂等检查：已 SHARD_SUCCESS 的不再处理，直接返回（防重复上报）
    /// 3. 更新 ShardStore：status → SUCCESS/FAILED，回填 attempt_id / output_path
    /// 4. CheckJobDone：扫描 job 下所有 shard → 全部 SUCCESS → JOB_SUCCESS
    ///
    /// @note 并发安全：多个 Worker 可能同时上报同一 job 的不同 shard，
    ///       ShardStore 的读写锁保证单次 Update 原子，但 CheckJobDone
    ///       存在 TOCTOU 窗口（两次并发 SUCCESS 都会尝试更新 job 终态，
    ///       但幂等处理保证了安全性 — 第二次更新只是重复写 JOB_SUCCESS）。
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
        //    （ShardStore 是进程内存储，ResultCollector 需要自己的本地副本）
        auto shard_opt = ShardStore::GetInstance().Get(shard_id);
        if (!shard_opt.has_value())
        {
            // 首次收到该 shard 的结果：从请求参数构造本地 ShardRecord
            ShardRecord shard_copy;
            shard_copy.shard_id      = shard_id;
            shard_copy.job_id        = job_id;
            shard_copy.shard_index   = request->shard_index();  // 从请求透传，不再硬编码 0
            shard_copy.status        = static_cast<int32_t>(ShardStatus::SHARD_CREATED);
            shard_copy.assigned_worker_id = worker_id;
            shard_copy.attempt_id    = attempt_id;
            shard_copy.retry_count   = 0;
            shard_copy.max_retry     = 3;
            shard_copy.output_path   = request->output_path();
            shard_copy.created_at    = NowMs();
            shard_copy.updated_at    = NowMs();
            ShardStore::GetInstance().Insert(shard_copy);
            shard_opt = ShardStore::GetInstance().Get(shard_id);
            LOG_INFO("ResultCollectorService: created local shard copy shard_id=%s", shard_id.c_str());
        }

        auto shard = shard_opt.value();

        // 2. 幂等检查：已 SUCCESS 的不再处理
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

        // 3. 更新 shard 状态
        shard.status = request->is_success()
            ? static_cast<int32_t>(ShardStatus::SHARD_SUCCESS)
            : static_cast<int32_t>(ShardStatus::SHARD_FAILED);
        shard.attempt_id = attempt_id;
        shard.output_path = request->output_path();
        shard.updated_at = NowMs();
        ShardStore::GetInstance().Update(shard_id, shard);

        LOG_INFO("ResultCollectorService: shard %s → %s",
                 shard_id.c_str(),
                 request->is_success() ? "SUCCESS" : "FAILED");

        // 4. 判断 job 是否全部完成
        bool job_done = CheckJobDone(job_id);
        response->set_error_code(0);
        response->set_error_msg("");
        response->set_accepted(true);
        response->set_job_done(job_done);

        if (job_done)
        {
            LOG_INFO("ResultCollectorService: job %s ALL SHARDS DONE → JOB_SUCCESS", job_id.c_str());
        }

        done->Run();
    }

private:
    /// @brief 检查 job 下所有 shard 是否都已 SUCCESS，若是则标记 JOB_SUCCESS
    ///
    /// 由 ReportShardResult() 在每次收到 shard 结果后调用。
    ///
    /// 逻辑（阶段 4 修复：防止过早判定）：
    /// 1. ShardStore.ListByJob(job_id) 获取该 job 所有已上报 shard
    /// 2. 从 JobStore 读取预期的 shard_count（由 Scheduler 切分时设定）
    /// 3. 仅当 success_count >= expected_shard_count 时才判定完成，防止
    ///    ResultCollector 本地只有部分 shard 时过早标记 JOB_SUCCESS
    /// 4. 全部 SUCCESS → JobStore 更新为 JOB_SUCCESS
    /// 5. 若本地 JobStore 无此 job（跨进程存储），创建最小副本记录终态
    ///
    /// @return true 表示所有 shard 已 SUCCESS（job 终态已置）
    /// @return false 表示还有未完成的 shard
    bool CheckJobDone(const std::string& job_id)
    {
        auto shards = ShardStore::GetInstance().ListByJob(job_id);
        if (shards.empty()) return false;

        // 从 JobStore 获取预期的 shard 总数（由 Scheduler 切分时写入）
        int32_t expected_shard_count = 0;
        auto job_opt = JobStore::GetInstance().Get(job_id);
        if (job_opt.has_value())
        {
            expected_shard_count = job_opt->shard_count;
        }

        int success_count = 0;
        for (const auto& s : shards)
        {
            if (s.status != static_cast<int32_t>(ShardStatus::SHARD_SUCCESS))
            {
                // 有未完成的 shard
                return false;
            }
            ++success_count;
        }

        // 防止过早判定：已上报的 SUCCESS shard 数必须 >= 预期 shard 总数
        if (expected_shard_count > 0 && success_count < expected_shard_count)
        {
            LOG_INFO("ResultCollectorService::CheckJobDone job=%s: %d/%d shards SUCCESS, "
                     "waiting for remaining %d",
                     job_id.c_str(), success_count, expected_shard_count,
                     expected_shard_count - success_count);
            return false;
        }

        // 所有 shard 都是 SUCCESS → 更新/创建本地 job 状态
        if (job_opt.has_value())
        {
            auto job = job_opt.value();
            job.status = static_cast<int32_t>(JobStatus::JOB_SUCCESS);
            job.updated_at = NowMs();
            JobStore::GetInstance().Update(job_id, job);
        }
        else
        {
            // job 不在本地 JobStore 中（跨进程），创建最小副本
            JobRecord local_job;
            local_job.job_id     = job_id;
            local_job.status     = static_cast<int32_t>(JobStatus::JOB_SUCCESS);
            local_job.shard_count = static_cast<int32_t>(shards.size());
            local_job.created_at = NowMs();
            local_job.updated_at = NowMs();
            JobStore::GetInstance().Insert(local_job);
        }

        // ── 反向通知 JobService 更新状态（跨进程同步） ─────────────────
        // JobStore 是进程内存储，ResultCollector 的更新不会影响 JobService。
        // 通过 RPC 将终态推送给 JobService，让 QueryJob 能查到最新状态。
        {
            MprpcChannel js_channel;
            JobService_Stub js_stub(&js_channel);

            UpdateJobStatusRequest update_req;
            update_req.set_job_id(job_id);
            update_req.set_status(JobStatus::JOB_SUCCESS);
            // 不设置 shard_count — Scheduler 已在切分时正确写入，避免用本地
            // ShardStore 的不完整数据覆盖

            UpdateJobStatusResponse update_resp;
            MprpcController update_ctrl;
            update_ctrl.SetTimeoutMs(3000);

            js_stub.UpdateJobStatus(&update_ctrl, &update_req, &update_resp, nullptr);

            if (!update_ctrl.Failed() && update_resp.error_code() == 0)
            {
                LOG_INFO("ResultCollectorService: notified JobService job=%s → SUCCESS",
                         job_id.c_str());
            }
            else
            {
                LOG_WARN("ResultCollectorService: failed to notify JobService for job=%s: %s",
                         job_id.c_str(),
                         update_ctrl.Failed() ? update_ctrl.ErrorText().c_str()
                                              : update_resp.error_msg().c_str());
            }
        }

        LOG_INFO("ResultCollectorService::CheckJobDone job=%s: %d/%zu shards SUCCESS",
                 job_id.c_str(), success_count, shards.size());
        return true;
    }
};

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

    if (!provider.Run())
    {
        LOG_ERROR("ResultCollectorService start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
