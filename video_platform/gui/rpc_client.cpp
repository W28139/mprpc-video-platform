#include "video_platform/gui/rpc_client.h"

#include <QPointer>
#include <QRunnable>
#include <QThreadPool>

#include "mprpcchannel.h"
#include "mprpccontroller.h"

namespace video_platform {

// ============================================================================
// QRunnable — 后台线程执行一次同步 RPC，完成后 emit 信号
// ============================================================================

namespace {

template <typename T>
struct RunResult {
    bool    ok = false;
    T       resp;
    QString err;
};

// SubmitJob
class SubmitTask : public QRunnable {
public:
    SubmitTask(std::shared_ptr<JobService_Stub> stub, SubmitJobRequest req,
               std::function<void(bool, const QString&, const QString&)> cb)
        : stub_(std::move(stub)), req_(std::move(req)), cb_(std::move(cb)) {}
    void run() override
    {
        SubmitJobResponse resp;
        MprpcController ctrl;
        ctrl.SetTimeoutMs(10000);   // 提交要等 Scheduler 切分，给足时间
        stub_->SubmitJob(&ctrl, &req_, &resp, nullptr);
        if (ctrl.Failed())
        {
            cb_(false, "", QString::fromStdString(ctrl.ErrorText()));
            return;
        }
        if (resp.error_code() != 0)
        {
            cb_(false, QString::fromStdString(resp.job_id()),
                QString::fromStdString(resp.error_msg()));
            return;
        }
        cb_(true, QString::fromStdString(resp.job_id()), "");
    }
private:
    std::shared_ptr<JobService_Stub> stub_;
    SubmitJobRequest req_;
    std::function<void(bool, const QString&, const QString&)> cb_;
};

// QueryJob
class QueryTask : public QRunnable {
public:
    QueryTask(std::shared_ptr<JobService_Stub> stub, QString job_id,
              std::function<void(bool, const QString&, int, int, int,
                                 std::vector<ShardItem>, const QString&)> cb)
        : stub_(std::move(stub)), job_id_(std::move(job_id)), cb_(std::move(cb)) {}
    void run() override
    {
        QueryJobRequest req;
        req.set_job_id(job_id_.toStdString());
        QueryJobResponse resp;
        MprpcController ctrl;
        ctrl.SetTimeoutMs(5000);
        stub_->QueryJob(&ctrl, &req, &resp, nullptr);
        if (ctrl.Failed())
        {
            cb_(false, job_id_, 0, 0, 0, {}, QString::fromStdString(ctrl.ErrorText()));
            return;
        }
        if (resp.error_code() != 0)
        {
            cb_(false, job_id_, 0, 0, 0, {},
                QString::fromStdString(resp.error_msg()));
            return;
        }
        int done = 0;
        std::vector<ShardItem> shards;
        shards.reserve(resp.shards_size());
        for (const auto& s : resp.shards())
        {
            ShardItem item;
            item.shard_id = QString::fromStdString(s.shard_id());
            item.shard_index = s.shard_index();
            item.status = static_cast<int>(s.status());
            item.assigned_worker = QString::fromStdString(s.assigned_worker_id());
            if (s.status() == ShardStatus::SHARD_SUCCESS) ++done;
            shards.push_back(std::move(item));
        }
        cb_(true, job_id_, static_cast<int>(resp.job_info().status()),
            done, resp.shards_size(), std::move(shards), "");
    }
private:
    std::shared_ptr<JobService_Stub> stub_;
    QString job_id_;
    std::function<void(bool, const QString&, int, int, int,
                       std::vector<ShardItem>, const QString&)> cb_;
};

// ListJobs
class ListJobsTask : public QRunnable {
public:
    ListJobsTask(std::shared_ptr<JobService_Stub> stub, int limit,
                 std::function<void(bool, std::vector<JobItem>, const QString&)> cb)
        : stub_(std::move(stub)), limit_(limit), cb_(std::move(cb)) {}
    void run() override
    {
        ListJobsRequest req;
        req.set_limit(limit_);
        ListJobsResponse resp;
        MprpcController ctrl;
        ctrl.SetTimeoutMs(5000);
        stub_->ListJobs(&ctrl, &req, &resp, nullptr);
        if (ctrl.Failed())
        {
            cb_(false, {}, QString::fromStdString(ctrl.ErrorText()));
            return;
        }
        if (resp.error_code() != 0)
        {
            cb_(false, {}, QString::fromStdString(resp.error_msg()));
            return;
        }
        std::vector<JobItem> jobs;
        jobs.reserve(resp.jobs_size());
        for (const auto& j : resp.jobs())
        {
            JobItem item;
            item.job_id = QString::fromStdString(j.job_id());
            item.user_id = QString::fromStdString(j.user_id());
            item.input_path = QString::fromStdString(j.input_path());
            item.output_path = QString::fromStdString(j.output_path());
            item.target_resolution = QString::fromStdString(j.target_resolution());
            item.target_format = QString::fromStdString(j.target_format());
            item.status = static_cast<int>(j.status());
            item.shard_count = j.shard_count();
            item.priority = j.priority();
            item.created_at = j.created_at();
            item.updated_at = j.updated_at();
            jobs.push_back(std::move(item));
        }
        cb_(true, std::move(jobs), "");
    }
private:
    std::shared_ptr<JobService_Stub> stub_;
    int limit_;
    std::function<void(bool, std::vector<JobItem>, const QString&)> cb_;
};

// ListWorkers
class ListWorkersTask : public QRunnable {
public:
    ListWorkersTask(std::shared_ptr<WorkerManagerService_Stub> stub,
                    std::function<void(bool, std::vector<WorkerItem>,
                                       const QString&)> cb)
        : stub_(std::move(stub)), cb_(std::move(cb)) {}
    void run() override
    {
        ListWorkersRequest req;   // filter 留空 = 全部
        ListWorkersResponse resp;
        MprpcController ctrl;
        ctrl.SetTimeoutMs(5000);
        stub_->ListWorkers(&ctrl, &req, &resp, nullptr);
        if (ctrl.Failed())
        {
            cb_(false, {}, QString::fromStdString(ctrl.ErrorText()));
            return;
        }
        if (resp.error_code() != 0)
        {
            cb_(false, {}, QString::fromStdString(resp.error_msg()));
            return;
        }
        std::vector<WorkerItem> workers;
        workers.reserve(resp.workers_size());
        for (const auto& w : resp.workers())
        {
            WorkerItem item;
            item.worker_id = QString::fromStdString(w.worker_id());
            item.ip = QString::fromStdString(w.ip());
            item.port = w.port();
            item.cpu_usage = w.cpu_usage();
            item.memory_usage = w.memory_usage();
            item.running_shards = w.current_running_shards();
            item.max_running_shards = w.max_running_shards();
            item.status = static_cast<int>(w.status());
            workers.push_back(std::move(item));
        }
        cb_(true, std::move(workers), "");
    }
private:
    std::shared_ptr<WorkerManagerService_Stub> stub_;
    std::function<void(bool, std::vector<WorkerItem>, const QString&)> cb_;
};

// CancelJob
class CancelTask : public QRunnable {
public:
    CancelTask(std::shared_ptr<JobService_Stub> stub, QString job_id,
               std::function<void(bool, const QString&, const QString&)> cb)
        : stub_(std::move(stub)), job_id_(std::move(job_id)), cb_(std::move(cb)) {}
    void run() override
    {
        CancelJobRequest req;
        req.set_job_id(job_id_.toStdString());
        req.set_reason("GUI cancel");
        CancelJobResponse resp;
        MprpcController ctrl;
        ctrl.SetTimeoutMs(10000);
        stub_->CancelJob(&ctrl, &req, &resp, nullptr);
        if (ctrl.Failed())
        {
            cb_(false, job_id_, QString::fromStdString(ctrl.ErrorText()));
            return;
        }
        cb_(resp.error_code() == 0, job_id_,
            QString::fromStdString(resp.error_msg()));
    }
private:
    std::shared_ptr<JobService_Stub> stub_;
    QString job_id_;
    std::function<void(bool, const QString&, const QString&)> cb_;
};

} // namespace

// ============================================================================
// RpcClient
// ============================================================================

struct RpcClient::Impl {
    std::shared_ptr<JobService_Stub> job_stub;
    std::shared_ptr<WorkerManagerService_Stub> wm_stub;
};

RpcClient::RpcClient(QObject* parent) : QObject(parent), impl_(new Impl)
{
    impl_->job_stub = std::make_shared<JobService_Stub>(new MprpcChannel());
    impl_->wm_stub =
        std::make_shared<WorkerManagerService_Stub>(new MprpcChannel());
}

RpcClient::~RpcClient() = default;

// ⚠️ 悬垂防护：QRunnable 在 QThreadPool 后台线程执行，若本对象（RpcClient）
// 在任务完成前被析构（主窗口关闭），lambda 捕获的 this 悬垂——emit 到
// 已析构对象是 UB。统一捕获 QPointer<RpcClient> 守护：对象析构后 QPointer
// 自动置 null，emit 前判空跳过（任务结果丢弃，进程正在退出）。
void RpcClient::submitJob(const SubmitJobRequest& req)
{
    auto stub = impl_->job_stub;
    QPointer<RpcClient> guard(this);
    QThreadPool::globalInstance()->start(new SubmitTask(
        stub, req,
        [guard](bool ok, const QString& id, const QString& err) {
            if (guard) guard->emit submitFinished(ok, id, err);
        }));
}

void RpcClient::queryJob(const QString& job_id)
{
    auto stub = impl_->job_stub;
    QPointer<RpcClient> guard(this);
    QThreadPool::globalInstance()->start(new QueryTask(
        stub, job_id,
        [guard](bool ok, const QString& id, int status, int done, int total,
                std::vector<ShardItem> shards, const QString& err) {
            if (guard)
                guard->emit queryFinished(ok, id, status, done, total,
                                          std::move(shards), err);
        }));
}

void RpcClient::listJobs(int limit)
{
    auto stub = impl_->job_stub;
    QPointer<RpcClient> guard(this);
    QThreadPool::globalInstance()->start(new ListJobsTask(
        stub, limit,
        [guard](bool ok, std::vector<JobItem> jobs, const QString& err) {
            if (guard) guard->emit listJobsFinished(ok, std::move(jobs), err);
        }));
}

void RpcClient::listWorkers()
{
    auto stub = impl_->wm_stub;
    QPointer<RpcClient> guard(this);
    QThreadPool::globalInstance()->start(new ListWorkersTask(
        stub,
        [guard](bool ok, std::vector<WorkerItem> workers, const QString& err) {
            if (guard)
                guard->emit listWorkersFinished(ok, std::move(workers), err);
        }));
}

void RpcClient::cancelJob(const QString& job_id, const QString& reason)
{
    auto stub = impl_->job_stub;
    QPointer<RpcClient> guard(this);
    QThreadPool::globalInstance()->start(new CancelTask(
        stub, job_id,
        [guard](bool ok, const QString& id, const QString& err) {
            if (guard) guard->emit cancelFinished(ok, id, err);
        }));
}

} // namespace video_platform
