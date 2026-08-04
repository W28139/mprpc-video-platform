#pragma once

// ============================================================================
// RpcClient — mprpc 异步调用封装（阶段 12 GUI 客户端）
// ============================================================================
//
// 关键约束：mprpc stub 是**同步阻塞**调用（MprpcChannel::CallMethod 等
// 响应返回），绝不能在 GUI 线程直接调用（会卡死界面）。本类把所有 RPC
// 提交到 QThreadPool 后台线程执行，结果通过 Qt 信号（跨线程队列投递）
// 回到 UI 线程。
//
// 线程安全：单实例 stub 可跨线程并发调用——MprpcChannel 内部有按
// endpoint 分片的连接池 + 互斥，CallMethod 自身线程安全。每个 QRunnable
// 持 shared_ptr 拷贝，保证运行期内 stub/channel 不被析构。
//
// 配置：MprpcApplication::Init 后自动通过 ZooKeeper 发现服务地址
// （video_gui.conf 只需 zookeeperip / zookeeperport）。
// ============================================================================

#include <QObject>
#include <QString>
#include <memory>
#include <vector>

#include "job.pb.h"
#include "worker.pb.h"

namespace video_platform {

// ── 跨线程传输的数据结构（从 protobuf 转成轻量 struct，避免线程间共享
//    proto 对象）──────────────────────────────────────────────────────────

struct JobItem {
    QString job_id;
    QString user_id;
    QString input_path;
    QString output_path;
    QString target_resolution;
    QString target_format;
    int     status = 0;
    int     shard_count = 0;
    int     priority = 0;
    qint64  created_at = 0;
    qint64  updated_at = 0;
};

struct ShardItem {
    QString shard_id;
    int     shard_index = 0;
    int     status = 0;
    QString assigned_worker;
};

struct WorkerItem {
    QString worker_id;
    QString ip;
    int     port = 0;
    int     cpu_usage = 0;
    int     memory_usage = 0;
    int     running_shards = 0;
    int     max_running_shards = 0;
    int     status = 0;  // WorkerStatus: ONLINE=1 / OFFLINE=2
};

/// @brief mprpc 异步调用封装（QObject，信号在 UI 线程接收）
class RpcClient : public QObject {
    Q_OBJECT
public:
    explicit RpcClient(QObject* parent = nullptr);
    ~RpcClient() override;

    // ── 异步发起（不阻塞调用线程）────────────────────────────────────

    void submitJob(const SubmitJobRequest& req);
    void queryJob(const QString& job_id);
    void listJobs(int limit = 0);          // <=0 返回全部
    void listWorkers();
    void cancelJob(const QString& job_id, const QString& reason);

signals:
    void submitFinished(bool ok, const QString& job_id,
                        const QString& err_msg);
    void queryFinished(bool ok, const QString& job_id, int job_status,
                       int done_shards, int total_shards,
                       const std::vector<ShardItem>& shards,
                       const QString& err_msg);
    void listJobsFinished(bool ok, const std::vector<JobItem>& jobs,
                          const QString& err_msg);
    void listWorkersFinished(bool ok, const std::vector<WorkerItem>& workers,
                             const QString& err_msg);
    void cancelFinished(bool ok, const QString& job_id,
                        const QString& err_msg);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace video_platform
