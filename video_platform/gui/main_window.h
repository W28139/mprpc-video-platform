#pragma once

// ============================================================================
// MainWindow — 主窗口（阶段 12 GUI）
// ============================================================================
//
// 三个页签（QTabWidget）：
//   任务页   — 任务表格 + 选中任务详情（shard 进度条 + 状态）+ 操作按钮
//   Worker 页 — Worker 负载面板
//   日志页   — 服务端日志 tail
//
// 刷新策略：QTimer 2s → ListJobs + ListWorkers（并行异步）；选中任务行 →
// QueryJob 刷新详情。任务提交/取消/预览见对应按钮与双击处理。
// ============================================================================

#include <QMainWindow>

#include "video_platform/gui/rpc_client.h"

class QTableView;
class QProgressBar;
class QLabel;
class QPushButton;
class QTimer;

namespace video_platform {

class JobTableModel;
class JobTableView;
class WorkerPanel;
class LogPanel;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(QWidget* parent = nullptr);

private slots:
    void refreshAll();                       // 2s 定时器
    void onListJobs(bool ok, const std::vector<JobItem>& jobs,
                    const QString& err);
    void onListWorkers(bool ok, const std::vector<WorkerItem>& workers,
                       const QString& err);
    void onQuery(bool ok, const QString& job_id, int job_status,
                 int done_shards, int total_shards,
                 const std::vector<ShardItem>& shards, const QString& err);
    void onSelectionChanged();
    void onSubmitClicked();
    void onSubmitFinished(bool ok, const QString& job_id,
                          const QString& err);
    void onCancelClicked();
    void onCancelFinished(bool ok, const QString& job_id,
                          const QString& err);
    void onRowDoubleClicked(const QModelIndex& index);

private:
    void previewMergedVideo(const JobItem& job);
    QString selectedJobId() const;

    RpcClient* rpc_;
    QTimer* refresh_timer_;

    JobTableView* job_view_;
    JobTableModel* job_model_;
    WorkerPanel* worker_panel_;
    LogPanel* log_panel_;

    QLabel* detail_status_;
    QLabel* detail_meta_;
    QProgressBar* detail_progress_;
    QLabel* detail_shards_;
    QPushButton* submit_btn_;
    QPushButton* cancel_btn_;
    QLabel* status_bar_label_;
};

} // namespace video_platform
