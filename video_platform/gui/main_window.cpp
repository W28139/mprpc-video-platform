#include "video_platform/gui/main_window.h"

#include <QFileInfo>
#include <QHeaderView>
#include <QHBoxLayout>
#include <QLabel>
#include <QMessageBox>
#include <QProcess>
#include <QProgressBar>
#include <QPushButton>
#include <QStatusBar>
#include <QTabWidget>
#include <QTimer>
#include <QVBoxLayout>

#include "video_platform/gui/job_table.h"
#include "video_platform/gui/log_panel.h"
#include "video_platform/gui/submit_dialog.h"
#include "video_platform/gui/worker_panel.h"

namespace video_platform {

// 任务状态 → 中文显示（job_client.cpp 同源）
static QString jobStatusText(int status)
{
    switch (status) {
        case 0: return "UNKNOWN";
        case 1: return "PENDING";
        case 2: return "SPLITTING";
        case 3: return "SCHEDULING";
        case 4: return "RUNNING";
        case 5: return "MERGING";
        case 6: return "SUCCESS";
        case 7: return "FAILED";
        case 8: return "CANCELED";
        default: return "?";
    }
}

MainWindow::MainWindow(QWidget* parent) : QMainWindow(parent)
{
    setWindowTitle("视频转码平台 — 客户端");
    resize(1080, 680);

    rpc_ = new RpcClient(this);

    auto* central = new QWidget(this);
    auto* root_layout = new QVBoxLayout(central);

    // ── 页签 ──────────────────────────────────────────────────────────
    auto* tabs = new QTabWidget(central);

    // 任务页
    auto* job_page = new QWidget(tabs);
    auto* job_layout = new QVBoxLayout(job_page);

    auto* toolbar = new QHBoxLayout;
    submit_btn_ = new QPushButton("提交任务", job_page);
    cancel_btn_ = new QPushButton("取消选中任务", job_page);
    cancel_btn_->setObjectName("dangerBtn");   // 次要按钮（深蓝灰）
    toolbar->addWidget(submit_btn_);
    toolbar->addWidget(cancel_btn_);
    toolbar->addStretch(1);
    job_layout->addLayout(toolbar);

    job_view_ = new JobTableView(job_page);
    job_model_ = static_cast<JobTableModel*>(job_view_->model());
    job_layout->addWidget(job_view_, 1);

    // 选中任务详情条
    detail_status_ = new QLabel("未选中任务", job_page);
    detail_status_->setStyleSheet("font-size: 16px; font-weight: bold;");
    detail_meta_ = new QLabel(job_page);
    detail_meta_->setProperty("role", "detail");
    detail_progress_ = new QProgressBar(job_page);
    detail_progress_->setRange(0, 100);
    detail_progress_->setValue(0);
    detail_progress_->setTextVisible(false);
    detail_shards_ = new QLabel(job_page);
    detail_shards_->setProperty("role", "detail");
    job_layout->addWidget(detail_status_);
    job_layout->addWidget(detail_meta_);
    job_layout->addWidget(detail_progress_);
    job_layout->addWidget(detail_shards_);
    tabs->addTab(job_page, "任务");

    // Worker 页
    worker_panel_ = new WorkerPanel(tabs);
    tabs->addTab(worker_panel_, "Worker");

    // 日志页
    log_panel_ = new LogPanel("./program_log", tabs);
    tabs->addTab(log_panel_, "日志");

    root_layout->addWidget(tabs, 1);
    setCentralWidget(central);

    // ── 底部状态栏 ────────────────────────────────────────────────────
    status_bar_label_ = new QLabel("正在连接服务…", this);
    statusBar()->addWidget(status_bar_label_);

    // ── 信号连接 ──────────────────────────────────────────────────────
    connect(rpc_, &RpcClient::listJobsFinished,
            this, &MainWindow::onListJobs);
    connect(rpc_, &RpcClient::listWorkersFinished,
            this, &MainWindow::onListWorkers);
    connect(rpc_, &RpcClient::queryFinished, this, &MainWindow::onQuery);
    connect(rpc_, &RpcClient::submitFinished,
            this, &MainWindow::onSubmitFinished);
    connect(rpc_, &RpcClient::cancelFinished,
            this, &MainWindow::onCancelFinished);

    connect(submit_btn_, &QPushButton::clicked,
            this, &MainWindow::onSubmitClicked);
    connect(cancel_btn_, &QPushButton::clicked,
            this, &MainWindow::onCancelClicked);
    connect(job_view_, &QTableView::doubleClicked,
            this, &MainWindow::onRowDoubleClicked);
    connect(job_view_->selectionModel(), &QItemSelectionModel::selectionChanged,
            this, &MainWindow::onSelectionChanged);

    // 2s 刷新
    refresh_timer_ = new QTimer(this);
    refresh_timer_->setInterval(2000);
    connect(refresh_timer_, &QTimer::timeout, this, &MainWindow::refreshAll);
    refresh_timer_->start();

    refreshAll();
}

void MainWindow::refreshAll()
{
    rpc_->listJobs(100);
    rpc_->listWorkers();
    // 选中行时刷新详情（QueryJob 有 Redis 进度缓存，服务端开销低）
    if (job_view_->selectionModel()->hasSelection())
    {
        const QString id = selectedJobId();
        if (!id.isEmpty()) rpc_->queryJob(id);
    }
}

QString MainWindow::selectedJobId() const
{
    const QModelIndex idx = job_view_->currentIndex();
    if (!idx.isValid()) return {};
    const JobItem* job = job_model_->jobAt(idx.row());
    return job ? job->job_id : QString();
}

void MainWindow::onListJobs(bool ok, const std::vector<JobItem>& jobs,
                            const QString& err)
{
    if (!ok)
    {
        status_bar_label_->setText("任务列表刷新失败: " + err);
        return;
    }
    job_model_->setJobs(jobs);
    status_bar_label_->setText(QString("服务连接正常 · %1 个任务 · 每 2s 刷新")
                                   .arg(jobs.size()));
}

void MainWindow::onListWorkers(bool ok, const std::vector<WorkerItem>& workers,
                               const QString& err)
{
    if (ok)
        worker_panel_->updateWorkers(workers);
    else
        worker_panel_->updateWorkers({});
}

void MainWindow::onQuery(bool ok, const QString& job_id, int job_status,
                         int done_shards, int total_shards,
                         const std::vector<ShardItem>& shards,
                         const QString& err)
{
    if (!ok || selectedJobId() != job_id)
        return;   // 过期响应（用户已切换选中行）

    detail_status_->setText(QString("任务 %1 · 状态: %2")
                                .arg(job_id, jobStatusText(job_status)));
    if (total_shards > 0)
    {
        detail_progress_->setValue(done_shards * 100 / total_shards);
        detail_shards_->setText(
            QString("Shard 进度: %1/%2 完成 · 共 %3 个")
                .arg(done_shards).arg(total_shards).arg(total_shards));
    }
    else
    {
        detail_progress_->setValue(0);
        detail_shards_->setText("暂无 shard（任务切分中…）");
    }
}

void MainWindow::onSelectionChanged()
{
    const QString id = selectedJobId();
    if (id.isEmpty())
    {
        detail_status_->setText("未选中任务");
        detail_meta_->clear();
        detail_progress_->setValue(0);
        detail_shards_->clear();
        return;
    }
    const JobItem* job = job_model_->jobAt(job_view_->currentIndex().row());
    if (job)
    {
        detail_status_->setText(QString("任务 %1 · 状态: %2")
                                    .arg(job->job_id, jobStatusText(job->status)));
        detail_meta_->setText(QString("输入: %1 · 分辨率: %2 · Shard: %3")
                                  .arg(job->input_path,
                                       job->target_resolution.isEmpty()
                                           ? "原分辨率" : job->target_resolution)
                                  .arg(job->shard_count));
    }
    rpc_->queryJob(id);
}

void MainWindow::onSubmitClicked()
{
    SubmitDialog dlg(this);
    if (dlg.exec() != QDialog::Accepted) return;
    rpc_->submitJob(dlg.request());
    status_bar_label_->setText("正在提交任务…");
}

void MainWindow::onSubmitFinished(bool ok, const QString& job_id,
                                  const QString& err)
{
    if (ok)
    {
        QMessageBox::information(this, "提交成功",
                                 QString("任务已提交！\nJob ID: %1").arg(job_id));
        status_bar_label_->setText("提交成功: " + job_id);
    }
    else
    {
        QMessageBox::warning(this, "提交失败", err);
        status_bar_label_->setText("提交失败: " + err);
    }
    refreshAll();
}

void MainWindow::onCancelClicked()
{
    const QString id = selectedJobId();
    if (id.isEmpty())
    {
        QMessageBox::information(this, "未选中任务", "请先在列表中选择一个任务");
        return;
    }
    if (QMessageBox::question(this, "确认取消",
                              QString("确定取消任务 %1 吗？").arg(id))
        != QMessageBox::Yes)
        return;
    rpc_->cancelJob(id, "GUI cancel");
}

void MainWindow::onCancelFinished(bool ok, const QString& job_id,
                                  const QString& err)
{
    if (ok)
        status_bar_label_->setText("已取消: " + job_id);
    else
        QMessageBox::warning(this, "取消失败", err);
    refreshAll();
}

void MainWindow::onRowDoubleClicked(const QModelIndex& index)
{
    const JobItem* job = job_model_->jobAt(index.row());
    if (!job) return;
    if (job->status != 6)   // 仅 SUCCESS 可预览
    {
        QMessageBox::information(this, "无法预览",
                                 "只有 SUCCESS 状态的任务可以预览视频");
        return;
    }
    previewMergedVideo(*job);
}

void MainWindow::previewMergedVideo(const JobItem& job)
{
    // 阶段 6 Merge 产出约定：output_path/{job_id}_merged.mp4
    QString out_dir = job.output_path;
    if (out_dir.isEmpty()) out_dir = "/tmp/transcode_worker/out";
    const QString merged = out_dir + "/" + job.job_id + "_merged.mp4";

    if (!QFileInfo::exists(merged))
    {
        QMessageBox::warning(this, "文件不存在",
                             QString("未找到合并视频:\n%1").arg(merged));
        return;
    }
    // ffplay 子进程播放（GUI 不阻塞；ffplay 窗口独立）
    if (!QProcess::startDetached("ffplay", {merged, "-autoexit", "-loglevel", "quiet"}))
        QMessageBox::warning(this, "播放失败", "无法启动 ffplay 播放视频");
}

} // namespace video_platform
