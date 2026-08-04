#include "video_platform/gui/log_panel.h"

#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QScrollBar>
#include <QTimer>
#include <QVBoxLayout>

namespace video_platform {

LogPanel::LogPanel(const QString& log_dir, QWidget* parent)
    : QWidget(parent), log_dir_(log_dir)
{
    auto* layout = new QVBoxLayout(this);
    layout->setContentsMargins(0, 0, 0, 0);

    view_ = new QPlainTextEdit(this);
    view_->setReadOnly(true);
    view_->setMaximumBlockCount(2000);   // 防内存无限增长
    layout->addWidget(view_);

    auto* timer = new QTimer(this);
    timer->setInterval(2000);
    connect(timer, &QTimer::timeout, this, &LogPanel::refresh);
    timer->start();
}

void LogPanel::scanNewestFile()
{
    QDir dir(log_dir_);
    dir.setNameFilters({"*.log", "*.txt"});
    // ⚠️ 必须按修改时间倒序（最新在前），不能按名称倒序：
    // 目录里存在 0 字节的 job_service.log 等空文件，名称字典序
    // （j > 2）会错误地把空文件选成"最新"
    dir.setSorting(QDir::Time | QDir::Reversed);
    const QFileInfoList entries = dir.entryInfoList();
    if (entries.isEmpty()) return;

    // 跳过 0 字节空文件（某些服务创建的占位日志）
    QFileInfo newest;
    for (const auto& entry : entries)
    {
        if (entry.size() > 0)
        {
            newest = entry;
            break;
        }
    }
    if (newest.fileName().isEmpty()) return;

    const QString path = newest.absoluteFilePath();
    if (path != current_file_)
    {
        current_file_ = path;
        offset_ = 0;   // 新文件从头读
        view_->clear();
    }
}

void LogPanel::refresh()
{
    scanNewestFile();
    if (current_file_.isEmpty()) return;

    QFile file(current_file_);
    if (!file.open(QIODevice::ReadOnly)) return;

    if (file.size() < offset_) offset_ = 0;   // 文件被轮转/截断
    if (!file.seek(offset_)) return;

    const QByteArray data = file.readAll();
    offset_ = file.size();
    file.close();

    if (data.isEmpty()) return;
    view_->appendPlainText(QString::fromUtf8(data).trimmed());
    // 自动滚动到底部
    QScrollBar* bar = view_->verticalScrollBar();
    bar->setValue(bar->maximum());
}

} // namespace video_platform
