#pragma once

// ============================================================================
// LogPanel — 服务端日志面板（阶段 12 GUI）
// ============================================================================
//
// 路线图"服务端推送的关键日志"的轻量实现：GUI 与 5 个服务同机部署，
// 直接 tail 本地 program_log/ 下最新的日志文件（AsyncLogger 按天轮转）。
// QTimer 每 2s 读取文件新增内容（记录已读偏移），自动跟随最新文件。
// ============================================================================

#include <QPlainTextEdit>
#include <QWidget>

#include <string>

namespace video_platform {

class LogPanel : public QWidget {
    Q_OBJECT
public:
    /// @param log_dir 日志目录（默认 ./program_log）
    explicit LogPanel(const QString& log_dir, QWidget* parent = nullptr);

private slots:
    void refresh();

private:
    void scanNewestFile();

    QString log_dir_;
    QString current_file_;
    qint64  offset_ = 0;      // 已读偏移
    QPlainTextEdit* view_;
};

} // namespace video_platform
