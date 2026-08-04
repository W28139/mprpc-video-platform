#pragma once

// ============================================================================
// SubmitDialog — 任务提交面板（阶段 12 GUI）
// ============================================================================
//
// 路线图要求：拖拽视频文件 → 自动填充路径 → 下拉选分辨率/格式/码率 →
// 点击提交。所有字段对应 SubmitJobRequest（job.proto）。
// 提交走 RpcClient::submitJob（异步），结果经信号回 MainWindow 提示。
// ============================================================================

#include <QDialog>

#include "job.pb.h"

class QLineEdit;
class QComboBox;
class QSpinBox;
class QPushButton;

namespace video_platform {

class SubmitDialog : public QDialog {
    Q_OBJECT
public:
    explicit SubmitDialog(QWidget* parent = nullptr);

    /// @brief 取用户填写的提交请求（accept 后调用）
    SubmitJobRequest request() const;

protected:
    void dragEnterEvent(QDragEnterEvent* event) override;
    void dropEvent(QDropEvent* event) override;

private:
    void chooseInputFile();
    void chooseOutputDir();
    void onAccept();

    QLineEdit* user_id_;
    QLineEdit* input_path_;
    QLineEdit* output_path_;
    QComboBox* format_;
    QComboBox* resolution_;
    QSpinBox*  bitrate_;
    QSpinBox*  priority_;
    QSpinBox*  shard_duration_;
    QPushButton* submit_btn_;
};

} // namespace video_platform
