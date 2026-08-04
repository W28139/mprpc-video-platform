#include "video_platform/gui/submit_dialog.h"

#include <QComboBox>
#include <QMimeData>
#include <QDir>
#include <QDragEnterEvent>
#include <QDropEvent>
#include <QFileDialog>
#include <QFormLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QMessageBox>
#include <QPushButton>
#include <QSpinBox>
#include <QUrl>

namespace video_platform {

SubmitDialog::SubmitDialog(QWidget* parent) : QDialog(parent)
{
    setWindowTitle("提交转码任务");
    setMinimumWidth(560);
    setAcceptDrops(true);   // 拖拽视频文件到对话框

    auto* form = new QFormLayout;

    user_id_ = new QLineEdit("alice", this);
    form->addRow("用户 ID:", user_id_);

    input_path_ = new QLineEdit(this);
    input_path_->setPlaceholderText("拖拽视频文件到此处，或点击选择…");
    auto* input_btn = new QPushButton("浏览…", this);
    auto* input_row = new QHBoxLayout;
    input_row->addWidget(input_path_, 1);
    input_row->addWidget(input_btn);
    form->addRow("输入视频 *:", input_row);
    connect(input_btn, &QPushButton::clicked, this,
            &SubmitDialog::chooseInputFile);

    output_path_ = new QLineEdit("/tmp/transcode_worker/out", this);
    auto* output_btn = new QPushButton("浏览…", this);
    auto* output_row = new QHBoxLayout;
    output_row->addWidget(output_path_, 1);
    output_row->addWidget(output_btn);
    form->addRow("输出目录:", output_row);
    connect(output_btn, &QPushButton::clicked, this,
            &SubmitDialog::chooseOutputDir);

    format_ = new QComboBox(this);
    format_->addItem("保持原格式", QString());
    format_->addItem("mp4", "mp4");
    format_->addItem("mkv", "mkv");
    format_->addItem("flv", "flv");
    form->addRow("目标格式:", format_);

    resolution_ = new QComboBox(this);
    resolution_->addItem("保持原分辨率", QString());
    resolution_->addItem("720p", "720p");
    resolution_->addItem("1080p", "1080p");
    resolution_->addItem("4k", "4k");
    form->addRow("目标分辨率:", resolution_);

    bitrate_ = new QSpinBox(this);
    bitrate_->setRange(0, 100000);
    bitrate_->setValue(0);
    bitrate_->setSuffix(" kbps（0=保持原码率）");
    form->addRow("目标码率:", bitrate_);

    priority_ = new QSpinBox(this);
    priority_->setRange(0, 100);
    priority_->setValue(0);
    priority_->setSuffix("（越大越优先）");
    form->addRow("优先级:", priority_);

    shard_duration_ = new QSpinBox(this);
    shard_duration_->setRange(0, 3600);
    shard_duration_->setValue(0);
    shard_duration_->setSuffix(" 秒（0=默认 30s）");
    form->addRow("Shard 时长:", shard_duration_);

    submit_btn_ = new QPushButton("提交任务", this);
    submit_btn_->setObjectName("primaryBtn");   // 全局主题放大加粗
    submit_btn_->setMinimumHeight(38);          // 主按钮更大更醒目
    form->addRow(submit_btn_);
    connect(submit_btn_, &QPushButton::clicked, this, &SubmitDialog::onAccept);

    auto* hint = new QLabel("提示：可以直接把视频文件拖进对话框自动填路径",
                            this);
    hint->setProperty("role", "hint");
    form->addRow(hint);

    setLayout(form);
}

SubmitJobRequest SubmitDialog::request() const
{
    SubmitJobRequest req;
    req.set_user_id(user_id_->text().toStdString());
    req.set_input_path(input_path_->text().toStdString());
    if (!output_path_->text().isEmpty())
        req.set_output_path(output_path_->text().toStdString());
    const QString fmt = format_->currentData().toString();
    if (!fmt.isEmpty()) req.set_target_format(fmt.toStdString());
    const QString res = resolution_->currentData().toString();
    if (!res.isEmpty()) req.set_target_resolution(res.toStdString());
    req.set_target_bitrate(bitrate_->value());
    req.set_priority(priority_->value());
    req.set_shard_duration_sec(shard_duration_->value());
    return req;
}

void SubmitDialog::chooseInputFile()
{
    const QString path = QFileDialog::getOpenFileName(
        this, "选择视频文件", QDir::homePath(),
        "视频文件 (*.mp4 *.mkv *.flv *.avi *.mov);;所有文件 (*)");
    if (!path.isEmpty()) input_path_->setText(path);
}

void SubmitDialog::chooseOutputDir()
{
    const QString dir = QFileDialog::getExistingDirectory(
        this, "选择输出目录", output_path_->text());
    if (!dir.isEmpty()) output_path_->setText(dir);
}

void SubmitDialog::dragEnterEvent(QDragEnterEvent* event)
{
    // 只接受本地文件拖拽
    if (event->mimeData()->hasUrls())
        event->acceptProposedAction();
}

void SubmitDialog::dropEvent(QDropEvent* event)
{
    const QList<QUrl> urls = event->mimeData()->urls();
    if (!urls.isEmpty() && urls.first().isLocalFile())
    {
        input_path_->setText(urls.first().toLocalFile());
        event->acceptProposedAction();
    }
}

void SubmitDialog::onAccept()
{
    if (input_path_->text().trimmed().isEmpty())
    {
        QMessageBox::warning(this, "输入缺失", "请先选择输入视频文件");
        return;
    }
    accept();
}

} // namespace video_platform
