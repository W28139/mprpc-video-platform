#include "video_platform/gui/theme.h"

#include <QApplication>
#include <QFont>

namespace video_platform {

// 状态色（深色背景下使用亮色调）
// 进行中蓝 #4d9fff / 成功绿 #34d399 / 失败红 #f87171 / 取消灰 #9ca3af

void Theme::Apply(QApplication& app)
{
    // ── 全局字体：默认 Qt 字体在 WSLg 下偏小，统一放大 ─────────────
    QFont font = app.font();
    font.setPointSize(11);          // 对应 ~14px，中文显示清晰
    font.setFamily("Microsoft YaHei, WenQuanYi Micro Hei, Noto Sans CJK SC, "
                   "Sans Serif");
    app.setFont(font);

    // ── 全局样式表（深色现代主题） ──────────────────────────────────
    app.setStyleSheet(R"(
QMainWindow, QDialog, QWidget {
    background: #1e1e2e;
    color: #e4e4ef;
    font-size: 14px;
}

/* ── 页签 ─────────────────────────────────────────── */
QTabWidget::pane {
    border: 1px solid #34344a;
    border-radius: 8px;
    background: #1e1e2e;
    top: -1px;
}
QTabBar::tab {
    background: #26263a;
    color: #9aa0b5;
    padding: 10px 26px;
    margin-right: 2px;
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
    font-size: 15px;
}
QTabBar::tab:selected {
    background: #3b82f6;
    color: #ffffff;
    font-weight: bold;
}
QTabBar::tab:hover:!selected {
    background: #33334a;
    color: #e4e4ef;
}

/* ── 表格 ─────────────────────────────────────────── */
QTableView {
    background: #232337;
    alternate-background-color: #26263a;
    border: none;
    border-radius: 6px;
    gridline-color: #2e2e44;
    selection-background-color: #3b82f6;
    selection-color: #ffffff;
    font-size: 14px;
}
QTableView::item {
    padding: 4px 6px;
}
QHeaderView::section {
    background: #2c2c42;
    color: #b6bcd0;
    padding: 9px 8px;
    border: none;
    border-right: 1px solid #34344a;
    border-bottom: 1px solid #34344a;
    font-size: 14px;
    font-weight: bold;
}

/* ── 按钮 ─────────────────────────────────────────── */
QPushButton {
    background: #3b82f6;
    color: #ffffff;
    border: none;
    border-radius: 6px;
    padding: 8px 18px;
    font-size: 14px;
}
QPushButton:hover { background: #2f6fe0; }
QPushButton:pressed { background: #2560c8; }
QPushButton:disabled { background: #3a3a52; color: #88889c; }
QPushButton#dangerBtn {
    background: #37415a;
    color: #e4e4ef;
}
QPushButton#dangerBtn:hover { background: #4a5a80; }
QPushButton#primaryBtn {
    font-size: 15px;
    font-weight: bold;
}

/* ── 输入控件 ─────────────────────────────────────── */
QLineEdit, QSpinBox, QComboBox {
    background: #2a2a3e;
    color: #e4e4ef;
    border: 1px solid #3f3f5a;
    border-radius: 5px;
    padding: 7px 9px;
    font-size: 14px;
    selection-background-color: #3b82f6;
}
QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
    border-color: #3b82f6;
}
QComboBox QAbstractItemView {
    background: #2a2a3e;
    color: #e4e4ef;
    border: 1px solid #3f3f5a;
    selection-background-color: #3b82f6;
}
QSpinBox::up-button, QSpinBox::down-button {
    background: #33334c;
    border: none;
    width: 18px;
}
QSpinBox::up-arrow { width: 0; height: 0; border-left: 5px solid transparent;
                     border-right: 5px solid transparent;
                     border-bottom: 6px solid #9aa0b5; }
QSpinBox::down-arrow { width: 0; height: 0; border-left: 5px solid transparent;
                       border-right: 5px solid transparent;
                       border-top: 6px solid #9aa0b5; }

/* ── 进度条 ───────────────────────────────────────── */
QProgressBar {
    background: #2a2a3e;
    border: none;
    border-radius: 5px;
    height: 14px;
    text-align: center;
}
QProgressBar::chunk {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 #3b82f6, stop:1 #5aa2ff);
    border-radius: 5px;
}

/* ── 日志面板 ─────────────────────────────────────── */
QPlainTextEdit {
    background: #17171f;
    color: #d4d4d4;
    border: none;
    border-radius: 6px;
    font-family: "JetBrains Mono, Consolas, monospace";
    font-size: 13px;
}

/* ── 提示/次要文字 ────────────────────────────────── */
QLabel[role="hint"] { color: #9aa0b5; font-size: 12px; }
QLabel[role="detail"] { color: #b6bcd0; font-size: 13px; }
)");
}

} // namespace video_platform
