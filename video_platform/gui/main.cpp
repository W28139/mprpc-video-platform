// ============================================================================
// video_gui — 视频转码平台桌面客户端（阶段 12）
// ============================================================================
//
// 启动：./bin/video_gui -i video_platform/conf/video_gui.conf
// 依赖：ZooKeeper（服务发现）+ 5 个业务服务运行中；WSLg / X11 显示环境。
//
// 初始化顺序（与 job_client.cpp 一致）：
//   AsyncLogger::init() → MprpcApplication::Init(-i conf) → 主窗口
// ============================================================================

#include <QApplication>
#include <csignal>
#include <cstring>

#include "mprpcapplication.h"
#include "video_platform/gui/main_window.h"
#include "video_platform/gui/theme.h"
#include "wevix_muduo/AsyncLogger.h"

int main(int argc, char** argv)
{
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    signal(SIGPIPE, SIG_IGN);

    // QApplication 必须在 MprpcApplication::Init 前创建（Qt 事件循环）
    QApplication app(argc, argv);
    app.setApplicationName("video_gui");
    app.setApplicationDisplayName("视频转码平台客户端");

    // 全局主题（字体放大 + 深色样式）
    video_platform::Theme::Apply(app);

    // 透传 -i <conf> 给 mprpc 框架（QApplication 会消费部分参数，需重组）
    int real_argc = 0;
    const char* real_argv[16] = {};
    for (int i = 0; i < argc && real_argc < 15; ++i)
    {
        real_argv[real_argc++] = argv[i];
    }

    if (!MprpcApplication::Init(real_argc, const_cast<char**>(real_argv)))
    {
        fprintf(stderr, "Failed to init MprpcApplication. "
                        "Usage: %s -i <config_file>\n", argv[0]);
        return EXIT_FAILURE;
    }

    video_platform::MainWindow window;
    window.show();

    const int rc = app.exec();
    wevix_muduo::AsyncLogger::GetInstance().stop();
    return rc;
}
