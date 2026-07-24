// 简易 Echo Server —— 用于端到端压测
// 收到什么就返回什么，测试 Channel + Connection 在高并发下的表现

#include "wevix_muduo/TcpServer.h"
#include "wevix_muduo/AsyncLogger.h"
#include <csignal>
#include <atomic>

std::atomic<bool> g_running{true};

void OnSignal(int) { g_running = false; }

void OnMessage(const wevix_muduo::TcpServer::ConnectionPtr& conn, std::string& message)
{
    conn->send(message);
}

int main()
{
    // 初始化异步日志：Debug模式输出终端，Release模式仅写文件
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    signal(SIGINT, OnSignal);
    signal(SIGTERM, OnSignal);

    wevix_muduo::TcpServer server("0.0.0.0", 8888, 16);
    server.setOnMessageCallback(OnMessage);
    LOG_INFO("Echo Server listening on 0.0.0.0:8888");
    server.start();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
