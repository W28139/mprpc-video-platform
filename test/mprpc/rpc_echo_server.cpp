// RPC Echo 服务端：原样返回 payload，用于压测

#include "rpc_echo.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include <cstdlib>
#include <csignal>
#include <string>

// Echo 服务实现：原样返回 payload
class EchoServiceImpl : public rpc_test::EchoService
{
public:
    void Echo(::google::protobuf::RpcController* controller,
              const ::rpc_test::EchoRequest* request,
              ::rpc_test::EchoResponse* response,
              ::google::protobuf::Closure* done) override
    {
        // 原样返回 payload，不产生任何额外开销
        response->set_payload(request->payload());
        done->Run();
    }
};

int main(int argc, char* argv[])
{
    // 初始化日志：Debug 输出终端，Release 仅写文件
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    // 忽略 SIGPIPE，防止 socket 写入失败导致进程退出
    signal(SIGPIPE, SIG_IGN);

    LOG_INFO("Starting RPC Echo Server...");

    // 初始化 mprpc 框架（读取配置文件）
    if (!MprpcApplication::Init(argc, argv))
    {
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // 注册并启动服务
    RpcProvider provider;
    provider.NotifyService(new EchoServiceImpl());

    LOG_INFO("RPC Echo Server ready, waiting for requests...");
    if (!provider.Run())
    {
        LOG_ERROR("RPC Echo Server start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
