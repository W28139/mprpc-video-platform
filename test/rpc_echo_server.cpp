// ============================================================================
// RPC Echo 服务端 —— 用于 mprpc 框架端到端压测
//
// 收到 EchoRequest，原样返回 payload 到 EchoResponse。
// 通过 RpcProvider 发布服务，使用 wevix_muduo 网络层 + protobuf 序列化。
//
// 启动方式：
//   ./rpc_echo_server -i rpc_test.conf
//
// 注意：
//   1. 需要 ZooKeeper 已启动（用于服务注册）
//   2. 配置文件指定 rpcserverip / rpcserverport
//   3. 服务端 IO 线程数由 mprpc 框架内部决定（当前为 4）
// ============================================================================

#include "proto/rpc_echo.pb.h"
#include "mprpcapplication.h"
#include "rpcprovider.h"
#include "wevix_muduo/AsyncLogger.h"
#include <cstdlib>
#include <csignal>
#include <string>

// ============================================================================
// Echo 服务实现
// ============================================================================
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

// ============================================================================
int main(int argc, char* argv[])
{
    // 初始化日志：Debug 模式输出终端，Release 模式仅写文件
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
    provider.Run();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
