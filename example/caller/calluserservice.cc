#include<csignal>
#include"mprpcapplication.h"
#include"user.pb.h"
#include"mprpcchannel.h"
#include"mprpccontroller.h"
#include"wevix_muduo/AsyncLogger.h"
int main(int argc, char **argv)
{
    // 初始化日志：Debug模式输出终端，Release模式仅写文件
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    signal(SIGPIPE, SIG_IGN);

    // 程序启动以后，想使用mprpc框架来享受rpc服务调用，需要先调用框架的初始化函数(初始化一次即可)
    MprpcApplication::Init(argc,argv);

    // 创建连接通道对象
    fixbug::UserServiceRpc_Stub stub(new MprpcChannel());

    // 演示调用远程发布的rpc方法的Login
    // rpc方法的请求参数
    fixbug::LoginRequest request;
    request.set_name("111");
    request.set_pwd("123456");
    // rpc方法的响应
    fixbug::LoginResponse response;
    MprpcController controller;

    // 发起rpc方法的调用，同步的rpc调用过程
    // RpcChannel->RpcChannel::callMethod 集中做所有rpc方法调用参数序列化和网络发送
    stub.Login(&controller,&request,&response,nullptr);

    // 一次rpc调用结束，读取调用结果
    if(!controller.Failed())
    {
        if(response.result().errcode()==0)
        {
            LOG_INFO("rpc login response success: %d", response.success());
        }
        else
        {
            LOG_ERROR("rpc login response error: %s", response.result().errmsg().c_str());
        }
    }
    else
    {
        LOG_ERROR("rpc login failed: %s", controller.ErrorText().c_str());
    }

    // 演示调用远程发布的rpc方法Register
    fixbug::RegisterRequest req;
    req.set_id(2000);
    req.set_name("mprpc");
    req.set_pwd("123456");
    fixbug::RegisterResponse rsp;

    stub.Register(&controller,&req,&rsp,nullptr);
    if(!controller.Failed())
    {
        if(rsp.result().errcode()==0)
        {
            LOG_INFO("rpc register response success: %d", rsp.success());
        }
        else
        {
            LOG_ERROR("rpc register response error: %s", rsp.result().errmsg().c_str());
        }
    }
    else
    {
        LOG_ERROR("rpc register failed: %s", controller.ErrorText().c_str());
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}