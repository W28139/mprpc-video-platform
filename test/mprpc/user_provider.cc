#include<string>
#include<cstdlib>
#include"user.pb.h"
#include"mprpcapplication.h"
#include"rpcprovider.h"
#include"wevix_muduo/AsyncLogger.h"
/*
UserService原来是一个本地服务，提供了两个进程内的本地方法，Login 和 GetFriendLists

*/
class UserService : public fixbug::UserServiceRpc  // 使用在rpc服务的发布端(rpc服务者)
{
public:
    bool Login(std::string name, std::string pwd)
    {
        LOG_INFO("doing local service: Login, name=%s, pwd=%s", name.c_str(), pwd.c_str());
        return true;
    }

    bool Register(uint32_t id,std::string name,std::string pwd)
    {
        LOG_INFO("doing local service: Register, name=%s, id=%u", name.c_str(), id);
        return true;
    }
   
    // 重写基类UserServiceRpc的虚函数,下面这些方法都是框架直接调用的
    void Login(::google::protobuf::RpcController* controller,
                    const ::fixbug::LoginRequest* request,
                    ::fixbug::LoginResponse* response,
                    ::google::protobuf::Closure* done)
    {
        // 框架给业务上报请求参数 LoginRequest，业务获取相应数据做本地业务
        std::string name = request->name();
        std::string pwd = request->pwd();

        // 开始做本地业务
        bool login_result = Login(name,pwd);

        // 把响应写入Response
        fixbug::ResultCode *code = response->mutable_result();
        code->set_errcode(0);
        code->set_errmsg("");
        response->set_success(login_result);

        // 调用执行回调操作
        done->Run();
    }
void Register(::google::protobuf::RpcController* controller,
                       const ::fixbug::RegisterRequest* request,
                       ::fixbug::RegisterResponse* response,
                       ::google::protobuf::Closure* done)
    {
        uint32_t id = request->id();
        std::string name = request->name();
        std::string pwd = request->pwd();

        bool ret = Register(id,name,pwd);

        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        response->set_success(ret);

        done->Run();
    }
};

int main(int argc,char** argv)
{
    // 初始化日志：Debug模式输出终端，Release模式仅写文件
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    // 调用框架的初始化操作 provider -i config.conf
    if (!MprpcApplication::Init(argc,argv))
    {
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // 把UserService对象发布到rpc节点上
    // provider是一个rpc网络服务对象，把UserService对象发布到rpc节点上
    RpcProvider provider;
    provider.NotifyService(new UserService());

    // 启动一个rpc服务器发布节点 Run以后，进程进入阻塞状态，等待远程的rpc调用请求
    if (!provider.Run())
    {
        LOG_ERROR("UserService start failed");
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
