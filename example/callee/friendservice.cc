#include<string>
#include<csignal>
#include<cstdlib>
#include"friend.pb.h"
#include"mprpcapplication.h"
#include"rpcprovider.h"
#include<vector>
#include"wevix_muduo/AsyncLogger.h"

class FriendService:public fixbug::FriendServiceRpc
{
public:
    std::vector<std::string>GetFriendsList(uint32_t userid)
    {
        LOG_INFO("do GetFriendsList service!");
        std::vector<std::string>vec;
        vec.push_back("Tianyu Wang");
        vec.push_back("Yongheng Du");
        vec.push_back("Peiyu Niu");
        return vec;
    }

    void GetFriendsList(google::protobuf::RpcController* controller,
                       const ::fixbug::GetFriendsListRequest* request,
                       ::fixbug::GetFriendsListResponse* response,
                       ::google::protobuf::Closure* done)
    {
        uint32_t userid = request->userid();
        std::vector<std::string>friendsList = GetFriendsList(userid);
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");

        for (std::string &name : friendsList)
        {
            std::string *p = response->add_friends();
            *p = name;
        }
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

    // 忽略 SIGPIPE，防止 socket 写入失败导致进程退出
    signal(SIGPIPE, SIG_IGN);

    LOG_INFO("first log message!");
    LOG_ERR("%s:%s:%d",__FILE__,__FUNCTION__,__LINE__);

    if (!MprpcApplication::Init(argc,argv))
    {
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    RpcProvider provider;
    provider.NotifyService(new FriendService());

    provider.Run();

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
