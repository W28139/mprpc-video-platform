#include<csignal>
#include"mprpcapplication.h"
#include"friend.pb.h"
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
    MprpcApplication::Init(argc,argv);

    fixbug::FriendServiceRpc_Stub stub(new MprpcChannel());

    fixbug::GetFriendsListRequest request;
    request.set_userid(1000);
    fixbug::GetFriendsListResponse response;

    MprpcController controller;

    stub.GetFriendsList(&controller,&request,&response,nullptr);

    if(!controller.Failed())
    {
        if(response.result().errcode()==0)
        {
            LOG_INFO("rpc getfriendlist response success!");
            int size = response.friends_size();
            LOG_INFO("friend count: %d", size);
            for(int i=0;i<size;i++)
            {
                LOG_INFO("  friend[%d]: %s", i+1, response.friends(i).c_str());
            }
        }
        else
        {
            LOG_ERROR("rpc getfriendlist response error: %s", response.result().errmsg().c_str());
        }
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}