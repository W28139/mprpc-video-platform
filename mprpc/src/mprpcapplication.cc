#include"mprpcapplication.h"
#include"wevix_muduo/AsyncLogger.h"
#include <iostream>
#include <unistd.h>
#include <string>

// 定义静态成员变量
MprpcConfig MprpcApplication::m_config; 
bool MprpcApplication::m_initialized = false;

void ShowArgsHelp()
{
    std::cout<<"format: command -i <configfile>"<<std::endl;
}


MprpcApplication::MprpcApplication(){}


bool MprpcApplication::Init(int argc, char **argv)
{
    // getopt 使用全局状态，测试或同进程多次 Init 时需要重置。
    optind = 1;
    m_initialized = false;

    if(argc<2)
    {
        ShowArgsHelp();
        return false;
    }

    int c = 0; 
    std::string config_file;

    while((c = getopt(argc,argv,"i:"))!=-1)
    {
        switch (c)
        {
        case 'i':
            config_file = optarg;
            break;
        case '?':
            ShowArgsHelp();
            return false;
        case ':':
            ShowArgsHelp();
            return false;

        default:
            break;
        }
    }
    
    if (config_file.empty()) {
        ShowArgsHelp();
        return false;
    }

    // 开始加载配置文件 rpcserver_ip=   rpcserver_port=   zookeeper_ip=   zookeeper_ip=
    // 普通成员变量无法在静态成员函数中访问，要改为静态成员函数
    if (!m_config.LoadConfigFile(config_file.c_str()))
    {
        return false;
    }

    std::string value;
    std::string error;
    if (!m_config.LoadRequired("zookeeperip", value, error))
    {
        LOG_ERROR("%s", error.c_str());
        return false;
    }
    if (m_config.LoadInt("zookeeperport", -1, 1, 65535) == -1)
    {
        LOG_ERROR("required config key invalid: zookeeperport");
        return false;
    }

    LOG_INFO("MprpcApplication init success, config_file=%s", config_file.c_str());
    m_initialized = true;
    return true;

    // std::cout << "rpcserverip:" << m_config.Load("rpcserverip") << std::endl;
    // std::cout << "rpcserverport:" << m_config.Load("rpcserverport") << std::endl;
    // std::cout << "zookeeperip:" << m_config.Load("zookeeperip") << std::endl;
    // std::cout << "zookeeperport:" << m_config.Load("zookeeperport") << std::endl;
}

MprpcApplication& MprpcApplication::GetInstance()
{
    static MprpcApplication app;
    return app;
}

MprpcConfig& MprpcApplication::GetConfig()
{
    return m_config;
}

bool MprpcApplication::IsInitialized()
{
    return m_initialized;
}
