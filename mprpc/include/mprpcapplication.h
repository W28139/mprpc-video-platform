#pragma once
#include"mprpcconfig.h"
#include"mprpcchannel.h"
#include"mprpccontroller.h"
// mprpc 框架的基础类，设置为单例

class MprpcApplication
{
public:
    static void Init(int argc,char **argv);
    static  MprpcApplication& GetInstance();
    static MprpcConfig& GetConfig();
private:

    static MprpcConfig m_config;

    MprpcApplication();
    MprpcApplication(const MprpcApplication&) = delete;
    MprpcApplication(MprpcApplication&&) = delete;
};