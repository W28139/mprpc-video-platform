#pragma once
#include"google/protobuf/service.h"
#include<memory>
#include"wevix_muduo/TcpServer.h"
#include"wevix_muduo/Connection.h"
#include"mprpcapplication.h"
#include<functional>
#include<google/protobuf/descriptor.h>
#include<string>
#include<unordered_map>

// 框架提供的 专门用于发布rpc服务的 网络对象类

class RpcProvider
{
public:
    // 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
    void NotifyService(google::protobuf::Service *service);

    // 启动rpc服务节点，开始提供rpc远程网络调用服务
    void Run();
private:
    // protobuf::NewCallback 当前版本最多方便绑定两个参数。
    // 这里用上下文结构把 conn、response、requestId 合到一个参数里传给回包回调。
    struct RpcResponseContext;

    // 服务类型信息
    struct ServiceInfo
    {
        google::protobuf::Service *m_service;   // 保存服务对象
        std::unordered_map<std::string,const google::protobuf::MethodDescriptor*>m_methodMap;   // 保存服务方法
    };
    // 存储注册成功的服务对象和其服务方法的所有信息
    std::unordered_map<std::string,ServiceInfo> m_serviceMap;

    // 新的socket连接回调
    void OnConnection(const wevix_muduo::TcpServer::ConnectionPtr& conn);
    // 已建立连接用户的读写事件回调 (wevix_muduo 已从 Buffer 提取为 string)
    void OnMessage(const wevix_muduo::TcpServer::ConnectionPtr& conn, std::string& message);
    // 连接关闭回调
    void OnClose(const wevix_muduo::TcpServer::ConnectionPtr& conn);

    // Closure 的回调操作：序列化业务 response，并封装 RpcResponseHeader 后发送。
    void SendRpcResponse(RpcResponseContext* context);
};
