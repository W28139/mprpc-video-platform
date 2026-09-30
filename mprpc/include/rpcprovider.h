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
class RpcProvider
{
public:
    // 发布 RPC 服务到 Provider
    // Provider 仅存储裸指针用于方法分发，不接管对象所有权，不会 delete service
    void NotifyService(google::protobuf::Service *service);

    // 启动rpc服务节点，开始提供rpc远程网络调用服务
    bool Run();
private:
    // 这里把 conn、response、requestId 合到一个参数里传给回包回调
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
    // 收到完整 RPC 请求帧后解析并分发到业务 Service。
    void OnMessage(const wevix_muduo::TcpServer::ConnectionPtr& conn, std::string& message);
    void OnClose(const wevix_muduo::TcpServer::ConnectionPtr& conn);

    // Closure 的回调操作：序列化业务 response，并封装 RpcResponseHeader 后发送。
    void SendRpcResponse(RpcResponseContext* context);
};
