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
    // 发布 RPC 服务到 Provider。
    // Provider 仅存储裸指针用于方法分发，不接管对象所有权，不会 delete service。
    // 调用方必须保证 service 对象在 Provider 生命周期内一直有效。
    // 典型用法：栈对象或全局对象，不要用 new 后把生命周期管理丢给 Provider。
    void NotifyService(google::protobuf::Service *service);

    // 启动rpc服务节点，开始提供rpc远程网络调用服务。
    // 返回 true 表示服务正常进入事件循环；false 表示启动失败（配置错误/ZK 连接失败等）。
    bool Run();
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
    // 收到完整 RPC 请求帧后解析并分发到业务 Service。
    //
    // done 闭包调用规范：
    //   框架将 done 闭包绑定到 SendRpcResponse，统一负责序列化业务响应并发送回包。
    //   - 同步方法：protobuf 生成的默认 CallMethod 在返回前自动调用 done->Run()，
    //     框架无需额外处理，响应在 CallMethod 返回时已发出。
    //   - 异步方法：业务覆写 CallMethod 后，必须在异步操作完成时手动调用
    //     done->Run()。漏调会导致请求永久悬挂，客户端一直等到超时。
    void OnMessage(const wevix_muduo::TcpServer::ConnectionPtr& conn, std::string& message);
    // 连接关闭回调
    void OnClose(const wevix_muduo::TcpServer::ConnectionPtr& conn);

    // Closure 的回调操作：序列化业务 response，并封装 RpcResponseHeader 后发送。
    void SendRpcResponse(RpcResponseContext* context);
};
