#pragma once
#include<google/protobuf/service.h>
#include <string>

class MprpcChannel:public google::protobuf::RpcChannel
{
public:
    /// @brief 默认构造：通过 ZK 服务发现获取 endpoint
    MprpcChannel() = default;

    /// @brief 直连构造：跳过 ZK 服务发现，直接连接指定 IP:Port
    /// @param ip   目标 IP
    /// @param port 目标端口
    /// 用途：Scheduler 需要调用特定 Worker 的 AssignShard，而非随机轮询。
    MprpcChannel(const std::string& ip, uint16_t port)
        : direct_ip_(ip), direct_port_(port), use_direct_(true) {}

    // 所有通过stub代理对象调用的rpc方法，都走到这里，统一做rpc方法调用的数据序列化与发送
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    std::string direct_ip_;
    uint16_t direct_port_ = 0;
    bool use_direct_ = false;
};