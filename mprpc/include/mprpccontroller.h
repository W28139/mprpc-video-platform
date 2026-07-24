#pragma once
#include <google/protobuf/service.h>
#include <cstdint>
#include <string>

class MprpcController : public google::protobuf::RpcController
{
public:
    MprpcController();
    void Reset() override;
    bool Failed() const override;
    std::string ErrorText() const override;
    void SetFailed(const std::string& reason) override;

    // 框架内部使用的结构化失败接口：既保留错误文本，也保留可编程判断的错误码。
    void SetFailed(int errorCode, const std::string& reason);
    int ErrorCode() const;

    // 同步 RPC 调用的超时时间，单位毫秒；不设置时由 MprpcChannel 使用默认值。
    void SetTimeoutMs(int64_t timeoutMs);
    int64_t TimeoutMs() const;
    bool HasTimeout() const;

    // 目前未实现具体的功能
    void StartCancel() override;
    bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure* callback) override;
private:
    bool m_failed; // RPC方法执行过程中的状态
    int m_errorCode; // 框架错误码，对应 rpcheader.proto 中的 RpcErrorCode
    int64_t m_timeoutMs; // 本次调用的超时设置，0 表示使用默认超时
    std::string m_errText; // RPC方法执行过程中的错误信息
};
