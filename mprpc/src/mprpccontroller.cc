#include "mprpccontroller.h"

MprpcController::MprpcController()
{
    m_failed = false;
    m_errorCode = 0;
    m_timeoutMs = 0;
    m_errText = "";
}

void MprpcController::Reset()
{
    m_failed = false;
    m_errorCode = 0;
    m_timeoutMs = 0;
    m_errText = "";
}

bool MprpcController::Failed() const
{
    return m_failed;
}

std::string MprpcController::ErrorText() const
{
    return m_errText;
}

void MprpcController::SetFailed(const std::string& reason)
{
    // 兼容 protobuf RpcController 的原始接口；没有明确错误码时用通用错误码。
    SetFailed(1, reason);
}

void MprpcController::SetFailed(int errorCode, const std::string& reason)
{
    // 新接口保留结构化错误码，方便调用方做重试、降级或分类统计。
    m_failed = true;
    m_errorCode = errorCode;
    m_errText = reason;
}

int MprpcController::ErrorCode() const
{
    return m_errorCode;
}

void MprpcController::SetTimeoutMs(int64_t timeoutMs)
{
    // 非正数视为不设置超时，由 MprpcChannel 使用默认超时。
    m_timeoutMs = timeoutMs > 0 ? timeoutMs : 0;
}

int64_t MprpcController::TimeoutMs() const
{
    return m_timeoutMs;
}

bool MprpcController::HasTimeout() const
{
    return m_timeoutMs > 0;
}

// 目前未实现具体的功能
void MprpcController::StartCancel(){}
bool MprpcController::IsCanceled() const {return false;}
void MprpcController::NotifyOnCancel(google::protobuf::Closure* callback) {}
