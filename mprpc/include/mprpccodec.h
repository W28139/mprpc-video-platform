#pragma once
#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <string>
#include "wevix_muduo/Buffer.h"

namespace mprpc
{

// 单帧最大 64MB：防止异常长度字段导致 Buffer 无限扩容或内存被打爆。
constexpr uint32_t kRpcMaxFrameSize = 64 * 1024 * 1024;
constexpr uint16_t kRpcMagic = 0x4d52;   // "MR"，用于快速识别 mprpc 协议帧
constexpr uint16_t kRpcVersion = 1;      // 当前 RPC 协议版本
constexpr uint32_t kRpcFrameHeaderSize = sizeof(uint16_t) + sizeof(uint16_t);

// 写入 4 字节网络字节序整数。
// RPC 协议里的 total_len/header_size 都走这个函数，避免客户端和服务端各自处理字节序。
inline void AppendNetworkUint32(std::string* out, uint32_t value)
{
    uint32_t networkValue = htonl(value);
    out->append(reinterpret_cast<const char*>(&networkValue), sizeof(networkValue));
}

// 写入 2 字节网络字节序整数，用于 magic/version 这种固定协议字段。
inline void AppendNetworkUint16(std::string* out, uint16_t value)
{
    uint16_t networkValue = htons(value);
    out->append(reinterpret_cast<const char*>(&networkValue), sizeof(networkValue));
}

// 从原始字节中读取 4 字节网络字节序整数。
// len 参数用于在解析前做边界保护，避免坏包触发越界读取。
inline bool ReadNetworkUint32(const char* data, size_t len, uint32_t* value)
{
    if (data == nullptr || value == nullptr || len < sizeof(uint32_t))
    {
        return false;
    }

    uint32_t networkValue = 0;
    std::memcpy(&networkValue, data, sizeof(networkValue));
    *value = ntohl(networkValue);
    return true;
}

// 从原始字节中读取 2 字节网络字节序整数。
inline bool ReadNetworkUint16(const char* data, size_t len, uint16_t* value)
{
    if (data == nullptr || value == nullptr || len < sizeof(uint16_t))
    {
        return false;
    }

    uint16_t networkValue = 0;
    std::memcpy(&networkValue, data, sizeof(networkValue));
    *value = ntohs(networkValue);
    return true;
}

// 封装完整 RPC 帧：[total_len] + [payload]。
// payload 内部可以是请求，也可以是响应，由上层决定。
inline std::string BuildRpcFrame(const std::string& payload)
{
    std::string frame;
    frame.reserve(sizeof(uint32_t) + kRpcFrameHeaderSize + payload.size());
    AppendNetworkUint32(&frame, static_cast<uint32_t>(kRpcFrameHeaderSize + payload.size()));
    AppendNetworkUint16(&frame, kRpcMagic);
    AppendNetworkUint16(&frame, kRpcVersion);
    frame += payload;
    return frame;
}

// 解析 total_len 后面的帧体，验证 magic/version 后返回真正的业务 payload。
inline bool DecodeRpcFramePayload(const std::string& frameBody,
                                  std::string* payload,
                                  std::string* errorMsg)
{
    if (payload == nullptr)
    {
        if (errorMsg != nullptr)
        {
            *errorMsg = "payload output is null";
        }
        return false;
    }

    if (frameBody.size() < kRpcFrameHeaderSize)
    {
        if (errorMsg != nullptr)
        {
            *errorMsg = "rpc frame body too small";
        }
        return false;
    }

    uint16_t magic = 0;
    uint16_t version = 0;
    if (!ReadNetworkUint16(frameBody.data(), frameBody.size(), &magic) ||
        !ReadNetworkUint16(frameBody.data() + sizeof(uint16_t),
                           frameBody.size() - sizeof(uint16_t), &version))
    {
        if (errorMsg != nullptr)
        {
            *errorMsg = "read rpc magic/version failed";
        }
        return false;
    }

    if (magic != kRpcMagic)
    {
        if (errorMsg != nullptr)
        {
            *errorMsg = "invalid rpc magic:" + std::to_string(magic);
        }
        return false;
    }

    if (version != kRpcVersion)
    {
        if (errorMsg != nullptr)
        {
            *errorMsg = "unsupported rpc version:" + std::to_string(version);
        }
        return false;
    }

    *payload = frameBody.substr(kRpcFrameHeaderSize);
    return true;
}

} // namespace mprpc

// RPC 帧编解码器：从 muduo Buffer 中提取一个完整帧。
//
// 帧格式（客户端发送的 wire format）：
//   [total_len(4B, network order)] + [magic(2B)] + [version(2B)] + [payload]
//
// 编解码器职责：
//   1. 检查 Buffer 中是否有 ≥4 字节
//   2. 读取 total_len
//   3. 检查 Buffer 中是否有 ≥4+total_len 字节
//   4. 消费 total_len 头部，提取 payload 交给应用层
//   5. 若数据不足返回 false，剩余数据留在 Buffer 中等待下次追加
//
// 应用层收到的 message 是 payload，请求 payload 为
// [header_size(4B, network order) + RpcHeader + args]。

inline bool RpcMessageCodec(wevix_muduo::Buffer* buf, std::string& message)
{
    if (buf->readableBytes() < 4)
    {
        return false;
    }

    uint32_t total_len = 0;
    if (!mprpc::ReadNetworkUint32(buf->peek(), buf->readableBytes(), &total_len))
    {
        return false;
    }

    if (total_len < mprpc::kRpcFrameHeaderSize || total_len > mprpc::kRpcMaxFrameSize)
    {
        // 遇到非法长度时清空缓冲区，避免这个连接后续一直卡在坏帧上。
        buf->retrieveAll();
        return false;
    }

    if (buf->readableBytes() - 4 < total_len)
    {
        return false;
    }

    buf->retrieve(4); // 消费 total_len 头部
    std::string frameBody = buf->retrieveAsString(total_len);
    std::string errorMsg;
    if (!mprpc::DecodeRpcFramePayload(frameBody, &message, &errorMsg))
    {
        // magic/version 错误时直接拒绝这一帧，上层不会收到伪造或旧版本协议数据。
        message.clear();
        return false;
    }
    return true;
}
