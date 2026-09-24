#pragma once
#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <string>
#include "wevix_muduo/Buffer.h"
#include "wevix_muduo/Connection.h"

namespace mprpc
{

// 单帧最大 64MB：防止异常长度字段导致 Buffer 无限扩容或内存被打爆。
constexpr uint32_t kRpcMaxFrameSize = 64 * 1024 * 1024;

// 帧体最小长度：至少要能装下 payload 开头的 header_size(4B)，否则业务层无从解析。
constexpr uint32_t kRpcMinFrameSize = sizeof(uint32_t);

// 写入 4 字节网络字节序整数。
// RPC 协议里的 total_len/header_size 都走这个函数，避免客户端和服务端各自处理字节序。
inline void AppendNetworkUint32(std::string* out, uint32_t value)
{
    uint32_t networkValue = htonl(value);
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

// 封装完整 RPC 帧：[total_len] + [payload]。
// payload 内部可以是请求，也可以是响应，由上层决定。
inline std::string BuildRpcFrame(const std::string& payload)
{
    std::string frame;
    frame.reserve(sizeof(uint32_t) + payload.size());
    AppendNetworkUint32(&frame, static_cast<uint32_t>(payload.size()));
    frame += payload;
    return frame;
}

} // namespace mprpc

// RPC 帧编解码器：帧格式 [total_len(4B, network order)] + [payload]。
// 数据不足返回 kNeedMoreData，剩余数据留在 Buffer 等下次追加。
// 长度字段非法说明流已错位，因此返回 kFatal 交由 Connection 关闭连接。
// 应用层收到的 message 即 payload：[header_size(4B, network order) + RpcHeader + args]。

inline wevix_muduo::CodecResult RpcMessageCodec(wevix_muduo::Buffer* buf, std::string& message)
{
    if (buf->readableBytes() < sizeof(uint32_t))
    {
        return wevix_muduo::CodecResult::kNeedMoreData;
    }

    uint32_t total_len = 0;
    if (!mprpc::ReadNetworkUint32(buf->peek(), buf->readableBytes(), &total_len))
    {
        return wevix_muduo::CodecResult::kNeedMoreData;
    }

    if (total_len < mprpc::kRpcMinFrameSize || total_len > mprpc::kRpcMaxFrameSize)
    {
        return wevix_muduo::CodecResult::kFatal;
    }

    if (buf->readableBytes() - sizeof(uint32_t) < total_len)
    {
        return wevix_muduo::CodecResult::kNeedMoreData;
    }

    buf->retrieve(sizeof(uint32_t)); // 消费 total_len 头部
    message = buf->retrieveAsString(total_len);
    return wevix_muduo::CodecResult::kFrameReady;
}
