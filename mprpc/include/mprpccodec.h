#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include "wevix_muduo/Buffer.h"

// RPC 帧编解码器：从 muduo Buffer 中提取一个完整帧
//
// 帧格式（客户端发送的 wire format）：
//   [total_len(4字节)] + [header_size(4B) + RpcHeader + args]
//
// 编解码器职责：
//   1. 检查 Buffer 中是否有 ≥4 字节
//   2. 读取 total_len
//   3. 检查 Buffer 中是否有 ≥4+total_len 字节
//   4. 消费 total_len 头部，提取 payload 交给应用层
//   5. 若数据不足返回 false，剩余数据留在 Buffer 中等待下次追加
//
// 应用层收到的 message 仍然是 [header_size(4B) + RpcHeader + args]，
// 与之前完全兼容，只是不再需要自己做帧边界判断。

inline bool RpcMessageCodec(wevix_muduo::Buffer* buf, std::string& message)
{
    if (buf->readableBytes() < 4)
        return false;

    uint32_t total_len = 0;
    std::memcpy(&total_len, buf->peek(), 4);

    if (buf->readableBytes() < 4 + total_len)
        return false;

    buf->retrieve(4);                          // 消费 total_len 头部
    message = buf->retrieveAsString(total_len); // 提取 payload（header_size + RpcHeader + args）
    return true;
}
