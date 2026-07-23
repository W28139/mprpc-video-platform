#pragma once

#include <functional>
#include <memory>
#include <atomic>
#include <string>
#include <new>

#include "wevix_muduo/Buffer.h"
#include "wevix_muduo/Timestamp.h"
#include "wevix_muduo/memory_pool/MemoryPool.h"

namespace wevix_muduo
{

class EventLoop;
class Socket;
class Channel;

/**
 * @brief TCP 连接类
 * 封装了一个已建立的客户端连接、对应的 Socket 和 Channel，
 * 以及该连接特有的输入输出缓冲区。
 */
class Connection : public std::enable_shared_from_this<Connection>
{
public:
    using ConnectionPtr = std::shared_ptr<Connection>;
    using MessageCallback = std::function<void(const ConnectionPtr&, std::string&)>;
    using Callback = std::function<void(const ConnectionPtr&)>;

    // 帧编解码器：从 Buffer 中尝试提取一个完整帧
    // 返回 true 表示成功提取一帧（写入 message），false 表示数据不足需等待
    // 编解码器负责从 Buffer 中消费已提取的数据
    using MessageCodec = std::function<bool(Buffer*, std::string&)>;

    Connection(EventLoop* loop, std::unique_ptr<Socket> clientSock);
    ~Connection();

    // 获取基本信息
    int fd() const;
    std::string ip() const;
    uint16_t port() const;

    // 发送数据
    void send(const std::string& data);

    // 设置各类回调
    void setOnMessageCallback(MessageCallback cb) { onMessageCallback_ = std::move(cb); }
    void setSendCompleteCallback(Callback cb) { sendCompleteCallback_ = std::move(cb); }
    void setCloseCallback(Callback cb) { closeCallback_ = std::move(cb); }
    void setErrorCallback(Callback cb) { errorCallback_ = std::move(cb); }

    // 设置帧编解码器：若设置，handleRead 中循环提取完整帧再回调 onMessage
    // 若未设置，行为不变（每次读到多少就回调多少）
    void setMessageCodec(MessageCodec cb) { messageCodec_ = std::move(cb); }

    // 超时判断
    bool isTimeout(time_t now, int seconds) const;

    // 状态管理
    bool connected() const { return !disconnected_; }

    // 主动关闭写端（发送 FIN），用于短连接场景（如 RPC 响应后断开）
    void shutdown();

private:
    // Channel 的事件分发回调
    void handleRead();
    void handleWrite();
    void handleClose();
    void handleError();

    // 在所属的 Loop 线程中发送数据
    void sendInLoop(const std::string& data);

private:
    EventLoop* loop_;
    std::unique_ptr<Socket> socket_;
    std::unique_ptr<Channel> channel_;

    Buffer inputBuffer_;  // 接收缓冲区
    Buffer outputBuffer_; // 发送缓冲区

    std::atomic_bool disconnected_; // 连接断开标志

    // 业务层回调
    MessageCallback onMessageCallback_;
    Callback sendCompleteCallback_;
    Callback closeCallback_;
    Callback errorCallback_;

    // 帧编解码器（可选）：从 Buffer 中提取完整帧
    MessageCodec messageCodec_;

    Timestamp lastActiveTime_; // 最后活跃时间戳
};

} // namespace wevix_muduo