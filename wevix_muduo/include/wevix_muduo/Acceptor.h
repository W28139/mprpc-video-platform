#pragma once

#include "wevix_muduo/Socket.h"
#include "wevix_muduo/Channel.h"
#include "wevix_muduo/Noncopyable.h"

#include <functional>
#include <memory>

namespace wevix_muduo
{

class EventLoop;
class InetAddress;

/**
 * @brief 接收器：专门负责监听新连接并进行 accept
 */
class Acceptor : private Noncopyable
{
public:
    using NewConnectionCallback = std::function<void(std::unique_ptr<Socket>)>;

    Acceptor(EventLoop* loop, const std::string& ip, uint16_t port);
    ~Acceptor();

    // 设置新连接到来时的处理函数
    void setNewConnectionCallback(NewConnectionCallback cb)
    {
        newConnectionCallback_ = std::move(cb);
    }

private:
    // 当监听 fd 可读时调用的函数
    void handleRead();

private:
    EventLoop* loop_;
    Socket serverSock_;     // 监听套接字
    Channel acceptChannel_; // 监听套接字对应的通道

    NewConnectionCallback newConnectionCallback_;
};

} // namespace wevix_muduo