#include "wevix_muduo/Acceptor.h"
#include "wevix_muduo/EventLoop.h"
#include "wevix_muduo/InetAddress.h"
#include "wevix_muduo/AsyncLogger.h"
#include <errno.h>

namespace wevix_muduo
{

Acceptor::Acceptor(EventLoop* loop, const std::string& ip, uint16_t port)
    : loop_(loop)
    , serverSock_(Socket::createNonblocking())
    , acceptChannel_(loop_, serverSock_.fd())
{
    InetAddress serverAddr(port, ip);

    // 配置监听 Socket 选项
    serverSock_.setReuseAddr(true);
    serverSock_.setReusePort(true);
    serverSock_.setTcpNoDelay(true);
    serverSock_.setKeepAlive(true);

    // 绑定并监听
    serverSock_.bind(serverAddr);
    serverSock_.listen();

    // 设置 Channel 的可读回调为接收新连接的逻辑
    acceptChannel_.setReadCallback(std::bind(&Acceptor::handleRead, this));
    acceptChannel_.enableReading();
}

Acceptor::~Acceptor()
{
    acceptChannel_.disableAll();
    acceptChannel_.remove();
}

void Acceptor::handleRead()
{
    // 循环 accept，一次性取出 Accept Queue 中所有待处理的连接。
    // 非阻塞 socket：accept() 返回 -1 且 errno == EAGAIN 时表示队列已空。
    // 旧实现每次 epoll 事件只 accept 一个连接，高并发时会造成
    // 大量连接的 Accept 延迟累积——每次都要等待下一轮 epoll_wait 返回。
    while (true)
    {
        InetAddress clientAddr;
        int connFd = serverSock_.accept(clientAddr);

        if (connFd >= 0)
        {
            auto clientSock = std::make_unique<Socket>(connFd);
            clientSock->setIpPort(clientAddr.toIp(), clientAddr.toPort());

            if (newConnectionCallback_)
            {
                newConnectionCallback_(std::move(clientSock));
            }
        }
        else
        {
            // EAGAIN/EWOULDBLOCK：队列已空，退出循环
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;

            LOG_WARN("Acceptor::handleRead accept failed, errno=%d", errno);
            break;
        }
    }
}

} // namespace wevix_muduo
