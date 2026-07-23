#include "wevix_muduo/Acceptor.h"
#include "wevix_muduo/EventLoop.h"
#include "wevix_muduo/InetAddress.h"
#include "wevix_muduo/AsyncLogger.h"

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
    InetAddress clientAddr;
    int connFd = serverSock_.accept(clientAddr);

    if (connFd >= 0)
    {
        // 封装为智能指针管理 Socket
        auto clientSock = std::make_unique<Socket>(connFd);
        clientSock->setIpPort(clientAddr.toIp(), clientAddr.toPort());

        // 执行上层（TcpServer）传入的回调函数
        if (newConnectionCallback_)
        {
            newConnectionCallback_(std::move(clientSock));
        }
        LOG_INFO("Acceptor accepted new fd=%d from %s:%u",
                 connFd, clientAddr.toIp().c_str(), clientAddr.toPort());
    }
    else
    {
        // EAGAIN/EWOULDBLOCK 是正常的（非阻塞 accept 无连接可接），不记录
        if (errno != EAGAIN && errno != EWOULDBLOCK)
        {
            LOG_WARN("Acceptor::handleRead accept failed, errno=%d", errno);
        }
    }
}

} // namespace wevix_muduo