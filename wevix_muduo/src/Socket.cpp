#include "wevix_muduo/Socket.h"
#include "wevix_muduo/InetAddress.h"
#include "wevix_muduo/AsyncLogger.h"

#include <unistd.h>
#include <fcntl.h>
#include <cstdio>
#include <cstring>
// 给acceptor调用，在主事件循环里，不断接受新的连接
namespace wevix_muduo
{

int Socket::createNonblocking()
{
    // 创建非阻塞、带 CLOEXEC 标志的套接字
    int listenFd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (listenFd < 0)
    {
        LOG_ERROR("socket create failed, errno=%d", errno);
    }
    return listenFd;
}

Socket::Socket(int fd)
    : fd_(fd)
{
}

Socket::~Socket()
{
    // RAII 自动关闭描述符
    ::close(fd_);
}

void Socket::setIpPort(const std::string& ip, uint16_t port)
{
    ip_ = ip;
    port_ = port;
}

void Socket::setTcpNoDelay(bool on)
{
    int optval = on ? 1 : 0;
    if (::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) < 0)
    {
        LOG_ERROR("setsockopt TCP_NODELAY failed, fd=%d, errno=%d", fd_, errno);
    }
}

void Socket::setReuseAddr(bool on)
{
    int optval = on ? 1 : 0;
    if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0)
    {
        LOG_ERROR("setsockopt SO_REUSEADDR failed, fd=%d, errno=%d", fd_, errno);
    }
}

void Socket::setReusePort(bool on)
{
    int optval = on ? 1 : 0;
    if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) < 0)
    {
        LOG_ERROR("setsockopt SO_REUSEPORT failed, fd=%d, errno=%d", fd_, errno);
    }
}

void Socket::setKeepAlive(bool on)
{
    int optval = on ? 1 : 0;
    if (::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) < 0)
    {
        LOG_ERROR("setsockopt SO_KEEPALIVE failed, fd=%d, errno=%d", fd_, errno);
    }
}

void Socket::bind(const InetAddress& serverAddr)
{
    int ret = ::bind(fd_, serverAddr.getSockAddr(), sizeof(struct sockaddr_in));
    if (ret < 0)
    {
        LOG_ERROR("bind fd:%d to %s failed, errno=%d", fd_, serverAddr.toIpPort().c_str(), errno);
    }
}

void Socket::listen(int backlog)
{
    int ret = ::listen(fd_, backlog);
    if (ret < 0)
    {
        LOG_ERROR("listen fd:%d failed, errno=%d", fd_, errno);
    }
}

int Socket::accept(InetAddress& clientAddr)
{
    struct sockaddr_in addr;
    ::memset(&addr, 0, sizeof(addr));
    socklen_t len = sizeof(addr);

    // 使用 accept4 一步到位地设置非阻塞和 CLOEXEC
    int connFd = ::accept4(fd_, (struct sockaddr*)&addr, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (connFd >= 0)
    {
        clientAddr.setSockAddr(addr);
    }
    else
    {
        LOG_ERROR("accept failed, errno=%d", errno);
    }
    return connFd;
}

} // namespace wevix_muduo