#pragma once

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include "wevix_muduo/Noncopyable.h"

namespace wevix_muduo
{

class InetAddress;

/**
 * @brief 封装底层 socket 文件描述符及相关操作
 */
class Socket : private Noncopyable
{
public:
    explicit Socket(int fd);
    ~Socket();

    // 基础接口
    int fd() const { return fd_; }
    std::string ip() const { return ip_; }
    uint16_t port() const { return port_; }

    // 设置辅助信息
    void setIpPort(const std::string& ip, uint16_t port);

    // 套接字选项设置
    void setReuseAddr(bool on);
    void setReusePort(bool on);
    void setTcpNoDelay(bool on);
    void setKeepAlive(bool on);

    // 服务端操作
    void bind(const InetAddress& serverAddr);
    void listen(int backlog = 4096);
    int accept(InetAddress& clientAddr);

    // 静态工具函数：创建非阻塞套接字
    static int createNonblocking();

private:
    const int fd_;
    std::string ip_;
    uint16_t port_;
};

} // namespace wevix_muduo
