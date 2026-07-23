#include "wevix_muduo/InetAddress.h"

#include <strings.h>
#include <cstring>

namespace wevix_muduo
{

InetAddress::InetAddress(uint16_t port, std::string ip)
{
    // 结构体清零
    ::bzero(&addr_, sizeof(addr_));
    
    addr_.sin_family = AF_INET;
    // 转换 IP 格式 (线程安全)
    ::inet_pton(AF_INET, ip.c_str(), &addr_.sin_addr);
    // 转换端口（大端字节序）
    addr_.sin_port = htons(port);
}

InetAddress::InetAddress(const sockaddr_in &addr)
    : addr_(addr)
{
}

InetAddress::~InetAddress()
{
}

std::string InetAddress::toIp() const
{
    char buf[64] = {0};
    // 使用 inet_ntop 代替 inet_ntoa 以保证线程安全
    ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof(buf));
    return buf;
}

std::string InetAddress::toIpPort() const
{
    // 拼接格式 "127.0.0.1:8080"
    char buf[64] = {0};
    ::inet_ntop(AF_INET, &addr_.sin_addr, buf, sizeof(buf));
    
    size_t end = ::strlen(buf);
    uint16_t port = ntohs(addr_.sin_port);
    ::sprintf(buf + end, ":%u", port);
    
    return buf;
}

uint16_t InetAddress::toPort() const
{
    return ntohs(addr_.sin_port);
}

const struct sockaddr* InetAddress::getSockAddr() const
{
    return reinterpret_cast<const struct sockaddr*>(&addr_);
}

void InetAddress::setSockAddr(const sockaddr_in &addr)
{
    addr_ = addr;
}

} // namespace wevix_muduo