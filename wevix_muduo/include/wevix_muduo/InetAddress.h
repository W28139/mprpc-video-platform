#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <string>

namespace wevix_muduo
{

/*
  封装 IPv4 地址和端口信息
*/
class InetAddress
{
public:
    // 构造函数：指定端口和IP
    explicit InetAddress(uint16_t port = 0, std::string ip = "127.0.0.1");

    // 构造函数：直接使用底层 sockaddr_in
    explicit InetAddress(const sockaddr_in &addr);

    // 析构函数
    ~InetAddress();

    // 获取字符串格式 IP
    std::string toIp() const;

    // 获取字符串格式 IP:Port
    std::string toIpPort() const;

    // 获取主机字节序的端口
    uint16_t toPort() const;

    // 获取底层结构体指针
    const struct sockaddr* getSockAddr() const;

    // 设置底层结构体
    void setSockAddr(const sockaddr_in &addr);

private:
    struct sockaddr_in addr_;
};

} // namespace wevix_muduo