#pragma once

// GetLocalIp()：探测本机首个非 loopback IPv4 地址。
// Docker 部署 rpcserverip=0.0.0.0 时，注册 ZK 需用本机实际 IP（0.0.0.0 不可路由）。

#include <ifaddrs.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <cstring>
#include <string>

namespace mprpc {

/// 探测本机首个非 loopback IPv4 地址；失败返回空串
inline std::string GetLocalIp()
{
    struct ifaddrs* ifa = nullptr;
    if (getifaddrs(&ifa) != 0) return "";

    std::string result;
    for (struct ifaddrs* p = ifa; p; p = p->ifa_next)
    {
        if (!p->ifa_addr || p->ifa_addr->sa_family != AF_INET) continue;
        const auto* sa = reinterpret_cast<const struct sockaddr_in*>(p->ifa_addr);
        char buf[INET_ADDRSTRLEN] = {0};
        if (!inet_ntop(AF_INET, &sa->sin_addr, buf, sizeof(buf))) continue;
        std::string ip(buf);
        if (ip == "127.0.0.1" || ip == "0.0.0.0") continue;
        result = ip;
        break;
    }
    freeifaddrs(ifa);
    return result;
}

}  // namespace mprpc