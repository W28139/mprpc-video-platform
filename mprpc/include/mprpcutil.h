#pragma once

// ============================================================================
// mprpc 公共工具（阶段 13：容器化支持）
// ============================================================================
//
// GetLocalIp()：探测本机首个非 loopback IPv4 地址。
//
// 背景：Provider 注册到 ZooKeeper 的服务发现地址取自配置 rpcserverip。
// 本机部署（127.0.0.1）无碍；Docker 部署绑定 0.0.0.0（全接口监听）时，
// 若把 0.0.0.0 直接注册到 ZK，消费者拿到的 endpoint 是不可路由的
// 死地址（实测 connect 0.0.0.0:9002 → errno 111）。因此 rpcserverip 为
// 0.0.0.0/空时，注册与上报一律用本机实际 IP（容器内 = 容器 IP，经
// compose 网络可达）。
//
// @see RpcProvider::Run     ZK 服务注册地址
// @see RunHeartbeatLoop     TranscodeWorker 注册上报地址

#include <ifaddrs.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <cstring>
#include <string>

namespace mprpc {

/// @brief 探测本机首个非 loopback IPv4 地址；失败返回空串
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
        result = ip;   // 取第一个非 loopback 网卡地址
        break;
    }
    freeifaddrs(ifa);
    return result;
}

}  // namespace mprpc
