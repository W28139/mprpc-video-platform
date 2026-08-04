#pragma once

#include <semaphore.h>

// ZooKeeper C API 的同步接口（zoo_create / zoo_exists / zoo_delete / ...）
// 在 3.5+ 头文件中被 #ifdef THREADED 包裹，仅线程安全（_mt）模式下可见。
// 链接 libzookeeper_mt 时必须在使用头文件前定义 THREADED，否则同步 API
// 全部不可见（编译期报 "zoo_create was not declared"）。
// （阶段 13 容器实测暴露：开发机手动安装的旧版头文件无此限制，而 Ubuntu
//  24.04 的 libzookeeper-mt-dev 3.9.1 有；详见 doc/更新业务日志/15.）
#define THREADED
#include <zookeeper/zookeeper.h>
#include <string>
#include <vector>

class ZkClient
{
public:
    ZkClient();
    ~ZkClient();

    bool Start();
    bool Create(const char *path, const char *data, int datalen,
                int state=0, std::string* actualPath=nullptr);
    std::string GetData(const char *path);
    std::vector<std::string> GetChildren(const char *path);
    bool IsStarted() const;

private:
    zhandle_t *m_zhandle;
};
