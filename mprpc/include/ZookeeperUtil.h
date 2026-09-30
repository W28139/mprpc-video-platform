#pragma once

#include <semaphore.h>

// ZooKeeper C API 的同步接口（zoo_create / zoo_exists / zoo_delete / ...）
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

private:
    zhandle_t *m_zhandle;
};
