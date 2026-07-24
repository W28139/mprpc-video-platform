#pragma once

#include <semaphore.h>
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
