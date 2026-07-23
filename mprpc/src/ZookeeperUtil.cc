#include "ZookeeperUtil.h"
#include "mprpcapplication.h"
#include "wevix_muduo/AsyncLogger.h"
#include <semaphore.h>

/**
 * 全局的 Watcher 观察器回调函数
 * Zookeeper 客户端会在一个单独的线程（回调线程）中执行这个函数。
 * 
 * @param zh 句柄
 * @param type 事件类型（如创建、删除、连接状态改变等）
 * @param state 当前连接状态
 * @param path 触发事件的节点路径
 * @param watcherCtx 用户自定义的上下文指针
 */
void global_watcher(zhandle_t *zh, int type,
                    int state, const char *path, void *watcherCtx)
{
    // 如果是会话相关的事件
    if (type == ZOO_SESSION_EVENT)
    {
        // 如果连接成功（建立了与服务器的会话）
        if (state == ZOO_CONNECTED_STATE)
        {
            // 从句柄中取出我们在 Start 函数里设置的信号量
            sem_t *sem = (sem_t*)zoo_get_context(zh);
            // 给信号量资源 +1，唤醒正在阻塞等待连接的 Start 线程
            sem_post(sem);
        }
    }
}

ZkClient::ZkClient() : m_zhandle(nullptr)
{
}

/**
 * 析构函数：负责释放 Zookeeper 句柄，关闭连接
 */
ZkClient::~ZkClient()
{
    if (m_zhandle != nullptr)
    {
        zookeeper_close(m_zhandle); // 类似关闭 socket，回收资源
    }
}

/**
 * 启动并连接 Zookeeper 服务器
 */
void ZkClient::Start()
{
    // 从配置文件中读取 Zookeeper 的 IP 和 端口
    std::string host = MprpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    std::string port = MprpcApplication::GetInstance().GetConfig().Load("zookeeperport");
    std::string connstr = host + ":" + port;

    /*
     * zookeeper_init 是异步启动的。
     * Zookeeper 的 C SDK 内部会启动三个线程：
     * 1. 执行 zookeeper_init 的主线程（API 调用线程）
     * 2. 网络 I/O 线程：负责发送请求、心跳以及接收响应（基于 poll/select）
     * 3. Watcher 回调线程：负责执行像 global_watcher 这样的回调函数
     */
    m_zhandle = zookeeper_init(connstr.c_str(), global_watcher, 30000, nullptr, nullptr, 0);
    
    if (nullptr == m_zhandle)
    {
        LOG_ERROR("zookeeper_init error!");
        exit(EXIT_FAILURE);
    }

    // --- 同步等待连接成功 ---
    // 因为 zookeeper_init 调用完立刻返回，并不代表连接已经建立。
    // 我们需要使用信号量来等待 global_watcher 收到 CONNECTED_STATE 信号。
    sem_t sem;
    sem_init(&sem, 0, 0); // 初始化信号量为 0
    zoo_set_context(m_zhandle, &sem); // 将信号量地址存入句柄上下文，方便回调函数取出

    sem_wait(&sem); // 阻塞在此，等待回调线程执行 sem_post
    LOG_INFO("zookeeper_init success!");
}

/**
 * 在 Zookeeper 上创建节点
 * @param path 节点路径
 * @param data 节点存储的数据（如 IP:Port）
 * @param datalen 数据长度
 * @param state 节点类型：0 为永久节点，ZOO_EPHEMERAL 为临时节点
 */
void ZkClient::Create(const char *path, const char *data, int datalen, int state)
{
    char path_buffer[128];
    int bufferlen = sizeof(path_buffer);
    int flag;

    // 先检查该节点是否存在，避免重复创建
    flag = zoo_exists(m_zhandle, path, 0, nullptr);
    if (ZNONODE == flag) // 如果节点不存在
    {
        // 创建节点
        // ZOO_OPEN_ACL_UNSAFE 表示该节点对所有人可见，完全开放权限
        flag = zoo_create(m_zhandle, path, data, datalen,
                          &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, bufferlen);
        if (flag == ZOK)
        {
            LOG_INFO("znode create success, path:%s", path);
        }
        else
        {
            LOG_ERROR("znode create error, flag:%d, path:%s", flag, path);
            exit(EXIT_FAILURE);
        }
    }
}

/**
 * 根据路径获取节点存储的内容（服务发现）
 * @param path 节点路径
 * @return 节点中的数据字符串
 */
std::string ZkClient::GetData(const char *path)
{
    char buffer[128];
    int bufferlen = sizeof(buffer);
    
    // 同步获取节点数据
    // 参数 0 表示不需要在该节点上设置 Watcher 监听
    int flag = zoo_get(m_zhandle, path, 0, buffer, &bufferlen, nullptr);
    
    if (flag != ZOK)
    {
        LOG_WARN("get znode data error, path:%s", path);
        return "";
    }
    else
    {
        // 将获取到的二进制缓冲区转为 std::string 返回
        return std::string(buffer, bufferlen);
    }
}