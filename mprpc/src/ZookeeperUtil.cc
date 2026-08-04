#include "ZookeeperUtil.h"
#include "mprpcapplication.h"
#include "wevix_muduo/AsyncLogger.h"
#include <semaphore.h>
#include <errno.h>
#include <mutex>
#include <atomic>
#include <ctime>
#include <vector>

// ============================================================================
// 异步回调上下文（GetData/GetChildren 超时保护，阶段 11 修复）
// ============================================================================
// 背景：zoo_get / zoo_get_children 是同步 API，内部阻塞等待 watcher 线程
// 收到响应。若 ZK 连接处于「会话未建立/断线重连」状态（zookeeper_init 后
// 连接握手未完成，或重连中），同步调用**无限阻塞**——实测 SchedulingLoop
// 卡死 7+ 分钟（线程 wchan=futex_wait_queue）。
// 修复：改用 zoo_aget / zoo_aget_children（异步）+ 信号量 3s 超时等待；
// 超时后调用方按失败返回，ctx 交由回调线程延迟释放（caller_gone 标记），
// 杜绝悬垂指针。入口先查 zoo_state：非 CONNECTED 直接失败（快速路径）。
struct ZooAsyncCtx
{
    int rc = -1;                        ///< 回调返回码（ZOK=0）
    std::string data;                   ///< GetData 结果
    std::vector<std::string> children;  ///< GetChildren 结果
    sem_t sem;
    std::atomic<bool> caller_gone{false};
};

/// @brief zoo_aget 回调（watcher 线程执行）：填结果 → post → 按需自回收
static void GetDataCb(int rc, const char* value, int value_len,
                      const struct Stat* /*stat*/, const void* data)
{
    auto* ctx = const_cast<ZooAsyncCtx*>(static_cast<const ZooAsyncCtx*>(data));
    ctx->rc = rc;
    if (rc == ZOK && value != nullptr && value_len > 0)
        ctx->data.assign(value, value_len);
    sem_post(&ctx->sem);
    // 调用方已超时离开：回调负责回收（调用方不再碰 ctx，无竞态）
    if (ctx->caller_gone.load(std::memory_order_acquire))
        delete ctx;
}

/// @brief zoo_aget_children 回调（watcher 线程执行）
static void GetChildrenCb(int rc, const struct String_vector* strings, const void* data)
{
    auto* ctx = const_cast<ZooAsyncCtx*>(static_cast<const ZooAsyncCtx*>(data));
    ctx->rc = rc;
    if (rc == ZOK && strings != nullptr)
    {
        ctx->children.reserve(strings->count);
        for (int i = 0; i < strings->count; ++i)
            ctx->children.emplace_back(strings->data[i]);
    }
    sem_post(&ctx->sem);
    if (ctx->caller_gone.load(std::memory_order_acquire))
        delete ctx;
}

/// @brief 等待异步结果，最多 wait_ms 毫秒。
/// @return true=回调已完成（ctx 由调用方回收）；false=超时（ctx 归回调回收）
static bool WaitZooAsync(ZooAsyncCtx* ctx, int64_t wait_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += wait_ms / 1000;
    ts.tv_nsec += (wait_ms % 1000) * 1000000;
    if (ts.tv_nsec >= 1000000000)
    {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000;
    }
    int r = 0;
    while ((r = sem_timedwait(&ctx->sem, &ts)) == -1 && errno == EINTR) {}
    return r == 0;
}

/// @brief 检查 ZK 连接是否就绪（非 CONNECTED 时同步/异步调用都会阻塞等待）
static bool ZooConnected(zhandle_t* zh, const char* what)
{
    if (zh == nullptr) return false;
    int state = zoo_state(zh);
    if (state != ZOO_CONNECTED_STATE)
    {
        LOG_WARN("zookeeper not connected (state=%d), skip %s", state, what);
        return false;
    }
    return true;
}

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
            if (sem != nullptr)
            {
                // 给信号量资源 +1，唤醒正在阻塞等待连接的 Start 线程
                sem_post(sem);
            }
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
bool ZkClient::Start()
{
    if (m_zhandle != nullptr)
    {
        return true;
    }

    static std::once_flag zkLogOnce;
    std::call_once(zkLogOnce, []()
    {
        zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    });

    // 从配置文件中读取 Zookeeper 的 IP 和 端口
    std::string host = MprpcApplication::GetInstance().GetConfig().Load("zookeeperip");
    std::string port = MprpcApplication::GetInstance().GetConfig().Load("zookeeperport");
    if (host.empty() || port.empty())
    {
        LOG_ERROR("zookeeper config is invalid, host=%s, port=%s",
                  host.c_str(), port.c_str());
        return false;
    }
    std::string connstr = host + ":" + port;

    // --- 同步等待连接成功 ---
    // 因为 zookeeper_init 调用完立刻返回，并不代表连接已经建立。
    // 信号量必须在 zookeeper_init 前准备好，并通过 context 传入，避免连接事件先于
    // zoo_set_context 到达导致永久阻塞。
    sem_t sem;
    sem_init(&sem, 0, 0);

    /*
     * zookeeper_init 是异步启动的。
     * Zookeeper 的 C SDK 内部会启动三个线程：
     * 1. 执行 zookeeper_init 的主线程（API 调用线程）
     * 2. 网络 I/O 线程：负责发送请求、心跳以及接收响应（基于 poll/select）
     * 3. Watcher 回调线程：负责执行像 global_watcher 这样的回调函数
     */
    m_zhandle = zookeeper_init(connstr.c_str(), global_watcher, 30000, nullptr, &sem, 0);
    
    if (nullptr == m_zhandle)
    {
        LOG_ERROR("zookeeper_init error!");
        sem_destroy(&sem);
        return false;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 5;
    int waitRet = 0;
    while ((waitRet = sem_timedwait(&sem, &ts)) == -1 && errno == EINTR)
    {
    }

    if (waitRet == -1)
    {
        LOG_ERROR("zookeeper connect failed, errno=%d, connstr=%s", errno, connstr.c_str());
        zookeeper_close(m_zhandle);
        m_zhandle = nullptr;
        sem_destroy(&sem);
        return false;
    }

    zoo_set_context(m_zhandle, nullptr);
    sem_destroy(&sem);
    LOG_INFO("zookeeper_init success!");
    return true;
}

/**
 * 在 Zookeeper 上创建节点
 * @param path 节点路径
 * @param data 节点存储的数据（如 IP:Port）
 * @param datalen 数据长度
 * @param state 节点类型：0 为永久节点，ZOO_EPHEMERAL 为临时节点
 */
bool ZkClient::Create(const char *path, const char *data, int datalen,
                      int state, std::string* actualPath)
{
    if (m_zhandle == nullptr)
    {
        LOG_ERROR("zookeeper handle is null, create path:%s", path);
        return false;
    }

    char path_buffer[128];
    int bufferlen = sizeof(path_buffer);
    int flag;

    if (state & ZOO_SEQUENCE)
    {
        flag = zoo_create(m_zhandle, path, data, datalen,
                          &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, bufferlen);
        if (flag == ZOK)
        {
            if (actualPath != nullptr)
            {
                *actualPath = path_buffer;
            }
            LOG_INFO("znode sequence create success, path:%s", path_buffer);
            return true;
        }

        LOG_ERROR("znode sequence create error, flag:%d, path:%s", flag, path);
        return false;
    }

    // 先检查该节点是否存在
    flag = zoo_exists(m_zhandle, path, 0, nullptr);
    if (ZNONODE == flag) // 如果节点不存在
    {
        // 创建节点
        // ZOO_OPEN_ACL_UNSAFE 表示该节点对所有人可见，完全开放权限
        flag = zoo_create(m_zhandle, path, data, datalen,
                          &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, bufferlen);
        if (flag == ZOK)
        {
            if (actualPath != nullptr)
            {
                *actualPath = path_buffer;
            }
            LOG_INFO("znode create success, path:%s", path);
            return true;
        }
        else
        {
            LOG_ERROR("znode create error, flag:%d, path:%s", flag, path);
            return false;
        }
    }
    else if (flag == ZOK && state == ZOO_EPHEMERAL)
    {
        // 临时节点已存在（可能是上一个服务实例的 session 残留，ZK 30s 超时未到）
        // 删除旧节点后用当前 session 重建，确保节点绑定到当前会话
        LOG_INFO("ephemeral znode exists (stale session), recreating: %s", path);
        zoo_delete(m_zhandle, path, -1);  // -1 表示不检查版本号
        flag = zoo_create(m_zhandle, path, data, datalen,
                          &ZOO_OPEN_ACL_UNSAFE, state, path_buffer, bufferlen);
        if (flag == ZOK)
        {
            if (actualPath != nullptr)
            {
                *actualPath = path_buffer;
            }
            LOG_INFO("znode recreate success, path:%s", path);
            return true;
        }
        else
        {
            LOG_ERROR("znode recreate error, flag:%d, path:%s", flag, path);
            return false;
        }
    }

    if (flag == ZOK)
    {
        if (actualPath != nullptr)
        {
            *actualPath = path;
        }
        return true;
    }

    LOG_ERROR("znode exists check error, flag:%d, path:%s", flag, path);
    return false;
}

/**
 * 根据路径获取节点存储的内容（服务发现）
 * @param path 节点路径
 * @return 节点中的数据字符串
 */
std::string ZkClient::GetData(const char *path)
{
    if (m_zhandle == nullptr)
    {
        LOG_ERROR("zookeeper handle is null, get path:%s", path);
        return "";
    }
    // 快速失败：连接未就绪时 zoo_aget 同样会阻塞等待连接（阶段 11 修复）
    if (!ZooConnected(m_zhandle, path)) return "";

    auto* ctx = new ZooAsyncCtx;
    sem_init(&ctx->sem, 0, 0);
    // 异步获取节点数据（watcher=0：不设置 Watcher 监听）
    int rc = zoo_aget(m_zhandle, path, 0, GetDataCb, ctx);
    if (rc != ZOK)
    {
        sem_destroy(&ctx->sem);
        delete ctx;
        LOG_WARN("zoo_aget failed, path:%s rc=%d", path, rc);
        return "";
    }
    if (!WaitZooAsync(ctx, 3000))
    {
        // 3s 无响应：按发现失败返回（调用方已有重试/失效缓存回退）；
        // ctx 由回调线程最终回收（caller_gone 标记），不泄漏不悬垂
        ctx->caller_gone.store(true, std::memory_order_release);
        LOG_WARN("zookeeper get data timeout (3s), path:%s", path);
        return "";
    }
    std::string result = ctx->data;
    sem_destroy(&ctx->sem);
    delete ctx;
    return result;
}

std::vector<std::string> ZkClient::GetChildren(const char *path)
{
    std::vector<std::string> children;
    if (m_zhandle == nullptr)
    {
        LOG_ERROR("zookeeper handle is null, get children path:%s", path);
        return children;
    }
    // 快速失败：连接未就绪时不进入阻塞等待（阶段 11 修复）
    if (!ZooConnected(m_zhandle, path)) return children;

    auto* ctx = new ZooAsyncCtx;
    sem_init(&ctx->sem, 0, 0);
    int rc = zoo_aget_children(m_zhandle, path, 0, GetChildrenCb, ctx);
    if (rc != ZOK)
    {
        sem_destroy(&ctx->sem);
        delete ctx;
        LOG_WARN("zoo_aget_children failed, path:%s rc=%d", path, rc);
        return children;
    }
    if (!WaitZooAsync(ctx, 3000))
    {
        ctx->caller_gone.store(true, std::memory_order_release);
        LOG_WARN("zookeeper get children timeout (3s), path:%s", path);
        return children;
    }
    children = std::move(ctx->children);
    sem_destroy(&ctx->sem);
    delete ctx;
    return children;
}

bool ZkClient::IsStarted() const
{
    return m_zhandle != nullptr;
}
