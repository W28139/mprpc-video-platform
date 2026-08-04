#pragma once

#include <mutex>
#include <string>

struct redisContext;
struct redisReply;

// ============================================================================
// MprpcRedisClient — mprpc 框架层轻量 Redis 客户端（阶段 10 第三批）
// ============================================================================
//
// 用途：ZK 服务地址缓存的集中管理。MprpcChannel 把 method_path → endpoint
// 列表写入 Redis HSET（key=mprpc:endpoints），多进程共享服务发现结果，
// 减少每个进程各自访问 ZK 的次数。
//
// 为什么框架自建而不是复用 video_platform 的 RedisClient：
//   mprpc 是独立框架层，不应反向依赖业务子项目；这里只需要 4 个命令
//   （HGET/HSET/EXPIRE/HDEL），~150 行足够，hiredis 是系统库。
//
// 与业务层 RedisClient 相同的关键语义——**Redis 是可降级组件**：
// - 懒初始化：首次命令调用时读配置并连接（框架层无法要求调用方改 main
//   显式 Init）。连接失败只 WARN，enabled()=false，调用方直落本地缓存路径
// - 命令失败返回 false，不抛异常不阻塞主流程；命令失败自动重建连接重试 1 次
// - 单连接 + mutex：redisContext 非线程安全，所有命令锁内执行；
//   connect/command 均设 2s 超时，Redis 卡死不阻塞 RPC 调用
//
// 配置项（与 video_platform RedisClient 复用同一批键，业务 conf 零改动）：
//   redis_enable / redis_host / redis_port
//   （4 个业务服务 conf 已配 redis_enable=1；未配置时默认启用、连本机 6379）
// ============================================================================

/// @brief mprpc 框架层 Redis 客户端（单例，线程安全，懒初始化）
class MprpcRedisClient
{
public:
    /// @brief 获取全局唯一实例
    static MprpcRedisClient& GetInstance();

    /// @brief Redis 当前是否可用（enable 且已连接）。未初始化时返回 false，
    /// 但不会触发初始化——首次命令调用（HGet/HSet/...）才做懒初始化。
    bool enabled();

    /// @brief HGET。key 或 field 不存在（nil）时返回 true 且 found=false。
    bool HGet(const std::string& key, const std::string& field,
              std::string& out, bool& found);

    /// @brief HSET，成功返回 true
    bool HSet(const std::string& key, const std::string& field,
              const std::string& value);

    /// @brief EXPIRE key ttl_sec，成功返回 true
    bool Expire(const std::string& key, int64_t ttl_sec);

    /// @brief HDEL，成功返回 true（field 不存在返回 0，不算失败）
    bool HDel(const std::string& key, const std::string& field);

private:
    MprpcRedisClient() = default;
    MprpcRedisClient(const MprpcRedisClient&) = delete;
    MprpcRedisClient& operator=(const MprpcRedisClient&) = delete;

    /// @brief 懒初始化（需持有 mutex_）：读配置 + 连接。只执行一次。
    void EnsureInitLocked();

    /// @brief 建立新连接（需持有 mutex_）。失败返回 false。
    bool Connect();

    /// @brief 执行命令（需持有 mutex_）。失败时断开重建连接并重试 1 次。
    /// @return 非空 reply 指针，调用方负责 freeReplyObject
    redisReply* ExecLocked(const char* fmt, ...);

    std::string  host_ = "127.0.0.1";
    int          port_ = 6379;
    bool         enabled_ = false;   ///< redis_enable 配置开关 + 连接状态
    bool         inited_ = false;    ///< 懒初始化已完成（成功或失败都置 true）
    std::once_flag initOnce_;
    redisContext* ctx_ = nullptr;
    std::mutex    mutex_;
};
