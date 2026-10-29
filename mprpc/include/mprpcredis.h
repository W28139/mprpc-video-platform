#pragma once

#include <mutex>
#include <string>

struct redisContext;
struct redisReply;

// MprpcRedisClient — 框架层 Redis 客户端（ZK 服务地址缓存集中管理）。
// Redis 是可降级组件：连接失败只 WARN，命令失败返回 false。
// 配置项：redis_enable / redis_host / redis_port

/// mprpc 框架层 Redis 客户端（单例，线程安全，懒初始化）
class MprpcRedisClient
{
public:
    static MprpcRedisClient& GetInstance();

    bool enabled();

    /// HGET。key/field 不存在（nil）时 found=false
    bool HGet(const std::string& key, const std::string& field,
              std::string& out, bool& found);

    /// HSET
    bool HSet(const std::string& key, const std::string& field,
              const std::string& value);

    /// EXPIRE key ttl_sec
    bool Expire(const std::string& key, int64_t ttl_sec);

    /// HDEL（field 不存在不算失败）
    bool HDel(const std::string& key, const std::string& field);

private:
    MprpcRedisClient() = default;
    MprpcRedisClient(const MprpcRedisClient&) = delete;
    MprpcRedisClient& operator=(const MprpcRedisClient&) = delete;

    void EnsureInitLocked();    // 懒初始化：读配置 + 连接，只执行一次
    bool Connect();             // 建立新连接
    redisReply* ExecLocked(const char* fmt, ...);  // 执行命令，失败重建连接重试 1 次

    std::string  host_ = "127.0.0.1";
    int          port_ = 6379;
    bool         enabled_ = false;
    bool         inited_ = false;
    std::once_flag initOnce_;
    redisContext* ctx_ = nullptr;
    std::mutex    mutex_;
};