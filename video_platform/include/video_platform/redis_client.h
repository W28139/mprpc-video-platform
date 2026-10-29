#pragma once

#include <mutex>
#include <string>
#include <utility>
#include <vector>

struct redisContext;
struct redisReply;

namespace video_platform {

// RedisClient — Redis 客户端（hiredis 封装）。
// Redis 是可降级组件：Init 失败只 WARN 不拒绝启动，命令失败返回 false。
// 单连接 + mutex（调用频率低，无需连接池）。
// 配置项：redis_enable / redis_host / redis_port

/// Redis 客户端（单例，线程安全）
class RedisClient {
public:
    static RedisClient& GetInstance();

    /// 从配置初始化。连接失败时 enabled() 为 false，调用方应降级
    bool Init();

    bool enabled() const { return enabled_; }
    bool inited() const { return inited_; }

    /// HSET
    bool HSet(const std::string& key, const std::string& field, const std::string& value);

    /// HGETALL：全部 (field, value) 写入 out，key 不存在返回空
    bool HGetAll(const std::string& key,
                 std::vector<std::pair<std::string, std::string>>& out);

    /// GET。key 不存在（nil）时 found=false
    bool Get(const std::string& key, std::string& out, bool& found);

    /// SET key value NX EX ttl：仅 key 不存在时设置（分布式锁）
    bool SetNxEx(const std::string& key, const std::string& value, int64_t ttl_sec);

    /// DEL
    bool Del(const std::string& key);

private:
    RedisClient() = default;
    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    bool Connect();             // 建立新连接
    redisReply* ExecLocked(const char* fmt, ...);  // 执行命令，失败重建连接重试 1 次

    std::string host_ = "127.0.0.1";
    int         port_ = 6379;
    bool        enabled_ = false;
    bool        inited_ = false;
    redisContext* ctx_ = nullptr;
    mutable std::mutex mutex_;
};

} // namespace video_platform