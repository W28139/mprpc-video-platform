#pragma once

#include <mutex>
#include <string>
#include <utility>
#include <vector>

struct redisContext;
struct redisReply;

namespace video_platform {

// ============================================================================
// RedisClient — Redis 客户端封装（hiredis C API，阶段 10）
// ============================================================================
//
// 用于读路径加速：Worker 负载快照 / Job 进度缓存 / 分布式锁。
//
// 与 MysqlPool 的关键差异——**Redis 是可降级组件**：
// - Init() 连接失败只打 WARN，不拒绝服务启动（MySQL 是唯一数据源，
//   连不上必须 fail-fast；Redis 只是加速，故障时降级回原路径即可）
// - 所有命令失败返回 false，不抛异常、不阻塞主流程，调用方负责降级
// - 单连接 + mutex：调用频率低（心跳 3s/worker、QueryJob、分配锁），
//   不需要连接池；redisContext 非线程安全，用锁保护
// - 命令失败自动断开重建连接并重试 1 次（redisContext 断连后不可复用）
//
// 配置项（从 MprpcApplication 读取，ini 风格）：
//   redis_enable / redis_host / redis_port
// ============================================================================

/// @brief Redis 客户端（单例，线程安全）
class RedisClient {
public:
    /// @brief 获取全局唯一实例
    static RedisClient& GetInstance();

    /// @brief 从配置初始化。redis_enable=0 或连接失败时 enabled() 为 false，
    /// 调用方应降级（与 MySQL 不同，不拒绝服务启动）。
    bool Init();

    /// @brief Redis 是否可用（enable 且已连接）
    bool enabled() const { return enabled_; }

    /// @brief 是否已调用过 Init（未调用时所有命令返回 false）
    bool inited() const { return inited_; }

    // ── 哈希操作（Worker 负载快照：HSET worker:load {worker_id} → 快照串） ──

    /// @brief HSET，成功返回 true
    bool HSet(const std::string& key, const std::string& field, const std::string& value);

    /// @brief HGETALL，全部 (field, value) 写入 out。key 不存在时 out 为空且返回 true。
    bool HGetAll(const std::string& key,
                 std::vector<std::pair<std::string, std::string>>& out);

    // ── KV 操作（进度缓存 / 分布式锁） ──

    /// @brief GET。key 不存在（nil）时返回 true 且 out 不变、found=false。
    bool Get(const std::string& key, std::string& out, bool& found);

    /// @brief SETEX（带 TTL 秒），成功返回 true
    bool SetEx(const std::string& key, const std::string& value, int64_t ttl_sec);

    /// @brief SET key value NX EX ttl —— 仅当 key 不存在时设置。
    /// @return true=本次成功取得锁；false=key 已存在（他人持锁）或 Redis 失败
    bool SetNxEx(const std::string& key, const std::string& value, int64_t ttl_sec);

    /// @brief DEL，成功返回 true
    bool Del(const std::string& key);

private:
    RedisClient() = default;
    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    /// @brief 建立新连接（需持有 mutex_）。设置 connect/command 超时，失败返回 false。
    bool Connect();

    /// @brief 执行命令（需持有 mutex_）。失败时断开重建连接并重试 1 次。
    /// @return 非空 reply 指针，调用方负责 freeReplyObject
    redisReply* ExecLocked(const char* fmt, ...);

    std::string host_ = "127.0.0.1";
    int         port_ = 6379;
    bool        enabled_ = false;   ///< redis_enable 配置开关
    bool        inited_ = false;    ///< Init 已调用
    redisContext* ctx_ = nullptr;
    mutable std::mutex mutex_;
};

} // namespace video_platform
