#include"mprpcredis.h"

#include <hiredis/hiredis.h>

#include <cstdarg>
#include <cstdio>

#include"mprpcapplication.h"
#include"wevix_muduo/AsyncLogger.h"

// ============================================================================
// MprpcRedisClient — hiredis 同步 API 封装（框架层服务发现缓存专用）
// ============================================================================
//
// 设计要点（与业务层 RedisClient 一脉相承，可降级哲学）：
// - 懒初始化：首次命令调用时读配置并连接，框架使用者无需改 main。
//   连接失败只 WARN 并置 enabled_=false——服务发现退化到原"进程内缓存 +
//   ZK 直读"路径，与改造前行为完全一致
// - 失败重连：redisContext 断连后必须 redisFree 重建，每条命令失败重建
//   并重试 1 次；重建仍失败返回 false，调用方降级
// - 超时保护：connect/command 均设 2s 超时，Redis 卡死不阻塞 RPC 调用
// - 命令集刻意最小化（HGET/HSET/EXPIRE/HDEL）：只服务 ZK 缓存集中管理，
//   不复制业务层的锁/快照命令，保持框架轻量
// ============================================================================

MprpcRedisClient& MprpcRedisClient::GetInstance()
{
    static MprpcRedisClient instance;
    return instance;
}

bool MprpcRedisClient::enabled()
{
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureInitLocked();
    return enabled_;
}

void MprpcRedisClient::EnsureInitLocked()
{
    if (inited_)
    {
        return;
    }
    inited_ = true;

    // 配置键与业务层 RedisClient 完全一致（redis_enable/redis_host/redis_port），
    // 业务服务的 conf 无需为框架层单独配置。
    auto& cfg = MprpcApplication::GetConfig();
    enabled_ = cfg.LoadInt("redis_enable", 1, 0, 1) != 0;
    host_ = cfg.Load("redis_host");
    if (host_.empty())
    {
        host_ = "127.0.0.1";
    }
    port_ = cfg.LoadInt("redis_port", 6379, 1, 65535);

    if (!enabled_)
    {
        LOG_INFO("MprpcRedisClient disabled by config (redis_enable=0), "
                 "service discovery keeps local cache only");
        return;
    }

    if (Connect())
    {
        LOG_INFO("MprpcRedisClient connected to %s:%d", host_.c_str(), port_);
        return;
    }

    // 可降级组件语义：连接失败不拒绝任何调用，服务发现走原本地缓存路径。
    enabled_ = false;
    LOG_WARN("MprpcRedisClient connect to %s:%d failed, service discovery "
             "degrades to local cache/ZK direct read",
             host_.c_str(), port_);
}

bool MprpcRedisClient::Connect()
{
    // 2s 连接超时 + 2s 命令超时，Redis 故障不阻塞 RPC 热路径
    struct timeval timeout {2, 0};
    ctx_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
    if (ctx_ == nullptr)
    {
        LOG_WARN("MprpcRedisClient: OOM allocating redisContext");
        return false;
    }
    if (ctx_->err)
    {
        LOG_WARN("MprpcRedisClient: connect error: %s", ctx_->errstr);
        redisFree(ctx_);
        ctx_ = nullptr;
        return false;
    }
    redisSetTimeout(ctx_, timeout);
    return true;
}

redisReply* MprpcRedisClient::ExecLocked(const char* fmt, ...)
{
    if (!inited_ || !enabled_ || ctx_ == nullptr)
    {
        return nullptr;
    }

    va_list ap;
    va_start(ap, fmt);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(ctx_, fmt, ap));
    va_end(ap);

    if (reply == nullptr)
    {
        // 连接已断（或 Redis 重启/网络抖动）：重建连接重试 1 次
        LOG_WARN("MprpcRedisClient: command failed (%s), reconnecting and retry once",
                 ctx_ && ctx_->err ? ctx_->errstr : "unknown error");
        redisFree(ctx_);
        ctx_ = nullptr;
        if (!Connect())
        {
            return nullptr;
        }

        va_start(ap, fmt);
        reply = static_cast<redisReply*>(redisvCommand(ctx_, fmt, ap));
        va_end(ap);
        if (reply == nullptr)
        {
            LOG_WARN("MprpcRedisClient: retry failed (%s), degrade for this call",
                     ctx_ && ctx_->err ? ctx_->errstr : "unknown error");
            return nullptr;
        }
    }
    return reply;
}

bool MprpcRedisClient::HGet(const std::string& key, const std::string& field,
                            std::string& out, bool& found)
{
    found = false;
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureInitLocked();
    redisReply* reply = ExecLocked("HGET %s %s", key.c_str(), field.c_str());
    if (reply == nullptr)
    {
        return false;
    }

    if (reply->type == REDIS_REPLY_STRING)
    {
        out = std::string(reply->str, reply->len);
        found = true;
    }
    // nil（key 或 field 不存在）不算失败，调用方按 miss 处理
    freeReplyObject(reply);
    return true;
}

bool MprpcRedisClient::HSet(const std::string& key, const std::string& field,
                            const std::string& value)
{
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureInitLocked();
    redisReply* reply = ExecLocked("HSET %s %s %s",
                                   key.c_str(), field.c_str(), value.c_str());
    if (reply == nullptr)
    {
        return false;
    }
    bool ok = reply->type == REDIS_REPLY_INTEGER;  // HSET 返回 1（新增）/ 0（覆盖）
    freeReplyObject(reply);
    return ok;
}

bool MprpcRedisClient::Expire(const std::string& key, int64_t ttl_sec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureInitLocked();
    redisReply* reply = ExecLocked("EXPIRE %s %lld", key.c_str(),
                                   static_cast<long long>(ttl_sec));
    if (reply == nullptr)
    {
        return false;
    }
    bool ok = reply->type == REDIS_REPLY_INTEGER;  // EXPIRE 返回 1（已设置）/ 0（key 不存在）
    freeReplyObject(reply);
    return ok;
}

bool MprpcRedisClient::HDel(const std::string& key, const std::string& field)
{
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureInitLocked();
    redisReply* reply = ExecLocked("HDEL %s %s", key.c_str(), field.c_str());
    if (reply == nullptr)
    {
        return false;
    }
    bool ok = reply->type == REDIS_REPLY_INTEGER;  // HDEL 返回删除个数，field 不存在为 0
    freeReplyObject(reply);
    return ok;
}
