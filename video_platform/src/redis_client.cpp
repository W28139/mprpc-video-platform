#include "video_platform/redis_client.h"

#include <hiredis/hiredis.h>

#include <cstdarg>
#include <cstdio>

#include "mprpcapplication.h"
#include "wevix_muduo/AsyncLogger.h"

namespace video_platform {

// ============================================================================
// RedisClient — hiredis 同步 API 封装
// ============================================================================
//
// 设计要点：
// - 单连接 + mutex：调用频率低，连接池没有收益；redisContext 非线程安全，
//   所有命令在锁内执行
// - 失败重连：redisContext 在连接断开后必须 redisFree 重建（不可复用），
//   每条命令失败时重建并重试 1 次；重建仍失败则本次返回 false 降级
// - 超时保护：connect/command 均设 2s 超时，Redis 卡死不会阻塞业务线程
// - 二进制安全：hiredis 格式化命令（%s 记录长度），key/value 含空格无碍
// ============================================================================

RedisClient& RedisClient::GetInstance()
{
    static RedisClient instance;
    return instance;
}

bool RedisClient::Init()
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto& cfg = MprpcApplication::GetConfig();

    // 开关：显式 redis_enable=0 直接禁用（用于故障演练 / 纯降级部署）
    enabled_ = cfg.LoadInt("redis_enable", 1, 0, 1) != 0;
    host_    = cfg.Load("redis_host");
    if (host_.empty()) host_ = "127.0.0.1";
    port_    = cfg.LoadInt("redis_port", 6379, 1, 65535);

    inited_ = true;
    if (!enabled_)
    {
        LOG_INFO("RedisClient disabled by config (redis_enable=0)");
        return false;
    }

    if (Connect())
    {
        LOG_INFO("RedisClient connected to %s:%d", host_.c_str(), port_);
        return true;
    }

    // 与 MysqlPool 不同：Redis 是可降级组件，连接失败不拒绝服务启动。
    // 注意：连接失败把 enabled_ 置 false——调用方以 enabled() 判断"当前
    // 是否可用"直接走降级路径，避免每次调用都发命令失败再降级。
    // 运行中 Redis 故障：命令失败 → 调用方按次降级；Redis 恢复后自动切回。
    enabled_ = false;
    LOG_WARN("RedisClient connect to %s:%d failed, degrade to local "
             "cache/RPC fallback (MySQL remains source of truth)",
             host_.c_str(), port_);
    return false;
}

bool RedisClient::Connect()
{
    // 2s 连接超时 + 2s 命令超时，Redis 故障不阻塞业务线程
    struct timeval timeout {2, 0};
    ctx_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
    if (ctx_ == nullptr)
    {
        LOG_WARN("RedisClient: OOM allocating redisContext");
        return false;
    }
    if (ctx_->err)
    {
        LOG_WARN("RedisClient: connect error: %s", ctx_->errstr);
        redisFree(ctx_);
        ctx_ = nullptr;
        return false;
    }
    redisSetTimeout(ctx_, timeout);
    return true;
}

redisReply* RedisClient::ExecLocked(const char* fmt, ...)
{
    if (!inited_ || !enabled_ || ctx_ == nullptr) return nullptr;

    va_list ap;
    va_start(ap, fmt);
    redisReply* reply = static_cast<redisReply*>(redisvCommand(ctx_, fmt, ap));
    va_end(ap);

    if (reply == nullptr)
    {
        // 连接已断（或 Redis 重启/网络抖动）：重建连接重试 1 次
        LOG_WARN("RedisClient: command failed (%s), reconnecting and retry once",
                 ctx_ && ctx_->err ? ctx_->errstr : "unknown error");
        redisFree(ctx_);
        ctx_ = nullptr;
        if (!Connect()) return nullptr;

        va_start(ap, fmt);
        reply = static_cast<redisReply*>(redisvCommand(ctx_, fmt, ap));
        va_end(ap);
        if (reply == nullptr)
        {
            LOG_WARN("RedisClient: retry failed (%s), degrade for this call",
                     ctx_ && ctx_->err ? ctx_->errstr : "unknown error");
            return nullptr;
        }
    }
    return reply;
}

bool RedisClient::HSet(const std::string& key, const std::string& field,
                       const std::string& value)
{
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = ExecLocked("HSET %s %s %s",
                                   key.c_str(), field.c_str(), value.c_str());
    if (reply == nullptr) return false;
    bool ok = reply->type == REDIS_REPLY_INTEGER;  // HSET 返回 1（新增）/ 0（覆盖）
    freeReplyObject(reply);
    return ok;
}

bool RedisClient::HGetAll(const std::string& key,
                          std::vector<std::pair<std::string, std::string>>& out)
{
    out.clear();
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = ExecLocked("HGETALL %s", key.c_str());
    if (reply == nullptr) return false;

    // 数组：field/value 成对出现；key 不存在时返回空数组
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements % 2 == 0)
    {
        out.reserve(reply->elements / 2);
        for (size_t i = 0; i + 1 < reply->elements; i += 2)
        {
            redisReply* f = reply->element[i];
            redisReply* v = reply->element[i + 1];
            if (f->type == REDIS_REPLY_STRING && v->type == REDIS_REPLY_STRING)
                out.emplace_back(std::string(f->str, f->len),
                                 std::string(v->str, v->len));
        }
    }
    freeReplyObject(reply);
    return true;
}

bool RedisClient::Get(const std::string& key, std::string& out, bool& found)
{
    found = false;
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = ExecLocked("GET %s", key.c_str());
    if (reply == nullptr) return false;

    if (reply->type == REDIS_REPLY_STRING)
    {
        out = std::string(reply->str, reply->len);
        found = true;
    }
    // nil（key 不存在）不算失败
    freeReplyObject(reply);
    return true;
}

bool RedisClient::SetEx(const std::string& key, const std::string& value,
                        int64_t ttl_sec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = ExecLocked("SETEX %s %lld %s", key.c_str(),
                                   static_cast<long long>(ttl_sec), value.c_str());
    if (reply == nullptr) return false;
    bool ok = reply->type == REDIS_REPLY_STATUS;  // 返回 "+OK"
    freeReplyObject(reply);
    return ok;
}

bool RedisClient::SetNxEx(const std::string& key, const std::string& value,
                          int64_t ttl_sec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    // SET key value NX EX ttl：仅当 key 不存在时设置。
    // 成功 → +OK；key 已存在 → nil（不是失败，返回 false 表示未取得锁）
    redisReply* reply = ExecLocked("SET %s %s NX EX %lld", key.c_str(),
                                   value.c_str(), static_cast<long long>(ttl_sec));
    if (reply == nullptr) return false;
    bool ok = reply->type == REDIS_REPLY_STATUS;
    freeReplyObject(reply);
    return ok;
}

bool RedisClient::Del(const std::string& key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    redisReply* reply = ExecLocked("DEL %s", key.c_str());
    if (reply == nullptr) return false;
    bool ok = reply->type == REDIS_REPLY_INTEGER;
    freeReplyObject(reply);
    return ok;
}

} // namespace video_platform
