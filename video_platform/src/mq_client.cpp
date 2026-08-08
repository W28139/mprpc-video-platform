#include "video_platform/mq_client.h"

#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>

#include <cstring>
#include <poll.h>
#include <set>
#include <sys/socket.h>
#include <sys/time.h>

#include "mprpcapplication.h"
#include "wevix_muduo/AsyncLogger.h"

namespace video_platform {

namespace {
constexpr char kChannel = 1;  // AMQP 通道号

// 事件拓扑（与 mq_client.h 注释保持一致）
constexpr const char* kExchangeJobEvents    = "job.events";
constexpr const char* kExchangeShardEvents  = "shard.events";
constexpr const char* kQueueShardWaiting    = "shard.waiting";
constexpr const char* kQueueResultPending   = "result.pending";

// 检查 RPC 回复是否正常；异常时打 WARN 并返回 false
bool CheckReply(amqp_connection_state_t conn, const char* what)
{
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) return true;
    LOG_WARN("MqClient: %s failed: %s", what,
             amqp_error_string2(reply.library_error));
    return false;
}

static void SetSocketTimeouts(amqp_connection_state_t conn, int sec)
{
    if (conn == nullptr) return;
    int fd = amqp_get_sockfd(conn);   // 公开 API：取底层 socket fd
    if (fd < 0) return;
    struct timeval tv;
    tv.tv_sec = sec;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

amqp_connection_state_t OpenChannel(const std::string& host, int port,
                                    const char* label)
{
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);
    if (socket == nullptr)
    {
        LOG_WARN("MqClient: %s OOM allocating amqp socket", label);
        amqp_destroy_connection(conn);
        return nullptr;
    }
    if (amqp_socket_open(socket, host.c_str(), port) < 0)
    {
        LOG_WARN("MqClient: %s socket open %s:%d failed", label,
                 host.c_str(), port);
        amqp_destroy_connection(conn);
        return nullptr;
    }
    // 连接建立后立即设置收发超时：login/channel_open 的握手读写有界
    SetSocketTimeouts(conn, 2);

    amqp_rpc_reply_t reply = amqp_login(conn, "/", 0, 131072, 0,
                                        AMQP_SASL_METHOD_PLAIN, "guest", "guest");
    if (reply.reply_type != AMQP_RESPONSE_NORMAL)
    {
        LOG_WARN("MqClient: %s login failed: %s", label,
                 amqp_error_string2(reply.library_error));
        amqp_destroy_connection(conn);
        return nullptr;
    }

    amqp_channel_open(conn, kChannel);
    if (!CheckReply(conn, "channel_open"))
    {
        amqp_destroy_connection(conn);
        return nullptr;
    }
    return conn;
}
} // namespace

MqClient& MqClient::GetInstance()
{
    static MqClient instance;
    return instance;
}

bool MqClient::Init()
{
    std::lock_guard<std::mutex> lock(publish_mutex_);
    auto& cfg = MprpcApplication::GetConfig();

    enabled_ = cfg.LoadInt("mq_enable", 1, 0, 1) != 0;
    host_    = cfg.Load("mq_host");
    if (host_.empty()) host_ = "127.0.0.1";
    port_    = cfg.LoadInt("mq_port", 5672, 1, 65535);

    inited_ = true;
    if (!enabled_)
    {
        LOG_INFO("MqClient disabled by config (mq_enable=0)");
        return false;
    }

    bool ok = ConnectPublish();
    if (ok) ok = ConnectConsume();  // 消费锁内建消费连接

    if (ok)
    {
        LOG_INFO("MqClient connected to %s:%d, topology declared "
                 "(job.events/shard.events)", host_.c_str(), port_);
        return true;
    }

    // 与 RedisClient 同策略：MQ 是可降级组件，连接失败不拒绝服务启动
    enabled_ = false;
    LOG_WARN("MqClient connect to %s:%d failed, degrade to RPC/Pull "
             "fallback (SchedulingLoop polling remains as safety net)",
             host_.c_str(), port_);
    return false;
}

bool MqClient::ConnectPublish()
{
    // 清理旧连接（发布失败后的重建路径）
    if (publish_conn_ != nullptr)
    {
        amqp_destroy_connection(
            static_cast<amqp_connection_state_t>(publish_conn_));
        publish_conn_ = nullptr;
    }

    amqp_connection_state_t conn = OpenChannel(host_, port_, "publish");
    if (conn == nullptr) return false;

    // 声明 exchange（durable=1，幂等）
    amqp_exchange_declare(conn, kChannel,
                          amqp_cstring_bytes(kExchangeJobEvents),
                          amqp_cstring_bytes("direct"),
                          0, 1, 0, 0, amqp_empty_table);
    if (!CheckReply(conn, "exchange_declare job.events")) { amqp_destroy_connection(conn); return false; }
    amqp_exchange_declare(conn, kChannel,
                          amqp_cstring_bytes(kExchangeShardEvents),
                          amqp_cstring_bytes("direct"),
                          0, 1, 0, 0, amqp_empty_table);
    if (!CheckReply(conn, "exchange_declare shard.events")) { amqp_destroy_connection(conn); return false; }

    // 声明队列（durable=1，幂等）
    amqp_queue_declare(conn, kChannel,
                       amqp_cstring_bytes(kQueueShardWaiting),
                       0, 1, 0, 0, amqp_empty_table);
    if (!CheckReply(conn, "queue_declare shard.waiting")) { amqp_destroy_connection(conn); return false; }
    amqp_queue_declare(conn, kChannel,
                       amqp_cstring_bytes(kQueueResultPending),
                       0, 1, 0, 0, amqp_empty_table);
    if (!CheckReply(conn, "queue_declare result.pending")) { amqp_destroy_connection(conn); return false; }

    // 绑定（routing key 与队列同名，direct 精确匹配）
    amqp_queue_bind(conn, kChannel,
                    amqp_cstring_bytes(kQueueShardWaiting),
                    amqp_cstring_bytes(kExchangeJobEvents),
                    amqp_cstring_bytes(kQueueShardWaiting), amqp_empty_table);
    if (!CheckReply(conn, "queue_bind shard.waiting")) { amqp_destroy_connection(conn); return false; }
    amqp_queue_bind(conn, kChannel,
                    amqp_cstring_bytes(kQueueResultPending),
                    amqp_cstring_bytes(kExchangeShardEvents),
                    amqp_cstring_bytes(kQueueResultPending), amqp_empty_table);
    if (!CheckReply(conn, "queue_bind result.pending")) { amqp_destroy_connection(conn); return false; }

    publish_conn_ = conn;
    return true;
}

bool MqClient::ConnectConsume()
{
    // 清理旧连接
    if (consume_conn_ != nullptr)
    {
        amqp_destroy_connection(
            static_cast<amqp_connection_state_t>(consume_conn_));
        consume_conn_ = nullptr;
    }
    consume_alive_.store(false);   // 重建期间标记不可用
    consumed_queues_.clear();

    amqp_connection_state_t conn = OpenChannel(host_, port_, "consume");
    if (conn == nullptr) return false;
    consume_conn_ = conn;
    consume_alive_.store(true);
    return true;
}


bool MqClient::PublishShardWaiting(const std::string& shard_id)
{
    std::lock_guard<std::mutex> lock(publish_mutex_);
    if (!inited_ || !enabled_ || publish_conn_ == nullptr) return false;

    amqp_basic_properties_t props = {};
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type  = amqp_cstring_bytes("text/plain");
    props.delivery_mode = 2;  // persistent：Broker 重启不丢

    amqp_bytes_t body = amqp_cstring_bytes(shard_id.c_str());
    int rc = amqp_basic_publish(static_cast<amqp_connection_state_t>(publish_conn_),
                                kChannel,
                                amqp_cstring_bytes(kExchangeJobEvents),
                                amqp_cstring_bytes(kQueueShardWaiting),
                                0, 0, &props, body);
    if (rc != AMQP_STATUS_OK)
    {
        // 连接已断：重建重试 1 次；仍失败返回 false（轮询兜底，不丢任务）
        LOG_WARN("MqClient: publish shard.waiting failed (%s), reconnecting",
                 amqp_error_string2(rc));
        if (!ConnectPublish()) return false;
        rc = amqp_basic_publish(static_cast<amqp_connection_state_t>(publish_conn_),
                                kChannel,
                                amqp_cstring_bytes(kExchangeJobEvents),
                                amqp_cstring_bytes(kQueueShardWaiting),
                                0, 0, &props, body);
        if (rc != AMQP_STATUS_OK) return false;
    }
    return true;
}

bool MqClient::PublishResult(const std::string& serialized_request)
{
    std::lock_guard<std::mutex> lock(publish_mutex_);
    if (!inited_ || !enabled_ || publish_conn_ == nullptr) return false;

    amqp_basic_properties_t props = {};
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type  = amqp_cstring_bytes("application/octet-stream");
    props.delivery_mode = 2;

    amqp_bytes_t body{ serialized_request.size(), (void*)serialized_request.data() };
    int rc = amqp_basic_publish(static_cast<amqp_connection_state_t>(publish_conn_),
                                kChannel,
                                amqp_cstring_bytes(kExchangeShardEvents),
                                amqp_cstring_bytes(kQueueResultPending),
                                0, 0, &props, body);
    if (rc != AMQP_STATUS_OK)
    {
        LOG_WARN("MqClient: publish result.pending failed (%s), reconnecting",
                 amqp_error_string2(rc));
        if (!ConnectPublish()) return false;
        rc = amqp_basic_publish(static_cast<amqp_connection_state_t>(publish_conn_),
                                kChannel,
                                amqp_cstring_bytes(kExchangeShardEvents),
                                amqp_cstring_bytes(kQueueResultPending),
                                0, 0, &props, body);
        if (rc != AMQP_STATUS_OK) return false;
    }
    return true;
}

bool MqClient::ConsumeBlocking(const std::string& queue, std::string& body,
                               int64_t& delivery_tag, int64_t timeout_ms)
{

    if (!inited_ || !enabled_ || !consume_alive_.load()) return false;
    amqp_connection_state_t conn =
        static_cast<amqp_connection_state_t>(consume_conn_);
    if (conn == nullptr) return false;

    {
        std::lock_guard<std::timed_mutex> lock(consume_mutex_);
        if (consume_conn_ != conn) return false;   // 防御：连接已被重建
        if (consumed_queues_.count(queue) == 0)
        {
            amqp_basic_consume_ok_t* consume_ok =
                amqp_basic_consume(conn, kChannel, amqp_cstring_bytes(queue.c_str()),
                                   amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
            if (consume_ok == nullptr || !CheckReply(conn, "basic_consume"))
            {
                LOG_WARN("MqClient: basic_consume %s failed", queue.c_str());
                return false;
            }
            consumed_queues_.insert(queue);
        }
    }

    SetSocketTimeouts(conn, 2);
    int fd = amqp_get_sockfd(conn);
    if (fd < 0)
    {
        LOG_WARN("MqClient: consume socket fd invalid, connection dropped");
        std::lock_guard<std::timed_mutex> lock(consume_mutex_);
        if (consume_conn_ == conn)
        {
            consume_alive_.store(false);
            amqp_destroy_connection(conn);
            consume_conn_ = nullptr;
            consumed_queues_.clear();
        }
        return false;
    }
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    pfd.revents = 0;
    int pr = poll(&pfd, 1, static_cast<int>(timeout_ms));
    if (pr == 0) return false;                 // 正常超时（非故障）
    if (pr < 0)
    {
        if (errno == EINTR) return false;      // 信号打断：下轮再试
        LOG_WARN("MqClient: consume poll failed (%s), connection dropped",
                 strerror(errno));
        std::lock_guard<std::timed_mutex> lock(consume_mutex_);
        if (consume_conn_ == conn)
        {
            consume_alive_.store(false);
            amqp_destroy_connection(conn);
            consume_conn_ = nullptr;
            consumed_queues_.clear();
        }
        return false;
    }

    // ③ 有数据：持锁 read（毫秒级窗口；{0,0} 立即尝试，数据已在 socket）
    std::lock_guard<std::timed_mutex> lock(consume_mutex_);
    if (consume_conn_ != conn) return false;   // 防御：poll 期间连接被重建

    struct timeval tv0 = {0, 0};
    amqp_envelope_t envelope;
    amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &tv0, 0);
    if (res.reply_type == AMQP_RESPONSE_NORMAL)
    {
        body.assign(static_cast<const char*>(envelope.message.body.bytes),
                    envelope.message.body.len);
        delivery_tag = static_cast<int64_t>(envelope.delivery_tag);
        amqp_destroy_envelope(&envelope);
        return true;
    }

    if (res.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION
        && res.library_error == AMQP_STATUS_TIMEOUT)
    {
        return false;  // 数据不足一帧：非故障，等待下轮再 poll
    }

    // 连接故障：销毁连接，下次调用重连；调用方检查 connected() 走降级
    LOG_WARN("MqClient: consume %s failed (%s), connection dropped",
             queue.c_str(), amqp_error_string2(res.library_error));
    consume_alive_.store(false);
    amqp_destroy_connection(conn);
    consume_conn_ = nullptr;
    consumed_queues_.clear();
    return false;
}

bool MqClient::Ack(int64_t delivery_tag)
{
    // 阶段 11 修复：与 connected() 同策略——消费锁被卡时 1s 后放弃 ack，
    // 消息留在队列由 Broker 重投（不确认 ≠ 丢失），不阻塞调用线程
    if (!consume_mutex_.try_lock_for(std::chrono::milliseconds(1000)))
    {
        LOG_WARN("MqClient: consume lock busy, ack skipped "
                 "(delivery_tag=%lld, msg will redeliver)",
                 static_cast<long long>(delivery_tag));
        return false;
    }
    std::lock_guard<std::timed_mutex> lock(consume_mutex_, std::adopt_lock);
    if (consume_conn_ == nullptr) return false;
    int rc = amqp_basic_ack(static_cast<amqp_connection_state_t>(consume_conn_),
                            kChannel, static_cast<uint64_t>(delivery_tag), 0);
    if (rc != AMQP_STATUS_OK)
    {
        LOG_WARN("MqClient: ack failed (%s)", amqp_error_string2(rc));
        return false;
    }
    return true;
}

bool MqClient::connected() const
{
    return consume_alive_.load() && consume_conn_ != nullptr;
}

bool MqClient::Reconnect()
{
    // 与 connected() 同策略：消费锁被卡（消费线程异常）时 1s 放弃，
    // 由调用方（消费线程的重连循环）下一轮再试
    if (!consume_mutex_.try_lock_for(std::chrono::milliseconds(1000)))
    {
        LOG_WARN("MqClient: consume lock busy, reconnect skipped");
        return false;
    }
    std::lock_guard<std::timed_mutex> lock(consume_mutex_, std::adopt_lock);
    if (!inited_ || !enabled_) return false;
    if (ConnectConsume())
    {
        LOG_INFO("MqClient consume connection reconnected to %s:%d",
                 host_.c_str(), port_);
        return true;
    }
    return false;
}

} // namespace video_platform
