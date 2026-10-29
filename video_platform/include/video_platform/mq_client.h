#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>

namespace video_platform {

// MqClient — RabbitMQ 客户端（rabbitmq-c / AMQP 0-9-1）。
// 事件驱动调度：Scheduler 发布 shard.waiting → 消费线程即时分配。
// MQ 是可降级组件：故障时回退 Pull 轮询。
// 线程模型：发布/消费各一条连接 + 各自锁。
// 配置项：mq_enable / mq_host / mq_port

/// RabbitMQ 客户端（单例，线程安全）
class MqClient {
public:
    static MqClient& GetInstance();

    /// 初始化：连接 + 声明 exchange/queue/binding。失败只 WARN，enabled()=false
    bool Init();

    bool inited() const { return inited_; }
    bool enabled() const { return enabled_; }

    /// 发布 shard 等待分配事件（消息体 = shard_id）
    bool PublishShardWaiting(const std::string& shard_id);

    /// 阻塞消费一条消息（timeout_ms 超时返回 false）
    bool ConsumeBlocking(const std::string& queue, std::string& body,
                         int64_t& delivery_tag, int64_t timeout_ms);

    /// 手动确认消息
    bool Ack(int64_t delivery_tag);

    /// AMQP 连接是否可用
    bool connected() const;

    /// 重建连接（MQ 恢复后调度自动切回 Push）
    bool Reconnect();

private:
    MqClient() = default;
    MqClient(const MqClient&) = delete;
    MqClient& operator=(const MqClient&) = delete;

    bool ConnectPublish();
    bool ConnectConsume();

    std::string host_ = "127.0.0.1";
    int         port_ = 5672;
    bool        enabled_ = false;
    bool        inited_  = false;
    void*       publish_conn_ = nullptr;
    void*       consume_conn_ = nullptr;
    std::set<std::string> consumed_queues_;  // 已 basic_consume 的队列

    mutable std::mutex publish_mutex_;

    // timed_mutex：connected()/Ack() 用 try_lock_for(1s)，拿不到锁降级（MQ 不可用）
    // ConsumeBlocking 的 poll 已移出锁，锁只保护 conn 内部状态读写
    mutable std::timed_mutex consume_mutex_;
    std::atomic<bool> consume_alive_{false};
};

} // namespace video_platform