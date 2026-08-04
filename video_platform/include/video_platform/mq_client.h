#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>

namespace video_platform {

// ============================================================================
// MqClient — RabbitMQ 客户端封装（rabbitmq-c / AMQP 0-9-1，阶段 10 第 2 批）
// ============================================================================
//
// 事件驱动调度（Pull→Push）：消除 SchedulingLoop 2s 轮询的调度延迟。
// 两条事件流：
//   JobService 切分完成 ──PublishShardWaiting──► "job.events" → "shard.waiting"
//   Worker 转码完成   ──PublishResult───────► "shard.events" → "result.pending"
//
// 消息语义：
// - shard.waiting 消息体 = shard_id 纯文本——**通知通道**，消费方（Scheduler）
//   收到后重新查 MySQL 取最新数据（MySQL 是唯一数据源，防快照陈旧）
// - result.pending 消息体 = ReportShardResultRequest 的 protobuf 序列化串——
//   **结果数据通道**：执行结果（is_success/exit_code/output_path 等）不在
//   MySQL 中，必须随消息传递，RC 消费后走原聚合逻辑
//
// 可靠性（验收标准"MQ 宕机后消息不丢失"）：
// - exchange/queue 均 durable，消息 delivery-mode=2（持久化）
// - 消费手动 ACK：处理成功才 ack，失败不 ack 自动重回队列
// - 消费失败（连接断开）不 ack，Broker 保留消息
//
// 与 RedisClient 同策略：MQ 是可降级组件。
// - Init() 连接失败只 WARN 不拒绝服务启动，enabled_=false 走原 RPC/Pull 路径
// - 发布失败返回 false，调用方只打 WARN（SchedulingLoop 兜底轮询仍会扫到）
// - 消费线程通过 ConsumeBlocking 超时感知连接断开，负责重连或触发降级
//
// 线程模型：单连接 + mutex（rabbitmq-c 连接非线程安全）。
// 各服务要么只发布（JobService/Worker 多线程发布），要么只消费（Scheduler/RC
// 独立线程），本进程内不会同时收发。
//
// 配置项：mq_enable / mq_host / mq_port
// ============================================================================

/// @brief RabbitMQ 客户端（单例，线程安全）
class MqClient {
public:
    /// @brief 获取全局唯一实例
    static MqClient& GetInstance();

    /// @brief 从配置初始化：连接 + 声明 exchange/queue/binding（幂等）。
    /// 失败只 WARN，enabled() 为 false，调用方降级走原路径。
    bool Init();

    bool inited() const { return inited_; }
    bool enabled() const { return enabled_; }

    // ── 发布（thread-safe） ──────────────────────────────────────────

    /// @brief 发布 shard 等待分配事件（消息体 = shard_id）。
    /// @return true=发布成功（写入 Broker）；false=MQ 不可用/失败，调用方降级
    bool PublishShardWaiting(const std::string& shard_id);

    /// @brief 发布 shard 执行结果（消息体 = ReportShardResultRequest 序列化串）。
    /// @return true=发布成功；false=MQ 不可用/失败
    bool PublishResult(const std::string& serialized_request);

    // ── 消费（阻塞式，供独立消费线程单线程调用） ─────────────────────

    /// @brief 阻塞消费一条消息（最多 timeout_ms）。
    /// @param[out] body 消息体；delivery_tag 供 Ack() 确认
    /// @return true=取到消息；false=超时或连接故障（调用方检查 connected()）
    bool ConsumeBlocking(const std::string& queue, std::string& body,
                         int64_t& delivery_tag, int64_t timeout_ms);

    /// @brief 手动确认消息（处理成功后调用；不确认则 Broker 重回队列）
    bool Ack(int64_t delivery_tag);

    /// @brief 当前 AMQP 连接是否可用（消费线程据此决定降级/重连）
    bool connected() const;

    /// @brief 重建连接 + 重新声明拓扑（消费线程在 connected()==false 时周期调用；
    /// MQ 恢复后重连成功，调度自动切回 Push 模式）
    bool Reconnect();

private:
    MqClient() = default;
    MqClient(const MqClient&) = delete;
    MqClient& operator=(const MqClient&) = delete;

    /// @brief 建立发布连接并声明 exchange/queue/binding（需持有 publish_mutex_）。
    bool ConnectPublish();

    /// @brief 建立消费连接（登录 + channel，拓扑由发布连接声明；需持有 consume_mutex_）。
    bool ConnectConsume();

    std::string host_ = "127.0.0.1";
    int         port_ = 5672;
    bool        enabled_ = false;
    bool        inited_  = false;
    void*       publish_conn_ = nullptr;   ///< 发布连接（amqp_connection_state_t）
    void*       consume_conn_ = nullptr;   ///< 消费连接（amqp_connection_state_t）
    std::set<std::string> consumed_queues_;  ///< 已 basic_consume 的队列（消费连接重建后清空）
    mutable std::mutex publish_mutex_;
    // ⚠️ 阶段 11 修复：消费锁用 timed_mutex——amqp_consume_message 在
    // 连接异常（半开/对端 FIN 后 poll 返回可读但 read 阻塞）时可能无限
    // 阻塞持锁，导致 SchedulingLoop 的 connected() 永久等锁、调度停摆。
    // connected()/Ack() 用 try_lock_for(1s) 拿不到锁即降级返回（视为 MQ
    // 不可用），调度线程转 Pull 轮询，系统保持可用。
    // ⚠️ 阶段 12 前修复：ConsumeBlocking 的 poll(2s) 已移出锁——消费线程
    // 紧循环持锁导致 connected() 的 try_lock_for(1s) 永远失败（mq=push
    // 误判 + consume lock busy WARN 无限刷屏）。锁只保护 conn 内部状态
    // 的读写（read/销毁/重建），poll 期间无并发写者（消费连接只由消费
    // 线程销毁）。consume_alive_ 为原子标志，connected() 无锁读取。
    mutable std::timed_mutex consume_mutex_;
    std::atomic<bool> consume_alive_{false};  ///< 消费连接有效性（atomic，connected() 无锁读取）
};

} // namespace video_platform
