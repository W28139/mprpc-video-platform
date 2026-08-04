#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "wevix_muduo/TcpServer.h"
#include "wevix_muduo/Connection.h"

// ============================================================================
// MprpcMetrics — mprpc 框架层 Prometheus 指标库（阶段 11 可观测性）
// ============================================================================
//
// 用途：所有 video_platform 服务暴露 GET /metrics HTTP endpoint，
// 配合 Prometheus 抓取 + Grafana 面板 + 告警规则，从 grep 日志进化到标准
// 监控面板。
//
// 设计要点：
// - 指标类型：Counter（单调递增计数）/ Gauge（可增可减仪表）/
//   Histogram（固定桶直方图，导出时算前缀和 + _sum/_count）。
//   同一 name 下按标签分片，每个标签组合是独立系列。
// - 线程安全：Counter 用 atomic<uint64_t>；Gauge 用 AtomicDouble（位模式
//   CAS，C++17 不保证 atomic<double> 的 fetch_add）；Histogram 桶计数
//   逐桶 atomic——更新路径无锁，注册/导出加 registry mutex。
// - 可降级组件语义（与 Redis/MQ 一脉相承）：metrics_port<=0 时
//   MetricsHttpServer::enabled()==false，Start/Stop 为 no-op，服务照常运行。
// - Gauge 采样器：RegisterSampler(fn, interval_ms)，内部采样线程周期执行
//   fn(registry) 刷新 gauge（如 Scheduler 每 5s 查一次 MySQL 统计 shard
//   状态）。采样失败时 fn 内部自行跳过本轮，保持旧值避免 0 值尖刺。
// - 告警评估器：内置日志告警兜底——Prometheus 尚未部署时也能发现异常
//   （验收要求"至少日志打印"）。每条规则周期性 eval()，触发打
//   LOG_WARN/LOG_ERROR("ALERT [...] firing ...")，恢复打 LOG_INFO。
//   rate 型规则用 RateEstimator 环形缓冲估算窗口增量速率。
// - 生命周期：StartSamplers()/StopSamplers() 由 MetricsHttpServer::Start/Stop
//   联动调用，服务 main 只操作 MetricsHttpServer 一个对象。
// - HTTP 服务：自研 wevix_muduo TcpServer 起独立线程（框架无 HTTP codec，
//   onMessage 收到原始 read chunk，自建 fd 维度累积缓冲解析请求行）。
// ============================================================================

namespace mprpc
{

/// @brief 标签（name=value 对），同一 name 下不同标签组合为独立系列
struct MetricLabel
{
    std::string name;
    std::string value;
};

// ── AtomicDouble：C++17 不保证 atomic<double> 算术运算，用位模式 CAS ──
class AtomicDouble
{
public:
    double load() const;
    void store(double v);
    void add(double v);

private:
    std::atomic<uint64_t> bits_{0};
};

/// @brief 计数器：只增不减（Prometheus counter 语义）
class MetricCounter
{
public:
    void Inc(double v = 1.0);
    double Value() const;

private:
    std::atomic<uint64_t> value_{0};
};

/// @brief 仪表：可增可减，反映当前瞬时状态（Prometheus gauge 语义）
class MetricGauge
{
public:
    void Set(double v);
    void Add(double v);
    double Value() const;

private:
    AtomicDouble value_;
};

/// @brief 直方图：固定桶 + 累计计数（Prometheus histogram 语义）
///
/// 桶数组不含 +Inf（+Inf 桶由实现隐式兜底）。Observe(v) 用 upper_bound
/// 定位命中桶，仅对命中桶计数 +1（原子），导出/分位计算时才做前缀和。
/// _sum 与 _count 同步累加，供 rate() 与 histogram_quantile() 使用。
class MetricHistogram
{
public:
    explicit MetricHistogram(std::vector<double> buckets);

    /// @brief 记录一次观测值
    void Observe(double v);

    /// @brief 分位数（线性插值，跨全部分位累计计数）
    double Quantile(double q) const;

    double Sum() const { return sum_.load(); }
    uint64_t Count() const { return count_.load(); }
    const std::vector<double>& Buckets() const { return buckets_; }

    /// @brief 各 le 桶的累计计数（含 +Inf 尾桶，长度为 Buckets().size()+1）
    std::vector<uint64_t> CumulativeCounts() const;

private:
    std::vector<double> buckets_;                       ///< le 桶上界（不含 +Inf）
    // atomic 不可拷贝，用 unique_ptr 数组（vector<atomic> 无法 resize）
    std::unique_ptr<std::atomic<uint64_t>[]> bucketCounts_;  ///< 非累计命中数（尾桶为 +Inf）
    std::atomic<uint64_t> count_{0};
    AtomicDouble sum_;
};

// ============================================================================
// MetricsRegistry — 全局指标注册表（单例，线程安全）
// ============================================================================
class MetricsRegistry
{
public:
    static MetricsRegistry& GetInstance();

    /// @brief 获取（或注册）计数器系列。help 仅首次注册生效。
    MetricCounter& Counter(const std::string& name, const std::string& help,
                           const std::vector<MetricLabel>& labels = {});

    /// @brief 获取（或注册）仪表系列。help 仅首次注册生效。
    MetricGauge& Gauge(const std::string& name, const std::string& help,
                       const std::vector<MetricLabel>& labels = {});

    /// @brief 获取（或注册）直方图系列。help/buckets 仅首次注册生效。
    MetricHistogram& Histogram(const std::string& name, const std::string& help,
                               const std::vector<double>& buckets,
                               const std::vector<MetricLabel>& labels = {});

    /// @brief 聚合某直方图 name 的全部标签系列后计算分位数（告警 p99 用）
    double HistogramQuantile(const std::string& name, double q) const;

    // ── Gauge 采样器：采样线程按各自 interval 周期执行 ──
    void RegisterSampler(std::function<void(MetricsRegistry&)> fn, int64_t interval_ms);

    // ── 告警评估器：评估线程周期执行，触发/恢复打日志 ──
    struct AlertRule
    {
        std::string name;               ///< 告警名（日志前缀）
        std::string severity;           ///< "WARN" / "ERROR"
        std::function<double()> eval;   ///< 返回当前指标值
        double threshold = 0;
        bool above = true;              ///< true: value > threshold 触发；false: value < threshold
        int64_t for_ms = 0;             ///< 持续判定时长（0=立即触发）
        std::string description;        ///< 告警描述（日志输出）
    };
    void RegisterAlertRule(AlertRule rule);

    /// @brief 导出 Prometheus text format 0.0.4
    std::string ExportText() const;

    // ── 生命周期（MetricsHttpServer::Start/Stop 联动调用） ──
    void StartSamplers();
    void StopSamplers();

private:
    MetricsRegistry() = default;
    MetricsRegistry(const MetricsRegistry&) = delete;
    MetricsRegistry& operator=(const MetricsRegistry&) = delete;

    // 指标分族存储：name → 族（类型/help/桶 + 标签系列分片）
    struct Family
    {
        std::string name;
        std::string help;
        std::string type;                                  ///< "counter"/"gauge"/"histogram"
        std::vector<double> buckets;                       ///< 仅 histogram
        std::unordered_map<std::string, std::shared_ptr<MetricCounter>> counters;    ///< key=标签序列化
        std::unordered_map<std::string, std::shared_ptr<MetricGauge>> gauges;
        std::unordered_map<std::string, std::shared_ptr<MetricHistogram>> histograms;
    };

    // 按 name 找族，不存在则创建（需持有 mutex_）
    Family& FindOrCreateFamilyLocked(const std::string& name, const std::string& help,
                                     const std::string& type,
                                     const std::vector<double>& buckets);

    void SamplerLoop();    // 采样线程入口
    void AlertLoop();      // 告警评估线程入口

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Family> families_;     ///< name → 族
    // 采样器：fn + 间隔 + 下次执行时间戳（毫秒）
    struct Sampler { std::function<void(MetricsRegistry&)> fn; int64_t interval_ms; int64_t next_ms; };
    std::vector<Sampler> samplers_;
    std::vector<AlertRule> alertRules_;
    std::atomic<bool> samplersRunning_{false};
    std::atomic<bool> alertRunning_{false};
    std::thread samplerThread_;
    std::thread alertThread_;
};

/// @brief rate 型告警的速率估算器：环形缓冲 (t_ms, counter_value)
/// 窗口内至少 2 个样本时返回 (v_newest - v_oldest) / (t_newest - t_oldest)
class RateEstimator
{
public:
    explicit RateEstimator(int64_t window_ms);
    /// @brief 喂入计数器当前值，返回窗口内平均速率（值/秒）；样本不足返回 0
    double Observe(double counter_value);

private:
    int64_t window_ms_;
    std::deque<std::pair<int64_t, double>> samples_;
    mutable std::mutex mutex_;
};

// ============================================================================
// MetricsHttpServer — Prometheus 抓取端点（GET /metrics）
// ============================================================================
//
// 独立线程 + wevix_muduo TcpServer(127.0.0.1, port, threadNum=1)。
// TcpServer::start() 阻塞在 mainLoop_->run()，故在专用线程构造并 start；
// stop() 跨线程安全（EventLoop::stop 带 wakeup，ThreadPool::stop 内部 join）。
// 框架无 HTTP codec：onMessage 收到的是原始 read chunk（可能半个请求头），
// 自建 fd 维度累积缓冲，找到 "\r\n\r\n" 后只解析请求行路由。
// 响应 send + shutdown()（只关写端让内核 flush 完数据），不用 forceClose
// （会丢弃未发完的 outputBuffer）。
class MetricsHttpServer
{
public:
    /// @brief 配置端口。port<=0 时不启用（可降级组件，Start/Stop 为 no-op）
    bool Init(int port);

    /// @brief 启动：Registry::StartSamplers() + 起 HTTP 服务线程
    void Start();

    /// @brief 停止：server_->stop() + join 线程 + StopSamplers()
    void Stop();

    bool enabled() const { return port_ > 0; }

private:
    using ConnectionPtr = wevix_muduo::Connection::ConnectionPtr;

    void RunHttpLoop();   ///< HTTP 线程入口：构造 TcpServer + start()（阻塞）
    void OnMessage(const ConnectionPtr& conn, std::string& message);
    void OnClose(const ConnectionPtr& conn);
    void SendResponse(const ConnectionPtr& conn, int code,
                      const std::string& body);

    int port_ = 0;
    std::unique_ptr<wevix_muduo::TcpServer> server_;
    std::thread thread_;
    std::atomic<bool> started_{false};

    // fd → 累积的请求字节（HTTP 半包累积）。onMessage 在唯一 subLoop 线程
    // 回调，锁仅为防御性（OnClose 同线程，实际无竞争）
    std::mutex pendingMutex_;
    std::map<int, std::string> pendingBufs_;
};

/// @brief RPC 延迟直方图（method 维度）。热路径优化：首次调用注册并缓存
/// shared_ptr，此后每次调用仅一次 hash 查找，无锁。
/// 桶：[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000] ms
MetricHistogram& RpcLatencyHistogram(const std::string& method);

} // namespace mprpc
