#include "mprpcmetrics.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <sstream>

#include "wevix_muduo/AsyncLogger.h"

// ============================================================================
// MprpcMetrics — mprpc 框架层 Prometheus 指标库实现（阶段 11 可观测性）
// ============================================================================
//
// 实现要点：
// - 指标更新路径无锁：Counter/Gauge/Histogram 全部基于原子量，
//   注册与导出加 registry mutex（低频路径）
// - Histogram 桶计数按「命中桶」原子自增，导出/分位时做前缀和，
//   避免每次 Observe 写 N 个桶
// - 采样线程与告警评估线程均不得持有 mutex_ 执行用户回调
//   （回调内部会再次进入 registry 加锁），先拷贝快照再执行
// - HTTP 服务：独立线程 + TcpServer(127.0.0.1, port, 1)，
//   无 codec 时 onMessage 收到原始 read chunk，自建累积缓冲
// ============================================================================

namespace mprpc
{

// ── 内部工具 ──────────────────────────────────────────────────────────────
namespace
{

int64_t NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

/// @brief 标签数组序列化为 "k1=v1,k2=v2"（系列分片 key，顺序敏感）
std::string SerializeLabels(const std::vector<MetricLabel>& labels)
{
    if (labels.empty()) return "";
    std::string s;
    for (size_t i = 0; i < labels.size(); ++i)
    {
        if (i > 0) s += ",";
        s += labels[i].name;
        s += "=";
        s += labels[i].value;
    }
    return s;
}

/// @brief 标签 key 展开为 Prometheus 标签写法 `{k="v",...}`；空标签返回 ""
/// 值中引号/反斜杠按 Prometheus text format 转义
std::string FormatLabels(const std::string& serialized)
{
    if (serialized.empty()) return "";
    std::string out = "{";
    size_t pos = 0;
    bool first = true;
    while (pos < serialized.size())
    {
        size_t eq = serialized.find('=', pos);
        if (eq == std::string::npos) break;
        size_t comma = serialized.find(',', eq + 1);
        if (comma == std::string::npos) comma = serialized.size();
        std::string k = serialized.substr(pos, eq - pos);
        std::string v = serialized.substr(eq + 1, comma - eq - 1);
        if (!first) out += ",";
        first = false;
        out += k;
        out += "=\"";
        for (char c : v)
        {
            if (c == '\\' || c == '"') out += '\\';
            out += c;
        }
        out += "\"";
        pos = comma + 1;
    }
    out += "}";
    return out;
}

/// @brief double 输出为 %g 格式（整数无尾零）
std::string FormatValue(double v)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%g", v);
    return buf;
}

/// @brief le 桶上界输出为固定 9 位小数（Prometheus 约定）
std::string FormatLe(double bound)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%.9f", bound);
    return buf;
}

/// @brief 累计计数数组的线性插值分位数（与 MetricHistogram::Quantile 共用）
/// @param bounds   le 桶上界（不含 +Inf）
/// @param cum      cumulative[i] = 小于等于 bounds[i] 的观测数（尾桶为 +Inf）
double InterpolateQuantile(double q, const std::vector<double>& bounds,
                           const std::vector<uint64_t>& cum)
{
    if (bounds.empty() || cum.empty()) return 0;
    uint64_t total = cum.back();
    if (total == 0) return 0;
    double target = q * static_cast<double>(total);
    if (target <= 0) return bounds.front();
    size_t i = 0;
    while (i < cum.size() && static_cast<double>(cum[i]) < target) ++i;
    if (i >= cum.size()) return bounds.back();  // 不应发生（total 即最大累计）
    // 命中 +Inf 尾桶时上界取最后一个有限桶，插值限于末桶
    size_t hiIdx = std::min(i, bounds.size() - 1);
    if (i == 0) return bounds[0];
    double prevCum = static_cast<double>(cum[i - 1]);
    double prevBound = (i == 1) ? 0.0 : bounds[i - 1];   // 首桶下界按 0 处理
    double frac = (target - prevCum) / static_cast<double>(cum[i] - prevCum);
    return prevBound + frac * (bounds[hiIdx] - prevBound);
}

} // namespace

// ── AtomicDouble ──────────────────────────────────────────────────────────
double AtomicDouble::load() const
{
    uint64_t b = bits_.load(std::memory_order_relaxed);
    double v;
    std::memcpy(&v, &b, sizeof(v));
    return v;
}

void AtomicDouble::store(double v)
{
    uint64_t b;
    std::memcpy(&b, &v, sizeof(b));
    bits_.store(b, std::memory_order_relaxed);
}

void AtomicDouble::add(double v)
{
    uint64_t old = bits_.load(std::memory_order_relaxed);
    for (;;)
    {
        double cur;
        std::memcpy(&cur, &old, sizeof(cur));
        double nxt = cur + v;
        uint64_t nb;
        std::memcpy(&nb, &nxt, sizeof(nb));
        if (bits_.compare_exchange_weak(old, nb, std::memory_order_relaxed))
            return;
    }
}

// ── MetricCounter ─────────────────────────────────────────────────────────
void MetricCounter::Inc(double v)
{
    value_.fetch_add(static_cast<uint64_t>(v), std::memory_order_relaxed);
}

double MetricCounter::Value() const
{
    return static_cast<double>(value_.load(std::memory_order_relaxed));
}

// ── MetricGauge ───────────────────────────────────────────────────────────
void MetricGauge::Set(double v) { value_.store(v); }
void MetricGauge::Add(double v) { value_.add(v); }
double MetricGauge::Value() const { return value_.load(); }

// ── MetricHistogram ───────────────────────────────────────────────────────
MetricHistogram::MetricHistogram(std::vector<double> buckets)
    : buckets_(std::move(buckets))
{
    // 桶计数含隐式 +Inf 尾桶；atomic 不可拷贝，new 数组 + 原地构造
    bucketCounts_.reset(new std::atomic<uint64_t>[buckets_.size() + 1]);
    for (size_t i = 0; i < buckets_.size() + 1; ++i)
        new (&bucketCounts_[i]) std::atomic<uint64_t>(0);
}

void MetricHistogram::Observe(double v)
{
    // upper_bound 返回第一个 > v 的桶上界位置；越界即 +Inf 尾桶
    size_t idx = static_cast<size_t>(
        std::upper_bound(buckets_.begin(), buckets_.end(), v) - buckets_.begin());
    bucketCounts_[idx].fetch_add(1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    sum_.add(v);
}

std::vector<uint64_t> MetricHistogram::CumulativeCounts() const
{
    size_t n = buckets_.size() + 1;   // 含隐式 +Inf 尾桶
    std::vector<uint64_t> cum(n, 0);
    uint64_t acc = 0;
    for (size_t i = 0; i < n; ++i)
    {
        acc += bucketCounts_[i].load(std::memory_order_relaxed);
        cum[i] = acc;
    }
    return cum;
}

double MetricHistogram::Quantile(double q) const
{
    return InterpolateQuantile(q, buckets_, CumulativeCounts());
}

// ── MetricsRegistry ───────────────────────────────────────────────────────
MetricsRegistry& MetricsRegistry::GetInstance()
{
    static MetricsRegistry instance;
    return instance;
}

MetricsRegistry::Family& MetricsRegistry::FindOrCreateFamilyLocked(
    const std::string& name, const std::string& help, const std::string& type,
    const std::vector<double>& buckets)
{
    auto it = families_.find(name);
    if (it == families_.end())
    {
        Family f;
        f.name = name;
        f.help = help;
        f.type = type;
        f.buckets = buckets;
        it = families_.emplace(name, std::move(f)).first;
    }
    else if (it->second.type != type)
    {
        // 同名不同类型属于编程错误：警告但继续（返回已存在的族）
        LOG_WARN("MetricsRegistry: metric %s re-registered as %s (was %s)",
                 name.c_str(), type.c_str(), it->second.type.c_str());
    }
    return it->second;
}

MetricCounter& MetricsRegistry::Counter(const std::string& name,
                                        const std::string& help,
                                        const std::vector<MetricLabel>& labels)
{
    std::lock_guard<std::mutex> lock(mutex_);
    Family& f = FindOrCreateFamilyLocked(name, help, "counter", {});
    std::string key = SerializeLabels(labels);
    auto it = f.counters.find(key);
    if (it != f.counters.end()) return *it->second;
    auto p = std::make_shared<MetricCounter>();
    f.counters[key] = p;
    return *p;
}

MetricGauge& MetricsRegistry::Gauge(const std::string& name,
                                    const std::string& help,
                                    const std::vector<MetricLabel>& labels)
{
    std::lock_guard<std::mutex> lock(mutex_);
    Family& f = FindOrCreateFamilyLocked(name, help, "gauge", {});
    std::string key = SerializeLabels(labels);
    auto it = f.gauges.find(key);
    if (it != f.gauges.end()) return *it->second;
    auto p = std::make_shared<MetricGauge>();
    f.gauges[key] = p;
    return *p;
}

MetricHistogram& MetricsRegistry::Histogram(const std::string& name,
                                            const std::string& help,
                                            const std::vector<double>& buckets,
                                            const std::vector<MetricLabel>& labels)
{
    std::lock_guard<std::mutex> lock(mutex_);
    Family& f = FindOrCreateFamilyLocked(name, help, "histogram", buckets);
    std::string key = SerializeLabels(labels);
    auto it = f.histograms.find(key);
    if (it != f.histograms.end()) return *it->second;
    auto p = std::make_shared<MetricHistogram>(f.buckets);
    f.histograms[key] = p;
    return *p;
}

double MetricsRegistry::HistogramQuantile(const std::string& name, double q) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = families_.find(name);
    if (it == families_.end() || it->second.type != "histogram" ||
        it->second.histograms.empty())
    {
        return 0;
    }
    const Family& f = it->second;
    // 聚合全部标签系列的累计计数
    std::vector<uint64_t> cum(f.buckets.size() + 1, 0);
    uint64_t total = 0;
    for (const auto& kv : f.histograms)
    {
        auto c = kv.second->CumulativeCounts();
        for (size_t i = 0; i < cum.size() && i < c.size(); ++i) cum[i] += c[i];
        total += kv.second->Count();
    }
    if (total == 0) return 0;
    return InterpolateQuantile(q, f.buckets, cum);
}

void MetricsRegistry::RegisterSampler(std::function<void(MetricsRegistry&)> fn,
                                      int64_t interval_ms)
{
    std::lock_guard<std::mutex> lock(mutex_);
    Sampler s;
    s.fn = std::move(fn);
    s.interval_ms = interval_ms;
    s.next_ms = NowMs() + interval_ms;
    samplers_.push_back(std::move(s));
}

void MetricsRegistry::RegisterAlertRule(AlertRule rule)
{
    std::lock_guard<std::mutex> lock(mutex_);
    alertRules_.push_back(std::move(rule));
}

std::string MetricsRegistry::ExportText() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::ostringstream os;

    // 按 name 字典序输出，保证抓取结果确定性
    std::vector<const Family*> fams;
    fams.reserve(families_.size());
    for (const auto& kv : families_) fams.push_back(&kv.second);
    std::sort(fams.begin(), fams.end(),
              [](const Family* a, const Family* b) { return a->name < b->name; });

    for (const Family* f : fams)
    {
        os << "# HELP " << f->name << " " << f->help << "\n";
        os << "# TYPE " << f->name << " " << f->type << "\n";
        if (f->type == "counter")
        {
            std::vector<std::pair<std::string, const MetricCounter*>> items;
            for (const auto& kv : f->counters) items.emplace_back(kv.first, kv.second.get());
            std::sort(items.begin(), items.end());
            for (const auto& kv : items)
            {
                os << f->name << FormatLabels(kv.first) << " "
                   << FormatValue(kv.second->Value()) << "\n";
            }
        }
        else if (f->type == "gauge")
        {
            std::vector<std::pair<std::string, const MetricGauge*>> items;
            for (const auto& kv : f->gauges) items.emplace_back(kv.first, kv.second.get());
            std::sort(items.begin(), items.end());
            for (const auto& kv : items)
            {
                os << f->name << FormatLabels(kv.first) << " "
                   << FormatValue(kv.second->Value()) << "\n";
            }
        }
        else  // histogram
        {
            std::vector<std::pair<std::string, const MetricHistogram*>> items;
            for (const auto& kv : f->histograms) items.emplace_back(kv.first, kv.second.get());
            std::sort(items.begin(), items.end());
            for (const auto& kv : items)
            {
                const std::string& labels = FormatLabels(kv.first);
                const MetricHistogram* h = kv.second;
                auto cum = h->CumulativeCounts();
                // labels 含完整 "{...}"（可能为空串）。直方图系列需要附加
                // le 标签：空 → "{le=...}"；非空 → 去掉 labels 结尾 '}'，
                // 拼 ",le=..." 再闭合，得到 "{k=v,le=...}"
                std::string base = labels.empty() ? "" : labels.substr(0, labels.size() - 1);
                for (size_t i = 0; i < f->buckets.size(); ++i)
                {
                    os << f->name << "_bucket" << base
                       << (labels.empty() ? "{le=\"" : ",le=\"")
                       << FormatLe(f->buckets[i]) << "\"} "
                       << cum[i] << "\n";
                }
                os << f->name << "_bucket" << base
                   << (labels.empty() ? "{le=\"" : ",le=\"") << "+Inf\"} "
                   << cum.back() << "\n";
                os << f->name << "_sum" << labels << " "
                   << FormatValue(h->Sum()) << "\n";
                os << f->name << "_count" << labels << " "
                   << h->Count() << "\n";
            }
        }
    }
    return os.str();
}

// ── 采样线程 / 告警评估线程 ──────────────────────────────────────────────
void MetricsRegistry::StartSamplers()
{
    // 幂等启动：compare_exchange_strong 成功（false→true）返回 true，
    // !true=false 不 return → 创建采样线程；已运行时 CAS 失败 → return
    bool expected = false;
    if (!samplersRunning_.compare_exchange_strong(expected, true))
        return;
    samplerThread_ = std::thread([this] { SamplerLoop(); });

    // ⚠️ 告警线程不能用同一 CAS 写法：CAS 成功返回 true 会误走 return，
    // 线程永远不创建（阶段 11 实测踩坑）。用 exchange 返回旧值判断：
    // 旧值 false → 创建；旧值 true（已在运行）→ 跳过
    if (alertRunning_.exchange(true)) return;
    alertThread_ = std::thread([this] { AlertLoop(); });
}

void MetricsRegistry::StopSamplers()
{
    if (samplersRunning_.exchange(false))
    {
        if (samplerThread_.joinable()) samplerThread_.join();
    }
    if (alertRunning_.exchange(false))
    {
        if (alertThread_.joinable()) alertThread_.join();
    }
}

void MetricsRegistry::SamplerLoop()
{
    LOG_INFO("MetricsRegistry: sampler thread started (%zu sampler(s))",
             samplers_.size());
    while (samplersRunning_.load(std::memory_order_relaxed))
    {
        int64_t now = NowMs();
        int64_t minWait = 1000;   // 兜底 1s，避免忙轮询

        // 收集到期的采样器（先拷贝 fn，锁外执行——回调会再进 registry 加锁）
        std::vector<std::function<void(MetricsRegistry&)>> due;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto& s : samplers_)
            {
                if (now >= s.next_ms)
                {
                    due.push_back(s.fn);
                    s.next_ms = now + s.interval_ms;
                }
                minWait = std::min(minWait, std::max<int64_t>(1, s.next_ms - now));
            }
        }
        for (auto& fn : due)
        {
            try { fn(*this); }
            catch (...)
            {
                LOG_WARN("MetricsRegistry: sampler fn threw, skip this round");
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(minWait));
    }
    LOG_INFO("MetricsRegistry: sampler thread stopped");
}

void MetricsRegistry::AlertLoop()
{
    // 每规则状态：active=条件成立中，alerted=已打过触发日志，since=成立起始
    struct RuleState { bool active = false; bool alerted = false; int64_t since = 0; };
    std::unordered_map<std::string, RuleState> states;
    LOG_INFO("MetricsRegistry: alert evaluator thread started (%zu rule(s))",
             alertRules_.size());
    while (alertRunning_.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::seconds(30));
        int64_t now = NowMs();
        std::vector<AlertRule> rules;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            rules = alertRules_;
        }
        for (const auto& r : rules)
        {
            double v = 0;
            try { v = r.eval(); }
            catch (...)
            {
                LOG_WARN("ALERT [%s] eval threw, skip", r.name.c_str());
                continue;
            }
            bool firing = r.above ? (v > r.threshold) : (v < r.threshold);
            RuleState& st = states[r.name];
            if (firing)
            {
                if (!st.active)
                {
                    st.active = true;
                    st.since = now;
                }
                // for_ms 持续判定：满足时长才真正触发（防抖动）
                if (!st.alerted && (now - st.since) >= r.for_ms)
                {
                    st.alerted = true;
                    if (r.severity == "ERROR")
                        LOG_ERROR("ALERT [%s] firing: value=%.2f, threshold=%.2f, %s",
                                  r.name.c_str(), v, r.threshold, r.description.c_str());
                    else
                        LOG_WARN("ALERT [%s] firing: value=%.2f, threshold=%.2f, %s",
                                 r.name.c_str(), v, r.threshold, r.description.c_str());
                }
            }
            else if (st.active)
            {
                if (st.alerted)
                {
                    LOG_INFO("ALERT [%s] recovered: value=%.2f, threshold=%.2f, %s",
                             r.name.c_str(), v, r.threshold, r.description.c_str());
                }
                st.active = false;
                st.alerted = false;
            }
        }
    }
    LOG_INFO("MetricsRegistry: alert evaluator thread stopped");
}

// ── RateEstimator ─────────────────────────────────────────────────────────
RateEstimator::RateEstimator(int64_t window_ms) : window_ms_(window_ms) {}

double RateEstimator::Observe(double counter_value)
{
    int64_t now = NowMs();
    std::lock_guard<std::mutex> lock(mutex_);
    samples_.push_back({now, counter_value});
    // 丢弃窗口外旧样本
    while (!samples_.empty() && now - samples_.front().first > window_ms_)
        samples_.pop_front();
    if (samples_.size() < 2) return 0.0;
    const auto& first = samples_.front();
    const auto& last = samples_.back();
    int64_t dt_ms = last.first - first.first;
    if (dt_ms <= 0) return 0.0;
    double rate = (last.second - first.second) / (static_cast<double>(dt_ms) / 1000.0);
    // 计数器重置（进程重启/手动清零）会产生负速率，钳制为 0
    return rate < 0 ? 0.0 : rate;
}

// ── MetricsHttpServer ─────────────────────────────────────────────────────
bool MetricsHttpServer::Init(int port)
{
    port_ = port;
    return port_ > 0;
}

void MetricsHttpServer::Start()
{
    if (port_ <= 0)
    {
        LOG_INFO("MetricsHttpServer: metrics_port<=0, metrics disabled");
        return;
    }
    MetricsRegistry::GetInstance().StartSamplers();
    if (started_.exchange(true)) return;
    thread_ = std::thread(&MetricsHttpServer::RunHttpLoop, this);
    LOG_INFO("MetricsHttpServer: listening on 127.0.0.1:%d/metrics", port_);
}

void MetricsHttpServer::Stop()
{
    if (!started_.exchange(false)) return;
    // TcpServer::stop 跨线程安全（EventLoop::stop 带 wakeup，线程池内部 join）
    if (server_) server_->stop();
    if (thread_.joinable()) thread_.join();
    server_.reset();
    MetricsRegistry::GetInstance().StopSamplers();
    LOG_INFO("MetricsHttpServer: stopped");
}

void MetricsHttpServer::RunHttpLoop()
{
    // 在线程内构造并 start：start() 阻塞在 mainLoop_->run() 直到 stop()
    server_ = std::make_unique<wevix_muduo::TcpServer>("127.0.0.1",
                                                       static_cast<uint16_t>(port_), 1);
    server_->setConnectionCallback([this](const wevix_muduo::TcpServer::ConnectionPtr& conn) {
        // 新连接绑定关闭回调，回收其累积缓冲
        conn->setCloseCallback([this](const wevix_muduo::Connection::ConnectionPtr& c) {
            OnClose(c);
        });
    });
    server_->setOnMessageCallback(
        [this](const wevix_muduo::TcpServer::ConnectionPtr& conn, std::string& msg) {
            OnMessage(conn, msg);
        });
    server_->start();
}

void MetricsHttpServer::OnMessage(const ConnectionPtr& conn, std::string& message)
{
    // 1. 累积到该连接的缓冲（HTTP 请求可能分多个 TCP 段到达）
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingBufs_[conn->fd()] += message;
    }

    // 2. 等待请求头完整（HTTP/1.1 以 CRLFCRLF 结束）
    std::string pending;
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        auto it = pendingBufs_.find(conn->fd());
        if (it != pendingBufs_.end()) pending = it->second;
    }
    size_t headEnd = pending.find("\r\n\r\n");
    if (headEnd == std::string::npos)
    {
        // 头没收全：等下一块；超过 8KB 视为异常直接 400 断开
        if (pending.size() > 8192)
        {
            LOG_WARN("MetricsHttpServer: request header too large from fd=%d", conn->fd());
            SendResponse(conn, 400, "Bad Request");
        }
        return;
    }

    // 3. 只解析请求行，忽略其余 header（GET 无 body）
    std::string requestLine = pending.substr(0, pending.find("\r\n"));
    std::istringstream iss(requestLine);
    std::string method, path, version;
    iss >> method >> path >> version;

    // 4. 路由：仅 GET /metrics 返回指标，其余一律 404
    if (method == "GET" && path == "/metrics")
    {
        SendResponse(conn, 200, MetricsRegistry::GetInstance().ExportText());
    }
    else
    {
        LOG_INFO("MetricsHttpServer: %s %s -> 404", method.c_str(), path.c_str());
        SendResponse(conn, 404, "Not Found");
    }

    // 5. 清缓冲（连接随后 shutdown，close 回调兜底再 erase）
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        pendingBufs_.erase(conn->fd());
    }
}

void MetricsHttpServer::OnClose(const ConnectionPtr& conn)
{
    std::lock_guard<std::mutex> lock(pendingMutex_);
    pendingBufs_.erase(conn->fd());
}

void MetricsHttpServer::SendResponse(const ConnectionPtr& conn, int code,
                                     const std::string& body)
{
    std::string status = (code == 200) ? "OK" : (code == 404) ? "Not Found"
                                                               : "Bad Request";
    std::string resp;
    resp.reserve(128 + body.size());
    resp += "HTTP/1.1 " + std::to_string(code) + " " + status + "\r\n";
    resp += "Content-Type: text/plain; version=0.0.4\r\n";
    resp += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    resp += "Connection: close\r\n\r\n";
    resp += body;
    conn->send(resp);
    // 只关写端：内核 flush 完剩余数据，对端 FIN 后框架 handleClose 回收
    // （不用 forceClose：会丢弃 outputBuffer 中未发完的响应）
    conn->shutdown();
}

// ── RpcLatencyHistogram：热路径缓存 ───────────────────────────────────────
MetricHistogram& RpcLatencyHistogram(const std::string& method)
{
    // 首次调用注册进 registry 并缓存指针（注册后 series 不再增删，指针稳定）
    static std::mutex cacheMutex;
    static std::unordered_map<std::string, MetricHistogram*> cache;
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = cache.find(method);
    if (it != cache.end()) return *it->second;
    MetricHistogram& h = MetricsRegistry::GetInstance().Histogram(
        "rpc_latency_ms", "RPC 调用延迟（毫秒）",
        std::vector<double>{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000},
        std::vector<MetricLabel>{{"method", method}});
    cache[method] = &h;
    return h;
}

} // namespace mprpc
