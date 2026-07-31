#include "video_platform/common_store.h"
#include "worker.pb.h"
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>
#include <thread>
#include <atomic>

namespace video_platform {

// ============================================================================
// 辅助函数
// ============================================================================

/// @brief 生成全局唯一 ID
///
/// 格式：prefix_timestamp_counter_random
/// - prefix: 调用方传入的标识前缀
/// - timestamp: 毫秒级时间戳
/// - counter: 进程内原子递增计数器（6 位补零）
/// - random: 线程本地 4 位随机数
///
/// 线程安全：counter 是原子变量；rng 和 dist 是 thread_local。
/// 不使用 std::random_device（/dev/urandom），因为在 WSL/容器中可能阻塞。
std::string GenerateId(const std::string& prefix)
{
    static std::atomic<uint64_t> counter{0};
    static thread_local std::mt19937_64 rng(
        std::chrono::steady_clock::now().time_since_epoch().count() +
        std::hash<std::thread::id>{}(std::this_thread::get_id()));
    static thread_local std::uniform_int_distribution<int> dist(1000, 9999);

    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();

    std::ostringstream oss;
    oss << prefix << "_" << ms << "_" << std::setfill('0') << std::setw(6)
        << (counter.fetch_add(1) % 1000000) << "_" << dist(rng);
    return oss.str();
}

int64_t NowMs()
{
    auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

int64_t NowSec()
{
    auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::seconds>(now).count();
}

// ============================================================================
// JobStore — 线程安全的任务数据存储
// ============================================================================

JobStore& JobStore::GetInstance()
{
    static JobStore instance;   // C++11 保证线程安全的静态局部变量初始化
    return instance;
}

bool JobStore::Insert(const JobRecord& job)
{
    std::unique_lock lock(mutex_);
    // try_emplace 若 key 已存在则不做任何事，返回 {iterator, false}
    auto result = jobs_.try_emplace(job.job_id, job);
    return result.second;
}

bool JobStore::Update(const std::string& job_id, const JobRecord& job)
{
    std::unique_lock lock(mutex_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) return false;
    it->second = job;       // 全量覆盖
    return true;
}

bool JobStore::Delete(const std::string& job_id)
{
    std::unique_lock lock(mutex_);
    return jobs_.erase(job_id) > 0;
}

std::optional<JobRecord> JobStore::Get(const std::string& job_id)
{
    std::shared_lock lock(mutex_);
    auto it = jobs_.find(job_id);
    if (it == jobs_.end()) return std::nullopt;
    // 返回副本，消除跨锁悬空指针风险
    return it->second;
}

std::vector<JobRecord> JobStore::ListAll() const
{
    std::shared_lock lock(mutex_);
    std::vector<JobRecord> result;
    result.reserve(jobs_.size());
    for (const auto& kv : jobs_)
        result.push_back(kv.second);
    return result;
}

size_t JobStore::Count() const
{
    std::shared_lock lock(mutex_);
    return jobs_.size();
}

// ============================================================================
// ShardStore — 线程安全的 Shard 数据存储
// ============================================================================

ShardStore& ShardStore::GetInstance()
{
    static ShardStore instance;
    return instance;
}

bool ShardStore::Insert(const ShardRecord& shard)
{
    std::unique_lock lock(mutex_);
    auto result = shards_.try_emplace(shard.shard_id, shard);
    return result.second;
}

bool ShardStore::Update(const std::string& shard_id, const ShardRecord& shard)
{
    std::unique_lock lock(mutex_);
    auto it = shards_.find(shard_id);
    if (it == shards_.end()) return false;
    it->second = shard;
    return true;
}

bool ShardStore::Delete(const std::string& shard_id)
{
    std::unique_lock lock(mutex_);
    return shards_.erase(shard_id) > 0;
}

std::optional<ShardRecord> ShardStore::Get(const std::string& shard_id)
{
    std::shared_lock lock(mutex_);
    auto it = shards_.find(shard_id);
    if (it == shards_.end()) return std::nullopt;
    return it->second;
}

std::vector<ShardRecord> ShardStore::ListByJob(const std::string& job_id) const
{
    std::shared_lock lock(mutex_);
    std::vector<ShardRecord> result;
    for (const auto& kv : shards_)
    {
        if (kv.second.job_id == job_id)
            result.push_back(kv.second);
    }
    return result;
}

std::vector<ShardRecord> ShardStore::ListByWorker(const std::string& worker_id) const
{
    std::shared_lock lock(mutex_);
    std::vector<ShardRecord> result;
    for (const auto& kv : shards_)
    {
        if (kv.second.assigned_worker_id == worker_id)
            result.push_back(kv.second);
    }
    return result;
}

bool ShardStore::InsertOrUpdate(const ShardRecord& shard)
{
    std::unique_lock lock(mutex_);
    auto result = shards_.try_emplace(shard.shard_id, shard);
    if (!result.second)
    {
        // key 已存在，覆盖旧记录
        result.first->second = shard;
    }
    return result.second;  // true = 新插入, false = 覆盖
}

std::vector<ShardRecord> ShardStore::ListByStatus(int32_t status_filter) const
{
    std::shared_lock lock(mutex_);
    std::vector<ShardRecord> result;
    for (const auto& kv : shards_)
    {
        if (status_filter < 0 || kv.second.status == status_filter)
            result.push_back(kv.second);
    }
    return result;
}

std::vector<ShardRecord> ShardStore::ListAll() const
{
    std::shared_lock lock(mutex_);
    std::vector<ShardRecord> result;
    result.reserve(shards_.size());
    for (const auto& kv : shards_)
        result.push_back(kv.second);
    return result;
}

size_t ShardStore::Count() const
{
    std::shared_lock lock(mutex_);
    return shards_.size();
}

// ============================================================================
// WorkerStore — 线程安全的 Worker 数据存储
// ============================================================================

WorkerStore& WorkerStore::GetInstance()
{
    static WorkerStore instance;
    return instance;
}

bool WorkerStore::Insert(const WorkerRecord& worker)
{
    std::unique_lock lock(mutex_);
    auto result = workers_.try_emplace(worker.worker_id, worker);
    return result.second;
}

bool WorkerStore::Update(const std::string& worker_id, const WorkerRecord& worker)
{
    std::unique_lock lock(mutex_);
    auto it = workers_.find(worker_id);
    if (it == workers_.end()) return false;
    it->second = worker;
    return true;
}

bool WorkerStore::Delete(const std::string& worker_id)
{
    std::unique_lock lock(mutex_);
    return workers_.erase(worker_id) > 0;
}

std::optional<WorkerRecord> WorkerStore::Get(const std::string& worker_id)
{
    std::shared_lock lock(mutex_);
    auto it = workers_.find(worker_id);
    if (it == workers_.end()) return std::nullopt;
    return it->second;
}

std::vector<WorkerRecord> WorkerStore::ListByStatus(int32_t status_filter) const
{
    std::shared_lock lock(mutex_);
    std::vector<WorkerRecord> result;
    for (const auto& kv : workers_)
    {
        // status_filter < 0 表示"不过滤，全部返回"
        if (status_filter < 0 || kv.second.status == status_filter)
            result.push_back(kv.second);
    }
    return result;
}

std::vector<WorkerRecord> WorkerStore::ListAll() const
{
    std::shared_lock lock(mutex_);
    std::vector<WorkerRecord> result;
    result.reserve(workers_.size());
    for (const auto& kv : workers_)
        result.push_back(kv.second);
    return result;
}

size_t WorkerStore::Count() const
{
    std::shared_lock lock(mutex_);
    return workers_.size();
}

// ── 原子方法（在 unique_lock 内完成读-改-写） ──

bool WorkerStore::UpdateHeartbeat(const std::string& worker_id, int32_t running_shards,
                                   int32_t cpu_usage, int32_t memory_usage)
{
    std::unique_lock lock(mutex_);
    auto it = workers_.find(worker_id);
    if (it == workers_.end()) return false;

    it->second.current_running_shards = running_shards;
    it->second.cpu_usage              = cpu_usage;
    it->second.memory_usage           = memory_usage;
    it->second.last_heartbeat         = NowMs();
    it->second.status                 = static_cast<int32_t>(WorkerStatus::WORKER_ONLINE);
    return true;
}

bool WorkerStore::MarkOfflineIfTimeout(const std::string& worker_id, int64_t now, int64_t timeout_ms)
{
    std::unique_lock lock(mutex_);
    auto it = workers_.find(worker_id);
    if (it == workers_.end()) return false;
    if (it->second.status != static_cast<int32_t>(WorkerStatus::WORKER_ONLINE)) return false;
    if (now - it->second.last_heartbeat <= timeout_ms) return false;

    it->second.status = static_cast<int32_t>(WorkerStatus::WORKER_OFFLINE);
    return true;
}

bool WorkerStore::InsertOrUpdate(const WorkerRecord& worker)
{
    std::unique_lock lock(mutex_);
    auto result = workers_.try_emplace(worker.worker_id, worker);
    if (!result.second)
    {
        // key 已存在，覆盖旧记录
        result.first->second = worker;
    }
    return result.second;  // true = 新插入, false = 覆盖
}

} // namespace video_platform
