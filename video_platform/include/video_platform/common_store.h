#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace video_platform {

// 阶段 9 起三个 Store 由 MySQL 持久化。MySQL 是唯一数据源，接口不变。

// JobStore — 任务元信息存储

struct JobRecord {
    std::string job_id;
    std::string user_id;
    std::string input_path;
    std::string output_path;
    std::string target_format;
    std::string target_resolution;
    int32_t     target_bitrate = 0;
    int64_t     duration_sec = 0;
    int32_t     priority = 0;
    int32_t     status = 0;
    int32_t     shard_count = 0;
    int32_t     shard_duration_sec = 0;
    int64_t     created_at = 0;
    int64_t     updated_at = 0;
};

class JobStore {
public:
    static JobStore& GetInstance();

    bool Insert(const JobRecord& job);
    bool Update(const std::string& job_id, const JobRecord& job);

    /// 条件状态推进：仅当前状态在 expect_statuses 中时才更新（防旧快照覆盖）
    bool UpdateIfStatus(const std::string& job_id,
                        const std::vector<int32_t>& expect_statuses,
                        const JobRecord& job);

    bool Delete(const std::string& job_id);
    std::optional<JobRecord> Get(const std::string& job_id);
    std::vector<JobRecord> ListAll() const;
    size_t Count() const;

private:
    JobStore() = default;
};

// ShardStore — Shard 元信息存储

struct ShardRecord {
    std::string shard_id;
    std::string job_id;
    int32_t     shard_index = 0;
    int64_t     start_ms = 0;
    int64_t     duration_ms = 0;
    int32_t     status = 0;
    std::string assigned_worker_id;
    std::string attempt_id;
    int32_t     retry_count = 0;
    int32_t     max_retry = 3;
    std::string input_path;
    std::string output_path;
    std::string target_resolution;
    int32_t     target_bitrate = 0;
    int64_t     created_at = 0;
    int64_t     updated_at = 0;
};

class ShardStore {
public:
    static ShardStore& GetInstance();

    bool Insert(const ShardRecord& shard);
    bool Update(const std::string& shard_id, const ShardRecord& shard);

    /// 条件状态推进（防旧快照覆盖）
    bool UpdateIfStatus(const std::string& shard_id,
                        const std::vector<int32_t>& expect_statuses,
                        const ShardRecord& shard);

    bool Delete(const std::string& shard_id);
    std::optional<ShardRecord> Get(const std::string& shard_id);
    std::vector<ShardRecord> ListByJob(const std::string& job_id) const;
    std::vector<ShardRecord> ListByWorker(const std::string& worker_id) const;
    std::vector<ShardRecord> ListByStatus(int32_t status_filter) const;
    std::vector<ShardRecord> ListAll() const;
    size_t Count() const;
    std::map<int32_t, size_t> CountByStatus() const;

    /// INSERT ... ON DUPLICATE KEY UPDATE
    bool InsertOrUpdate(const ShardRecord& shard);

private:
    ShardStore() = default;
};

// WorkerStore — Worker 注册信息存储

struct WorkerRecord {
    std::string worker_id;
    std::string ip;
    int32_t     port = 0;
    int32_t     cpu_cores = 0;
    int32_t     memory_mb = 0;
    int32_t     gpu_count = 0;
    int32_t     current_running_shards = 0;
    int32_t     max_running_shards = 0;
    int32_t     cpu_usage = 0;
    int32_t     memory_usage = 0;
    int32_t     status = 0;
    int64_t     last_heartbeat = 0;
};

class WorkerStore {
public:
    static WorkerStore& GetInstance();

    bool Insert(const WorkerRecord& worker);
    bool Update(const std::string& worker_id, const WorkerRecord& worker);
    bool Delete(const std::string& worker_id);
    std::optional<WorkerRecord> Get(const std::string& worker_id);

    /// 原子更新心跳（单条 UPDATE WHERE status<>OFFLINE）
    bool UpdateHeartbeat(const std::string& worker_id, int32_t running_shards,
                         int32_t cpu_usage = 0, int32_t memory_usage = 0);

    /// 原子标记超时 OFFLINE（单条条件 UPDATE，与 Heartbeat 行级互斥）
    bool MarkOfflineIfTimeout(const std::string& worker_id, int64_t now, int64_t timeout_ms);

    /// INSERT ... ON DUPLICATE KEY UPDATE
    bool InsertOrUpdate(const WorkerRecord& worker);

    std::vector<WorkerRecord> ListByStatus(int32_t status_filter) const;
    std::vector<WorkerRecord> ListAll() const;
    size_t Count() const;

private:
    WorkerStore() = default;
};

// 辅助函数

/// 生成全局唯一 ID（格式：prefix_timestamp_counter_random）
std::string GenerateId(const std::string& prefix);

/// 当前 Unix 时间戳（毫秒）
int64_t NowMs();

/// 当前 Unix 时间戳（秒）
int64_t NowSec();

} // namespace video_platform