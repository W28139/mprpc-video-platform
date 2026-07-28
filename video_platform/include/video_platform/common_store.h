#pragma once

#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <optional>

namespace video_platform {

// ════════════════════════════════════════════════════════════════════════════
// ⚠️ 重要：所有 Store 都是进程内单例（static 局部变量 + unordered_map）
//
// 在多进程部署中（每个服务独立进程），各进程的 Store 实例完全隔离。
// 跨进程的数据传递通过 RPC 的 proto 请求/响应完成，服务收到 RPC 后
// 需要从请求参数构造本地 Store 副本。不要假定"写入 Store 后另一进程能读到"。
//
// 相关：业务日志第 3 篇「踩坑记录：跨进程内存存储不可见」
// ════════════════════════════════════════════════════════════════════════════

// ============================================================================
// JobStore — 任务元信息内存存储
// ============================================================================

/// @brief 任务元信息记录
///
/// 对应 proto 中 JobInfo 的 C++ 内存表示。
/// 为什么不用 proto 生成的 JobInfo 直接做存储？
/// proto 生成的对象带 vtable、反射等开销，且接口全是 set_/has_ 风格，
/// 不适合高频读写和直接在锁内返回引用。用 POD-like struct 更轻量。
struct JobRecord {
    std::string job_id;             ///< 全局唯一任务 ID（GenerateId("job") 生成）
    std::string user_id;            ///< 提交用户标识
    std::string input_path;         ///< 输入视频文件路径
    std::string output_path;        ///< 期望输出目录路径
    std::string target_format;      ///< 目标容器格式（mp4 / mkv / flv 等）
    std::string target_resolution;  ///< 目标分辨率（720p / 1080p / 4k 等）
    int32_t     target_bitrate = 0; ///< 目标码率（kbps），0 表示不限制
    int64_t     duration_sec = 0;   ///< 视频总时长（秒），由 Scheduler 探测后回填
    int32_t     priority = 0;           ///< 调度优先级，数值越大越优先
    int32_t     status = 0;             ///< 当前状态，对应 JobStatus 枚举值
    int32_t     shard_count = 0;        ///< 拆分出的 shard 总数
    int32_t     shard_duration_sec = 0; ///< 每个 shard 的时间切片长度（秒），0=使用默认值
    int64_t     created_at = 0;     ///< 创建时间戳（毫秒）
    int64_t     updated_at = 0;     ///< 最后更新时间戳（毫秒）
};

/// @brief 任务数据存储（单例，线程安全）
///
/// 读写锁策略：
/// - Insert / Update / Delete 使用 unique_lock（写锁），互斥所有读者
/// - Get / ListAll / Count 使用 shared_lock（读锁），多个读可并发
///
class JobStore {
public:
    /// @brief 获取全局唯一实例
    static JobStore& GetInstance();

    /// @brief 插入新 job。job_id 已存在时返回 false（幂等拒绝）。
    bool Insert(const JobRecord& job);

    /// @brief 按 job_id 全量覆盖更新。不存在时返回 false。
    bool Update(const std::string& job_id, const JobRecord& job);

    /// @brief 删除指定 job。返回 true 表示成功删除。
    bool Delete(const std::string& job_id);

    /// @brief 查询单个 job，返回副本（线程安全）。
    /// @return std::nullopt 表示 job_id 不存在。
    /// 返回值拷贝消除了跨锁悬空指针风险。
    std::optional<JobRecord> Get(const std::string& job_id);

    /// @brief 列出所有 job 的副本（线程安全）。
    std::vector<JobRecord> ListAll() const;

    /// @brief 返回当前存储的 job 总数。
    size_t Count() const;

private:
    JobStore() = default;

    /// @brief 读写锁：读用 shared_lock，写用 unique_lock
    mutable std::shared_mutex mutex_;

    /// @brief job_id → JobRecord 映射表
    std::unordered_map<std::string, JobRecord> jobs_;
};

// ============================================================================
// ShardStore — Shard 元信息内存存储
// ============================================================================

/// @brief Shard（子任务）元信息记录
///
/// 一个 Job 被按时间段拆成多个 Shard，每个 Shard 可独立分配给 Worker 执行。
/// shard_id 格式：{job_id}_shard_{shard_index}
struct ShardRecord {
    std::string shard_id;              ///< 全局唯一 shard ID
    std::string job_id;                ///< 所属 job ID
    int32_t     shard_index = 0;       ///< 在 job 中的序号（0, 1, 2, ...）
    int64_t     start_ms = 0;          ///< 视频起始偏移（毫秒）
    int64_t     duration_ms = 0;       ///< 视频片段时长（毫秒）
    int32_t     status = 0;            ///< 当前状态，对应 ShardStatus 枚举值
    std::string assigned_worker_id;    ///< 被分配到的 Worker ID，未分配时为空
    std::string attempt_id;            ///< 当前执行尝试 ID，每次重试递增
    int32_t     retry_count = 0;       ///< 已重试次数
    int32_t     max_retry = 3;         ///< 最大重试次数（默认 3）
    std::string input_path;            ///< 输入文件路径（通常与 job 相同或切片路径）
    std::string output_path;           ///< 输出文件路径
    int64_t     created_at = 0;        ///< 创建时间戳（毫秒）
    int64_t     updated_at = 0;        ///< 最后更新时间戳（毫秒）
};

/// @brief Shard 数据存储（单例，线程安全）
///
/// 除了常规 CRUD，额外提供 ListByJob / ListByWorker 两个业务查询接口，
/// 用于 JobService 的 QueryJob 和 WorkerManager 的心跳超时恢复。
class ShardStore {
public:
    /// @brief 获取全局唯一实例
    static ShardStore& GetInstance();

    /// @brief 插入新 shard。shard_id 已存在时返回 false。
    bool Insert(const ShardRecord& shard);

    /// @brief 按 shard_id 全量覆盖更新。不存在时返回 false。
    bool Update(const std::string& shard_id, const ShardRecord& shard);

    /// @brief 删除指定 shard。返回 true 表示成功删除。
    bool Delete(const std::string& shard_id);

    /// @brief 查询单个 shard，返回副本（线程安全）。
    /// @return std::nullopt 表示 shard_id 不存在。
    std::optional<ShardRecord> Get(const std::string& shard_id);

    /// @brief 列出指定 job 下的所有 shard（线程安全）。
    std::vector<ShardRecord> ListByJob(const std::string& job_id) const;

    /// @brief 列出分配给指定 Worker 的所有 shard（线程安全）。
    /// 用途：Worker 心跳超时后，找出所有该 Worker 上 RUNNING 的 shard 重新调度。
    std::vector<ShardRecord> ListByWorker(const std::string& worker_id) const;

    /// @brief 原子插入或覆盖更新（upsert）。
    /// 在 unique_lock 内完成：key 不存在则插入，存在则覆盖。
    /// @return true 表示是新插入（key 之前不存在），false 表示覆盖了已存在的记录。
    bool InsertOrUpdate(const ShardRecord& shard);

    /// @brief 按状态过滤列出 shard。
    /// @param status_filter 状态值，传 -1 表示列出全部。
    /// 用途：Scheduler 调度循环查找 SHARD_WAITING 的待分配 shard。
    std::vector<ShardRecord> ListByStatus(int32_t status_filter) const;

    /// @brief 列出所有 shard 的副本（线程安全）。
    std::vector<ShardRecord> ListAll() const;

    /// @brief 返回当前存储的 shard 总数。
    size_t Count() const;

private:
    ShardStore() = default;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, ShardRecord> shards_;
};

// ============================================================================
// WorkerStore — Worker 注册信息内存存储
// ============================================================================

/// @brief Worker 节点信息记录
///
/// 由 WorkerManagerService 在 RegisterWorker 时创建，
/// 在 Heartbeat 时更新 load 字段和 last_heartbeat，
/// 心跳超时后标记为 WORKER_OFFLINE。
struct WorkerRecord {
    std::string worker_id;               ///< 全局唯一 Worker ID（GenerateId("worker") 生成）
    std::string ip;                      ///< Worker 所在机器 IP 地址
    int32_t     port = 0;                ///< Worker RPC 监听端口
    int32_t     cpu_cores = 0;           ///< 机器 CPU 核心数
    int32_t     memory_mb = 0;           ///< 机器总内存（MB）
    int32_t     gpu_count = 0;           ///< GPU 数量，0 表示无 GPU
    int32_t     current_running_shards = 0; ///< 当前正在执行的 shard 数
    int32_t     max_running_shards = 0;     ///< Worker 允许的最大并发 shard 数
    int32_t     status = 0;              ///< 当前状态，对应 WorkerStatus 枚举值
    int64_t     last_heartbeat = 0;      ///< 最后一次心跳时间戳（毫秒）
};

/// @brief Worker 数据存储（单例，线程安全）
///
/// ListByStatus 支持按 ONLINE/OFFLINE 过滤，用于 Scheduler 查询可用节点。
class WorkerStore {
public:
    /// @brief 获取全局唯一实例
    static WorkerStore& GetInstance();

    /// @brief 注册新 Worker。worker_id 已存在时返回 false（幂等拒绝）。
    bool Insert(const WorkerRecord& worker);

    /// @brief 按 worker_id 全量覆盖更新。不存在时返回 false。
    bool Update(const std::string& worker_id, const WorkerRecord& worker);

    /// @brief 注销 Worker。返回 true 表示成功删除。
    bool Delete(const std::string& worker_id);

    /// @brief 查询单个 Worker，返回副本（线程安全）。
    /// @return std::nullopt 表示 worker_id 不存在。
    std::optional<WorkerRecord> Get(const std::string& worker_id);

    // ── 原子方法（在 unique_lock 内完成读-改-写，消除 TOCTOU 竞争） ──

    /// @brief 原子更新心跳时间戳和运行中 shard 数。
    /// 在 unique_lock 内完成：查找 → 更新 last_heartbeat/status/running_shards。
    /// @return true 表示 worker_id 存在且已更新，false 表示未找到。
    bool UpdateHeartbeat(const std::string& worker_id, int32_t running_shards);

    /// @brief 原子检查心跳超时并标记 OFFLINE。
    /// 在 unique_lock 内完成：查找 → 检查 status==ONLINE → 检查 now-last_heartbeat>timeout
    /// → 标记 OFFLINE。全程持写锁，与 Heartbeat RPC 的 UpdateHeartbeat 互斥。
    /// @return true 表示确认超时并已标记 OFFLINE，false 表示未超时或不存在。
    bool MarkOfflineIfTimeout(const std::string& worker_id, int64_t now, int64_t timeout_ms);

    /// @brief 原子插入或覆盖更新（upsert）。
    /// 在 unique_lock 内完成：key 不存在则插入，存在则覆盖。全程持写锁。
    /// @return true 表示是新插入（key 之前不存在），false 表示覆盖了已存在的记录。
    bool InsertOrUpdate(const WorkerRecord& worker);

    /// @brief 按状态过滤列出 Worker。
    /// @param status_filter 状态值，传 -1 表示列出全部。
    std::vector<WorkerRecord> ListByStatus(int32_t status_filter) const;

    /// @brief 列出所有 Worker 的副本（线程安全）。
    std::vector<WorkerRecord> ListAll() const;

    /// @brief 返回当前注册的 Worker 总数。
    size_t Count() const;

private:
    WorkerStore() = default;
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, WorkerRecord> workers_;
};

// ============================================================================
// 辅助函数
// ============================================================================

/// @brief 生成全局唯一 ID
/// @param prefix ID 前缀（如 "job"、"shard"、"worker"）
/// @return 格式为 prefix_timestamp_counter_random 的唯一字符串
///
/// 使用进程内原子计数器 + 线程本地随机数，保证单进程内唯一。
/// 多进程场景下 timestamp 不同也可区分，不需要分布式 ID 方案。
std::string GenerateId(const std::string& prefix);

/// @brief 获取当前 Unix 时间戳（毫秒）
/// 用于 job/shard 创建时间、Worker 心跳时间等字段。
int64_t NowMs();

/// @brief 获取当前 Unix 时间戳（秒）
/// 用于日志和格式化时间展示。
int64_t NowSec();

} // namespace video_platform
