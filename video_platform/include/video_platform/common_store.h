#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace video_platform {

// ════════════════════════════════════════════════════════════════════════════
// ⚠️ 阶段 9（数据持久化）起：三个 Store 由进程内 unordered_map 改为 MySQL 持久化。
//
// - MySQL 是唯一数据源：读（SELECT）写（INSERT/UPDATE/DELETE）全部走数据库，
//   不再维护进程内副本。多进程部署时天然共享同一份数据，
//   各服务不再需要"从 RPC 请求构造本地副本"的跨进程同步逻辑。
// - 接口保持不变：调用方代码无需感知存储实现。
// - 原子性：UpdateHeartbeat / MarkOfflineIfTimeout / InsertOrUpdate 等
//   由单条条件 SQL 保证（强于原 shared_mutex 锁内检查）。
// - 服务启动时必须先调用 MysqlPool::GetInstance().Init()（见各服务 main()），
//   预创建连接失败会拒绝启动。
// ════════════════════════════════════════════════════════════════════════════

// ============================================================================
// JobStore — 任务元信息存储（MySQL 持久化）
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
/// 线程安全：每个方法独立借用 MySQL 连接池连接执行 SQL，
/// 连接池保证同一连接不会并发使用，MySQL 服务端保证单语句原子性。
class JobStore {
public:
    /// @brief 获取全局唯一实例
    static JobStore& GetInstance();

    /// @brief 插入新 job。job_id 已存在时返回 false（幂等拒绝）。
    bool Insert(const JobRecord& job);

    /// @brief 按 job_id 全量覆盖更新。不存在时返回 false。
    bool Update(const std::string& job_id, const JobRecord& job);

    /// @brief 条件状态推进：仅当当前行状态是 expect_statuses 之一时才全量覆盖。
    /// expect_statuses 为空表示不检查状态（等价 Update）。
    /// 用途：MySQL 化后 job 行由多进程共同推进（Scheduler/ResultCollector/
    /// JobService 都会更新），必须用"快照读到的前置状态"作为条件，
    /// 防止旧快照把其他进程已推进的状态（如终态）覆盖回退。
    /// @return true=更新成功；false=行不存在或状态不匹配
    bool UpdateIfStatus(const std::string& job_id,
                        const std::vector<int32_t>& expect_statuses,
                        const JobRecord& job);

    /// @brief 删除指定 job。返回 true 表示成功删除。
    bool Delete(const std::string& job_id);

    /// @brief 查询单个 job，返回副本（线程安全）。
    /// @return std::nullopt 表示 job_id 不存在。
    std::optional<JobRecord> Get(const std::string& job_id);

    /// @brief 列出所有 job 的副本（线程安全）。
    std::vector<JobRecord> ListAll() const;

    /// @brief 列出最近创建的 job（按 created_at 倒序，最多 limit 条）。
    /// limit <= 0 表示返回全部（等价 ListAll）。
    /// 阶段 12 新增：GUI 任务列表数据源。
    std::vector<JobRecord> ListRecent(int32_t limit) const;

    /// @brief 返回当前存储的 job 总数。
    size_t Count() const;

private:
    JobStore() = default;
};

// ============================================================================
// ShardStore — Shard 元信息存储（MySQL 持久化）
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
    std::string screenshot_path;       ///< 截图文件路径（转码成功后自动截取）
    std::string target_resolution;     ///< 目标分辨率（如 "1280x720"），从 JobInfo 透传
    int32_t     target_bitrate = 0;    ///< 目标码率（kbps），0=保持原码率，从 JobInfo 透传
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

    /// @brief 条件状态推进：仅当当前行状态是 expect_statuses 之一时才全量覆盖。
    /// expect_statuses 为空表示不检查状态（等价 Update）。
    /// 用途：MySQL 化后 shard 行由 Scheduler（分配/重扫/重试）与 ResultCollector
    /// （SUCCESS/FAILED）共同推进，必须用"快照读到的前置状态"作为条件，
    /// 防止旧快照覆盖其他进程已推进的状态（如把 SUCCESS 覆盖回 WAITING）。
    /// @return true=更新成功；false=行不存在或状态不匹配
    bool UpdateIfStatus(const std::string& shard_id,
                        const std::vector<int32_t>& expect_statuses,
                        const ShardRecord& shard);

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
    /// 单条 INSERT ... ON DUPLICATE KEY UPDATE，MySQL 服务端原子完成。
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

    /// @brief 按状态统计 shard 数量（阶段 11 可观测性采样用）。
    /// 单条 SELECT status, COUNT(*) ... GROUP BY status。
    /// @return status → count 映射（仅包含有数据的 status；查询失败返回空 map）
    std::map<int32_t, size_t> CountByStatus() const;

private:
    ShardStore() = default;
};

// ============================================================================
// WorkerStore — Worker 注册信息存储（MySQL 持久化）
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
    int32_t     cpu_usage = 0;              ///< CPU 使用率（0-100 百分比），心跳上报
    int32_t     memory_usage = 0;           ///< 内存使用率（0-100 百分比），心跳上报
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

    // ── 原子方法（单条条件 SQL，MySQL 服务端原子执行，消除 TOCTOU 竞争） ──

    /// @brief 原子更新心跳时间戳、运行中 shard 数、CPU/内存使用率。
    /// 单条 UPDATE ... WHERE worker_id，未命中（worker 不存在）返回 false。
    /// @return true 表示 worker_id 存在且已更新，false 表示未找到。
    bool UpdateHeartbeat(const std::string& worker_id, int32_t running_shards,
                         int32_t cpu_usage = 0, int32_t memory_usage = 0);

    /// @brief 原子检查心跳超时并标记 OFFLINE。
    /// 单条条件 UPDATE：status=ONLINE AND last_heartbeat < now-timeout 才生效，
    /// 与 Heartbeat 的 UpdateHeartbeat 天然互斥（数据库行级原子性）。
    /// @return true 表示确认超时并已标记 OFFLINE，false 表示未超时或不存在。
    bool MarkOfflineIfTimeout(const std::string& worker_id, int64_t now, int64_t timeout_ms);

    /// @brief 原子插入或覆盖更新（upsert）。
    /// 单条 INSERT ... ON DUPLICATE KEY UPDATE，MySQL 服务端原子完成。
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
