#include "video_platform/common_store.h"
#include "video_platform/mysql_pool.h"
#include "common.pb.h"   // WorkerStatus 等状态枚举
#include "wevix_muduo/AsyncLogger.h"

#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>
#include <thread>
#include <atomic>

namespace video_platform {

// ════════════════════════════════════════════════════════════════════════════
// 说明（阶段 9）：
//
// 三个 Store 全部以 MySQL 为唯一数据源。每个方法独立借用连接池连接执行
// SQL（MysqlConnectionGuard RAII 自动归还）。线程安全由连接池保证：
// 同一连接不会被并发使用，MySQL 服务端保证单语句原子性。
//
// 字符串字段一律经 MysqlConnectionGuard::Escape 转义（input_path 等
// 可能来自客户端输入，防 SQL 注入）。
// ════════════════════════════════════════════════════════════════════════════

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
// 公共辅助：列清单 / 行解析
//
// ⚠️ 列清单顺序必须与对应解析函数一一对应（SELECT 显式列名，不依赖表结构）。
// ============================================================================

static const char* kJobColumns =
    "job_id,user_id,input_path,output_path,target_format,target_resolution,"
    "target_bitrate,duration_sec,priority,status,shard_count,"
    "shard_duration_sec,created_at,updated_at";

static JobRecord ParseJobRow(const std::vector<std::string>& r)
{
    JobRecord job;
    job.job_id             = r[0];
    job.user_id            = r[1];
    job.input_path         = r[2];
    job.output_path        = r[3];
    job.target_format      = r[4];
    job.target_resolution  = r[5];
    job.target_bitrate     = std::stoi(r[6]);
    job.duration_sec       = std::stoll(r[7]);
    job.priority           = std::stoi(r[8]);
    job.status             = std::stoi(r[9]);
    job.shard_count        = std::stoi(r[10]);
    job.shard_duration_sec = std::stoi(r[11]);
    job.created_at         = std::stoll(r[12]);
    job.updated_at         = std::stoll(r[13]);
    return job;
}

static const char* kShardColumns =
    "shard_id,job_id,shard_index,start_ms,duration_ms,status,assigned_worker,"
    "attempt_id,retry_count,max_retry,input_path,output_path,"
    "target_resolution,target_bitrate,created_at,updated_at";

static ShardRecord ParseShardRow(const std::vector<std::string>& r)
{
    ShardRecord shard;
    shard.shard_id             = r[0];
    shard.job_id               = r[1];
    shard.shard_index          = std::stoi(r[2]);
    shard.start_ms             = std::stoll(r[3]);
    shard.duration_ms          = std::stoll(r[4]);
    shard.status               = std::stoi(r[5]);
    shard.assigned_worker_id   = r[6];
    shard.attempt_id           = r[7];
    shard.retry_count          = std::stoi(r[8]);
    shard.max_retry            = std::stoi(r[9]);
    shard.input_path           = r[10];
    shard.output_path          = r[11];
    shard.target_resolution    = r[12];
    shard.target_bitrate       = std::stoi(r[13]);
    shard.created_at           = std::stoll(r[14]);
    shard.updated_at           = std::stoll(r[15]);
    return shard;
}

static const char* kWorkerColumns =
    "worker_id,ip,port,cpu_cores,memory_mb,gpu_count,current_running_shards,"
    "max_running_shards,cpu_usage,memory_usage,status,last_heartbeat";

static WorkerRecord ParseWorkerRow(const std::vector<std::string>& r)
{
    WorkerRecord worker;
    worker.worker_id              = r[0];
    worker.ip                     = r[1];
    worker.port                   = std::stoi(r[2]);
    worker.cpu_cores              = std::stoi(r[3]);
    worker.memory_mb              = std::stoi(r[4]);
    worker.gpu_count              = std::stoi(r[5]);
    worker.current_running_shards = std::stoi(r[6]);
    worker.max_running_shards     = std::stoi(r[7]);
    worker.cpu_usage              = std::stoi(r[8]);
    worker.memory_usage           = std::stoi(r[9]);
    worker.status                 = std::stoi(r[10]);
    worker.last_heartbeat         = std::stoll(r[11]);
    return worker;
}

// ============================================================================
// JobStore — 任务数据存储（MySQL 持久化）
// ============================================================================

JobStore& JobStore::GetInstance()
{
    static JobStore instance;   // C++11 保证线程安全的静态局部变量初始化
    return instance;
}

bool JobStore::Insert(const JobRecord& job)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "INSERT INTO jobs (" << kJobColumns << ") VALUES ('"
        << g.Escape(job.job_id) << "','" << g.Escape(job.user_id) << "','"
        << g.Escape(job.input_path) << "','" << g.Escape(job.output_path) << "','"
        << g.Escape(job.target_format) << "','" << g.Escape(job.target_resolution) << "',"
        << job.target_bitrate << "," << job.duration_sec << "," << job.priority << ","
        << job.status << "," << job.shard_count << "," << job.shard_duration_sec << ","
        << job.created_at << "," << job.updated_at << ")";
    if (!g.Execute(sql.str())) {
        if (g.Errno() == 1062) return false;   // duplicate key：幂等拒绝（非错误）
        LOG_ERROR("JobStore::Insert failed (job=%s): %s",
                  job.job_id.c_str(), g.Error().c_str());
        return false;
    }
    return true;
}

bool JobStore::Update(const std::string& job_id, const JobRecord& job)
{
    // 无条件更新 = 空 expect_statuses 的条件更新
    return UpdateIfStatus(job_id, {}, job);
}

bool JobStore::UpdateIfStatus(const std::string& job_id,
                              const std::vector<int32_t>& expect_statuses,
                              const JobRecord& job)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "UPDATE jobs SET user_id='" << g.Escape(job.user_id)
        << "',input_path='" << g.Escape(job.input_path)
        << "',output_path='" << g.Escape(job.output_path)
        << "',target_format='" << g.Escape(job.target_format)
        << "',target_resolution='" << g.Escape(job.target_resolution)
        << "',target_bitrate=" << job.target_bitrate
        << ",duration_sec=" << job.duration_sec
        << ",priority=" << job.priority
        << ",status=" << job.status
        << ",shard_count=" << job.shard_count
        << ",shard_duration_sec=" << job.shard_duration_sec
        << ",created_at=" << job.created_at
        << ",updated_at=" << job.updated_at
        << " WHERE job_id='" << g.Escape(job_id) << "'";
    if (!expect_statuses.empty())
    {
        sql << " AND status IN (";
        for (size_t i = 0; i < expect_statuses.size(); ++i)
        {
            if (i > 0) sql << ",";
            sql << expect_statuses[i];
        }
        sql << ")";
    }
    if (!g.Execute(sql.str())) {
        LOG_ERROR("JobStore::Update failed (job=%s): %s",
                  job_id.c_str(), g.Error().c_str());
        return false;
    }
    // CLIENT_FOUND_ROWS：值未变化时也返回匹配行数，不会误判为未命中
    return g.AffectedRows() > 0;
}

bool JobStore::Delete(const std::string& job_id)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::string sql = "DELETE FROM jobs WHERE job_id='" + g.Escape(job_id) + "'";
    if (!g.Execute(sql)) {
        LOG_ERROR("JobStore::Delete failed (job=%s): %s",
                  job_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() > 0;
}

std::optional<JobRecord> JobStore::Get(const std::string& job_id)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return std::nullopt;
    std::string sql = "SELECT " + std::string(kJobColumns) +
                      " FROM jobs WHERE job_id='" + g.Escape(job_id) + "' LIMIT 1";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("JobStore::Get failed (job=%s): %s",
                  job_id.c_str(), g.Error().c_str());
        return std::nullopt;
    }
    if (rows.empty()) return std::nullopt;
    return ParseJobRow(rows[0]);
}

std::vector<JobRecord> JobStore::ListAll() const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::vector<JobRecord> result;
    if (!g.ok()) return result;
    std::string sql = "SELECT " + std::string(kJobColumns) + " FROM jobs";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("JobStore::ListAll failed: %s", g.Error().c_str());
        return result;
    }
    result.reserve(rows.size());
    for (const auto& r : rows) result.push_back(ParseJobRow(r));
    return result;
}

size_t JobStore::Count() const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return 0;
    std::vector<std::vector<std::string>> rows;
    if (!g.Query("SELECT COUNT(*) FROM jobs", rows) || rows.empty()) return 0;
    return static_cast<size_t>(std::stoll(rows[0][0]));
}

// ============================================================================
// ShardStore — Shard 数据存储（MySQL 持久化）
// ============================================================================

ShardStore& ShardStore::GetInstance()
{
    static ShardStore instance;
    return instance;
}

bool ShardStore::Insert(const ShardRecord& shard)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "INSERT INTO shards (" << kShardColumns << ") VALUES ('"
        << g.Escape(shard.shard_id) << "','" << g.Escape(shard.job_id) << "',"
        << shard.shard_index << "," << shard.start_ms << "," << shard.duration_ms << ","
        << shard.status << ",'" << g.Escape(shard.assigned_worker_id) << "','"
        << g.Escape(shard.attempt_id) << "'," << shard.retry_count << "," << shard.max_retry
        << ",'" << g.Escape(shard.input_path) << "','" << g.Escape(shard.output_path) << "','"
        << g.Escape(shard.target_resolution) << "',"
        << shard.target_bitrate << "," << shard.created_at << "," << shard.updated_at << ")";
    if (!g.Execute(sql.str())) {
        if (g.Errno() == 1062) return false;   // duplicate key：幂等拒绝（非错误）
        LOG_ERROR("ShardStore::Insert failed (shard=%s): %s",
                  shard.shard_id.c_str(), g.Error().c_str());
        return false;
    }
    return true;
}

bool ShardStore::Update(const std::string& shard_id, const ShardRecord& shard)
{
    // 无条件更新 = 空 expect_statuses 的条件更新
    return UpdateIfStatus(shard_id, {}, shard);
}

bool ShardStore::UpdateIfStatus(const std::string& shard_id,
                                const std::vector<int32_t>& expect_statuses,
                                const ShardRecord& shard)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "UPDATE shards SET job_id='" << g.Escape(shard.job_id)
        << "',shard_index=" << shard.shard_index
        << ",start_ms=" << shard.start_ms
        << ",duration_ms=" << shard.duration_ms
        << ",status=" << shard.status
        << ",assigned_worker='" << g.Escape(shard.assigned_worker_id)
        << "',attempt_id='" << g.Escape(shard.attempt_id)
        << "',retry_count=" << shard.retry_count
        << ",max_retry=" << shard.max_retry
        << ",input_path='" << g.Escape(shard.input_path)
        << "',output_path='" << g.Escape(shard.output_path)
        << "',target_resolution='" << g.Escape(shard.target_resolution)
        << "',target_bitrate=" << shard.target_bitrate
        << ",created_at=" << shard.created_at
        << ",updated_at=" << shard.updated_at
        << " WHERE shard_id='" << g.Escape(shard_id) << "'";
    if (!expect_statuses.empty())
    {
        sql << " AND status IN (";
        for (size_t i = 0; i < expect_statuses.size(); ++i)
        {
            if (i > 0) sql << ",";
            sql << expect_statuses[i];
        }
        sql << ")";
    }
    if (!g.Execute(sql.str())) {
        LOG_ERROR("ShardStore::Update failed (shard=%s): %s",
                  shard_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() > 0;
}

bool ShardStore::Delete(const std::string& shard_id)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::string sql = "DELETE FROM shards WHERE shard_id='" + g.Escape(shard_id) + "'";
    if (!g.Execute(sql)) {
        LOG_ERROR("ShardStore::Delete failed (shard=%s): %s",
                  shard_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() > 0;
}

std::optional<ShardRecord> ShardStore::Get(const std::string& shard_id)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return std::nullopt;
    std::string sql = "SELECT " + std::string(kShardColumns) +
                      " FROM shards WHERE shard_id='" + g.Escape(shard_id) + "' LIMIT 1";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("ShardStore::Get failed (shard=%s): %s",
                  shard_id.c_str(), g.Error().c_str());
        return std::nullopt;
    }
    if (rows.empty()) return std::nullopt;
    return ParseShardRow(rows[0]);
}

std::vector<ShardRecord> ShardStore::ListByJob(const std::string& job_id) const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::vector<ShardRecord> result;
    if (!g.ok()) return result;
    std::string sql = "SELECT " + std::string(kShardColumns) +
                      " FROM shards WHERE job_id='" + g.Escape(job_id) + "'";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("ShardStore::ListByJob failed (job=%s): %s",
                  job_id.c_str(), g.Error().c_str());
        return result;
    }
    result.reserve(rows.size());
    for (const auto& r : rows) result.push_back(ParseShardRow(r));
    return result;
}

std::vector<ShardRecord> ShardStore::ListByWorker(const std::string& worker_id) const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::vector<ShardRecord> result;
    if (!g.ok()) return result;
    std::string sql = "SELECT " + std::string(kShardColumns) +
                      " FROM shards WHERE assigned_worker='" + g.Escape(worker_id) + "'";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("ShardStore::ListByWorker failed (worker=%s): %s",
                  worker_id.c_str(), g.Error().c_str());
        return result;
    }
    result.reserve(rows.size());
    for (const auto& r : rows) result.push_back(ParseShardRow(r));
    return result;
}

bool ShardStore::InsertOrUpdate(const ShardRecord& shard)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    // INSERT ... ON DUPLICATE KEY UPDATE：MySQL 服务端原子完成 upsert
    std::ostringstream sql;
    sql << "INSERT INTO shards (" << kShardColumns << ") VALUES ('"
        << g.Escape(shard.shard_id) << "','" << g.Escape(shard.job_id) << "',"
        << shard.shard_index << "," << shard.start_ms << "," << shard.duration_ms << ","
        << shard.status << ",'" << g.Escape(shard.assigned_worker_id) << "','"
        << g.Escape(shard.attempt_id) << "'," << shard.retry_count << "," << shard.max_retry
        << ",'" << g.Escape(shard.input_path) << "','" << g.Escape(shard.output_path) << "','"
        << g.Escape(shard.target_resolution) << "',"
        << shard.target_bitrate << "," << shard.created_at << "," << shard.updated_at << ") "
        << "ON DUPLICATE KEY UPDATE job_id=VALUES(job_id),shard_index=VALUES(shard_index),"
        << "start_ms=VALUES(start_ms),duration_ms=VALUES(duration_ms),status=VALUES(status),"
        << "assigned_worker=VALUES(assigned_worker),attempt_id=VALUES(attempt_id),"
        << "retry_count=VALUES(retry_count),max_retry=VALUES(max_retry),"
        << "input_path=VALUES(input_path),output_path=VALUES(output_path),"
        << "target_resolution=VALUES(target_resolution),"
        << "target_bitrate=VALUES(target_bitrate),created_at=VALUES(created_at),"
        << "updated_at=VALUES(updated_at)";
    if (!g.Execute(sql.str())) {
        LOG_ERROR("ShardStore::InsertOrUpdate failed (shard=%s): %s",
                  shard.shard_id.c_str(), g.Error().c_str());
        return false;
    }
    // affected==1 → 新插入（true）；==2 或 ==0（值未变化）→ 覆盖（false）
    return g.AffectedRows() == 1;
}

std::vector<ShardRecord> ShardStore::ListByStatus(int32_t status_filter) const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::vector<ShardRecord> result;
    if (!g.ok()) return result;
    std::string sql = "SELECT " + std::string(kShardColumns) + " FROM shards";
    if (status_filter >= 0)
        sql += " WHERE status=" + std::to_string(status_filter);
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("ShardStore::ListByStatus failed: %s", g.Error().c_str());
        return result;
    }
    result.reserve(rows.size());
    for (const auto& r : rows) result.push_back(ParseShardRow(r));
    return result;
}

std::vector<ShardRecord> ShardStore::ListAll() const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::vector<ShardRecord> result;
    if (!g.ok()) return result;
    std::string sql = "SELECT " + std::string(kShardColumns) + " FROM shards";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("ShardStore::ListAll failed: %s", g.Error().c_str());
        return result;
    }
    result.reserve(rows.size());
    for (const auto& r : rows) result.push_back(ParseShardRow(r));
    return result;
}

size_t ShardStore::Count() const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return 0;
    std::vector<std::vector<std::string>> rows;
    if (!g.Query("SELECT COUNT(*) FROM shards", rows) || rows.empty()) return 0;
    return static_cast<size_t>(std::stoll(rows[0][0]));
}

std::map<int32_t, size_t> ShardStore::CountByStatus() const
{
    // 阶段 11：可观测性采样用。单条 GROUP BY 查询一次拿到全部分布，
    // 替代多次 ListByStatus 全行拉取（5s 一次，MySQL 往返成本低）
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::map<int32_t, size_t> result;
    if (!g.ok()) return result;
    std::vector<std::vector<std::string>> rows;
    // shards.status 为 INT（ShardStatus 枚举值）
    if (!g.Query("SELECT status, COUNT(*) FROM shards GROUP BY status", rows))
    {
        LOG_ERROR("ShardStore::CountByStatus failed: %s", g.Error().c_str());
        return result;
    }
    for (const auto& r : rows)
    {
        if (r.size() < 2) continue;
        result[std::stoi(r[0])] = static_cast<size_t>(std::stoll(r[1]));
    }
    return result;
}

// ============================================================================
// WorkerStore — Worker 数据存储（MySQL 持久化）
// ============================================================================

WorkerStore& WorkerStore::GetInstance()
{
    static WorkerStore instance;
    return instance;
}

bool WorkerStore::Insert(const WorkerRecord& worker)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "INSERT INTO workers (" << kWorkerColumns << ") VALUES ('"
        << g.Escape(worker.worker_id) << "','" << g.Escape(worker.ip) << "',"
        << worker.port << "," << worker.cpu_cores << "," << worker.memory_mb << ","
        << worker.gpu_count << "," << worker.current_running_shards << ","
        << worker.max_running_shards << "," << worker.cpu_usage << ","
        << worker.memory_usage << "," << worker.status << "," << worker.last_heartbeat << ")";
    if (!g.Execute(sql.str())) {
        if (g.Errno() == 1062) return false;
        LOG_ERROR("WorkerStore::Insert failed (worker=%s): %s",
                  worker.worker_id.c_str(), g.Error().c_str());
        return false;
    }
    return true;
}

bool WorkerStore::Update(const std::string& worker_id, const WorkerRecord& worker)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "UPDATE workers SET ip='" << g.Escape(worker.ip)
        << "',port=" << worker.port
        << ",cpu_cores=" << worker.cpu_cores
        << ",memory_mb=" << worker.memory_mb
        << ",gpu_count=" << worker.gpu_count
        << ",current_running_shards=" << worker.current_running_shards
        << ",max_running_shards=" << worker.max_running_shards
        << ",cpu_usage=" << worker.cpu_usage
        << ",memory_usage=" << worker.memory_usage
        << ",status=" << worker.status
        << ",last_heartbeat=" << worker.last_heartbeat
        << " WHERE worker_id='" << g.Escape(worker_id) << "'";
    if (!g.Execute(sql.str())) {
        LOG_ERROR("WorkerStore::Update failed (worker=%s): %s",
                  worker_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() > 0;
}

bool WorkerStore::Delete(const std::string& worker_id)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::string sql = "DELETE FROM workers WHERE worker_id='" + g.Escape(worker_id) + "'";
    if (!g.Execute(sql)) {
        LOG_ERROR("WorkerStore::Delete failed (worker=%s): %s",
                  worker_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() > 0;
}

std::optional<WorkerRecord> WorkerStore::Get(const std::string& worker_id)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return std::nullopt;
    std::string sql = "SELECT " + std::string(kWorkerColumns) +
                      " FROM workers WHERE worker_id='" + g.Escape(worker_id) + "' LIMIT 1";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("WorkerStore::Get failed (worker=%s): %s",
                  worker_id.c_str(), g.Error().c_str());
        return std::nullopt;
    }
    if (rows.empty()) return std::nullopt;
    return ParseWorkerRow(rows[0]);
}

bool WorkerStore::UpdateHeartbeat(const std::string& worker_id, int32_t running_shards,
                                  int32_t cpu_usage, int32_t memory_usage)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "UPDATE workers SET current_running_shards=" << running_shards
        << ",cpu_usage=" << cpu_usage
        << ",memory_usage=" << memory_usage
        << ",last_heartbeat=" << NowMs()
        << ",status=" << static_cast<int32_t>(WorkerStatus::WORKER_ONLINE)
        << " WHERE worker_id='" << g.Escape(worker_id) << "'"
        << " AND status<>" << static_cast<int32_t>(WorkerStatus::WORKER_OFFLINE);
    if (!g.Execute(sql.str())) {
        LOG_ERROR("WorkerStore::UpdateHeartbeat failed (worker=%s): %s",
                  worker_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() > 0;
}

bool WorkerStore::MarkOfflineIfTimeout(const std::string& worker_id, int64_t now, int64_t timeout_ms)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    // 单条条件 UPDATE：仅当仍 ONLINE 且心跳确实超时才生效，
    // 与 Heartbeat 的 UpdateHeartbeat 由数据库行级原子性互斥
    std::ostringstream sql;
    sql << "UPDATE workers SET status=" << static_cast<int32_t>(WorkerStatus::WORKER_OFFLINE)
        << " WHERE worker_id='" << g.Escape(worker_id) << "'"
        << " AND status=" << static_cast<int32_t>(WorkerStatus::WORKER_ONLINE)
        << " AND last_heartbeat < " << (now - timeout_ms);
    if (!g.Execute(sql.str())) {
        LOG_ERROR("WorkerStore::MarkOfflineIfTimeout failed (worker=%s): %s",
                  worker_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() > 0;
}

bool WorkerStore::InsertOrUpdate(const WorkerRecord& worker)
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return false;
    std::ostringstream sql;
    sql << "INSERT INTO workers (" << kWorkerColumns << ") VALUES ('"
        << g.Escape(worker.worker_id) << "','" << g.Escape(worker.ip) << "',"
        << worker.port << "," << worker.cpu_cores << "," << worker.memory_mb << ","
        << worker.gpu_count << "," << worker.current_running_shards << ","
        << worker.max_running_shards << "," << worker.cpu_usage << ","
        << worker.memory_usage << "," << worker.status << "," << worker.last_heartbeat << ") "
        << "ON DUPLICATE KEY UPDATE ip=VALUES(ip),port=VALUES(port),"
        << "cpu_cores=VALUES(cpu_cores),memory_mb=VALUES(memory_mb),"
        << "gpu_count=VALUES(gpu_count),current_running_shards=VALUES(current_running_shards),"
        << "max_running_shards=VALUES(max_running_shards),cpu_usage=VALUES(cpu_usage),"
        << "memory_usage=VALUES(memory_usage),status=VALUES(status),"
        << "last_heartbeat=VALUES(last_heartbeat)";
    if (!g.Execute(sql.str())) {
        LOG_ERROR("WorkerStore::InsertOrUpdate failed (worker=%s): %s",
                  worker.worker_id.c_str(), g.Error().c_str());
        return false;
    }
    return g.AffectedRows() == 1;   // true = 新插入，false = 覆盖
}

std::vector<WorkerRecord> WorkerStore::ListByStatus(int32_t status_filter) const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::vector<WorkerRecord> result;
    if (!g.ok()) return result;
    std::string sql = "SELECT " + std::string(kWorkerColumns) + " FROM workers";
    // status_filter < 0 表示"不过滤，全部返回"
    if (status_filter >= 0)
        sql += " WHERE status=" + std::to_string(status_filter);
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("WorkerStore::ListByStatus failed: %s", g.Error().c_str());
        return result;
    }
    result.reserve(rows.size());
    for (const auto& r : rows) result.push_back(ParseWorkerRow(r));
    return result;
}

std::vector<WorkerRecord> WorkerStore::ListAll() const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    std::vector<WorkerRecord> result;
    if (!g.ok()) return result;
    std::string sql = "SELECT " + std::string(kWorkerColumns) + " FROM workers";
    std::vector<std::vector<std::string>> rows;
    if (!g.Query(sql, rows)) {
        LOG_ERROR("WorkerStore::ListAll failed: %s", g.Error().c_str());
        return result;
    }
    result.reserve(rows.size());
    for (const auto& r : rows) result.push_back(ParseWorkerRow(r));
    return result;
}

size_t WorkerStore::Count() const
{
    MysqlConnectionGuard g(MysqlPool::GetInstance());
    if (!g.ok()) return 0;
    std::vector<std::vector<std::string>> rows;
    if (!g.Query("SELECT COUNT(*) FROM workers", rows) || rows.empty()) return 0;
    return static_cast<size_t>(std::stoll(rows[0][0]));
}

} // namespace video_platform
