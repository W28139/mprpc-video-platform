#include "video_platform/mysql_pool.h"

#include "mprpcapplication.h"
#include "wevix_muduo/AsyncLogger.h"

#include <cstring>

namespace video_platform {

// ⚠️ 以下 DDL 必须与 video_platform/sql/schema.sql 保持一致
static const char* kCreateTables[] = {
    "CREATE TABLE IF NOT EXISTS jobs ("
        "job_id VARCHAR(64) NOT NULL, user_id VARCHAR(64) NOT NULL DEFAULT '', "
        "input_path VARCHAR(512) NOT NULL DEFAULT '', output_path VARCHAR(512) NOT NULL DEFAULT '', "
        "target_format VARCHAR(16) NOT NULL DEFAULT '', target_resolution VARCHAR(16) NOT NULL DEFAULT '', "
        "target_bitrate INT NOT NULL DEFAULT 0, duration_sec BIGINT NOT NULL DEFAULT 0, "
        "priority INT NOT NULL DEFAULT 0, status INT NOT NULL DEFAULT 0, shard_count INT NOT NULL DEFAULT 0, "
        "shard_duration_sec INT NOT NULL DEFAULT 0, created_at BIGINT NOT NULL DEFAULT 0, "
        "updated_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (job_id), "
        "INDEX idx_status (status), INDEX idx_user (user_id)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",

    "CREATE TABLE IF NOT EXISTS shards ("
        "shard_id VARCHAR(128) NOT NULL, job_id VARCHAR(64) NOT NULL DEFAULT '', "
        "shard_index INT NOT NULL DEFAULT 0, start_ms BIGINT NOT NULL DEFAULT 0, "
        "duration_ms BIGINT NOT NULL DEFAULT 0, status INT NOT NULL DEFAULT 0, "
        "assigned_worker VARCHAR(64) NOT NULL DEFAULT '', attempt_id VARCHAR(128) NOT NULL DEFAULT '', "
        "retry_count INT NOT NULL DEFAULT 0, max_retry INT NOT NULL DEFAULT 3, "
        "input_path VARCHAR(512) NOT NULL DEFAULT '', output_path VARCHAR(512) NOT NULL DEFAULT '', "
        "screenshot_path VARCHAR(512) NOT NULL DEFAULT '', target_resolution VARCHAR(16) NOT NULL DEFAULT '', "
        "target_bitrate INT NOT NULL DEFAULT 0, created_at BIGINT NOT NULL DEFAULT 0, "
        "updated_at BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (shard_id), "
        "INDEX idx_job (job_id), INDEX idx_worker (assigned_worker), INDEX idx_status (status)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",

    "CREATE TABLE IF NOT EXISTS workers ("
        "worker_id VARCHAR(64) NOT NULL, ip VARCHAR(45) NOT NULL DEFAULT '', "
        "port INT NOT NULL DEFAULT 0, cpu_cores INT NOT NULL DEFAULT 0, memory_mb INT NOT NULL DEFAULT 0, "
        "gpu_count INT NOT NULL DEFAULT 0, current_running_shards INT NOT NULL DEFAULT 0, "
        "max_running_shards INT NOT NULL DEFAULT 0, cpu_usage INT NOT NULL DEFAULT 0, "
        "memory_usage INT NOT NULL DEFAULT 0, status INT NOT NULL DEFAULT 0, "
        "last_heartbeat BIGINT NOT NULL DEFAULT 0, PRIMARY KEY (worker_id), "
        "INDEX idx_status (status)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
};

// ============================================================================
// MysqlPool
// ============================================================================

MysqlPool& MysqlPool::GetInstance()
{
    static MysqlPool instance;   // C++11 保证线程安全的静态局部变量初始化
    return instance;
}

MYSQL* MysqlPool::CreateConnection()
{
    MYSQL* conn = mysql_init(nullptr);
    if (!conn) return nullptr;

    // utf8mb4：路径/用户名等字段可能包含中文
    mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8mb4");

    if (!mysql_real_connect(conn, host_.c_str(), user_.c_str(), password_.c_str(),
                            dbname_.c_str(), port_, nullptr, CLIENT_FOUND_ROWS)) {
        LOG_ERROR("MysqlPool: connect to %s:%d db=%s failed: %s",
                  host_.c_str(), port_, dbname_.c_str(), mysql_error(conn));
        mysql_close(conn);
        return nullptr;
    }
    return conn;
}

bool MysqlPool::Init()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (inited_) return true;

    auto& cfg = MprpcApplication::GetConfig();
    host_     = cfg.Load("mysqlhost");
    if (host_.empty()) host_ = "127.0.0.1";
    port_     = cfg.LoadInt("mysqlport", 3306, 1, 65535);
    user_     = cfg.Load("mysqluser");
    password_ = cfg.Load("mysqlpassword");
    dbname_   = cfg.Load("mysqldbname");
    if (dbname_.empty()) dbname_ = "video_platform";
    pool_size_ = cfg.LoadInt("mysql_pool_size", 4, 1, 64);
    if (user_.empty()) {
        LOG_ERROR("MysqlPool: mysqluser not configured");
        return false;
    }

    // 预创建全部连接
    for (int i = 0; i < pool_size_; ++i) {
        MYSQL* conn = CreateConnection();
        if (!conn) {
            LOG_ERROR("MysqlPool: failed to pre-create connection %d/%d, abort init",
                      i + 1, pool_size_);
            return false;
        }
        idle_.push_back(conn);
    }

    // 自动建表（幂等），用刚创建的一条连接执行
    MYSQL* conn = idle_.back();
    for (const char* ddl : kCreateTables) {
        if (mysql_query(conn, ddl) != 0) {
            LOG_ERROR("MysqlPool: auto create table failed: %s", mysql_error(conn));
            return false;
        }
    }

    inited_ = true;
    LOG_INFO("MysqlPool initialized: %s:%d db=%s pool_size=%d",
             host_.c_str(), port_, dbname_.c_str(), pool_size_);
    return true;
}

bool MysqlPool::inited() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return inited_;
}

MYSQL* MysqlPool::Acquire(int64_t wait_ms)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (!inited_) return nullptr;
    if (idle_.empty()) {
        cv_.wait_for(lock, std::chrono::milliseconds(wait_ms),
                     [this] { return !idle_.empty(); });
        if (idle_.empty()) return nullptr;   // 借用超时
    }
    MYSQL* conn = idle_.back();
    idle_.pop_back();
    lock.unlock();   // ping/重建是耗时操作，不持池锁

    if (mysql_ping(conn) != 0) {
        // 服务端已回收的死连接：销毁重建
        LOG_WARN("MysqlPool: connection dead (%s), reconnecting",
                 mysql_error(conn));
        mysql_close(conn);
        conn = CreateConnection();
    }
    return conn;
}

void MysqlPool::Release(MYSQL* conn)
{
    if (!conn) return;
    std::lock_guard<std::mutex> lock(mutex_);
    idle_.push_back(conn);
    cv_.notify_one();
}

// ============================================================================
// MysqlConnectionGuard
// ============================================================================

MysqlConnectionGuard::MysqlConnectionGuard(MysqlPool& pool, int64_t wait_ms)
    : pool_(pool)
{
    conn_ = pool_.Acquire(wait_ms);
    if (!conn_) {
        last_error_ = "acquire mysql connection timeout";
        LOG_WARN("MysqlConnectionGuard: %s", last_error_.c_str());
    }
}

MysqlConnectionGuard::~MysqlConnectionGuard()
{
    pool_.Release(conn_);
}

bool MysqlConnectionGuard::Execute(const std::string& sql)
{
    if (!conn_) return false;
    last_errno_ = 0;
    last_error_.clear();
    if (mysql_query(conn_, sql.c_str()) != 0) {
        last_errno_  = mysql_errno(conn_);
        last_error_  = mysql_error(conn_);
        return false;
    }
    return true;
}

bool MysqlConnectionGuard::Query(const std::string& sql,
                                 std::vector<std::vector<std::string>>& rows)
{
    rows.clear();
    if (!conn_) return false;
    last_errno_ = 0;
    last_error_.clear();
    if (mysql_query(conn_, sql.c_str()) != 0) {
        last_errno_ = mysql_errno(conn_);
        last_error_ = mysql_error(conn_);
        return false;
    }
    MYSQL_RES* res = mysql_store_result(conn_);
    if (!res) {
        if (mysql_errno(conn_) == 0) return true;   // 非 SELECT（无结果集）也视为成功
        last_errno_ = mysql_errno(conn_);
        last_error_ = mysql_error(conn_);
        return false;
    }
    unsigned int cols = mysql_num_fields(res);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
        unsigned long* lens = mysql_fetch_lengths(res);
        std::vector<std::string> r;
        r.reserve(cols);
        for (unsigned int i = 0; i < cols; ++i) {
            r.emplace_back(row[i] ? std::string(row[i], lens[i]) : std::string());
        }
        rows.push_back(std::move(r));
    }
    mysql_free_result(res);
    return true;
}

int64_t MysqlConnectionGuard::AffectedRows() const
{
    return conn_ ? static_cast<int64_t>(mysql_affected_rows(conn_)) : 0;
}

std::string MysqlConnectionGuard::Escape(const std::string& s)
{
    if (!conn_) return s;
    std::string out(2 * s.size() + 1, '\0');
    unsigned long len = mysql_real_escape_string(conn_, &out[0], s.data(), s.size());
    out.resize(len);
    return out;
}

} // namespace video_platform
