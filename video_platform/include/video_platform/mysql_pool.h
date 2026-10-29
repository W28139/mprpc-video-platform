#pragma once

#include <mysql/mysql.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace video_platform {

// MysqlPool — MySQL 连接池（libmysqlclient C API 封装）。
// 固定大小连接池：预创建全部连接，借用/归还是线程安全。
// 借出时 mysql_ping 保活，死连接销毁重建。空闲队列空时最多等待 wait_ms。
// 配置项：mysqlhost / mysqlport / mysqluser / mysqlpassword / mysqldbname / mysql_pool_size

/// MySQL 连接池（单例，线程安全）
class MysqlPool {
public:
    static MysqlPool& GetInstance();

    /// 预创建连接 + 自动建表。失败返回 false，调用方应拒绝启动
    bool Init();

    bool inited() const;

    /// 借用一个连接，空闲队列空时阻塞等待 wait_ms
    MYSQL* Acquire(int64_t wait_ms = 1000);

    /// 归还连接
    void Release(MYSQL* conn);

private:
    MysqlPool() = default;
    ~MysqlPool() = default;
    MysqlPool(const MysqlPool&) = delete;
    MysqlPool& operator=(const MysqlPool&) = delete;

    MYSQL* CreateConnection();  // 创建新连接（含 utf8mb4 字符集）

    std::string host_    = "127.0.0.1";
    int         port_    = 3306;
    std::string user_;
    std::string password_;
    std::string dbname_  = "video_platform";
    int         pool_size_ = 4;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<MYSQL*> idle_;
    bool inited_ = false;
};

/// RAII 连接借用：构造时 Acquire，析构时 Release
class MysqlConnectionGuard {
public:
    explicit MysqlConnectionGuard(MysqlPool& pool, int64_t wait_ms = 1000);
    ~MysqlConnectionGuard();
    MysqlConnectionGuard(const MysqlConnectionGuard&) = delete;
    MysqlConnectionGuard& operator=(const MysqlConnectionGuard&) = delete;

    bool ok() const { return conn_ != nullptr; }

    /// 执行 DML/DDL
    bool Execute(const std::string& sql);

    /// 执行 SELECT，所有行以字符串形式填入 rows
    bool Query(const std::string& sql, std::vector<std::vector<std::string>>& rows);

    /// 匹配行数（CLIENT_FOUND_ROWS 下值未变化也算 1 行）
    int64_t AffectedRows() const;

    int Errno() const { return last_errno_; }
    const std::string& Error() const { return last_error_; }

    /// SQL 字符串转义
    std::string Escape(const std::string& s);

private:
    MysqlPool& pool_;
    MYSQL*     conn_ = nullptr;
    int        last_errno_ = 0;
    std::string last_error_;
};

} // namespace video_platform