#pragma once

#include <mysql/mysql.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace video_platform {

// ============================================================================
// MysqlPool — MySQL 连接池（libmysqlclient C API 封装）
// ============================================================================
//
// 固定大小连接池：启动时预创建全部连接，运行时线程安全借用/归还。
// 为阶段 9（Store MySQL 化）提供统一的数据库访问入口。
//
// 设计要点：
// - 所有连接创建时设置 CLIENT_FOUND_ROWS 标志：UPDATE 值未变化时
//   affected_rows 仍返回匹配行数，避免"全量覆盖更新"被误判为未命中
// - 借用时 mysql_ping 保活，死连接销毁重建
// - 空闲队列空时最多等待 wait_ms，超时返回 nullptr，调用方按失败处理
// - 初始化时自动执行 CREATE TABLE IF NOT EXISTS（与 sql/schema.sql 一致，
//   部署零步骤；schema.sql 保留作为参考与手动管理入口）
//
// 配置项（从 MprpcApplication 读取，ini 风格）：
//   mysqlhost / mysqlport / mysqluser / mysqlpassword / mysqldbname / mysql_pool_size
// ============================================================================

/// @brief MySQL 连接池（单例，线程安全）
class MysqlPool {
public:
    /// @brief 获取全局唯一实例
    static MysqlPool& GetInstance();

    /// @brief 从配置初始化：预创建连接 + 自动建表（幂等）。
    /// 预创建失败返回 false，调用方应拒绝服务启动（与 ffmpeg 启动检查同策略）。
    /// 必须在 MprpcApplication::Init 之后调用。
    bool Init();

    bool inited() const;

    /// @brief 借用一个连接。空闲队列为空时最多阻塞等待 wait_ms。
    /// @return nullptr 表示未初始化 / 超时 / 重建失败
    MYSQL* Acquire(int64_t wait_ms = 1000);

    /// @brief 归还连接（thread-safe，允许传入 nullptr）
    void Release(MYSQL* conn);

private:
    MysqlPool() = default;
    ~MysqlPool() = default;
    MysqlPool(const MysqlPool&) = delete;
    MysqlPool& operator=(const MysqlPool&) = delete;

    /// @brief 创建一条新连接（含 utf8mb4 字符集设置）。失败返回 nullptr。
    MYSQL* CreateConnection();

    std::string host_    = "127.0.0.1";
    int         port_    = 3306;
    std::string user_;
    std::string password_;
    std::string dbname_  = "video_platform";
    int         pool_size_ = 4;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<MYSQL*> idle_;   ///< 空闲连接
    bool inited_ = false;
};

/// @brief RAII 连接借用：构造时 Acquire，析构时 Release。
/// 提供 Execute / Query / Escape / AffectedRows / Errno 便捷方法。
class MysqlConnectionGuard {
public:
    explicit MysqlConnectionGuard(MysqlPool& pool, int64_t wait_ms = 1000);
    ~MysqlConnectionGuard();
    MysqlConnectionGuard(const MysqlConnectionGuard&) = delete;
    MysqlConnectionGuard& operator=(const MysqlConnectionGuard&) = delete;

    bool ok() const { return conn_ != nullptr; }

    /// @brief 执行 DML/DDL。失败返回 false，可通过 Errno()/Error() 判断原因。
    bool Execute(const std::string& sql);

    /// @brief 执行 SELECT，所有行以字符串形式填入 rows。
    /// @return false 表示 SQL 失败；成功但无结果时 rows 为空且返回 true。
    bool Query(const std::string& sql, std::vector<std::vector<std::string>>& rows);

    /// @brief 最近一次执行影响的匹配行数（CLIENT_FOUND_ROWS 模式下为匹配行数，
    /// 值未变化的 UPDATE 也算 1 行）
    int64_t AffectedRows() const;

    /// @brief 最近一次执行的 MySQL 错误码（0=无错误；1062=duplicate key）
    int Errno() const { return last_errno_; }

    /// @brief 最近一次执行的错误信息
    const std::string& Error() const { return last_error_; }

    /// @brief SQL 字符串转义（防注入；连接不可用时原样返回）
    std::string Escape(const std::string& s);

private:
    MysqlPool& pool_;
    MYSQL*     conn_ = nullptr;
    int        last_errno_ = 0;
    std::string last_error_;
};

} // namespace video_platform
