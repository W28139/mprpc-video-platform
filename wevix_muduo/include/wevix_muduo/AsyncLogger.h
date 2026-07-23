#pragma once

#include <string>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <memory>
#include <cstdio>
#include <cstdint>

namespace wevix_muduo {

// 日志级别
enum class LogLevel : uint8_t {
    DEBUG = 0,
    INFO  = 1,
    WARN  = 2,
    ERROR = 3,
    FATAL = 4,
};

// 默认日志目录
constexpr const char* kDefaultLogDir = "./program_log";

/**
 * @brief 异步日志系统
 *
 * 架构：
 *   业务线程                        后台写线程
 *   LOG_INFO ──► append() ──► queue ──► writerThread() ──► 控制台 + 文件
 *   LOG_FATAL ──► 同步写 stderr + queue ──► abort()
 *
 * 设计要点：
 *   - 异步写入：业务线程完成格式化、入队后立即返回，不阻塞 I/O
 *   - 日期轮转：按天自动切换日志文件，保持文件打开避免频繁 open/close
 *   - Release 裁剪：LOG_DEBUG 在 -DNDEBUG 下编译为空操作，零开销
 *   - FATAL 保证：LOG_FATAL 先同步写 stderr 再 abort，保证崩溃前消息不丢
 *
 * 输出格式：
 *   [2026-07-23 18:30:05.123][T12345][INFO ][file.cpp:42] func() - message
 */
class AsyncLogger {
public:
    static AsyncLogger& GetInstance();

    // 初始化（main() 启动时调用一次，自动创建日志目录）
    void init(const std::string& logDir = kDefaultLogDir,
              LogLevel minLevel = LogLevel::INFO,
              bool consoleOutput = true);

    // 异步追加日志（由 LOG_INFO / LOG_WARN / LOG_ERROR / LOG_DEBUG 调用）
    void append(LogLevel level, const char* file, int line,
                const char* func, std::string msg);

    // 同步写 FATAL 日志到 stderr（绕过队列，保证崩溃前刷出）
    void fatal(const char* file, int line, const char* func, std::string msg);

    // 阻塞等待队列清空并刷盘
    void flush();

    // 获取当前最低输出级别（供宏做前置过滤）
    LogLevel level() const { return minLevel_.load(std::memory_order_relaxed); }

    // 优雅关闭（main() 返回前调用）
    void stop();

private:
    AsyncLogger() = default;
    ~AsyncLogger();

    AsyncLogger(const AsyncLogger&) = delete;
    AsyncLogger& operator=(const AsyncLogger&) = delete;

    void writerThread();
    void openLogFile();
    static void makeDir(const std::string& path);
    static const char* levelToStr(LogLevel level);
    static std::string formatLine(LogLevel level, const char* file, int line,
                                  const char* func, std::string msg);

    struct LogLine { std::string text; };

    std::atomic<bool> running_{false};
    std::atomic<LogLevel> minLevel_{LogLevel::INFO};
    std::atomic<bool> consoleOutput_{true};
    std::string logDir_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<LogLine> buffer_;

    std::unique_ptr<std::thread> writer_;
    struct FileCloser { void operator()(std::FILE* fp) const { if (fp) std::fclose(fp); } };
    std::unique_ptr<std::FILE, FileCloser> logFile_;
    int currentDay_{0};

    // 时间戳缓存（避免每条日志都调 localtime_r）
    mutable std::mutex timeMutex_;
    time_t cachedSecond_{0};
    char cachedDate_[64]{};   // "2026-07-23 18:30:05"
};

// ============================================================================
// 日志宏
// ============================================================================

// LOG_DEBUG: Debug 构建有效，Release（-DNDEBUG）下编译为空操作
#ifndef NDEBUG
#define LOG_DEBUG(fmt, ...) \
    do { \
        auto& _logger = ::wevix_muduo::AsyncLogger::GetInstance(); \
        if (_logger.level() <= ::wevix_muduo::LogLevel::DEBUG) { \
            char _buf[4096]; \
            ::snprintf(_buf, sizeof(_buf), fmt, ##__VA_ARGS__); \
            _logger.append(::wevix_muduo::LogLevel::DEBUG, \
                           __FILE__, __LINE__, __func__, _buf); \
        } \
    } while (0)
#else
#define LOG_DEBUG(fmt, ...) ((void)0)
#endif

#define LOG_INFO(fmt, ...) \
    do { \
        auto& _logger = ::wevix_muduo::AsyncLogger::GetInstance(); \
        if (_logger.level() <= ::wevix_muduo::LogLevel::INFO) { \
            char _buf[4096]; \
            ::snprintf(_buf, sizeof(_buf), fmt, ##__VA_ARGS__); \
            _logger.append(::wevix_muduo::LogLevel::INFO, \
                           __FILE__, __LINE__, __func__, _buf); \
        } \
    } while (0)

#define LOG_WARN(fmt, ...) \
    do { \
        auto& _logger = ::wevix_muduo::AsyncLogger::GetInstance(); \
        if (_logger.level() <= ::wevix_muduo::LogLevel::WARN) { \
            char _buf[4096]; \
            ::snprintf(_buf, sizeof(_buf), fmt, ##__VA_ARGS__); \
            _logger.append(::wevix_muduo::LogLevel::WARN, \
                           __FILE__, __LINE__, __func__, _buf); \
        } \
    } while (0)

#define LOG_ERROR(fmt, ...) \
    do { \
        auto& _logger = ::wevix_muduo::AsyncLogger::GetInstance(); \
        if (_logger.level() <= ::wevix_muduo::LogLevel::ERROR) { \
            char _buf[4096]; \
            ::snprintf(_buf, sizeof(_buf), fmt, ##__VA_ARGS__); \
            _logger.append(::wevix_muduo::LogLevel::ERROR, \
                           __FILE__, __LINE__, __func__, _buf); \
        } \
    } while (0)

#define LOG_FATAL(fmt, ...) \
    do { \
        auto& _logger = ::wevix_muduo::AsyncLogger::GetInstance(); \
        char _buf[4096]; \
        ::snprintf(_buf, sizeof(_buf), fmt, ##__VA_ARGS__); \
        _logger.fatal(__FILE__, __LINE__, __func__, _buf); \
        std::abort(); \
    } while (0)

// 兼容 mprpc 旧宏
#define LOG_ERR(fmt, ...) LOG_ERROR(fmt, ##__VA_ARGS__)

} // namespace wevix_muduo
