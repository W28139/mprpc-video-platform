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

enum class LogLevel : uint8_t {
    DEBUG = 0,
    INFO  = 1,
    WARN  = 2,
    ERROR = 3,
    FATAL = 4,
};

constexpr const char* kDefaultLogDir = "./program_log";

// 异步日志系统：业务线程格式化入队 → 后台写线程落盘 + 控制台。
// 按天自动轮转，Release 构建 LOG_DEBUG 编译为零开销。
// 输出格式：[2026-07-23 18:30:05.123][T12345][INFO ][file.cpp:42] func() - message

class AsyncLogger {
public:
    static AsyncLogger& GetInstance();

    void init(const std::string& logDir = kDefaultLogDir,
              LogLevel minLevel = LogLevel::INFO,
              bool consoleOutput = true);

    void append(LogLevel level, const char* file, int line,
                const char* func, std::string msg);

    /// 同步写 FATAL 到 stderr（绕过队列，保证崩溃前刷出）
    void fatal(const char* file, int line, const char* func, std::string msg);

    void flush();     // 阻塞等待队列清空并刷盘
    LogLevel level() const { return minLevel_.load(std::memory_order_relaxed); }
    void stop();      // 优雅关闭

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

    mutable std::mutex timeMutex_;
    time_t cachedSecond_{0};
    char cachedDate_[64]{};   // "2026-07-23 18:30:05"
};

// 日志宏

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

#define LOG_ERR(fmt, ...) LOG_ERROR(fmt, ##__VA_ARGS__)

} // namespace wevix_muduo