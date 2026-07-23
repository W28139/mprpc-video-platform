#include "wevix_muduo/AsyncLogger.h"

#include <sys/syscall.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>
#include <chrono>
#include <cstring>

namespace wevix_muduo {

// ============================================================================
// 单例
// ============================================================================
AsyncLogger& AsyncLogger::GetInstance() {
    static AsyncLogger logger;
    return logger;
}

AsyncLogger::~AsyncLogger() {
    stop();
}

// ============================================================================
// 工具函数
// ============================================================================

// 获取内核线程 ID
static pid_t gettid() {
    return static_cast<pid_t>(::syscall(SYS_gettid));
}

const char* AsyncLogger::levelToStr(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG: return "DEBUG";
        case LogLevel::INFO:  return "INFO ";
        case LogLevel::WARN:  return "WARN ";
        case LogLevel::ERROR: return "ERROR";
        case LogLevel::FATAL: return "FATAL";
    }
    return "UNKN ";
}

void AsyncLogger::makeDir(const std::string& path) {
    // 递归创建目录（类似 mkdir -p）
    std::string dir;
    for (size_t i = 0; i < path.size(); ++i) {
        dir += path[i];
        if (path[i] == '/' || i + 1 == path.size()) {
            if (!dir.empty() && dir != "/") {
                ::mkdir(dir.c_str(), 0755);
            }
        }
    }
}

// 格式化一条完整日志行（带时间戳缓存）
std::string AsyncLogger::formatLine(LogLevel level, const char* file, int line,
                                    const char* func, std::string msg) {
    auto& self = GetInstance();

    // 获取毫秒
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) % 1000;
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(
                   now.time_since_epoch()).count();

    // 时间戳缓存：同一秒内复用日期字符串，只更新毫秒部分
    char timestamp[128];
    {
        std::lock_guard<std::mutex> lock(self.timeMutex_);
        if (static_cast<time_t>(sec) != self.cachedSecond_) {
            auto tt = std::chrono::system_clock::to_time_t(now);
            struct tm tm_buf;
            ::localtime_r(&tt, &tm_buf);
            ::snprintf(self.cachedDate_, sizeof(self.cachedDate_),
                       "%04d-%02d-%02d %02d:%02d:%02d",
                       tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                       tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec);
            self.cachedSecond_ = static_cast<time_t>(sec);
        }
        ::snprintf(timestamp, sizeof(timestamp), "%s.%03lld",
                   self.cachedDate_, static_cast<long long>(ms.count()));
    }

    // 提取文件名（去掉路径前缀）
    const char* basename = std::strrchr(file, '/');
    basename = basename ? basename + 1 : file;

    // 组装完整日志行
    char lineBuf[2048];
    ::snprintf(lineBuf, sizeof(lineBuf),
               "[%s][T%d][%s][%s:%d] %s() - %s",
               timestamp, gettid(), levelToStr(level),
               basename, line, func, msg.c_str());

    return std::string(lineBuf);
}

// ============================================================================
// 初始化
// ============================================================================
void AsyncLogger::init(const std::string& logDir,
                       LogLevel minLevel,
                       bool consoleOutput) {
    logDir_ = logDir;

    // 自动创建日志目录
    makeDir(logDir_);

    minLevel_.store(minLevel, std::memory_order_relaxed);
    consoleOutput_.store(consoleOutput, std::memory_order_relaxed);

    running_.store(true, std::memory_order_relaxed);
    writer_.reset(new std::thread(&AsyncLogger::writerThread, this));
}

void AsyncLogger::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false))
        return;

    flush();  // 等队列清空
    cv_.notify_all();
    if (writer_ && writer_->joinable()) {
        writer_->join();
    }

    if (logFile_) {
        std::fflush(logFile_.get());
        logFile_.reset();
    }
}

// ============================================================================
// 异步追加（业务线程 → 队列）
// ============================================================================
void AsyncLogger::append(LogLevel level, const char* file, int line,
                         const char* func, std::string msg) {
    if (level < minLevel_.load(std::memory_order_relaxed))
        return;

    std::string lineText = formatLine(level, file, line, func, std::move(msg));

    {
        std::lock_guard<std::mutex> lock(mutex_);
        buffer_.push({std::move(lineText)});
    }
    cv_.notify_one();
}

// ============================================================================
// 同步写 FATAL（绕过队列，直接 stderr，保证 abort 前一定刷出）
// ============================================================================
void AsyncLogger::fatal(const char* file, int line,
                        const char* func, std::string msg) {
    // 1. 也走队列写入文件（尽力而为，可能来不及但值得尝试）
    if (LogLevel::FATAL >= minLevel_.load(std::memory_order_relaxed)) {
        std::string lineText = formatLine(LogLevel::FATAL, file, line, func, msg);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            buffer_.push({lineText});  // 副本给队列
        }
        cv_.notify_one();

        // 2. 同步写 stderr — 这是保底，保证一定可见
        std::fprintf(stderr, "%s\n", lineText.c_str());
        std::fflush(stderr);
    }
}

// ============================================================================
// 阻塞等待队列清空并刷盘
// ============================================================================
void AsyncLogger::flush() {
    // 忙等待直到队列空
    while (true) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (buffer_.empty()) break;
        }
        cv_.notify_one();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // 刷文件
    if (logFile_) {
        std::fflush(logFile_.get());
    }
}

// ============================================================================
// 后台写线程
// ============================================================================
void AsyncLogger::openLogFile() {
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf;
    ::localtime_r(&tt, &tm_buf);

    if (currentDay_ != tm_buf.tm_mday) {
        char path[256];
        ::snprintf(path, sizeof(path), "%s/%04d-%02d-%02d-log.txt",
                   logDir_.c_str(),
                   tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday);

        logFile_.reset(std::fopen(path, "a+"));
        currentDay_ = tm_buf.tm_mday;

        if (!logFile_) {
            std::fprintf(stderr, "[AsyncLogger] Failed to open log file: %s\n", path);
        }
    }
}

void AsyncLogger::writerThread() {
    while (running_.load(std::memory_order_relaxed)) {
        std::queue<LogLine> localQueue;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() {
                return !buffer_.empty() || !running_.load(std::memory_order_relaxed);
            });

            if (!running_.load(std::memory_order_relaxed) && buffer_.empty())
                break;

            localQueue.swap(buffer_);
        }

        bool useConsole = consoleOutput_.load(std::memory_order_relaxed);
        openLogFile();

        while (!localQueue.empty()) {
            const auto& item = localQueue.front();

            if (useConsole) {
                std::fprintf(stdout, "%s\n", item.text.c_str());
            }
            if (logFile_) {
                std::fprintf(logFile_.get(), "%s\n", item.text.c_str());
            }

            localQueue.pop();
        }

        if (useConsole) std::fflush(stdout);
        if (logFile_) std::fflush(logFile_.get());
    }

    if (logFile_) std::fflush(logFile_.get());
}

} // namespace wevix_muduo
