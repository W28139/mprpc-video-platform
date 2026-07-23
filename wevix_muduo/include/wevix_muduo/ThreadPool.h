#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <atomic>
#include <mutex>
#include <functional>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <future>
#include <string>

namespace wevix_muduo
{

// 线程池模式
enum class PoolMode
{
    MODE_FIXED,   // 固定数量线程 (常用于 IO 线程池)
    MODE_CACHED,  // 动态增长线程 (常用于 Work 业务线程池)
};

/**
 * @brief 高性能线程池
 */
class ThreadPool
{
public:
    using Task = std::function<void()>;

    // 适配 TcpServer 的构造函数
    // initThreadSize: 初始线程数
    // name: 线程名前缀，方便区分 IO 线程和 Work 线程
    explicit ThreadPool(int initThreadSize = 0, std::string name = "ThreadPool");
    
    ~ThreadPool();

    // 开启线程池
    void start();

    // 停止线程池
    void stop();

    // 设置模式 (必须在 start 之前调用)
    void setMode(PoolMode mode);

    // 设置任务队列最大容量阈值
    void setTaskQueMaxThreshold(int threshold);

    // 设置 Cached 模式下线程数量上限
    void setThreadSizeThreshold(int threshold);

    /**
     * @brief 适配 TcpServer 的接口：添加任务
     */
    void addTask(Task task);

    /**
     * @brief 高级接口：提交任务并获取 Future 返回值
     */
    template <typename Func, typename... Args>
    auto submitTask(Func&& func, Args&&... args) -> std::future<decltype(func(args...))>
    {
        using RType = decltype(func(args...));

        auto task = std::make_shared<std::packaged_task<RType()>>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...));

        std::future<RType> result = task->get_future();

        std::unique_lock<std::mutex> lock(taskQueMtx_);

        // 背压策略：如果队列满了，阻塞等待 1 秒
        if (!notFull_.wait_for(lock, std::chrono::seconds(1),
                               [&]() { return taskQue_.size() < (size_t)taskQueMaxThreshold_; }))
        {
            // 如果还是满的，简单处理：抛出异常或返回空 future
            auto emptyTask = std::make_shared<std::packaged_task<RType()>>([]() { return RType(); });
            (*emptyTask)();
            return emptyTask->get_future();
        }

        taskQue_.emplace([task]() { (*task)(); });
        taskSize_++;

        notEmpty_.notify_one(); // 唤醒一个线程即可，避免惊群

        // Cached 模式下的扩容
        if (poolMode_ == PoolMode::MODE_CACHED &&
            taskSize_ > idleThreadSize_ &&
            curThreadSize_ < threadSizeThreshold_)
        {
            createThread();
        }

        return result;
    }

private:
    // 内部线程执行函数
    void threadFunc(int threadId);
    
    // 内部创建线程接口
    void createThread();

private:
    // 线程管理
    // 使用 vector 配合 join 保证析构安全，不再使用 detach
    std::unordered_map<int, std::thread> threads_; 
    int initThreadSize_;            
    int threadSizeThreshold_;       
    std::atomic_int curThreadSize_; 
    std::atomic_int idleThreadSize_;

    // 任务队列
    std::queue<Task> taskQue_;
    std::atomic_int taskSize_;      
    int taskQueMaxThreshold_;       

    // 同步与状态
    std::mutex taskQueMtx_;
    std::condition_variable notFull_;
    std::condition_variable notEmpty_;
    std::condition_variable exitCond_; 

    PoolMode poolMode_;
    std::atomic_bool isPoolRunning_;
    std::string poolName_;

    // 静态计数器用于生成线程 ID
    static std::atomic_int s_generateId_;

    // 默认常量
    static constexpr int kDefaultTaskThreshold = 10000;
    static constexpr int kDefaultThreadThreshold = 100;
    static constexpr int kMaxIdleTime = 60; // 秒
};

} // namespace wevix_muduo