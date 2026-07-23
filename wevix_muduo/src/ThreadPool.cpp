#include "wevix_muduo/ThreadPool.h"
#include "wevix_muduo/AsyncLogger.h"
#include <sys/prctl.h>
#include <unistd.h>

namespace wevix_muduo
{

std::atomic_int ThreadPool::s_generateId_{0};

ThreadPool::ThreadPool(int initThreadSize, std::string name)
    : initThreadSize_(initThreadSize)
    , threadSizeThreshold_(kDefaultThreadThreshold)
    , curThreadSize_(0)
    , idleThreadSize_(0)
    , taskSize_(0)
    , taskQueMaxThreshold_(kDefaultTaskThreshold)
    , poolMode_(PoolMode::MODE_FIXED)
    , isPoolRunning_(false)
    , poolName_(std::move(name))
{
    // 如果构造时指定了线程数，直接开启
    if (initThreadSize_ > 0)
    {
        start();
    }
}

ThreadPool::~ThreadPool()
{
    stop();
}

void ThreadPool::setMode(PoolMode mode)
{
    if (isPoolRunning_) return;
    poolMode_ = mode;
}

void ThreadPool::setTaskQueMaxThreshold(int threshold)
{
    if (isPoolRunning_) return;
    taskQueMaxThreshold_ = threshold;
}

void ThreadPool::setThreadSizeThreshold(int threshold)
{
    if (isPoolRunning_) return;
    if (poolMode_ == PoolMode::MODE_CACHED)
        threadSizeThreshold_ = threshold;
}

void ThreadPool::start()
{
    if (isPoolRunning_) return;
    isPoolRunning_ = true;

    LOG_INFO("ThreadPool [%s] starting, initThreadSize=%d", poolName_.c_str(), initThreadSize_);

    std::lock_guard<std::mutex> lock(taskQueMtx_);
    for (int i = 0; i < initThreadSize_; i++)
    {
        createThread();
    }
}

void ThreadPool::stop()
{
    bool expected = true;
    if (!isPoolRunning_.compare_exchange_strong(expected, false))
        return;

    LOG_INFO("ThreadPool [%s] stopping, curThreadSize=%d", poolName_.c_str(), curThreadSize_.load());

    {
        std::unique_lock<std::mutex> lock(taskQueMtx_);
        notEmpty_.notify_all(); // 唤醒所有线程准备自尽
    }

    // 真正的阻塞回收所有物理线程
    for (auto& iter : threads_)
    {
        if (iter.second.joinable())
        {
            iter.second.join(); 
        }
    }
    
    threads_.clear();
    curThreadSize_ = 0;
    idleThreadSize_ = 0;
    taskSize_ = 0;
}

void ThreadPool::addTask(Task task)
{
    // 直接复用模板函数逻辑，但不关心返回值
    submitTask(std::move(task));
}

void ThreadPool::createThread()
{
    int tid = s_generateId_++;
    
    // 创建线程并立即存入容器
    threads_.emplace(tid, std::thread(std::bind(&ThreadPool::threadFunc, this, tid)));
    
    curThreadSize_++;
    idleThreadSize_++;
}

void ThreadPool::threadFunc(int threadId)
{
    // 1. 设置线程名 (Linux 特有)
    // 名字格式例如: IO_LOOP-1, WORKER-2
    std::string tName = poolName_ + "-" + std::to_string(threadId);
    ::prctl(PR_SET_NAME, tName.c_str());

    LOG_DEBUG("Thread [%s] started.", tName.c_str());

    auto lastTime = std::chrono::high_resolution_clock::now();

    while (true)
    {
        Task task;
        {
            std::unique_lock<std::mutex> lock(taskQueMtx_);

            while (taskQue_.empty())
            {
                if (!isPoolRunning_) goto THREAD_EXIT;

                if (poolMode_ == PoolMode::MODE_CACHED)
                {
                    if (std::cv_status::timeout == notEmpty_.wait_for(lock, std::chrono::seconds(1)))
                    {
                        auto now = std::chrono::high_resolution_clock::now();
                        auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);
                        if (dur.count() >= kMaxIdleTime && curThreadSize_ > initThreadSize_)
                        {
                            goto THREAD_EXIT;
                        }
                    }
                }
                else
                {
                    notEmpty_.wait(lock);
                }
            }

            idleThreadSize_--;
            task = std::move(taskQue_.front());
            taskQue_.pop();
            taskSize_--;

            if (!taskQue_.empty()) notEmpty_.notify_one();
            notFull_.notify_one();
        }

        if (task) task();

        idleThreadSize_++;
        lastTime = std::chrono::high_resolution_clock::now();
    }

THREAD_EXIT:
    // 注意：这里不需要再从 threads_ map 里删除自己，因为析构时会统一 join
    // 只需要维护好计数即可
    curThreadSize_--;
    idleThreadSize_--;
    LOG_INFO("Thread [%s] exit.", tName.c_str());
}

} // namespace wevix_muduo