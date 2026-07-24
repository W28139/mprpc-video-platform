#pragma once

#include <functional>
#include <memory>
#include <vector>
#include <queue>
#include <mutex>
#include <atomic>
#include <map>
#include <sys/types.h>

namespace wevix_muduo
{

// 前向声明
class Epoll;
class Channel;
class Connection;

/**
 * @brief 事件循环类：一个线程一个循环 (One Loop Per Thread)
 */
class EventLoop
{
public:
    using Functor = std::function<void()>;
    using ConnectionPtr = std::shared_ptr<Connection>;

    EventLoop(bool isMainLoop, int timerInterval = 30, int timeout = 100);
    ~EventLoop();

    // 运行事件循环
    void run();
    // 停止事件循环
    void stop();

    // 在当前循环线程中执行任务
    void runInLoop(Functor cb);
    // 把任务放入队列，并在必要时唤醒循环线程执行
    void queueInLoop(Functor cb);

    // 唤醒事件循环线程 (通过 eventfd)
    void wakeup();

    // Channel 管理接口
    void updateChannel(Channel* ch);
    void removeChannel(Channel* ch);

    // 状态判断
    bool isInLoopThread() const;

    // 连接管理（针对超时处理）
    void newConnection(ConnectionPtr conn);
    void removeConnection(int fd);
    
    // 设置回调
    void setEpollTimeoutCallback(std::function<void(EventLoop*)> cb) 
    { 
        epollTimeoutCallback_ = std::move(cb); 
    }
    
    void setTimerCallback(std::function<void(int)> cb) 
    { 
        timerCallback_ = std::move(cb); 
    }

private:
    // eventfd 读回调
    void handleWakeup();
    // timerfd 读回调
    void handleTimer();
    // 执行任务队列中的任务
    void doPendingTasks();

private:
    using ChannelList = std::vector<Channel*>;

    std::atomic_bool stop_;
    std::atomic<pid_t> threadId_; // 记录该循环所属线程的 ID

    std::unique_ptr<Epoll> epoll_;
    
    // eventfd 唤醒相关
    int wakeupFd_;
    std::unique_ptr<Channel> wakeChannel_;

    // timerfd 定时器相关
    int timerFd_;
    std::unique_ptr<Channel> timerChannel_;
    int timerInterval_; // 闹钟间隔
    int timeout_;       // 连接超时阈值

    // 任务队列相关
    mutable std::mutex mutex_;
    std::queue<Functor> pendingTasks_;

    // 连接管理相关
    bool isMainLoop_;
    std::mutex connsMutex_;
    std::map<int, ConnectionPtr> conns_;

    // 回调函数
    std::function<void(EventLoop*)> epollTimeoutCallback_;
    std::function<void(int)> timerCallback_;
};

} // namespace wevix_muduo
