#include "wevix_muduo/EventLoop.h"
#include "wevix_muduo/Epoll.h"
#include "wevix_muduo/Channel.h"
#include "wevix_muduo/Connection.h"
#include "wevix_muduo/AsyncLogger.h"

#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cstring>

namespace wevix_muduo
{

// 匿名命名空间，用于辅助函数
namespace
{
// 获取当前线程 ID
pid_t getTid()
{
    return static_cast<pid_t>(::syscall(SYS_gettid));
}

// 创建定时器 fd
int createTimerFd(int sec)
{
    int tfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
    if (tfd < 0)
    {
        LOG_ERROR("timerfd_create failed, errno=%d", errno);
        return tfd;
    }
    struct itimerspec timeout;
    ::memset(&timeout, 0, sizeof(timeout));
    timeout.it_value.tv_sec = sec;
    timeout.it_value.tv_nsec = 0;
    if (::timerfd_settime(tfd, 0, &timeout, nullptr) < 0)
    {
        LOG_ERROR("timerfd_settime failed, fd=%d, errno=%d", tfd, errno);
    }
    return tfd;
}
} // namespace

EventLoop::EventLoop(bool isMainLoop, int timerInterval, int timeout)
    : stop_(false)
    , threadId_(0)
    , epoll_(new Epoll())
    , wakeupFd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC))
    , wakeChannel_(new Channel(this, wakeupFd_))
    , timerFd_(createTimerFd(timeout))
    , timerChannel_(new Channel(this, timerFd_))
    , timerInterval_(timerInterval)
    , timeout_(timeout)
    , isMainLoop_(isMainLoop)
{
    if (wakeupFd_ < 0)
    {
        LOG_ERROR("eventfd create failed, errno=%d", errno);
    }
    // 设置唤醒通道
    wakeChannel_->setReadCallback(std::bind(&EventLoop::handleWakeup, this));
    wakeChannel_->enableReading();

    // 设置定时器通道
    timerChannel_->setReadCallback(std::bind(&EventLoop::handleTimer, this));
    timerChannel_->enableReading();
}

EventLoop::~EventLoop()
{
    wakeChannel_->disableAll();
    wakeChannel_->remove();
    ::close(wakeupFd_);
    
    timerChannel_->disableAll();
    timerChannel_->remove();
    ::close(timerFd_);
}

void EventLoop::run()
{
    threadId_.store(getTid(), std::memory_order_relaxed);
    LOG_DEBUG("EventLoop started, tid=%d, isMainLoop=%d",
              threadId_.load(std::memory_order_relaxed), isMainLoop_);

    while (!stop_)
    {
        std::vector<Channel*> activeChannels;
        epoll_->poll(-1, &activeChannels); 

        if (activeChannels.empty())
        {
            if (epollTimeoutCallback_)
            {
                epollTimeoutCallback_(this);
            }
        }
        else
        {
            for (Channel* channel : activeChannels)
            {
                channel->handleEvent();
            }
        }

        // 处理其他线程发来的异步任务
        doPendingTasks();
    }
}

void EventLoop::stop()
{
    stop_ = true;
    // 如果在其他线程调用 stop，需要唤醒循环线程，使其从 epoll_wait 中跳出
    if (!isInLoopThread())
    {
        wakeup();
    }
}

void EventLoop::runInLoop(Functor cb)
{
    if (isInLoopThread())
    {
        cb();
    }
    else
    {
        queueInLoop(std::move(cb));
    }
}

void EventLoop::queueInLoop(Functor cb)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingTasks_.push(std::move(cb));
    }

    // 唤醒当前 Loop 线程
    wakeup();
}

void EventLoop::wakeup()
{
    uint64_t one = 1;
    ssize_t n = ::write(wakeupFd_, &one, sizeof(one));
    if (n < 0)
    {
        LOG_WARN("EventLoop::wakeup write failed, fd=%d, errno=%d", wakeupFd_, errno);
    }
}

void EventLoop::handleWakeup()
{
    uint64_t one = 1;
    ssize_t n = ::read(wakeupFd_, &one, sizeof(one));
    if (n < 0)
    {
        LOG_WARN("EventLoop::handleWakeup read failed, fd=%d, errno=%d", wakeupFd_, errno);
    }
    // 唤醒后，run 函数下方的 doPendingTasks 会被执行
}

void EventLoop::doPendingTasks()
{
    std::queue<Functor> tasks;
    {
        // 关键：为了减小锁范围，我们将队列交换到局部变量中处理
        std::lock_guard<std::mutex> lock(mutex_);
        tasks.swap(pendingTasks_);
    }

    while (!tasks.empty())
    {
        tasks.front()();
        tasks.pop();
    }
}

void EventLoop::handleTimer()
{
    // 重新设置闹钟
    struct itimerspec timeout;
    ::memset(&timeout, 0, sizeof(timeout));
    timeout.it_value.tv_sec = timerInterval_;
    ::timerfd_settime(timerFd_, 0, &timeout, nullptr);

    if (!isMainLoop_)
    {
        time_t now = ::time(nullptr);
        std::vector<ConnectionPtr> expired;

        // 检查超时连接
        {
            std::lock_guard<std::mutex> lock(connsMutex_);
            for (auto& entry : conns_)
            {
                if (entry.second->isTimeout(now, timeout_))
                {
                    expired.push_back(entry.second);
                }
            }
        }

        // 统一处理超时回调
        for (const auto& conn : expired)
        {
            int fd = conn->fd();
            LOG_INFO("EventLoop::handleTimer: connection timeout, removing fd=%d", fd);
            conn->forceClose();
            if (timerCallback_)
            {
                timerCallback_(fd);
            }
        }
    }
}

void EventLoop::newConnection(ConnectionPtr conn)
{
    std::lock_guard<std::mutex> lock(connsMutex_);
    conns_[conn->fd()] = conn;
}

void EventLoop::removeConnection(int fd)
{
    std::lock_guard<std::mutex> lock(connsMutex_);
    conns_.erase(fd);
}

bool EventLoop::isInLoopThread() const
{
    return threadId_.load(std::memory_order_relaxed) == getTid();
}

void EventLoop::updateChannel(Channel* ch)
{
    epoll_->updateChannel(ch);
}

void EventLoop::removeChannel(Channel* ch)
{
    epoll_->removeChannel(ch);
}

} // namespace wevix_muduo
