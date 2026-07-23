#pragma once

#include <functional>
#include <memory>
#include <new>
#include "wevix_muduo/Noncopyable.h"
#include "wevix_muduo/memory_pool/MemoryPool.h"

namespace wevix_muduo
{

// 前向声明，减少头文件包含
class EventLoop;

/**
 * @brief Channel 类封装了 fd 以及它感兴趣的事件（EPOLLIN, EPOLLOUT...）
 * 同时也绑定了事件发生时的回调函数。
 */
class Channel : private Noncopyable
{
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel();

    // 核心接口：当 fd 发生事件时，被 EventLoop 调用
    void handleEvent();

    // 设置回调函数
    void setReadCallback(EventCallback cb) { readCallback_ = std::move(cb); }
    void setWriteCallback(EventCallback cb) { writeCallback_ = std::move(cb); }
    void setCloseCallback(EventCallback cb) { closeCallback_ = std::move(cb); }
    void setErrorCallback(EventCallback cb) { errorCallback_ = std::move(cb); }

    // 获取底层文件描述符
    int fd() const { return fd_; }

    // 获取/设置感兴趣的事件
    uint32_t events() const { return events_; }
    void setRevents(uint32_t ev) { revents_ = ev; }

    // 判断当前 Channel 是否已经在 Epoll 中
    bool isNoneEvent() const { return events_ == kNoneEvent; }
    bool inEpoll() const { return inEpoll_; }
    void setInEpoll(bool in = true) { inEpoll_ = in; }

    // 开启/关闭读写事件
    void enableReading();
    void disableReading();
    void enableWriting();
    void disableWriting();
    void disableAll();
    void remove();

    bool isWriting() const { return events_ & kWriteEvent; }
    bool isReading() const { return events_ & kReadEvent; }

    // 是否使用了边缘触发 (ET)
    void useET();

    // 使用内存池分配（高频创建/销毁，减少 malloc/free 开销）
    static void* operator new(size_t size)
    {
        void* ptr = memory_pool::MemoryPool::allocate(size);
        if (!ptr) throw std::bad_alloc();
        return ptr;
    }
    static void operator delete(void* ptr, size_t size) noexcept
    {
        memory_pool::MemoryPool::deallocate(ptr, size);
    }

private:
    // 更新到 Poller (Epoll)
    void update();

private:
    // 事件常量
    static const uint32_t kNoneEvent;
    static const uint32_t kReadEvent;
    static const uint32_t kWriteEvent;

    EventLoop* loop_; // 当前 Channel 所属的 EventLoop
    const int fd_;    // 底层 fd
    uint32_t events_;  // 注册的感兴趣事件
    uint32_t revents_; // 当前活跃的事件（由 Epoll 返回）
    bool inEpoll_;    // 是否在 Epoll 监听队列中

    // 事件回调
    EventCallback readCallback_;
    EventCallback writeCallback_;
    EventCallback closeCallback_;
    EventCallback errorCallback_;
};

} // namespace wevix_muduo