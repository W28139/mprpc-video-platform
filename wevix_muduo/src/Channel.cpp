#include "wevix_muduo/Channel.h"
#include "wevix_muduo/EventLoop.h"
#include "wevix_muduo/AsyncLogger.h"

#include <sys/epoll.h>

namespace wevix_muduo
{

const uint32_t Channel::kNoneEvent = 0;
const uint32_t Channel::kReadEvent = EPOLLIN | EPOLLPRI;
const uint32_t Channel::kWriteEvent = EPOLLOUT;

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop)
    , fd_(fd)
    , events_(0)
    , revents_(0)
    , inEpoll_(false)
{
}

Channel::~Channel()
{
}

void Channel::useET()
{
    events_ |= EPOLLET;
    if (inEpoll_)
    {
        update();
    }
}

void Channel::enableReading()
{
    events_ |= kReadEvent;
    update();
}

void Channel::disableReading()
{
    events_ &= ~kReadEvent;
    update();
}

void Channel::enableWriting()
{
    events_ |= kWriteEvent;
    update();
}

void Channel::disableWriting()
{
    events_ &= ~kWriteEvent;
    update();
}

void Channel::disableAll()
{
    events_ = kNoneEvent;
    update();
}

void Channel::remove()
{
    loop_->removeChannel(this);
}

void Channel::update()
{
    // 通过所属的 EventLoop 通知 Epoll 更新该 Channel 的监听状态
    loop_->updateChannel(this);
}

void Channel::handleEvent()
{
    // 如果发生了挂断且没有读事件，则调用关闭回调
    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN))
    {
        LOG_DEBUG("Channel fd=%d: EPOLLHUP event", fd_);
        if (closeCallback_) { closeCallback_(); return; }
    }

    // 错误处理
    if (revents_ & EPOLLERR)
    {
        LOG_WARN("Channel fd=%d: EPOLLERR event, revents=0x%x", fd_, revents_);
        if (errorCallback_) { errorCallback_(); return; }
    }

    // 可写：必须先于读处理（#12 修复）。
    // ET 模式下同批 EPOLLIN|EPOLLOUT 时，若读回调先执行并 return，
    // 写回调被跳过；sendInLoop 的 enableWriting 仅在 !isWriting() 时才
    // EPOLL_CTL_MOD——已有 pending 输出时连 MOD 都不会发生，ET 下
    // EPOLLOUT 事件已被消费且无状态迁移可再触发 → 输出缓冲永久卡死、
    // 连接悬挂。先写后读：EPOLLOUT 被消费（数据发出），EPOLLIN 未处理
    // 只是延迟一轮（数据仍在内核缓冲，ET 下持续触发，不丢失）。
    if (revents_ & EPOLLOUT)
    {
        if (writeCallback_) { writeCallback_(); return; }
    }

    // 可读、优先级数据、挂断
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP))
    {
        if (readCallback_) { readCallback_(); return; }
    }
    // 注意：回调可能同步销毁 Channel 自身（如 handleClose → removeConnection
    // → 最后一个 shared_ptr 释放 → Connection/channel_ 析构），因此每个回调
    // 执行后必须立即 return，禁止再访问 this 的任何成员。
}

} // namespace wevix_muduo
