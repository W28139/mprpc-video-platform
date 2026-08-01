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

    // 可读、优先级数据、挂断
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP))
    {
        if (readCallback_) { readCallback_(); return; }
    }

    // 可写
    if (revents_ & EPOLLOUT)
    {
        if (writeCallback_) { writeCallback_(); }
    }
    // 注意：回调可能同步销毁 Channel 自身（如 handleClose → removeConnection
    // → 最后一个 shared_ptr 释放 → Connection/channel_ 析构），因此每个回调
    // 执行后必须立即 return，禁止再访问 this 的任何成员。
    // 未处理的事件（如同批次的 EPOLLOUT）由下一轮 epoll_wait 继续分发：
    // 若 Channel 已 remove，事件自然消失；若对象存活，sendInLoop 中的
    // enableWriting(EPOLL_CTL_MOD) 会保证 EPOLLOUT 重新触发，不会丢失。
}

} // namespace wevix_muduo
