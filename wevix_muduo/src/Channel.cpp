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
    update();
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
        if (closeCallback_) closeCallback_();
    }

    // 错误处理
    if (revents_ & EPOLLERR)
    {
        LOG_WARN("Channel fd=%d: EPOLLERR event, revents=0x%x", fd_, revents_);
        if (errorCallback_) errorCallback_();
    }

    // 可读、优先级数据、挂断
    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP))
    {
        if (readCallback_) readCallback_();
    }

    // 可写
    if (revents_ & EPOLLOUT)
    {
        if (writeCallback_) writeCallback_();
    }
}

} // namespace wevix_muduo