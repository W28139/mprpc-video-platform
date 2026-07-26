#include "wevix_muduo/Epoll.h"
#include "wevix_muduo/Channel.h"
#include "wevix_muduo/AsyncLogger.h"

#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <errno.h>

namespace wevix_muduo
{

Epoll::Epoll()
    : events_(kInitEventListSize)
{
    // 使用 EPOLL_CLOEXEC 防止子进程继承该 fd
    epollfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epollfd_ < 0)
    {
        LOG_ERROR("epoll_create error, errno: %d", errno);
    }
}

Epoll::~Epoll()
{
    ::close(epollfd_);
}

// 主事件循环管理
void Epoll::poll(int timeoutMs, ChannelList* activeChannels)
{
    // 调用 epoll_wait
    int numEvents = ::epoll_wait(epollfd_, events_.data(),
                                 static_cast<int>(events_.size()), timeoutMs);
    int savedErrno = errno;

    if (numEvents > 0)
    {
        // 动态扩容：如果返回数等于当前容量，说明可能还有就绪事件未取出
        if (numEvents == static_cast<int>(events_.size()))
        {
            events_.resize(events_.size() * 2);
        }

        for (int i = 0; i < numEvents; ++i)
        {
            // 通过 data.ptr 拿回 Channel 对象
            Channel* channel = static_cast<Channel*>(events_[i].data.ptr);
            channel->setRevents(events_[i].events);
            // 填充给 EventLoop 供后续 handleEvent 处理
            activeChannels->push_back(channel);
        }
    }
    else if (numEvents == 0)
    {
        // timeout, do nothing
    }
    else
    {
        if (savedErrno != EINTR)
        {
            LOG_ERROR("epoll_wait error, errno: %d", savedErrno);
        }
    }
}

// 从事件循环管理
void Epoll::updateChannel(Channel* ch)
{
    struct epoll_event ev;
    ::memset(&ev, 0, sizeof(ev));
    ev.events = ch->events();
    ev.data.ptr = ch;

    int fd = ch->fd();

    if (!ch->inEpoll())
    {
        // 如果不在 epoll 树上，执行 ADD
        if (::epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &ev) < 0)
        {
            LOG_ERROR("epoll_ctl add error, fd=%d, errno=%d", fd, errno);
        }
        else
        {
            ch->setInEpoll(true);
        }
    }
    else
    {
        // 如果已经在树上，执行 MOD (或者删除，取决于 events)
        if (ch->isNoneEvent())
        {
            if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, 0) < 0)
            {
                LOG_ERROR("epoll_ctl del error, fd=%d, errno=%d", fd, errno);
            }
            ch->setInEpoll(false);
        }
        else
        {
            if (::epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &ev) < 0)
            {
                LOG_ERROR("epoll_ctl mod error, fd=%d, errno=%d", fd, errno);
            }
        }
    }
}

// 从事件循环管理
void Epoll::removeChannel(Channel* ch)
{
    int fd = ch->fd();
    if (ch->inEpoll())
    {
        if (::epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, 0) < 0)
        {
            LOG_ERROR("epoll_ctl del error, fd=%d, errno=%d", fd, errno);
        }
        ch->setInEpoll(false);
    }
}

} // namespace wevix_muduo