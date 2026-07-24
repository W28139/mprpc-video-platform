#pragma once

#include <sys/epoll.h>
#include <vector>
#include "wevix_muduo/Noncopyable.h"

namespace wevix_muduo
{

class Channel;

/**
 * @brief 封装 epoll 系统调用，管理所有 Channel
 */
class Epoll : private Noncopyable
{
public:
    using ChannelList = std::vector<Channel*>;

    Epoll();
    ~Epoll();

    // 等待事件发生，返回活跃的 Channel 列表
    void poll(int timeoutMs, ChannelList* activeChannels);

    // 修改/添加监听的 Channel
    void updateChannel(Channel* ch);

    // 从监视队列中删除 Channel
    void removeChannel(Channel* ch);

private:
    static const int kMaxEvents = 4096;

    int epollfd_;
    struct epoll_event events_[kMaxEvents];
};

} // namespace wevix_muduo