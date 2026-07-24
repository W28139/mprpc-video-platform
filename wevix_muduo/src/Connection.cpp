#include "wevix_muduo/Connection.h"
#include "wevix_muduo/Socket.h"
#include "wevix_muduo/Channel.h"
#include "wevix_muduo/EventLoop.h"
#include "wevix_muduo/AsyncLogger.h"
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/socket.h>
#include <errno.h>

namespace wevix_muduo
{

Connection::Connection(EventLoop* loop, std::unique_ptr<Socket> clientSock)
    : loop_(loop)
    , socket_(std::move(clientSock))
    , channel_(new Channel(loop_, socket_->fd()))
    , disconnected_(false)
    , lastActiveTime_(Timestamp::now())
{
    // 设置 Channel 的回调函数
    channel_->setReadCallback(std::bind(&Connection::handleRead, this));
    channel_->setWriteCallback(std::bind(&Connection::handleWrite, this));
    channel_->setCloseCallback(std::bind(&Connection::handleClose, this));
    channel_->setErrorCallback(std::bind(&Connection::handleError, this));

    LOG_DEBUG("Connection created, fd=%d", socket_->fd());
}

Connection::~Connection()
{
    LOG_DEBUG("Connection destroyed, fd=%d", socket_->fd());
}

int Connection::fd() const
{
    return socket_->fd();
}

std::string Connection::ip() const
{
    return socket_->ip();
}

uint16_t Connection::port() const
{
    return socket_->port();
}

void Connection::handleRead()
{
    int savedErrno = 0;
    ssize_t totalRead = 0;
    bool peerClosed = false;

    while (true)
    {
        ssize_t n = inputBuffer_.readFd(fd(), &savedErrno);
        if (n > 0)
        {
            totalRead += n;
            continue;
        }
        if (n == 0)
        {
            peerClosed = true;
            break;
        }
        if (savedErrno == EINTR)
        {
            continue;
        }
        if (savedErrno == EAGAIN || savedErrno == EWOULDBLOCK)
        {
            break;
        }

        errno = savedErrno;
        LOG_WARN("handleRead fd=%d error, errno=%d", fd(), savedErrno);
        handleError();
        return;
    }

    if (totalRead > 0)
    {
        lastActiveTime_ = Timestamp::now();
        LOG_DEBUG("handleRead fd=%d, bytes_read=%zd", fd(), totalRead);
        if (messageCodec_)
        {
            // 有帧编解码器：循环提取完整帧，每帧回调一次 onMessage
            // 不完整的数据留在 inputBuffer_ 中，等待下次 handleRead 追加
            std::string message;
            while (messageCodec_(&inputBuffer_, message))
            {
                if (onMessageCallback_)
                {
                    onMessageCallback_(shared_from_this(), message);
                }
            }
        }
        else
        {
            // 无编解码器：保留旧行为，一次性提取所有数据透传给上层
            std::string message = inputBuffer_.retrieveAllAsString();
            if (onMessageCallback_)
            {
                onMessageCallback_(shared_from_this(), message);
            }
        }
    }

    if (peerClosed)
    {
        LOG_DEBUG("handleRead fd=%d: peer closed", fd());
        handleClose();
    }
}

void Connection::handleWrite()
{
    if (channel_->isWriting())
    {
        ssize_t n = ::send(fd(), outputBuffer_.peek(), outputBuffer_.readableBytes(), 0);
        if (n > 0)
        {
            outputBuffer_.retrieve(n);
            // 如果发完了
            if (outputBuffer_.readableBytes() == 0)
            {
                channel_->disableWriting();
                if (sendCompleteCallback_)
                {
                    // 在所属线程执行发送完成回调
                    loop_->runInLoop(std::bind(sendCompleteCallback_, shared_from_this()));
                }
            }
        }
        else
        {
            LOG_ERROR("Connection::handleWrite() send failed, fd=%d, errno=%d", fd(), errno);
        }
    }
}

void Connection::handleClose()
{
    if(disconnected_)
        return;

    disconnected_ = true;

    LOG_DEBUG("Connection::handleClose fd=%d, %s:%u", fd(), ip().c_str(), port());

    ConnectionPtr self(shared_from_this());

    loop_->runInLoop(
        [self]()
        {
            self->channel_->disableAll();
            self->channel_->remove();
            self->loop_->removeConnection(self->fd());

            if(self->closeCallback_)
            {
                self->closeCallback_(self);
            }
        });
}

void Connection::handleError()
{
    LOG_WARN("Connection::handleError fd=%d, %s:%u, errno=%d", fd(), ip().c_str(), port(), errno);

    if (!disconnected_)
    {
        disconnected_ = true;
        channel_->disableAll();
        channel_->remove();
        loop_->removeConnection(fd());

        if (errorCallback_)
        {
            errorCallback_(shared_from_this());
        }
    }
}

// 交给业务层调用，用于向客户端发送数据
void Connection::send(const std::string& data)
{
    if (disconnected_)
    {
        LOG_WARN("send on disconnected connection, fd=%d", fd());
        return;
    }
    // 当前线程就是该连接所属的 IO 线程，直接调用 sendInLoop
    if (loop_->isInLoopThread())
    {
        sendInLoop(data);
    }
    else
    {
        // 当前线程是其他线程（如计算工作池线程中调用的send(用户在回调函数中写的)）
        // 那就跳出工作线程，将发送任务封装成lambda对象，通过 runInLoop 丢给 IO 线程的任务队列
        loop_->runInLoop(std::bind(&Connection::sendInLoop, shared_from_this(), data));
    }
}
// 实际发送逻辑（内部）
void Connection::sendInLoop(const std::string& data)
{
    ssize_t nwrote = 0;
    size_t remaining = data.size();
    bool faultError = false;

    // 如果之前没有数据在排队，尝试直接发送
    if (!channel_->isWriting() && outputBuffer_.readableBytes() == 0)
    {
        nwrote = ::send(fd(), data.data(), data.size(), 0);
        if (nwrote >= 0)
        {
            remaining = data.size() - nwrote;
            if (remaining == 0 && sendCompleteCallback_)
            {
                loop_->runInLoop(std::bind(sendCompleteCallback_, shared_from_this()));
            }
        }
        else
        {
            nwrote = 0;
            if (errno != EWOULDBLOCK)
            {
                if (errno == EPIPE || errno == ECONNRESET)
                {
                    LOG_WARN("sendInLoop fd=%d: EPIPE or ECONNRESET", fd());
                    faultError = true;
                }
            }
        }
    }

    // 如果直接发送没发完，或者之前就有数据在排队，则放入输出缓冲区并监听写事件
    if (!faultError && remaining > 0)
    {
        outputBuffer_.append(data.data() + nwrote, remaining);
        if (!channel_->isWriting())
        {
            channel_->enableWriting();
        }
    }

    // 如果发生致命错误（对端关闭连接），主动清理本端连接
    if (faultError)
    {
        handleClose();
    }
}

void Connection::shutdown()
{
    if (!disconnected_)
    {
        ::shutdown(socket_->fd(), SHUT_WR);
    }
}

bool Connection::isTimeout(time_t now, int seconds) const
{
    return (now - lastActiveTime_.toSeconds()) > seconds;
}

void Connection::connectEstablished()
{
    ConnectionPtr self(shared_from_this());
    loop_->runInLoop([self]()
    {
        self->channel_->useET();
        self->channel_->enableReading();
    });
}

void Connection::forceClose()
{
    ConnectionPtr self(shared_from_this());
    loop_->runInLoop([self]()
    {
        self->handleClose();
    });
}

} // namespace wevix_muduo
