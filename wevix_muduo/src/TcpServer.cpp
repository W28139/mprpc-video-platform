#include "wevix_muduo/TcpServer.h"
#include "wevix_muduo/AsyncLogger.h"
#include <unistd.h>
#include <cstdio>
#include <functional>

namespace wevix_muduo
{

TcpServer::TcpServer(const std::string& ip, uint16_t port, int threadNum, int backlog)
    : mainLoop_(new EventLoop(true)) // 设置为主循环
    , threadNum_(threadNum)
    , ioThreadPool_(threadNum, "IO_LOOP")
    , acceptor_(mainLoop_.get(), ip, port, backlog)
    , backlog_(backlog)
{
    // 1. 设置 Acceptor 发现新连接时的内部回调
    acceptor_.setNewConnectionCallback(
        std::bind(&TcpServer::handleNewConnection, this, std::placeholders::_1)
    );

    // 2. 初始化从循环 (Sub Reactors)
    for (int i = 0; i < threadNum_; ++i)
    {
        // 创建从循环，设置较短的超时时间用于清理
		std::unique_ptr<EventLoop> loop(new EventLoop(false, 5, 10));
        
        // 设置从循环的定时器回调，用于超时剔除连接
        loop->setTimerCallback(
            std::bind(&TcpServer::removeConnection, this, std::placeholders::_1)
        );

        subLoops_.push_back(std::move(loop));
    }
}

TcpServer::~TcpServer()
{
    stop();
}

void TcpServer::start()
{
    // 1. 将所有从循环跑在线程池中
    for (int i = 0; i < threadNum_; ++i)
    {
        ioThreadPool_.addTask(std::bind(&EventLoop::run, subLoops_[i].get()));
    }

    // 2. 启动主循环（这会阻塞当前线程）
    LOG_INFO("TcpServer is running, MainLoop tid: %d, IO threads: %d",
             ::getpid(), threadNum_);
    mainLoop_->run();
}

void TcpServer::stop()
{
    // 停止主循环
    mainLoop_->stop();

    // 停止所有从循环
    for (auto& loop : subLoops_)
    {
        loop->stop();
    }

    // 停止线程池
    ioThreadPool_.stop();

    if(workThreadPool_)
    {
        workThreadPool_->stop();
    }

    LOG_INFO("TcpServer has stopped.");
}

void TcpServer::handleNewConnection(std::unique_ptr<Socket> clientSock)
{
    // 1. 简单的轮询算法选择一个从循环
    int fd = clientSock->fd();
    int idx = fd % threadNum_;
    EventLoop* subLoop = subLoops_[idx].get();

    // 2. 创建 Connection 对象
    ConnectionPtr conn = std::make_shared<Connection>(subLoop, std::move(clientSock));

    // 3. 绑定 Connection 的回调到 TcpServer 的内部处理函数
    conn->setCloseCallback(std::bind(&TcpServer::handleClose, this, std::placeholders::_1));
    conn->setErrorCallback(std::bind(&TcpServer::handleError, this, std::placeholders::_1));
    conn->setOnMessageCallback(std::bind(&TcpServer::handleMessage, this, std::placeholders::_1, std::placeholders::_2));
    conn->setSendCompleteCallback(std::bind(&TcpServer::handleSendComplete, this, std::placeholders::_1));
    if (messageCodec_) conn->setMessageCodec(messageCodec_);    // 若有帧编解码器，应用到该连接（让 Connection 自动处理粘包/拆包）

    // 4. 加入连接管理容器
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connections_[fd] = conn;
    }

    // 5. 让所属的从循环也感知到这个连接（用于超时扫描）
    subLoop->newConnection(conn);

    // 6. 执行用户注册的“新连接建立”回调
    if (connectionCallback_)
    {
        connectionCallback_(conn);
    }

    conn->connectEstablished();

    LOG_DEBUG("New connection from %s:%u, fd=%d, assigned to subLoop[%d]",
             conn->ip().c_str(), conn->port(), fd, idx);
}

void TcpServer::handleClose(const ConnectionPtr& conn)
{
    LOG_DEBUG("Connection closed: %s:%u, fd=%d", conn->ip().c_str(), conn->port(), conn->fd());

    if (closeCallback_)
    {
        closeCallback_(conn);
    }

    removeConnection(conn->fd());
}

void TcpServer::handleError(const ConnectionPtr& conn)
{
    LOG_WARN("Connection error: %s:%u, fd=%d", conn->ip().c_str(), conn->port(), conn->fd());

    if (closeCallback_)
    {
        closeCallback_(conn);
    }

    removeConnection(conn->fd());
}

// 此时执行 handleMessage 的是：IO 线程 (SubLoop)
void TcpServer::handleMessage(const ConnectionPtr& conn, std::string& message)
{
    if(!onMessageCallback_)
        return;

    if (workThreadPool_)
    {
        // 有 work pool：将数据移动到堆上，避免拷贝，异步执行业务回调
        auto msg = std::make_shared<std::string>(std::move(message));
        workThreadPool_->addTask([this, conn, msg]()
        {
            onMessageCallback_(conn, *msg);
        });
        // IO 线程执行完 addTask 后立刻返回，继续监听网络事件
    }
    else
    {
        // 无 work pool：直接透传引用，零额外拷贝，同步执行业务回调
        onMessageCallback_(conn, message);
    }
}

void TcpServer::handleSendComplete(const ConnectionPtr& conn)
{
    if (sendCompleteCallback_)
    {
        sendCompleteCallback_(conn);
    }
}

void TcpServer::removeConnection(int fd)
{
    std::lock_guard<std::mutex> lock(mutex_);
    connections_.erase(fd);
}

// 让用户调用，设置work
void TcpServer::enableWorkPool(int threadNum, PoolMode mode)
{
    if(workThreadPool_)
    {
        LOG_WARN("TcpServer::enableWorkPool already enabled, ignoring duplicate call");
        return;
    }

    workThreadPool_.reset(
        new ThreadPool(threadNum,"WORKER"));

    workThreadPool_->setMode(mode);

    workThreadPool_->start();

    LOG_INFO("WorkThreadPool start, size=%d", threadNum);
}

} // namespace wevix_muduo
