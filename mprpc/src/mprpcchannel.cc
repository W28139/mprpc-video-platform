#include"mprpcchannel.h"
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <climits>
#include <memory>
#include <mutex>
#include <poll.h>
#include <unordered_map>
#include <vector>
#include "rpcheader.pb.h"
#include"mprpcapplication.h"
#include"mprpccontroller.h"
#include"mprpccodec.h"
#include"ZookeeperUtil.h"
#include"wevix_muduo/AsyncLogger.h"
// 客户端间接调用，借助protubuf，序列化需求，找到对应服务器ip/port，发起连接，获取结果并反序列化

namespace
{
// 默认同步 RPC 超时时间。调用方可以通过 MprpcController::SetTimeoutMs 覆盖。
constexpr int64_t kDefaultRpcTimeoutMs = 5000;

// 统一设置 controller 失败状态。
// 如果传入的是 MprpcController，就记录结构化错误码；否则退化为 protobuf 原生 SetFailed。
void SetControllerFailed(google::protobuf::RpcController* controller,
                         mprpc::RpcErrorCode errorCode,
                         const std::string& reason)
{
    if (controller != nullptr)
    {
        auto* mprpcController = dynamic_cast<MprpcController*>(controller);
        if (mprpcController != nullptr)
        {
            mprpcController->SetFailed(static_cast<int>(errorCode), reason);
        }
        else
        {
            controller->SetFailed(reason);
        }
    }
}

// protobuf Stub 允许传入 done；同步调用中通常为空，但失败/成功路径都保持语义一致。
void RunDone(google::protobuf::Closure* done)
{
    if (done != nullptr)
    {
        done->Run();
    }
}

// 为每次 RPC 生成进程内递增 request_id，用于校验响应是否对应当前请求。
uint64_t NextRequestId()
{
    static std::atomic<uint64_t> nextRequestId{1};
    return nextRequestId.fetch_add(1, std::memory_order_relaxed);
}

// 当前时间戳，单位毫秒；用于填充 RpcHeader.deadline_ms。
uint64_t NowMs()
{
    auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

// 从 MprpcController 读取调用级超时；未设置时使用框架默认值。
int64_t GetRpcTimeoutMs(google::protobuf::RpcController* controller)
{
    auto* mprpcController = dynamic_cast<MprpcController*>(controller);
    if (mprpcController != nullptr && mprpcController->HasTimeout())
    {
        return mprpcController->TimeoutMs();
    }
    return kDefaultRpcTimeoutMs;
}

// 设置 send/recv 超时，避免服务端异常、协议不匹配或半开连接导致客户端永久阻塞。
bool SetSocketTimeout(int fd, int64_t timeoutMs, int& savedErrno)
{
    struct timeval tv;
    if (timeoutMs > 0)
    {
        tv.tv_sec = static_cast<time_t>(timeoutMs / 1000);
        tv.tv_usec = static_cast<suseconds_t>((timeoutMs % 1000) * 1000);
    }
    else
    {
        // 传 0 表示清掉上一轮请求留下的超时设置，让长连接复用时超时语义跟随当前调用。
        tv.tv_sec = 0;
        tv.tv_usec = 0;
    }
    if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1)
    {
        savedErrno = errno;
        return false;
    }
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1)
    {
        savedErrno = errno;
        return false;
    }
    return true;
}

// 简单 fd RAII，保证 CallMethod 任意 return 路径都会关闭短连接 socket。
class ScopedFd
{
public:
    explicit ScopedFd(int fd = -1) : fd_(fd) {}
    ~ScopedFd() { reset(); }

    int get() const { return fd_; }

    void reset(int fd = -1)
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
        }
        fd_ = fd;
    }

private:
    int fd_;
};

// TCP send 可能只发送部分数据；RPC 请求必须循环发送到完整帧全部写出。
bool SendAll(int fd, const char* data, size_t len, int& savedErrno)
{
    size_t sent = 0;
    while (sent < len)
    {
        ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0)
        {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        savedErrno = (n < 0) ? errno : 0;
        return false;
    }
    return true;
}

// 按指定长度精确读取响应，避免一次 recv 只读到部分帧造成 protobuf 解析失败。
bool RecvAll(int fd, char* data, size_t len, int& savedErrno)
{
    size_t recvd = 0;
    while (recvd < len)
    {
        ssize_t n = ::recv(fd, data + recvd, len - recvd, 0);
        if (n > 0)
        {
            recvd += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        savedErrno = (n < 0) ? errno : 0;
        return false;
    }
    return true;
}

// 进程级复用 ZooKeeper 客户端，避免每次 RPC 都创建/销毁 ZK 会话。
ZkClient& SharedZkClient()
{
    static ZkClient client; // 函数内 static，进程生命周期内唯一实例
    return client;
}

bool EnsureSharedZkClientStarted()
{
    static bool started = false;
    static std::once_flag once;
    std::call_once(once, []()   // 无论多少线程调用，Start() 只执行一次
    {
        started = SharedZkClient().Start();
    });
    return started;
}

struct EndpointCacheEntry
{
    std::vector<std::string> endpoints;
    size_t nextIndex = 0;
};

std::string MethodRegistryPath(const std::string& serviceName,
                               const std::string& methodName)
{
    return "/mprpc/services/" + serviceName + "/" + methodName;
}

std::string LegacyMethodPath(const std::string& serviceName,
                             const std::string& methodName)
{
    return "/" + serviceName + "/" + methodName;
}

// method_path -> endpoint 列表，本地轮询选择实例，减少 RPC 热路径上的 ZK 读请求。
std::unordered_map<std::string, EndpointCacheEntry>& ServiceCache()
{
    static std::unordered_map<std::string, EndpointCacheEntry> cache;
    return cache;
}

std::mutex& ServiceCacheMutex()
{
    static std::mutex mutex;
    return mutex;
}

std::vector<std::string> QueryEndpointList(const std::string& methodPath,
                                           const std::string& legacyPath)
{
    std::vector<std::string> endpoints;
    if (!EnsureSharedZkClientStarted())
    {
        return endpoints;
    }

    ZkClient& zk = SharedZkClient();
    std::vector<std::string> children = zk.GetChildren(methodPath.c_str());
    std::sort(children.begin(), children.end());
    for (const std::string& child : children)
    {
        std::string childPath = methodPath + "/" + child;
        std::string hostData = zk.GetData(childPath.c_str());
        if (!hostData.empty())
        {
            endpoints.push_back(hostData);
        }
    }

    if (endpoints.empty())
    {
        // 兼容旧注册路径：/Service/Method -> ip:port。
        // 新 Provider 会注册到 /mprpc/services/...，这里只作为过渡兜底。
        std::string legacyHostData = zk.GetData(legacyPath.c_str());
        if (!legacyHostData.empty())
        {
            endpoints.push_back(legacyHostData);
        }
    }

    std::lock_guard<std::mutex> lock(ServiceCacheMutex());
    if (!endpoints.empty())
    {
        EndpointCacheEntry& entry = ServiceCache()[methodPath];
        entry.endpoints = endpoints;
        entry.nextIndex = 0;
    }
    else
    {
        ServiceCache().erase(methodPath);
    }
    return endpoints;
}

std::string PickEndpoint(const std::vector<std::string>& endpoints)
{
    if (endpoints.empty())
    {
        return "";
    }
    static std::atomic<size_t> nextEndpoint{0};
    return endpoints[nextEndpoint.fetch_add(1, std::memory_order_relaxed) % endpoints.size()];
}

// 优先读本地缓存，cache miss 时再访问 ZK；缓存命中时按轮询选择 endpoint。
std::string GetHostData(const std::string& methodPath,
                        const std::string& legacyPath,
                        bool& fromCache)
{
    {
        std::lock_guard<std::mutex> lock(ServiceCacheMutex());
        auto it = ServiceCache().find(methodPath);
        if (it != ServiceCache().end() && !it->second.endpoints.empty())
        {
            fromCache = true;
            EndpointCacheEntry& entry = it->second;
            std::string endpoint = entry.endpoints[entry.nextIndex % entry.endpoints.size()];
            ++entry.nextIndex;
            return endpoint;
        }
    }

    fromCache = false;
    return PickEndpoint(QueryEndpointList(methodPath, legacyPath));
}

// 连接失败时主动失效缓存，下一次调用会重新从 ZK 拉取服务地址。
void InvalidateHostData(const std::string& methodPath)
{
    std::lock_guard<std::mutex> lock(ServiceCacheMutex());
    ServiceCache().erase(methodPath);
}

// 解析 ZK 节点中的 "ip:port" 字符串，并校验端口范围。
bool ParseHostData(const std::string& hostData,
                   std::string& ip,
                   uint16_t& port,
                   std::string& error)
{
    size_t idx = hostData.find(':');
    if (idx == std::string::npos || idx == 0 || idx + 1 >= hostData.size())
    {
        error = "address is invalid: " + hostData;
        return false;
    }

    ip = hostData.substr(0, idx);
    char* end = nullptr;
    long parsedPort = std::strtol(hostData.c_str() + idx + 1, &end, 10);
    if (*end != '\0' || parsedPort <= 0 || parsedPort > 65535)
    {
        error = "port is invalid: " + hostData;
        return false;
    }

    port = static_cast<uint16_t>(parsedPort);
    return true;
}

std::string EndpointKey(const std::string& ip, uint16_t port)
{
    return ip + ":" + std::to_string(port);
}

int MaxConnectionsPerEndpoint()
{
    if (!MprpcApplication::IsInitialized())
    {
        return 8;
    }
    return MprpcApplication::GetConfig().LoadInt("mprpcclient_connections_per_endpoint",
                                                8, 1, 128);
}

struct PooledConnection
{
    PooledConnection(std::string endpointKey, std::string endpointIp, uint16_t endpointPort)
        : key(std::move(endpointKey))
        , ip(std::move(endpointIp))
        , port(endpointPort)
    {
    }

    ~PooledConnection()
    {
        Close();
    }

    bool EnsureConnected(int64_t timeoutMs, int& savedErrno);

    void Close()
    {
        if (fd >= 0)
        {
            ::close(fd);
            fd = -1;
        }
    }

    std::string key;
    std::string ip;
    uint16_t port;
    int fd = -1;
    std::mutex mutex;
};

std::unordered_map<std::string, std::vector<std::shared_ptr<PooledConnection>>>& ConnectionPool()
{
    static std::unordered_map<std::string, std::vector<std::shared_ptr<PooledConnection>>> pool;
    return pool;
}

std::unordered_map<std::string, size_t>& ConnectionPoolNextIndex()
{
    static std::unordered_map<std::string, size_t> nextIndex;
    return nextIndex;
}

std::mutex& ConnectionPoolMutex()
{
    static std::mutex mutex;
    return mutex;
}

// 带超时的 TCP connect。
// 先切成非阻塞 socket，connect 返回 EINPROGRESS 后用 poll 等待可写，再恢复原 flags。
int ConnectToEndpoint(const std::string& ip, uint16_t port, int64_t timeoutMs, int& savedErrno)
{
    int clientfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (clientfd == -1)
    {
        savedErrno = errno;
        return -1;
    }

    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) != 1)
    {
        savedErrno = EINVAL;
        ::close(clientfd);
        return -1;
    }

    int oldFlags = ::fcntl(clientfd, F_GETFL, 0);
    if (oldFlags == -1 || ::fcntl(clientfd, F_SETFL, oldFlags | O_NONBLOCK) == -1)
    {
        savedErrno = errno;
        ::close(clientfd);
        return -1;
    }

    int ret = ::connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr));
    if (ret == -1 && errno != EINPROGRESS)
    {
        savedErrno = errno;
        ::close(clientfd);
        return -1;
    }

    if (ret == -1)
    {
        struct pollfd pfd;
        pfd.fd = clientfd;
        pfd.events = POLLOUT;
        pfd.revents = 0;

        int timeout = timeoutMs > 0 ? static_cast<int>(std::min<int64_t>(timeoutMs, INT32_MAX)) : -1;
        do
        {
            ret = ::poll(&pfd, 1, timeout);
        } while (ret == -1 && errno == EINTR);

        if (ret == 0)
        {
            // poll 超时说明 TCP 握手没有在调用超时时间内完成。
            savedErrno = ETIMEDOUT;
            ::close(clientfd);
            return -1;
        }
        if (ret == -1)
        {
            savedErrno = errno;
            ::close(clientfd);
            return -1;
        }

        int socketError = 0;
        socklen_t len = sizeof(socketError);
        if (::getsockopt(clientfd, SOL_SOCKET, SO_ERROR, &socketError, &len) == -1)
        {
            savedErrno = errno;
            ::close(clientfd);
            return -1;
        }
        if (socketError != 0)
        {
            // 非阻塞 connect 的真实错误需要从 SO_ERROR 读取。
            savedErrno = socketError;
            ::close(clientfd);
            return -1;
        }
    }

    if (::fcntl(clientfd, F_SETFL, oldFlags) == -1)
    {
        savedErrno = errno;
        ::close(clientfd);
        return -1;
    }

    if (!SetSocketTimeout(clientfd, timeoutMs, savedErrno))
    {
        ::close(clientfd);
        return -1;
    }

    return clientfd;
}

bool PooledConnection::EnsureConnected(int64_t timeoutMs, int& savedErrno)
{
    if (fd >= 0)
    {
        return true;
    }

    fd = ConnectToEndpoint(ip, port, timeoutMs, savedErrno);
    return fd >= 0;
}

std::shared_ptr<PooledConnection> GetPooledConnection(const std::string& ip, uint16_t port)
{
    std::string key = EndpointKey(ip, port);
    std::lock_guard<std::mutex> lock(ConnectionPoolMutex());
    auto& connections = ConnectionPool()[key];
    int maxConnections = MaxConnectionsPerEndpoint();

    if (static_cast<int>(connections.size()) < maxConnections)
    {
        auto conn = std::make_shared<PooledConnection>(key, ip, port);
        connections.push_back(conn);
        return conn;
    }

    size_t& nextIndex = ConnectionPoolNextIndex()[key];
    auto conn = connections[nextIndex % connections.size()];
    ++nextIndex;
    return conn;
}

void DropEndpointConnections(const std::string& endpointKey)
{
    std::lock_guard<std::mutex> lock(ConnectionPoolMutex());
    ConnectionPool().erase(endpointKey);
    ConnectionPoolNextIndex().erase(endpointKey);
}

// 解析服务端响应 payload：
// [response_header_size(4B, network order)] + [RpcResponseHeader] + [response_body]
// 这里同时校验 response_size，保证客户端不会把截断或拼错的响应交给业务层。
bool DecodeRpcResponsePayload(const std::string& payload,
                              mprpc::RpcResponseHeader& responseHeader,
                              std::string& responseBody,
                              std::string& errorMsg)
{
    if (payload.size() < sizeof(uint32_t))
    {
        errorMsg = "response payload too small";
        return false;
    }

    uint32_t headerSize = 0;
    if (!mprpc::ReadNetworkUint32(payload.data(), payload.size(), &headerSize))
    {
        errorMsg = "read response header size failed";
        return false;
    }

    if (headerSize == 0 || payload.size() - sizeof(uint32_t) < headerSize)
    {
        errorMsg = "invalid response header size:" + std::to_string(headerSize);
        return false;
    }

    std::string responseHeaderStr = payload.substr(sizeof(uint32_t), headerSize);
    if (!responseHeader.ParseFromString(responseHeaderStr))
    {
        errorMsg = "parse response rpc header failed";
        return false;
    }

    size_t bodyOffset = sizeof(uint32_t) + headerSize;
    size_t bodySize = payload.size() - bodyOffset;
    if (responseHeader.response_size() != bodySize)
    {
        // response_size 不一致时说明协议数据不完整或被破坏，必须判定为框架错误。
        errorMsg = "response body size mismatch, header response_size=" +
                   std::to_string(responseHeader.response_size()) +
                   ", actual=" + std::to_string(bodySize);
        return false;
    }

    responseBody = payload.substr(bodyOffset, bodySize);
    return true;
}

// 将系统 errno 分类成 RPC 错误码，超时类错误统一映射为 RPC_TIMEOUT。
mprpc::RpcErrorCode IoErrorCode(int savedErrno, mprpc::RpcErrorCode defaultCode)
{
    if (savedErrno == ETIMEDOUT || savedErrno == EAGAIN || savedErrno == EWOULDBLOCK)
    {
        return mprpc::RPC_TIMEOUT;
    }
    return defaultCode;
}

bool SendRequestAndReadResponse(const std::shared_ptr<PooledConnection>& conn,
                                const std::string& requestFrame,
                                int64_t timeoutMs,
                                std::string& responsePayload,
                                mprpc::RpcErrorCode& errorCode,
                                std::string& errorMsg)
{
    std::lock_guard<std::mutex> lock(conn->mutex);

    int savedErrno = 0;
    if (!conn->EnsureConnected(timeoutMs, savedErrno))
    {
        errorCode = IoErrorCode(savedErrno, mprpc::RPC_CONNECT_FAILED);
        errorMsg = "connect error! errno:" + std::to_string(savedErrno);
        return false;
    }

    // 长连接复用时，每次调用都按当前 timeoutMs 刷新 socket 选项。
    if (!SetSocketTimeout(conn->fd, timeoutMs, savedErrno))
    {
        errorCode = IoErrorCode(savedErrno, mprpc::RPC_CONNECT_FAILED);
        errorMsg = "set socket timeout error! errno:" + std::to_string(savedErrno);
        conn->Close();
        return false;
    }

    if (!SendAll(conn->fd, requestFrame.data(), requestFrame.size(), savedErrno))
    {
        errorCode = IoErrorCode(savedErrno, mprpc::RPC_SEND_FAILED);
        errorMsg = "send error! errno:" + std::to_string(savedErrno);
        conn->Close();
        return false;
    }

    char responseLenBuf[sizeof(uint32_t)] = {0};
    if (!RecvAll(conn->fd, responseLenBuf, sizeof(responseLenBuf), savedErrno))
    {
        errorCode = IoErrorCode(savedErrno, mprpc::RPC_RECV_FAILED);
        errorMsg = savedErrno == 0
                 ? "server closed connection before sending response header!"
                 : "recv response header error! errno:" + std::to_string(savedErrno);
        conn->Close();
        return false;
    }

    uint32_t responseFrameSize = 0;
    if (!mprpc::ReadNetworkUint32(responseLenBuf, sizeof(responseLenBuf), &responseFrameSize) ||
        responseFrameSize < mprpc::kRpcFrameHeaderSize ||
        responseFrameSize > mprpc::kRpcMaxFrameSize)
    {
        errorCode = mprpc::RPC_FRAME_TOO_LARGE;
        errorMsg = "invalid response frame size:" + std::to_string(responseFrameSize);
        conn->Close();
        return false;
    }

    std::string responseFrameBody(responseFrameSize, '\0');
    if (!RecvAll(conn->fd, &responseFrameBody[0], responseFrameSize, savedErrno))
    {
        errorCode = IoErrorCode(savedErrno, mprpc::RPC_RECV_FAILED);
        errorMsg = savedErrno == 0
                 ? "server closed connection before sending full response!"
                 : "recv response body error! errno:" + std::to_string(savedErrno);
        conn->Close();
        return false;
    }

    std::string frameError;
    if (!mprpc::DecodeRpcFramePayload(responseFrameBody, &responsePayload, &frameError))
    {
        errorCode = mprpc::RPC_INVALID_RESPONSE;
        errorMsg = frameError;
        conn->Close();
        return false;
    }

    return true;
}

} // namespace

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                                google::protobuf::RpcController* controller, 
                                const google::protobuf::Message* request,
                                google::protobuf::Message* response, 
                                google::protobuf::Closure* done)
{
    const google::protobuf::ServiceDescriptor* sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();
    // request_id 会写进请求头，响应回来后必须一致，防止长连接下串包或协议错配。
    uint64_t requestId = NextRequestId();
    int64_t timeoutMs = GetRpcTimeoutMs(controller);

    // 1. 获取参数的序列化字符串长度 args_size
    uint32_t args_size = 0;
    std::string args_str;
    // 进行序列化
    if (request->SerializeToString(&args_str))
    {
        if (args_str.size() > mprpc::kRpcMaxFrameSize)
        {
            SetControllerFailed(controller, mprpc::RPC_FRAME_TOO_LARGE,
                                "request frame too large:" + std::to_string(args_str.size()));
            RunDone(done);
            return;
        }
        args_size = args_str.size();
    }
    else
    {
        SetControllerFailed(controller, mprpc::RPC_BAD_REQUEST, "serialize request error!");
        RunDone(done);
        return;
    }

    // 2. 定义并序列化 rpc 的请求 header
    mprpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);
    rpcHeader.set_request_id(requestId);
    if (timeoutMs > 0)
    {
        // deadline_ms 先放入协议，当前服务端还未消费，后续可以用于 Provider 侧快速拒绝过期请求。
        rpcHeader.set_deadline_ms(NowMs() + static_cast<uint64_t>(timeoutMs));
    }

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if (rpcHeader.SerializeToString(&rpc_header_str))
    {
        if (rpc_header_str.size() > mprpc::kRpcMaxFrameSize)
        {
            SetControllerFailed(controller, mprpc::RPC_FRAME_TOO_LARGE,
                                "rpc header too large:" + std::to_string(rpc_header_str.size()));
            RunDone(done);
            return;
        }
        header_size = rpc_header_str.size();
    }
    else
    {
        SetControllerFailed(controller, mprpc::RPC_BAD_REQUEST, "serialize rpc header error!");
        RunDone(done);
        return;
    }

    // 3. 组织待发送的 rpc 请求的字符串
    // 帧格式：[total_len(4B, network order)] + [header_size(4B, network order) + RpcHeader + args]
    std::string request_payload;
    request_payload.reserve(sizeof(uint32_t) + rpc_header_str.size() + args_str.size());
    mprpc::AppendNetworkUint32(&request_payload, header_size);
    request_payload += rpc_header_str;
    request_payload += args_str;

    if (request_payload.size() > mprpc::kRpcMaxFrameSize)
    {
        SetControllerFailed(controller, mprpc::RPC_FRAME_TOO_LARGE,
                            "request frame too large:" + std::to_string(request_payload.size()));
        RunDone(done);
        return;
    }
    // 外层 total_len 由 BuildRpcFrame 写入，服务端 Connection 的 codec 用它做粘包/拆包。
    std::string send_rpc_str = mprpc::BuildRpcFrame(request_payload);

    // 4. 服务发现：优先使用新多实例路径，旧路径仅作为兼容兜底。
    std::string method_path = MethodRegistryPath(service_name, method_name);
    std::string legacy_method_path = LegacyMethodPath(service_name, method_name);
    bool fromCache = false;
    std::string host_data = GetHostData(method_path, legacy_method_path, fromCache);
    LOG_DEBUG("rpc call %s::%s discovered endpoint from %s",
              service_name.c_str(), method_name.c_str(),
              fromCache ? "cache" : "zookeeper");
    
    if (host_data == "")
    {
        LOG_ERROR("rpc call %s::%s: zk path %s not found", service_name.c_str(), method_name.c_str(), method_path.c_str());
        SetControllerFailed(controller, mprpc::RPC_SERVICE_DISCOVERY_FAILED,
                            method_path + " is not exist!");
        RunDone(done);
        return;
    }

    std::string ip;
    uint16_t port = 0;
    std::string parseError;
    if (!ParseHostData(host_data, ip, port, parseError))
    {
        LOG_ERROR("rpc call %s::%s: invalid host_data [%s]", service_name.c_str(), method_name.c_str(), host_data.c_str());
        InvalidateHostData(method_path);
        SetControllerFailed(controller, mprpc::RPC_SERVICE_DISCOVERY_FAILED,
                            method_path + " " + parseError);
        RunDone(done);
        return;
    }

    // 5. 从 endpoint 连接池取一条连接，发送请求并读取完整响应 payload。
    std::shared_ptr<PooledConnection> pooledConn = GetPooledConnection(ip, port);
    std::string recv_str;
    mprpc::RpcErrorCode callErrorCode = mprpc::RPC_SUCCESS;
    std::string callErrorMsg;
    bool callOk = SendRequestAndReadResponse(pooledConn, send_rpc_str, timeoutMs,
                                             recv_str, callErrorCode, callErrorMsg);

    if (!callOk &&
        (callErrorCode == mprpc::RPC_CONNECT_FAILED ||
         callErrorCode == mprpc::RPC_TIMEOUT ||
         callErrorCode == mprpc::RPC_SEND_FAILED ||
         callErrorCode == mprpc::RPC_RECV_FAILED))
    {
        // 缓存 endpoint 连接失败时，认为实例可能已经下线，失效缓存后重新发现并重试一次。
        DropEndpointConnections(pooledConn->key);
        InvalidateHostData(method_path);
        host_data = PickEndpoint(QueryEndpointList(method_path, legacy_method_path));
        if (!host_data.empty() && ParseHostData(host_data, ip, port, parseError))
        {
            pooledConn = GetPooledConnection(ip, port);
            callOk = SendRequestAndReadResponse(pooledConn, send_rpc_str, timeoutMs,
                                                recv_str, callErrorCode, callErrorMsg);
        }
    }

    if (!callOk)
    {
        LOG_ERROR("rpc call %s::%s to %s failed: %s",
                  service_name.c_str(), method_name.c_str(), host_data.c_str(), callErrorMsg.c_str());
        SetControllerFailed(controller, callErrorCode, callErrorMsg);
        RunDone(done);
        return;
    }

    mprpc::RpcResponseHeader responseHeader;
    std::string responseBody;
    std::string decodeError;
    if (!DecodeRpcResponsePayload(recv_str, responseHeader, responseBody, decodeError))
    {
        LOG_ERROR("rpc call %s::%s: decode response failed: %s",
                  service_name.c_str(), method_name.c_str(), decodeError.c_str());
        SetControllerFailed(controller, mprpc::RPC_RESPONSE_PARSE_FAILED, decodeError);
        RunDone(done);
        return;
    }

    if (responseHeader.request_id() != requestId)
    {
        // request_id 不一致说明响应不是本次调用的结果，不能交给业务 response 解析。
        std::string err = "response request_id mismatch, expect=" +
                          std::to_string(requestId) +
                          ", actual=" + std::to_string(responseHeader.request_id());
        LOG_ERROR("rpc call %s::%s: %s", service_name.c_str(), method_name.c_str(), err.c_str());
        SetControllerFailed(controller, mprpc::RPC_INVALID_RESPONSE, err);
        RunDone(done);
        return;
    }

    if (responseHeader.error_code() != mprpc::RPC_SUCCESS)
    {
        // 远端框架已经返回明确失败，例如 service/method 不存在或请求参数解析失败。
        std::string err = responseHeader.error_msg().empty()
                        ? "remote rpc failed, error_code=" + std::to_string(responseHeader.error_code())
                        : responseHeader.error_msg();
        LOG_ERROR("rpc call %s::%s failed remotely, code=%d, error=%s",
                  service_name.c_str(), method_name.c_str(),
                  responseHeader.error_code(), err.c_str());
        SetControllerFailed(controller, responseHeader.error_code(), err);
        RunDone(done);
        return;
    }

    // 6. 反序列化 rpc 响应
    if (!response->ParseFromString(responseBody))
    {
        LOG_ERROR("rpc call %s::%s: parse response failed, response_size=%zu",
                  service_name.c_str(), method_name.c_str(), responseBody.size());
        SetControllerFailed(controller, mprpc::RPC_RESPONSE_PARSE_FAILED,
                            "parse response body failed");
        RunDone(done);
        return;
    }
    RunDone(done);
}
