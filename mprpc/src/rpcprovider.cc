#include"rpcprovider.h"
#include"rpcheader.pb.h"
#include"wevix_muduo/AsyncLogger.h"
#include"ZookeeperUtil.h"
#include"mprpccodec.h"
#include"mprpcutil.h"
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

namespace
{

// 构造服务端响应帧：
// [total_len] + [response_header_size] + [RpcResponseHeader] + [responseBody]
// 无论成功还是失败，都通过 RpcResponseHeader 把 request_id 和错误码返回给客户端。
std::string BuildRpcResponseFrame(uint64_t requestId,
                                  mprpc::RpcErrorCode errorCode,
                                  const std::string& errorMsg,
                                  const std::string& responseBody)
{
    mprpc::RpcResponseHeader responseHeader;
    responseHeader.set_request_id(requestId);
    responseHeader.set_error_code(errorCode);
    responseHeader.set_error_msg(errorMsg);
    responseHeader.set_response_size(static_cast<uint32_t>(responseBody.size()));

    std::string responseHeaderStr;
    if (!responseHeader.SerializeToString(&responseHeaderStr))
    {
        return "";
    }

    std::string payload;
    payload.reserve(sizeof(uint32_t) + responseHeaderStr.size() + responseBody.size());
    mprpc::AppendNetworkUint32(&payload, static_cast<uint32_t>(responseHeaderStr.size()));
    payload += responseHeaderStr;
    payload += responseBody;

    // 最外层统一套 RPC 帧，交给 Connection::send 后由客户端按 total_len 精确读取。
    return mprpc::BuildRpcFrame(payload);
}

// 服务端解析请求失败时也必须回包，否则客户端只能等超时，压测会表现为“卡住”。
void SendRpcError(const wevix_muduo::TcpServer::ConnectionPtr& conn,
                  uint64_t requestId,
                  mprpc::RpcErrorCode errorCode,
                  const std::string& errorMsg)
{
    std::string sendStr = BuildRpcResponseFrame(requestId, errorCode, errorMsg, "");
    if (!sendStr.empty())
    {
        conn->send(sendStr);
    }
}

// 解析请求 payload：
// [header_size(4B, network order)] + [RpcHeader] + [args]
// 同时校验 header_size 和 args_size，避免坏包越界或把半包当完整包处理。
bool DecodeRequestHeader(const std::string& message,
                         mprpc::RpcHeader& rpcHeader,
                         std::string& argsStr,
                         std::string& errorMsg)
{
    if (message.size() < sizeof(uint32_t))
    {
        errorMsg = "request payload too small";
        return false;
    }

    uint32_t headerSize = 0;
    if (!mprpc::ReadNetworkUint32(message.data(), message.size(), &headerSize))
    {
        errorMsg = "read request header size failed";
        return false;
    }

    if (headerSize == 0 || message.size() - sizeof(uint32_t) < headerSize)
    {
        errorMsg = "invalid request header size:" + std::to_string(headerSize);
        return false;
    }

    std::string rpcHeaderStr = message.substr(sizeof(uint32_t), headerSize);
    if (!rpcHeader.ParseFromString(rpcHeaderStr))
    {
        errorMsg = "parse request rpc header failed";
        return false;
    }

    size_t argsOffset = sizeof(uint32_t) + headerSize;
    size_t remainSize = message.size() - argsOffset;
    if (rpcHeader.args_size() != remainSize)
    {
        // args_size 必须和实际剩余长度一致，否则说明客户端协议或数据已损坏。
        errorMsg = "request args size mismatch, header args_size=" +
                   std::to_string(rpcHeader.args_size()) +
                   ", actual=" + std::to_string(remainSize);
        return false;
    }

    argsStr = message.substr(argsOffset, remainSize);
    return true;
}

std::string MethodRegistryPath(const std::string& serviceName,
                               const std::string& methodName)
{
    return "/mprpc/services/" + serviceName + "/" + methodName;
}

} // namespace

struct RpcProvider::RpcResponseContext
{
    wevix_muduo::TcpServer::ConnectionPtr conn; // 保持连接对象存活，直到回包完成
    google::protobuf::Message* response;        // 业务响应对象，SendRpcResponse 中统一释放
    uint64_t requestId;                         // 原样带回客户端，用于请求/响应匹配
};

void RpcProvider::NotifyService(google::protobuf::Service *service)
{
    ServiceInfo service_info;

    // 获取服务对象的描述信息
    const google::protobuf::ServiceDescriptor *pserviceDesc = service->GetDescriptor();
    // 获取服务的名字
    std::string service_name = pserviceDesc->name();
    // 获取服务对象service的方法的数量
    int methodCnt = pserviceDesc->method_count();

    LOG_INFO("Register service: %s, methods=%d", service_name.c_str(), methodCnt);

    for(int i=0;i<methodCnt;i++)
    {
        // 获取了服务对象指定下标的服务方法的描述（抽象描述）
        const google::protobuf::MethodDescriptor* pmethodDesc = pserviceDesc->method(i);
        std::string method_name = pmethodDesc->name();
        LOG_DEBUG("  - method: %s", method_name.c_str());
        service_info.m_methodMap.insert({method_name,pmethodDesc});
    }
    service_info.m_service=service;
    m_serviceMap.insert({service_name,service_info});
}

bool RpcProvider::Run()
{
    MprpcConfig& config = MprpcApplication::GetInstance().GetConfig();
    std::string ip;
    std::string error;
    if (!config.LoadRequired("rpcserverip", ip, error))
    {
        LOG_ERROR("%s", error.c_str());
        return false;
    }

    int portValue = config.LoadInt("rpcserverport", -1, 1, 65535);
    if (portValue == -1)
    {
        LOG_ERROR("required config key invalid: rpcserverport");
        return false;
    }
    uint16_t port = static_cast<uint16_t>(portValue);

    // 阶段 13：服务发现地址（注册到 ZK）——rpcserverip 为 0.0.0.0/空
    // （Docker 全接口监听）时，探测本机实际 IP，否则消费者连 0.0.0.0 必失败
    std::string advertise_ip = ip;
    if (advertise_ip.empty() || advertise_ip == "0.0.0.0")
    {
        advertise_ip = mprpc::GetLocalIp();
        if (!advertise_ip.empty())
            LOG_INFO("rpcserverip=%s (container mode), advertise ip=%s for service discovery",
                     ip.c_str(), advertise_ip.c_str());
    }

    int ioThreads = config.LoadInt("rpcserverio_threads", 2, 1, 128);
    // 默认 work pool 保持小规模，WSL/测试环境友好，生产环境按需调大
    int defaultWorkThreads = 2;
    int workThreads = config.LoadInt("rpcserverwork_threads",
                                     defaultWorkThreads, 0, 256);

    // 创建 TcpServer 对象（wevix_muduo 内部自动管理 EventLoop）
    // 构造函数：(ip, port, threadNum)，IO 线程数可通过 rpcserverio_threads 配置。
    wevix_muduo::TcpServer server(ip, port, ioThreads);

    // 绑定连接回调和消息读写回调的方法
    server.setConnectionCallback(
        std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setOnMessageCallback(
        std::bind(&RpcProvider::OnMessage, this,
                  std::placeholders::_1, std::placeholders::_2));
    server.setCloseCallback(
        std::bind(&RpcProvider::OnClose, this, std::placeholders::_1));

    if (workThreads > 0)
    {
        // 业务 protobuf service 放到 work pool 执行，避免慢业务阻塞 IO 线程。
        server.enableWorkPool(workThreads, wevix_muduo::PoolMode::MODE_FIXED);
    }

    // 把当前rpc节点上要发布的服务全部注册到zk上面，让rpc client可以从zk上发现服务
    ZkClient zkCli;
    if (!zkCli.Start())
    {
        LOG_ERROR("RpcProvider start failed: connect zookeeper failed");
        return false;
    }

    // 多实例注册路径：
    // /mprpc/services/{service}/{method}/instance-0000000001 -> ip:port
    // 容器冷启动时 ZK 刚就绪（healthcheck ruok 通过但服务器瞬时过载/会话窗口），
    // 实测 zoo_create 会返回 ZOPERATIONTIMEOUT(-110)；一次失败直接退出会导致
    // 容器 restart 循环 + compose 依赖健康检查中止。此处退避重试 3 次兜底，
    // 恢复后随容器 restart 周期收敛（实测冷启动窗口仅数秒）。
    bool rootOk = false;
    for (int attempt = 1; attempt <= 3 && !rootOk; ++attempt)
    {
        rootOk = zkCli.Create("/mprpc", nullptr, 0) &&
                 zkCli.Create("/mprpc/services", nullptr, 0);
        if (!rootOk)
        {
            LOG_WARN("create root registry path failed, retry %d/3 after 1s...", attempt);
            sleep(1);
        }
    }
    if (!rootOk)
    {
        LOG_ERROR("RpcProvider start failed: create root registry path failed");
        return false;
    }

    // service_name 和 method_name 为永久节点，instance-* 为临时顺序节点。
    for (auto &sp : m_serviceMap)
    {
        // 组织服务路径 /mprpc/services/{service_name}
        std::string service_path = "/mprpc/services/" + sp.first;
        if (!zkCli.Create(service_path.c_str(), nullptr, 0))
        {
            LOG_ERROR("create service registry path failed: %s", service_path.c_str());
            return false;
        }

        for (auto &mp : sp.second.m_methodMap)
        {
            // 组织方法路径 /mprpc/services/{service}/{method}
            std::string method_path = MethodRegistryPath(sp.first, mp.first);
            if (!zkCli.Create(method_path.c_str(), nullptr, 0))
            {
                LOG_ERROR("create method registry path failed: %s", method_path.c_str());
                return false;
            }

            char method_path_data[128] = {0};
            // 临时顺序节点存储当前服务实例地址。顺序节点天然支持同一方法多实例。
            sprintf(method_path_data, "%s:%d", advertise_ip.c_str(), port);

            std::string instance_path = method_path + "/instance-";
            std::string actualPath;
            if (!zkCli.Create(instance_path.c_str(), method_path_data, strlen(method_path_data),
                              ZOO_EPHEMERAL | ZOO_SEQUENCE, &actualPath))
            {
                LOG_ERROR("create service instance failed: %s", instance_path.c_str());
                return false;
            }
            LOG_INFO("Register rpc instance: %s -> %s", actualPath.c_str(), method_path_data);
        }
    }

    // rpc服务端准备启动，打印信息
    LOG_INFO("RpcProvider start service at ip:%s port:%u, io_threads=%d, work_threads=%d",
             ip.c_str(), port, ioThreads, workThreads);

    // 设置帧编解码器：让 muduo 在 Connection 层自动处理粘包/拆包
    // OnMessage 回调保证收到完整一帧，无需再手动判断帧边界
    server.setMessageCodec(RpcMessageCodec);

    // 启动网络服务（内部启动 mainLoop + subLoops）
    server.start();
    LOG_INFO("RpcProvider service stopped.");
    return true;
}

// 新连接建立回调（wevix_muduo 的 connectionCallback 仅在新连接时触发）
void RpcProvider::OnConnection(const wevix_muduo::TcpServer::ConnectionPtr& conn)
{
    LOG_DEBUG("New connection from %s:%u", conn->ip().c_str(), conn->port());
}

// 连接关闭回调（原 muduo 在 OnConnection 里判断 !connected() 来 shutdown，现拆分为独立回调）
void RpcProvider::OnClose(const wevix_muduo::TcpServer::ConnectionPtr& conn)
{
    LOG_DEBUG("Connection closed: %s:%u", conn->ip().c_str(), conn->port());
}

void RpcProvider::OnMessage(const wevix_muduo::TcpServer::ConnectionPtr& conn,
                             std::string& message)
{
    // muduo 已通过 RpcMessageCodec 完成帧提取，message 保证是完整一帧
    // 请求 payload 格式：[header_size(4B, network order)] + [RpcHeader] + [args]

    mprpc::RpcHeader rpcHeader;
    std::string args_str;
    std::string errorMsg;
    if (!DecodeRequestHeader(message, rpcHeader, args_str, errorMsg))
    {
        LOG_ERROR("bad rpc request from %s:%u: %s",
                  conn->ip().c_str(), conn->port(), errorMsg.c_str());
        // header 都无法解析时拿不到 request_id，只能用 0 表示未知请求。
        SendRpcError(conn, 0, mprpc::RPC_BAD_REQUEST, errorMsg);
        return;
    }

    uint64_t requestId = rpcHeader.request_id();
    std::string service_name = rpcHeader.service_name();
    std::string method_name = rpcHeader.method_name();

    // 打印调试信息
    LOG_DEBUG("RPC request: request_id=%llu, service=%s, method=%s, args_size=%u",
              static_cast<unsigned long long>(requestId),
              service_name.c_str(), method_name.c_str(), rpcHeader.args_size());

    // 检查请求是否已过期：客户端通过 RpcHeader.deadline_ms 传递绝对截止时间戳（毫秒），
    // 如果请求在 work pool 排队后已经超过 deadline，直接丢弃并返回 RPC_TIMEOUT，
    // 避免做无效计算。
    // deadline_ms == 0 表示客户端未设置（不检查）。
    if (rpcHeader.deadline_ms() > 0)
    {
        uint64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if (nowMs > rpcHeader.deadline_ms())
        {
            LOG_DEBUG("RPC request expired: request_id=%llu, deadline=%llu, now=%llu",
                      static_cast<unsigned long long>(requestId),
                      static_cast<unsigned long long>(rpcHeader.deadline_ms()),
                      static_cast<unsigned long long>(nowMs));
            SendRpcError(conn, requestId, mprpc::RPC_TIMEOUT,
                        "request deadline exceeded before processing");
            return;
        }
    }

    if (service_name.empty() || method_name.empty())
    {
        std::string err = "empty service or method name";
        LOG_ERROR("bad rpc request_id=%llu: %s",
                  static_cast<unsigned long long>(requestId), err.c_str());
        // 服务名或方法名为空属于协议层坏请求，不进入业务分发。
        SendRpcError(conn, requestId, mprpc::RPC_BAD_REQUEST, err);
        return;
    }

    // 获取service对象和method对象
    auto it = m_serviceMap.find(service_name);
    if(it == m_serviceMap.end())
    {
        std::string err = "service not found:" + service_name;
        LOG_ERROR("%s", err.c_str());
        // 这里回包比只打日志更重要，客户端才能立刻失败并拿到明确原因。
        SendRpcError(conn, requestId, mprpc::RPC_SERVICE_NOT_FOUND, err);
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if(mit == it->second.m_methodMap.end())
    {
        std::string err = "method not found:" + service_name + "." + method_name;
        LOG_ERROR("%s", err.c_str());
        SendRpcError(conn, requestId, mprpc::RPC_METHOD_NOT_FOUND, err);
        return;
    }

    google::protobuf::Service *service = it->second.m_service;
    const google::protobuf::MethodDescriptor *method = mit->second;

    // 动态创建请求对象 (Request) — 使用 unique_ptr 自动管理生命周期
    std::unique_ptr<google::protobuf::Message> request(
        service->GetRequestPrototype(method).New());

    // 反序列化请求参数
    if (!request->ParseFromString(args_str))
    {
        std::string err = "parse request args failed, args_size=" + std::to_string(args_str.size());
        LOG_ERROR("%s", err.c_str());
        // 业务参数反序列化失败，说明请求体不是该方法期望的 protobuf 类型。
        SendRpcError(conn, requestId, mprpc::RPC_REQUEST_PARSE_FAILED, err);
        return;
    }

    // 动态创建响应对象 (Response) — 使用 unique_ptr 自动管理生命周期
    std::unique_ptr<google::protobuf::Message> response(
        service->GetResponsePrototype(method).New());

    // 提前获取原始指针，因为 release() 后 unique_ptr 变为空
    google::protobuf::Message* rawResponse = response.get();

    // 绑定回调函数（Closure）
    // 当业务层处理完业务后调用 done->Run()，实际执行 SendRpcResponse
    // 传递 response 原始指针，由 SendRpcResponse 负责删除
    // Closure 可能在业务实现里异步执行，所以把响应对象所有权交给 context。
    auto* context = new RpcResponseContext{conn, response.release(), requestId};
    google::protobuf::Closure *done =
        google::protobuf::NewCallback<RpcProvider,
                                      RpcResponseContext*>
                                      (this,
                                       &RpcProvider::SendRpcResponse,
                                       context);

    // 在框架上根据远端 RPC 请求，调用当前 RPC 节点上发布的方法
    // request/rawResponse 传递原始指针，CallMethod 返回后 request 自动释放
    service->CallMethod(method, nullptr, request.get(), rawResponse, done);
}

void RpcProvider::SendRpcResponse(RpcResponseContext* context)
{
    // context 和 response 都在这里统一接管，保证成功/失败路径不会泄漏。
    std::unique_ptr<RpcResponseContext> autoReleaseContext(context);
    // 使用 unique_ptr 确保 response 对象在任何路径下都能被释放
    std::unique_ptr<google::protobuf::Message> autoRelease(context->response);

    std::string response_str;

    // 序列化 response 对象
    if (context->response->SerializeToString(&response_str))
    {
        if (response_str.size() > mprpc::kRpcMaxFrameSize)
        {
            std::string err = "response frame too large:" + std::to_string(response_str.size());
            LOG_ERROR("%s", err.c_str());
            // 响应过大时仍然尝试返回错误头，让客户端不用等到读超时。
            SendRpcError(context->conn, context->requestId, mprpc::RPC_FRAME_TOO_LARGE, err);
            return;
        }

        // 响应帧格式：[total_len] + [response_header_size + RpcResponseHeader + response_data]
        std::string send_str = BuildRpcResponseFrame(context->requestId, mprpc::RPC_SUCCESS, "", response_str);
        if (send_str.empty())
        {
            LOG_ERROR("serialize response header error!");
            return;
        }

        // 通过 wevix_muduo 网络库发送
        context->conn->send(send_str);
        // 不主动调用 conn->shutdown()
        // 原因：send() 是异步的（可能进入 outputBuffer），shutdown() 是同步的
        //       若数据未发完就 shutdown，handleWrite 对已关闭 socket 调用 send()
        //       会触发 EPIPE 错误，且导致数据丢失
        // 客户端按 response_size 精确读完响应后自行关闭连接，这是安全的主动关闭
    }
    else
    {
        LOG_ERROR("serialize response_str error!");
        SendRpcError(context->conn, context->requestId, mprpc::RPC_RESPONSE_SERIALIZE_FAILED,
                     "serialize response body failed");
    }
}
