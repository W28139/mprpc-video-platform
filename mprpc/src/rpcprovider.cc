#include"rpcprovider.h"
#include"rpcheader.pb.h"
#include"wevix_muduo/AsyncLogger.h"
#include"ZookeeperUtil.h"
#include"mprpccodec.h"

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

void RpcProvider::Run()
{
    std::string ip =  MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());

    // 创建 TcpServer 对象（wevix_muduo 内部自动管理 EventLoop）
    // 构造函数：(ip, port, threadNum)，这里设置 4 个 IO 线程
    wevix_muduo::TcpServer server(ip, port, 4);

    // 绑定连接回调和消息读写回调的方法
    server.setConnectionCallback(
        std::bind(&RpcProvider::OnConnection, this, std::placeholders::_1));
    server.setOnMessageCallback(
        std::bind(&RpcProvider::OnMessage, this,
                  std::placeholders::_1, std::placeholders::_2));
    server.setCloseCallback(
        std::bind(&RpcProvider::OnClose, this, std::placeholders::_1));

    // 把当前rpc节点上要发布的服务全部注册到zk上面，让rpc client可以从zk上发现服务
    ZkClient zkCli;
    zkCli.Start();

    // service_name为永久性节点    method_name为临时性节点
    // session timeout 30s
    for (auto &sp : m_serviceMap)
    {
        // 组织服务路径 /service_name
        std::string service_path = "/" + sp.first;
        zkCli.Create(service_path.c_str(), nullptr, 0); // 0表示永久性节点

        for (auto &mp : sp.second.m_methodMap)
        {
            // 组织方法路径 /service_name/method_name  存储当前这个rpc服务节点主机的ip和port
            std::string method_path = service_path + "/" + mp.first;
            char method_path_data[128] = {0};
            // 节点存储的数据是当前提供该服务的ip和port
            sprintf(method_path_data, "%s:%d", ip.c_str(), port);

            // ZOO_EPHEMERAL表示设置为临时性节点
            zkCli.Create(method_path.c_str(), method_path_data, strlen(method_path_data), ZOO_EPHEMERAL);
        }
    }

    // rpc服务端准备启动，打印信息
    LOG_INFO("RpcProvider start service at ip:%s port:%u", ip.c_str(), port);

    // 设置帧编解码器：让 muduo 在 Connection 层自动处理粘包/拆包
    // OnMessage 回调保证收到完整一帧，无需再手动判断帧边界
    server.setMessageCodec(RpcMessageCodec);

    // 启动网络服务（内部启动 mainLoop + subLoops）
    server.start();
    LOG_INFO("RpcProvider service stopped.");
}

// 新连接建立回调（wevix_muduo 的 connectionCallback 仅在新连接时触发）
void RpcProvider::OnConnection(const wevix_muduo::TcpServer::ConnectionPtr& conn)
{
    LOG_INFO("New connection from %s:%u", conn->ip().c_str(), conn->port());
}

// 连接关闭回调（原 muduo 在 OnConnection 里判断 !connected() 来 shutdown，现拆分为独立回调）
void RpcProvider::OnClose(const wevix_muduo::TcpServer::ConnectionPtr& conn)
{
    LOG_INFO("Connection closed: %s:%u", conn->ip().c_str(), conn->port());
}

void RpcProvider::OnMessage(const wevix_muduo::TcpServer::ConnectionPtr& conn,
                             std::string& message)
{
    // muduo 已通过 RpcMessageCodec 完成帧提取，message 保证是完整一帧
    // 帧格式：[header_size(4B)] + [RpcHeader] + [args]

    // 1. 提取 header_size（头长度）
    uint32_t header_size = 0;
    message.copy((char*)&header_size, 4, 0);

    // 2. 根据 header_size 截取 RpcHeader 二进制，反序列化
    std::string rpc_header_str = message.substr(4, header_size);
    mprpc::RpcHeader rpcHeader;

    std::string service_name;
    std::string method_name;
    uint32_t args_size;

    if (!rpcHeader.ParseFromString(rpc_header_str))
    {
        LOG_ERROR("rpc_header_str parse error!");
        return;
    }

    service_name = rpcHeader.service_name();
    method_name = rpcHeader.method_name();
    args_size = rpcHeader.args_size();

    // 3. 提取业务参数二进制流
    std::string args_str = message.substr(4 + header_size, args_size);

    // 打印调试信息
    LOG_DEBUG("RPC request: header_size=%u, service=%s, method=%s, args_size=%u",
              header_size, service_name.c_str(), method_name.c_str(), args_size);

    // 获取service对象和method对象
    auto it = m_serviceMap.find(service_name);
    if(it == m_serviceMap.end())
    {
        LOG_ERROR("%s is not exist", service_name.c_str());
        return;
    }

    auto mit = it->second.m_methodMap.find(method_name);
    if(mit == it->second.m_methodMap.end())
    {
        LOG_ERROR("%s is not exist", method_name.c_str());
        return;
    }

    google::protobuf::Service *service = it->second.m_service;
    const google::protobuf::MethodDescriptor *method = mit->second;

    // 动态创建请求对象 (Request)
    google::protobuf::Message *request = service->GetRequestPrototype(method).New();

    // 反序列化请求参数
    if (!request->ParseFromString(args_str))
    {
        LOG_ERROR("request parse error, content_len=%zu", args_str.size());
        return;
    }

    // 动态创建响应对象 (Response)
    google::protobuf::Message *response = service->GetResponsePrototype(method).New();

    // 绑定回调函数（Closure）
    // 当业务层处理完业务后调用 done->Run()，实际执行 SendRpcResponse
    google::protobuf::Closure *done =
        google::protobuf::NewCallback<RpcProvider,
                                      const wevix_muduo::TcpServer::ConnectionPtr&,
                                      google::protobuf::Message*>
                                      (this,
                                       &RpcProvider::SendRpcResponse,
                                       conn, response);

    // 在框架上根据远端 RPC 请求，调用当前 RPC 节点上发布的方法
    service->CallMethod(method, nullptr, request, response, done);
}

void RpcProvider::SendRpcResponse(const wevix_muduo::TcpServer::ConnectionPtr& conn,
                                   google::protobuf::Message* response)
{
    std::string response_str;

    // 序列化 response 对象
    if (response->SerializeToString(&response_str))
    {
        // 响应帧格式：[response_size(4字节二进制)] + [response_data]
        // 客户端按长度精确读取，无需依赖 FIN/SHUT_WR，避免 SIGPIPE
        uint32_t response_size = response_str.size();
        std::string send_str;
        send_str.insert(0, std::string((char*)&response_size, 4));
        send_str += response_str;

        // 通过 wevix_muduo 网络库发送
        conn->send(send_str);
        // 不再调用 conn->shutdown()，客户端读完 response_size 字节后自行关闭连接
        // 服务端依赖 epoll 超时或客户端关闭来清理连接，彻底避免 SIGPIPE
    }
    else
    {
        LOG_ERROR("serialize response_str error!");
    }
}