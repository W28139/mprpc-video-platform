#include"mprpcchannel.h"
#include <google/protobuf/service.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include "rpcheader.pb.h"
#include"mprpcapplication.h"
#include"mprpccontroller.h"
#include"ZookeeperUtil.h"
#include"wevix_muduo/AsyncLogger.h"
// 客户端间接调用，借助protubuf，序列化需求，找到对应服务器ip/port，发起连接，获取结果并反序列化

void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                                google::protobuf::RpcController* controller, 
                                const google::protobuf::Message* request,
                                google::protobuf::Message* response, 
                                google::protobuf::Closure* done)
{
    const google::protobuf::ServiceDescriptor* sd = method->service();
    std::string service_name = sd->name();
    std::string method_name = method->name();

    // 1. 获取参数的序列化字符串长度 args_size
    uint32_t args_size = 0;
    std::string args_str;
    // 进行序列化
    if (request->SerializeToString(&args_str))
    {
        args_size = args_str.size();
    }
    else
    {
        controller->SetFailed("serialize request error!");
        return;
    }

    // 2. 定义并序列化 rpc 的请求 header
    mprpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if (rpcHeader.SerializeToString(&rpc_header_str))
    {
        header_size = rpc_header_str.size();
    }
    else
    {
        controller->SetFailed("serialize rpc header error!");
        return;
    }

    // 3. 组织待发送的 rpc 请求的字符串
    // 帧格式：[total_len(4字节)] + [header_size(4字节) + rpc_header_str + args_str]
    // total_len 使得服务端 muduo 的 RpcMessageCodec 能从 Buffer 中正确提取完整帧
    std::string send_rpc_str;
    send_rpc_str.insert(0, std::string((char*)&header_size, 4)); // header_size 二进制
    send_rpc_str += rpc_header_str; // 头部
    send_rpc_str += args_str;        // 参数

    // 在帧最前面加上 total_len 前缀，供服务端 codec 做帧提取
    uint32_t total_len = send_rpc_str.size(); // header_size字段 + rpc_header_str + args_str
    send_rpc_str.insert(0, std::string((char*)&total_len, 4));

    // 4. 使用 TCP 编程，完成 rpc 方法的远程调用 (这里通常使用同步阻塞模式)
    // 实际生产环境会从配置文件读取服务端 IP 和 Port
    int clientfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == clientfd)
    {
        LOG_ERROR("rpc call %s::%s create socket error! errno=%d", service_name.c_str(), method_name.c_str(), errno);
        controller->SetFailed("create socket error! errno:" + std::to_string(errno));
        return;
    }

    // std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    // uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    ZkClient zkCli;
    zkCli.Start();

    std::string method_path = "/" + service_name + "/" + method_name;
    LOG_DEBUG("querying path [%s]", method_path.c_str());

    std::string host_data = zkCli.GetData(method_path.c_str());
    LOG_DEBUG("got host_data [%s]", host_data.c_str());
    
    if (host_data == "")
    {
        LOG_ERROR("rpc call %s::%s: zk path %s not found", service_name.c_str(), method_name.c_str(), method_path.c_str());
        controller->SetFailed(method_path + " is not exist!");
        return;
    }

    int idx = host_data.find(":");
    if (idx == -1)
    {
        LOG_ERROR("rpc call %s::%s: invalid host_data [%s]", service_name.c_str(), method_name.c_str(), host_data.c_str());
        controller->SetFailed(method_path + " address is invalid!");
        return;
    }

    std::string ip = host_data.substr(0, idx);
    uint16_t port = atoi(host_data.substr(idx+1, host_data.size()-idx).c_str());

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port); // 示例端口
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str()); // 示例IP

    // 连接 rpc 服务节点
    if (-1 == connect(clientfd, (struct sockaddr*)&server_addr, sizeof(server_addr)))
    {
        LOG_ERROR("rpc call %s::%s connect to %s:%d failed, errno=%d", service_name.c_str(), method_name.c_str(), ip.c_str(), port, errno);
        close(clientfd);
        controller->SetFailed("connect error! errno:" + std::to_string(errno));
        return;
    }

    // 发送 rpc 请求
    if (-1 == send(clientfd, send_rpc_str.c_str(), send_rpc_str.size(), 0))
    {
        LOG_ERROR("rpc call %s::%s send failed, fd=%d, errno=%d", service_name.c_str(), method_name.c_str(), clientfd, errno);
        close(clientfd);
        controller->SetFailed("send error! errno:" + std::to_string(errno));
        return;
    }

    // 5. 接收 rpc 调用的响应值
    // 响应帧格式：[response_size(4字节二进制)] + [response_data]
    // 先读 4 字节获取长度，再精确读取对应长度的数据

    // 5a. 读取 4 字节的 response_size 头
    uint32_t response_size = 0;
    int total_read = 0;
    while (total_read < 4)
    {
        int n = recv(clientfd, (char*)&response_size + total_read, 4 - total_read, 0);
        if (n <= 0)
        {
            if (n == 0)
            {
                LOG_ERROR("rpc call %s::%s: server closed before sending response header", service_name.c_str(), method_name.c_str());
                controller->SetFailed("server closed connection before sending response header!");
            }
            else
            {
                LOG_ERROR("rpc call %s::%s: recv response header failed, errno=%d", service_name.c_str(), method_name.c_str(), errno);
                controller->SetFailed("recv response header error! errno:" + std::to_string(errno));
            }
            close(clientfd);
            return;
        }
        total_read += n;
    }

    // 5b. 根据 response_size 精确读取响应数据
    std::string recv_str;
    recv_str.reserve(response_size);
    total_read = 0;
    char recv_buf[1024];
    while (total_read < (int)response_size)
    {
        int to_read = std::min((int)sizeof(recv_buf), (int)response_size - total_read);
        int n = recv(clientfd, recv_buf, to_read, 0);
        if (n <= 0)
        {
            if (n == 0)
            {
                LOG_ERROR("rpc call %s::%s: server closed before sending full response", service_name.c_str(), method_name.c_str());
                controller->SetFailed("server closed connection before sending full response!");
            }
            else
            {
                LOG_ERROR("rpc call %s::%s: recv response body failed, errno=%d", service_name.c_str(), method_name.c_str(), errno);
                controller->SetFailed("recv response body error! errno:" + std::to_string(errno));
            }
            close(clientfd);
            return;
        }
        recv_str.append(recv_buf, n);
        total_read += n;
    }

    // 6. 反序列化 rpc 响应
    if (!response->ParseFromString(recv_str))
    {
        LOG_ERROR("rpc call %s::%s: parse response failed, response_size=%u", service_name.c_str(), method_name.c_str(), response_size);
        close(clientfd);
        controller->SetFailed("parse error! response_str:" + recv_str);
        return;
    }

    close(clientfd);
}