# mprpc 当前框架梳理与使用说明

## 1. 当前定位

`mprpc` 是一个基于自研 `wevix_muduo` 网络库和 Protobuf 的 C++ RPC 框架。

它的作用是把本地 C++ service 方法包装成远程可调用的方法，让调用方可以像调用本地函数一样，通过 Protobuf 生成的 Stub 发起远程调用。

当前框架更适合承担业务系统里的“服务间控制面通信”，例如：

- 调度器调用 Worker：下发任务、查询任务状态、取消任务
- Worker 调用管理服务：注册节点、上报心跳、上报执行结果
- Job 服务调用元数据服务：创建任务、更新状态、查询进度

对于视频转码平台这类项目，RPC 不应该直接传输大视频文件本体，而应该传输任务 ID、文件路径、对象存储地址、转码参数、状态信息等控制数据。大文件传输应交给文件系统、对象存储或专门的数据传输模块。

---

## 2. 当前具备的功能

### 2.0 源码结构与核心类职责

核心目录：

```text
mprpc/include/
mprpc/src/
```

主要文件职责：

```text
mprpcapplication.h/.cc
    框架初始化入口，解析 -i config，加载并校验配置。

mprpcconfig.h/.cc
    配置文件解析，提供 Load、LoadRequired、LoadInt 等接口。

mprpcchannel.h/.cc
    客户端 RPC 通道，继承 google::protobuf::RpcChannel。
    所有 Stub 调用最终都会进入 MprpcChannel::CallMethod()。

rpcprovider.h/.cc
    服务端 RPC 发布器，负责注册 Service、启动 TcpServer、注册 ZK 节点、分发请求、发送响应。

ZookeeperUtil.h/.cc
    ZooKeeper C API 的轻量封装，负责 Start、Create、GetData、GetChildren。

mprpccodec.h
    RPC frame 编解码工具，定义 magic/version、最大帧大小、网络字节序读写、粘包/半包解析。

mprpccontroller.h/.cc
    客户端调用控制器，保存失败状态、错误码、错误文本和超时配置。

rpcheader.proto
    RPC 框架协议头定义，包括 RpcHeader、RpcResponseHeader、RpcErrorCode。
```

核心函数职责：

```text
MprpcApplication::Init()
    初始化框架，加载配置，校验 zookeeperip/zookeeperport。

RpcProvider::NotifyService()
    读取 protobuf service 描述，建立 service/method 到对象和方法描述符的映射。

RpcProvider::Run()
    读取 server 配置，启动 TcpServer/work pool，向 ZK 注册服务实例。

RpcProvider::OnMessage()
    解析请求，查找 service/method，反序列化业务 request，调用业务方法。

RpcProvider::SendRpcResponse()
    序列化业务 response，封装 RpcResponseHeader，发送响应帧。

MprpcChannel::CallMethod()
    客户端调用核心：序列化请求、服务发现、连接池发送、读取响应、错误处理。

RpcMessageCodec()
    从 wevix_muduo::Buffer 中提取完整 RPC 帧，处理 TCP 粘包/半包。

ZkClient::Start()
    建立 ZooKeeper 会话，最多等待 5 秒，失败返回 false。

ZkClient::Create()
    创建永久节点、临时节点或临时顺序节点。

ZkClient::GetChildren()
    获取方法节点下的所有服务实例。

MprpcController::SetFailed()
    保存结构化错误码和错误文本。

MprpcController::SetTimeoutMs()
    设置本次同步 RPC 调用超时时间。
```

### 2.1 基于 Protobuf 的 RPC 接口描述

业务服务通过 `.proto` 定义：

```protobuf
syntax = "proto3";

package rpc_test;

option cc_generic_services = true;

message EchoRequest {
    bytes payload = 1;
}

message EchoResponse {
    bytes payload = 1;
}

service EchoService {
    rpc Echo(EchoRequest) returns (EchoResponse);
}
```

关键点是：

```protobuf
option cc_generic_services = true;
```

这个选项会让 Protobuf 生成 C++ 的 `Service`、`Stub`、`MethodDescriptor` 等 RPC 相关类。`mprpc` 正是基于这些通用接口完成服务发布和远程调用。

### 2.2 Provider 服务发布

服务端通过 `RpcProvider` 发布服务：

```cpp
RpcProvider provider;
provider.NotifyService(new EchoServiceImpl());
provider.Run();
```

`NotifyService()` 会读取 Protobuf 生成类里的 service 描述信息，把：

- service name
- method name
- method descriptor
- service 对象指针

注册到 `RpcProvider::m_serviceMap` 中。

`Run()` 会：

- 读取配置
- 启动 TCP 服务
- 启用业务 work pool
- 注册 ZooKeeper 服务节点
- 进入事件循环，等待 RPC 请求

### 2.3 Consumer 远程调用

客户端通过 Protobuf 生成的 Stub 发起调用：

```cpp
fixbug::UserServiceRpc_Stub stub(new MprpcChannel());

fixbug::LoginRequest request;
request.set_name("111");
request.set_pwd("123456");

fixbug::LoginResponse response;
MprpcController controller;
controller.SetTimeoutMs(3000);

stub.Login(&controller, &request, &response, nullptr);

if (controller.Failed()) {
    LOG_ERROR("rpc failed, code=%d, error=%s",
              controller.ErrorCode(),
              controller.ErrorText().c_str());
}
```

业务代码看起来是在调用：

```cpp
stub.Login(...)
```

实际会进入：

```cpp
MprpcChannel::CallMethod(...)
```

`MprpcChannel` 负责：

- 序列化请求
- 服务发现
- 选择服务实例
- 获取或创建 TCP 长连接
- 发送完整 RPC 帧
- 接收完整响应帧
- 解析框架响应头
- 反序列化业务响应
- 把错误写入 `MprpcController`

### 2.4 ZooKeeper 服务注册与发现

服务端注册路径：

```text
/mprpc/services/{service}/{method}/instance-*
```

例如：

```text
/mprpc/services/EchoService/Echo/instance-0000000001 -> 127.0.0.1:8000
/mprpc/services/EchoService/Echo/instance-0000000002 -> 127.0.0.1:8001
```

其中：

- `/mprpc` 是根节点
- `/mprpc/services` 是服务根路径
- `{service}` 是 Protobuf service 名
- `{method}` 是 Protobuf method 名
- `instance-*` 是临时顺序节点
- 节点数据目前是 `ip:port`

临时顺序节点的意义：

- 服务进程退出后，临时节点自动消失
- 同一个方法可以有多个实例
- 客户端可以读取 children 做负载均衡

客户端发现流程：

```text
先查本地缓存
缓存没有，再查 ZooKeeper children
拿到 instance-* 子节点
读取每个子节点的数据 ip:port
按轮询选择一个 endpoint
```

为了兼容旧路径，客户端仍然保留：

```text
/{service}/{method} -> ip:port
```

作为兜底。

### 2.5 客户端连接池

`MprpcChannel` 当前已经不是每次 RPC 都新建 TCP 连接。

连接池模型：

```text
endpoint(ip:port) -> 多条 PooledConnection
```

默认每个 endpoint 最多 8 条连接，可通过配置修改：

```text
mprpcclient_connections_per_endpoint=8
```

每条连接内部用 mutex 串行化请求：

```text
lock connection
send request frame
recv response frame
unlock connection
```

当前没有做真正的多路复用。也就是说，同一条 TCP 连接上不会同时并发多个 in-flight 请求。这样设计简单，能避免响应乱序处理复杂化。`request_id` 目前用于校验响应是否属于当前请求，而不是用于连接内并发响应分发。

### 2.6 结构化错误码

错误码定义在：

```text
mprpc/src/rpcheader.proto
```

当前包括：

```text
RPC_SUCCESS
RPC_BAD_REQUEST
RPC_HEADER_PARSE_FAILED
RPC_SERVICE_NOT_FOUND
RPC_METHOD_NOT_FOUND
RPC_REQUEST_PARSE_FAILED
RPC_RESPONSE_SERIALIZE_FAILED
RPC_CONNECT_FAILED
RPC_SEND_FAILED
RPC_RECV_FAILED
RPC_RESPONSE_PARSE_FAILED
RPC_TIMEOUT
RPC_FRAME_TOO_LARGE
RPC_INVALID_RESPONSE
RPC_SERVICE_DISCOVERY_FAILED
```

客户端通过 `MprpcController` 获取：

```cpp
controller.Failed()
controller.ErrorCode()
controller.ErrorText()
```

这比只返回字符串更有用，因为后续可以基于错误码做：

- 重试
- 降级
- 熔断
- 统计
- 日志分类

### 2.7 超时控制

客户端可以设置单次 RPC 超时：

```cpp
MprpcController controller;
controller.SetTimeoutMs(3000);
```

当前超时作用在：

- TCP connect
- socket send
- socket recv

长连接复用时，每次调用都会刷新当前 socket 的超时选项，避免上一轮调用的超时配置影响下一轮调用。

如果不设置，默认超时为：

```text
5000ms
```

### 2.8 服务端业务线程池

`RpcProvider::Run()` 会启动 `wevix_muduo::TcpServer`，并默认启用 work pool。

配置项：

```text
rpcserverio_threads=16
rpcserverwork_threads=8
```

含义：

- IO 线程负责 accept、read、write、编解码等网络事件
- work 线程负责执行业务 `service->CallMethod()`

这样慢业务不会直接阻塞 IO 线程。

---

## 3. 配置文件

示例：

```text
rpcserverip=127.0.0.1
rpcserverport=8000
zookeeperip=127.0.0.1
zookeeperport=2181
rpcserverio_threads=16
rpcserverwork_threads=8
mprpcclient_connections_per_endpoint=8
```

必填配置：

- `zookeeperip`
- `zookeeperport`

Provider 端额外必填：

- `rpcserverip`
- `rpcserverport`

可选配置：

- `rpcserverio_threads`
- `rpcserverwork_threads`
- `mprpcclient_connections_per_endpoint`

启动时通过 `-i` 指定配置：

```bash
./bin/rpc_echo_server -i test/rpc_test.conf
./bin/user_consumer -i example/callee/test.conf
```

`MprpcApplication::Init(argc, argv)` 当前返回 `bool`：

```cpp
if (!MprpcApplication::Init(argc, argv)) {
    return EXIT_FAILURE;
}
```

框架内部不再直接 `exit()`，由业务 main 决定失败后怎么处理。

---

## 4. RPC 协议格式

### 4.1 外层 frame

所有 RPC 请求和响应都使用同一个外层帧格式：

```text
[total_len(4B, network order)] + [magic(2B)] + [version(2B)] + [payload]
```

字段含义：

```text
total_len: magic + version + payload 的总长度
magic:     当前固定为 0x4d52，也就是 "MR"
version:   当前为 1
payload:   请求 payload 或响应 payload
```

当前最大帧大小：

```text
64MB
```

相关函数：

```cpp
mprpc::BuildRpcFrame()
mprpc::DecodeRpcFramePayload()
RpcMessageCodec()
```

`RpcMessageCodec()` 运行在 `wevix_muduo::Connection` 的读路径中，负责处理 TCP 粘包和半包。

### 4.2 请求 payload

请求 payload 格式：

```text
[rpc_header_size(4B, network order)] + [RpcHeader] + [request_body]
```

`RpcHeader` 内容：

```protobuf
message RpcHeader {
    bytes service_name = 1;
    bytes method_name = 2;
    uint32 args_size = 3;
    uint64 request_id = 4;
    bytes trace_id = 5;
    uint64 deadline_ms = 6;
}
```

当前已经实际使用：

- `service_name`
- `method_name`
- `args_size`
- `request_id`
- `deadline_ms`

`trace_id` 字段已预留，但还没有在框架中自动生成和传播。

### 4.3 响应 payload

响应 payload 格式：

```text
[response_header_size(4B, network order)] + [RpcResponseHeader] + [response_body]
```

`RpcResponseHeader` 内容：

```protobuf
message RpcResponseHeader {
    uint64 request_id = 1;
    RpcErrorCode error_code = 2;
    bytes error_msg = 3;
    uint32 response_size = 4;
}
```

客户端收到响应后会校验：

- frame 长度是否合法
- magic 是否正确
- version 是否支持
- response header 是否能解析
- `response_size` 是否等于实际 body 长度
- `responseHeader.request_id()` 是否等于本次请求的 `request_id`
- `error_code` 是否为 `RPC_SUCCESS`

任一校验失败，都会通过 `MprpcController` 返回框架错误。

---

## 5. 一次 RPC 的完整调用流程

### 5.1 客户端流程

```text
业务代码调用 stub.Method()
        |
        v
Protobuf Stub 调用 MprpcChannel::CallMethod()
        |
        v
序列化业务 request
        |
        v
构造 RpcHeader
service_name / method_name / args_size / request_id / deadline_ms
        |
        v
构造请求 payload
[header_size] + [RpcHeader] + [request_body]
        |
        v
BuildRpcFrame()
[total_len] + [magic] + [version] + [payload]
        |
        v
服务发现
本地缓存 -> ZooKeeper -> 多实例 endpoint 列表 -> 轮询选一个 endpoint
        |
        v
从 endpoint 连接池取连接
        |
        v
SendAll() 发送完整请求帧
        |
        v
RecvAll() 读取响应长度和响应体
        |
        v
DecodeRpcFramePayload() 校验 magic/version
        |
        v
解析 RpcResponseHeader
        |
        v
校验 request_id / error_code / response_size
        |
        v
反序列化业务 response
        |
        v
返回业务代码
```

如果连接失败、发送失败、接收失败或超时，客户端会：

```text
关闭当前连接
删除该 endpoint 的连接池
失效服务发现缓存
重新从 ZooKeeper 拉取 endpoint
重试一次
```

### 5.2 服务端流程

```text
main 中创建业务 ServiceImpl
        |
        v
RpcProvider::NotifyService(service)
读取 Protobuf ServiceDescriptor 和 MethodDescriptor
        |
        v
RpcProvider::Run()
读取配置，创建 TcpServer
        |
        v
启动 work pool
        |
        v
连接 ZooKeeper
        |
        v
注册 /mprpc/services/{service}/{method}/instance-*
        |
        v
设置 RpcMessageCodec
        |
        v
TcpServer::start() 进入事件循环
```

收到请求后：

```text
Connection 读取 socket 数据
        |
        v
RpcMessageCodec 处理粘包/半包，得到完整 payload
        |
        v
TcpServer::handleMessage()
如果启用了 work pool，把请求投递到业务线程
        |
        v
RpcProvider::OnMessage()
解析 RpcHeader 和 request_body
        |
        v
查找 service 和 method
        |
        v
通过 Protobuf 反射创建 request / response 对象
        |
        v
request.ParseFromString()
        |
        v
创建 done 回调
        |
        v
service->CallMethod(method, nullptr, request, response, done)
        |
        v
业务方法填充 response，并调用 done->Run()
        |
        v
RpcProvider::SendRpcResponse()
序列化 response，封装 RpcResponseHeader，发送响应帧
```

---

## 6. 如何新增一个 RPC 服务

### 6.1 定义 proto

```protobuf
syntax = "proto3";

package video;

option cc_generic_services = true;

message SubmitTaskRequest {
    bytes input_url = 1;
    bytes output_url = 2;
    bytes profile = 3;
}

message SubmitTaskResponse {
    int32 code = 1;
    bytes task_id = 2;
    bytes message = 3;
}

service TranscodeSchedulerService {
    rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse);
}
```

### 6.2 生成 C++ 代码

```bash
protoc --cpp_out=. transcode.proto
```

具体路径按你的 CMake 组织调整。

### 6.3 服务端实现

```cpp
class TranscodeSchedulerServiceImpl
    : public video::TranscodeSchedulerService {
public:
    void SubmitTask(::google::protobuf::RpcController* controller,
                    const ::video::SubmitTaskRequest* request,
                    ::video::SubmitTaskResponse* response,
                    ::google::protobuf::Closure* done) override
    {
        response->set_code(0);
        response->set_task_id("task-001");
        response->set_message("ok");

        done->Run();
    }
};
```

服务端 main：

```cpp
int main(int argc, char** argv)
{
    if (!MprpcApplication::Init(argc, argv)) {
        return EXIT_FAILURE;
    }

    RpcProvider provider;
    provider.NotifyService(new TranscodeSchedulerServiceImpl());
    provider.Run();
    return 0;
}
```

注意：当前框架要求业务方法最终调用 `done->Run()`，否则客户端不会收到响应。

### 6.4 客户端调用

```cpp
video::TranscodeSchedulerService_Stub stub(new MprpcChannel());

video::SubmitTaskRequest request;
request.set_input_url("s3://bucket/input.mp4");
request.set_output_url("s3://bucket/output.mp4");
request.set_profile("h264_720p");

video::SubmitTaskResponse response;
MprpcController controller;
controller.SetTimeoutMs(3000);

stub.SubmitTask(&controller, &request, &response, nullptr);

if (controller.Failed()) {
    LOG_ERROR("SubmitTask failed, code=%d, error=%s",
              controller.ErrorCode(),
              controller.ErrorText().c_str());
    return;
}

if (response.code() != 0) {
    LOG_ERROR("business failed: %s", response.message().c_str());
}
```

---

## 7. 当前设计里比较有价值的点

### 7.1 利用 Protobuf 反射减少重复代码

服务端不需要为每个 RPC 方法手写网络分发逻辑。

`RpcProvider::NotifyService()` 通过：

```cpp
service->GetDescriptor()
pserviceDesc->method(i)
```

拿到 service 和 method 的元信息。

请求进来后，`RpcProvider::OnMessage()` 根据 `service_name` 和 `method_name` 查表，再通过：

```cpp
service->GetRequestPrototype(method).New()
service->GetResponsePrototype(method).New()
service->CallMethod(...)
```

完成动态分发。

这就是 RPC 框架的核心价值：业务只定义 proto 和实现 service，网络收发、序列化、服务发现都由框架接管。

### 7.2 codec 下沉到 Connection 层

`RpcMessageCodec()` 不放在 `RpcProvider::OnMessage()` 里，而是挂在 `TcpServer` / `Connection` 的读路径中。

这样 `RpcProvider::OnMessage()` 收到的一定是完整 RPC payload，不需要每个业务层重复处理 TCP 粘包/半包。

这是网络库和 RPC 框架之间比较清晰的分层：

```text
wevix_muduo::Connection 负责字节流和帧提取
RpcProvider 负责 RPC 语义解析和业务分发
业务 Service 只负责业务逻辑
```

### 7.3 request_id 让长连接更可控

即使当前没有连接内多路复用，`request_id` 仍然有价值：

- 检测响应是否对应当前请求
- 避免长连接复用时误读到错误响应还继续解析
- 后续升级异步调用、多路复用时可以直接复用这个字段

### 7.4 响应头统一承载框架错误

以前服务端遇到 service 不存在、method 不存在、request parse 失败时，只打日志然后 return。客户端只能看到连接关闭或超时。

现在服务端会返回：

```text
RpcResponseHeader.error_code
RpcResponseHeader.error_msg
```

客户端可以立刻知道远端失败原因。

### 7.5 ZooKeeper 不进入每次调用热路径

早期设计每次 RPC 都创建 ZK 会话，压测时 QPS 很低，还会有大量 ZK 输出。

现在是：

```text
进程级共享 ZK client
本地 method -> endpoint list 缓存
失败时失效缓存
```

服务发现只在 cache miss 或失败重试时发生，普通调用不会频繁访问 ZK。

### 7.6 多实例路径为后续调度业务留好了入口

当前节点数据只有：

```text
ip:port
```

但路径已经改成多实例：

```text
/mprpc/services/{service}/{method}/instance-*
```

以后可以把节点数据扩展成 JSON：

```json
{
  "ip": "127.0.0.1",
  "port": 9001,
  "version": "v1",
  "weight": 100,
  "tags": ["worker", "ffmpeg"]
}
```

这对视频转码平台里的多 Worker、灰度版本、标签调度、权重调度都有用。

### 7.7 Provider work pool 防止业务阻塞 IO

RPC 服务端最怕把业务逻辑直接跑在 IO 线程里。

现在 `RpcProvider` 默认启用 work pool，让业务执行和网络事件分离：

```text
IO thread: accept/read/write/codec
work thread: protobuf service method
```

这对后面写调度器、Worker 心跳、任务管理服务都很重要。

---

## 8. 当前适合承担的业务角色

在分布式视频转码平台中，当前 RPC 适合用于：

### 8.1 SchedulerService

```text
SubmitJob
CancelJob
QueryJob
ListJobs
```

用于客户端或 API 层向调度器提交和查询任务。

### 8.2 WorkerService

```text
AssignTask
CancelTask
QueryTaskStatus
HealthCheck
```

用于调度器向 Worker 下发转码分片、取消任务或探测状态。

### 8.3 WorkerManagerService

```text
RegisterWorker
Heartbeat
ReportResource
ReportTaskProgress
ReportTaskResult
```

用于 Worker 主动上报资源、心跳、任务进度。

注意：心跳和结果事件如果频率很高，可以放到消息队列，RPC 主要负责强一致、需要立即响应的控制命令。

---

## 9. 当前还没有完成的能力

虽然基础 RPC 已经能作为业务底座使用，但还不是工业级完整 RPC。

当前仍然缺：

### 9.1 ZooKeeper session expired 自动重注册

服务端当前启动时注册临时节点，但没有完整处理 session expired 后的自动重连和重注册。

如果 ZK session 过期，临时节点会消失，Provider 需要重新创建：

```text
/mprpc/services/{service}/{method}/instance-*
```

### 9.2 客户端没有 children watcher

客户端现在是：

```text
cache miss 查 ZK
连接失败时失效缓存
```

还没有监听 ZK children 变化。

因此新增实例或实例下线后，客户端不一定立刻感知，主要靠失败重试或 cache miss 更新。

### 9.3 没有真正的异步客户端

当前客户端 `MprpcChannel::CallMethod()` 是同步阻塞式：

```text
send
recv
parse
return
```

虽然接口里有 `done`，但客户端侧还没有异步回调模型。

### 9.4 没有连接内多路复用

当前一条连接一次只处理一个请求：

```text
connection mutex lock
send one request
wait one response
unlock
```

这比短连接稳定，也比每次连接性能好，但还不是 HTTP/2 或 gRPC 那种 multiplexing。

### 9.5 服务对象所有权还不够清晰

当前 `RpcProvider::NotifyService()` 接收裸指针：

```cpp
void NotifyService(google::protobuf::Service *service);
```

示例里是：

```cpp
provider.NotifyService(new UserService());
```

长期运行服务问题不大，但从框架设计看，后续最好改成：

```cpp
NotifyService(std::unique_ptr<google::protobuf::Service> service)
```

或者明确由调用方管理生命周期。

### 9.6 done 生命周期依赖业务正确调用

当前服务端要求业务实现最终调用：

```cpp
done->Run();
```

如果业务忘记调用：

- response 不会发送
- 客户端会一直等到超时
- response/context 也可能无法及时释放

后续可以考虑：

- 同步 service 自动回包
- done 包装超时保护
- 框架文档强约束业务必须调用 done

### 9.7 metrics / tracing 还没真正落地

协议里有 `trace_id` 字段，但还没实现自动生成、透传和日志关联。

目前也没有统一指标：

```text
rpc_request_total
rpc_error_total
rpc_latency_us
rpc_inflight
connection_pool_size
```

这些可以放到后续框架增强阶段。

---

## 10. 构建、测试和压测

### 10.1 构建

```bash
cmake --build build -j 4
```

### 10.2 协议单测

```bash
./bin/test_rpc_protocol
```

当前覆盖：

- 网络字节序
- 半包处理
- 完整帧解析
- 超大帧拒绝
- magic/version 校验
- controller 错误码和超时

### 10.3 RPC Echo 服务端

```bash
./bin/rpc_echo_server -i test/rpc_test.conf
```

注意：服务端启动需要 ZooKeeper，因为 Provider 会注册服务节点。

### 10.4 Direct 模式压测

```bash
./bin/bench_rpc_stress --direct --host 127.0.0.1 --port 8000 -c 100 -m 100
```

Direct 模式绕过 ZooKeeper，直接连接指定地址，适合测试纯 RPC 协议和网络收发性能。

### 10.5 ZK 模式压测

```bash
./bin/bench_rpc_stress -i test/rpc_test.conf -c 100 -m 100
```

ZK 模式使用 `MprpcChannel`，会覆盖：

- 服务发现缓存
- 多实例 endpoint 选择
- 客户端连接池
- Controller 错误处理

---

## 11. 当前结论

当前 `mprpc` 已经具备一个基础 RPC 框架应有的主干：

- Protobuf 接口定义
- Provider 服务发布
- Stub 远程调用
- 自研 muduo 网络收发
- TCP 粘包/半包处理
- 统一 RPC frame
- magic/version 协议校验
- request_id 响应匹配
- 结构化错误码
- 调用超时
- ZooKeeper 服务注册发现
- 多实例发现
- 客户端服务发现缓存
- 客户端 endpoint 连接池
- 服务端 work pool

它现在适合作为后续业务项目的基础通信框架。

对视频转码平台来说，下一步可以基于它开始定义业务 RPC：

- `SchedulerService`
- `WorkerService`
- `WorkerManagerService`
- `TaskQueryService`

框架后续再补的重点不应该是继续改协议主干，而应该围绕业务真实需求补：

- 健康检查
- metrics
- ZK session expired 重注册
- 服务 metadata
- 权重/标签负载均衡
- 更清晰的 service 生命周期管理
