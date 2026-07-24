# 02. 框架改造清单

## 当前框架状态

当前框架已经具备：

- C++ 自研网络库
- epoll + Reactor
- Buffer
- TcpServer
- 线程池
- 内存池
- 异步日志
- protobuf RPC
- ZooKeeper 服务注册发现
- 基础压测工具

最近已经修过的问题：

- RPC 客户端每次调用都创建 ZooKeeper 连接
- 短连接关闭后 `Connection` 延迟释放
- `EventLoop` 线程 ID 归属错误
- `accept EAGAIN` 误打错误日志
- RPC 单次 `send()` 未完整发送的问题

但如果要支撑分布式视频处理平台，还需要继续补齐框架能力。

## 改造优先级

| 优先级 | 能力 | 是否必须 | 原因 |
|---|---|---|---|
| P0 | RPC request_id | 必须 | 结果回包和日志追踪需要 |
| P0 | RPC timeout/deadline | 必须 | 调度器不能无限等待 Worker |
| P0 | 框架级 error_code | 必须 | 业务需要区分网络失败、超时、服务错误 |
| P0 | 客户端长连接池 | 必须 | Worker 调度不能每次短连接 |
| P0 | 服务发现缓存 | 必须 | ZK 不能进入每次调用热路径 |
| P1 | 健康检查 | 必须 | Worker 故障需要摘除 |
| P1 | 负载均衡 | 必须 | 多 Worker 节点调度 RPC 需要 |
| P1 | 限流和过载保护 | 必须 | 防止调度器或 Worker 被打爆 |
| P1 | 异步 RPC/future | 建议 | 调度器需要并发调用多个服务 |
| P1 | metrics | 必须 | 项目必须可观测 |
| P2 | tracing | 建议 | 面试和排查问题有价值 |
| P2 | 熔断降级 | 建议 | Worker 大量失败时保护系统 |
| P2 | 流式日志回传 | 可选 | 长任务日志展示需要 |

## 1. RPC 协议增强

当前协议大致是：

```text
[total_len] + [header_size] + [RpcHeader] + [args]
```

建议升级为：

```text
[frame_len]
[magic]
[version]
[message_type]
[request_id]
[header_len]
[header]
[body]
```

其中：

- `magic`：快速识别非法包
- `version`：协议升级
- `message_type`：request/response/heartbeat/error
- `request_id`：请求唯一 ID
- `header_len`：protobuf header 长度
- `body`：业务请求体

RPC header 建议增加：

```protobuf
message RpcHeader {
  string service_name = 1;
  string method_name = 2;
  uint32 args_size = 3;
  uint64 request_id = 4;
  int64 deadline_ms = 5;
  string trace_id = 6;
}
```

Response header 建议增加：

```protobuf
message RpcResponseHeader {
  uint64 request_id = 1;
  int32 error_code = 2;
  string error_msg = 3;
  string trace_id = 4;
}
```

## 2. Controller 能力补齐

当前 Controller 只支持 `SetFailed()`。建议补：

- `SetTimeout(ms)`
- `Timeout()`
- `ErrorCode()`
- `ErrorText()`
- `TraceId()`
- `RequestId()`
- `StartTime()`
- `EndTime()`

目标是业务可以这样写：

```cpp
MprpcController ctl;
ctl.SetTimeout(3000);
stub.AssignShard(&ctl, &req, &rsp, nullptr);
if (ctl.Failed()) {
    // 区分 timeout、connect failed、server overload、business failed
}
```

## 3. 客户端长连接池

视频调度平台中，调度器会频繁调用 Worker：

- 分配 shard
- 查询进度
- 取消任务
- 拉取状态

不能每次 RPC 都创建短连接。

需要设计：

```text
ServiceChannelPool
  -> service_name
    -> endpoint(ip:port)
      -> N 条 TCP 连接
```

核心能力：

- 连接复用
- 自动重连
- 失败连接摘除
- endpoint 缓存
- 最大连接数限制
- 空闲连接回收

## 4. 服务发现增强

当前 ZK 节点只存 `ip:port`。

建议改成：

```json
{
  "ip": "127.0.0.1",
  "port": 9001,
  "weight": 100,
  "version": "v1",
  "role": "transcode_worker",
  "cpu_cores": 8,
  "memory_mb": 16384,
  "tags": ["ffmpeg", "cpu"]
}
```

需要支持：

- 本地缓存
- watcher 感知节点变更
- 节点 metadata
- 服务版本
- 权重
- 健康状态

注意：资源状态不一定都放 ZK。频繁变化的 CPU、内存、队列长度更适合通过心跳 RPC 上报给 `WorkerManagerService`。

## 5. 负载均衡

RPC 客户端至少支持：

- round-robin
- random
- weighted round-robin
- least-inflight

视频调度场景下，Scheduler 更关心资源匹配，所以业务调度会自己选择 Worker。但通用 RPC 框架仍应该具备基础负载均衡。

## 6. 超时与重试

必须区分：

- RPC 连接超时
- RPC 读写超时
- 服务端处理超时
- 业务任务超时

对调度平台来说：

- `AssignShard` 可以短超时，比如 1-3 秒
- `ReportProgress` 可以短超时
- `ExecuteShard` 不应该是长阻塞 RPC，而应该是提交任务后异步执行

重试规则：

- 网络失败可以重试
- timeout 可以重试
- 非幂等接口默认不重试
- AssignShard 需要 shard_id 保证幂等

## 7. 服务端过载保护

需要补：

- 最大连接数
- 最大请求包大小
- 最大 in-flight 请求数
- work queue 上限
- 超限快速失败

例如 Worker 忙时应该返回：

```text
ERROR_SERVER_OVERLOADED
```

而不是让调度器一直等待。

## 8. 异步 RPC / future RPC

Scheduler 可能同时调度很多 shard，需要并发 RPC。

建议先做 future 风格：

```cpp
auto fut = stub.AssignShardAsync(req);
auto rsp = fut.get();
```

如果短期实现成本高，可以业务层先用线程池并发同步调用，框架后续再补 async。

## 9. metrics

框架层需要暴露：

- 每个 service/method QPS
- 平均延迟
- P50/P90/P99
- 错误码计数
- 当前连接数
- in-flight 请求数
- 发送队列长度
- 接收包大小

业务层需要暴露：

- job 总数
- running job 数
- pending shard 数
- running shard 数
- failed shard 数
- retry 次数
- worker 在线数
- worker 负载

## 10. tracing

至少做轻量 trace：

```text
trace_id = job_id
span_id = current rpc call
parent_span_id = caller span
```

不一定第一版接 OpenTelemetry，可以先在日志中输出：

```text
[trace_id=xxx][job_id=xxx][shard_id=xxx][method=AssignShard][cost=2ms]
```

## 11. 测试与故障注入

框架需要补这些测试：

- RPC 正常调用
- RPC 超时
- 服务端关闭连接
- 客户端断线重连
- ZK 重启后服务发现恢复
- Worker 进程退出
- 大包请求
- 粘包/拆包
- 高并发短连接
- 高并发长连接

视频平台业务还要测：

- Worker 执行中宕机
- shard 超时
- shard 重复上报
- ResultCollector 重复收到结果
- Scheduler 重启后恢复任务

## 框架改造建议顺序

### 阶段 1：P0 必须项

1. request_id
2. error_code
3. timeout/deadline
4. 长连接池
5. 服务发现缓存

### 阶段 2：支撑业务闭环

1. Worker 注册
2. Worker 心跳
3. 健康检查
4. 服务端过载保护
5. metrics

### 阶段 3：增强能力

1. async/future RPC
2. tracing
3. 熔断
4. 权重负载均衡
5. 流式日志回传

## 关键原则

不要为了“框架完善”无限扩展。

每个框架能力都必须能回答：

> 它解决了视频调度平台中的哪个具体问题？

例如：

- timeout 解决 Worker 无响应
- request_id 解决响应和日志关联
- long connection pool 解决频繁调度 RPC 性能问题
- health check 解决坏 Worker 摘除
- metrics 解决任务慢在哪里的问题

