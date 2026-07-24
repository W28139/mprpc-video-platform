# mprpc 当前剩余问题与视频转码平台下一步计划

## 日期

2026-07-24

## 结论先行

当前 `mprpc` 已经完成了从 demo 级 RPC 到基础可用 RPC 的关键补强。

它现在没有新增具体业务，核心业务边界仍然是：

```text
Provider 发布 protobuf service
Consumer 通过 protobuf Stub 远程调用 service method
ZooKeeper 负责服务注册发现
wevix_muduo 负责网络收发
```

这次框架完善带来的变化主要是：

- 协议更严谨
- 错误语义更清楚
- 服务发现更适合多实例
- 客户端调用更稳定
- 长连接性能更好
- 服务端不再默认让业务堵塞 IO 线程

所以现在的判断是：

> 可以开始视频转码平台的业务开发，不建议继续在 RPC 框架上无限打磨。

但也要明确：

> 当前 RPC 还不是工业级完整框架，后续还需要围绕视频平台业务真实需求继续补 P1/P2 能力。

---

## 1. 旧路线图当前完成情况

旧文档：

```text
doc/mprpc基础框架审查与完善路线.md
```

这个文档是修改前的审查结果，里面很多“当前问题”现在已经过期。下面是按当前代码重新判断后的状态。

## 2. P0 项完成情况

P0 的目标是：

> 不做这些，后续业务会被 RPC 基础问题反复打断。

当前 P0 主线已经基本完成。

| 条目 | 当前状态 | 说明 |
|---|---|---|
| P0-1 服务端请求 payload 边界检查 | 已完成 | `RpcProvider::DecodeRequestHeader()` 已校验 header/body 边界和 `args_size` |
| P0-2 request_id / 错误响应 / 协议字段 | 已完成 | 已有 `request_id`、`deadline_ms`、`trace_id` 预留、`RpcResponseHeader`、`RpcErrorCode` |
| P0-3 magic/version/max frame/network byte order | 已完成 | `mprpccodec.h` 已统一处理 |
| P0-4 RPC timeout | 基础完成 | 客户端 connect/send/recv 有超时；服务端还未消费 `deadline_ms` |
| P0-5 客户端长连接池 | 基础完成 | endpoint 级连接池已做；还没有连接内多路复用 |
| P0-6 多实例服务发现 | 基础完成 | 新路径为 `/mprpc/services/{service}/{method}/instance-*`；metadata/权重/标签未做 |
| P0-7 服务端业务 work pool | 基础完成 | Provider 默认启用 work pool；work queue 上限和过载错误未做 |

当前 P0 的准确说法：

```text
可支撑业务 MVP，但不是完整服务治理形态。
```

---

## 3. P1/P2 剩余问题

这些问题不建议全部在业务开始前做完，否则会陷入“框架永远没写完”的状态。

更合理的方式是：

```text
先开始视频平台业务
业务遇到真实需求时，再把对应 RPC 能力补成通用框架能力
```

### 3.1 还没做，但后面应该做

| 条目 | 是否阻塞业务 MVP | 建议阶段 |
|---|---|---|
| ZK session expired 自动重注册 | 不阻塞 MVP | Worker 注册/故障恢复阶段 |
| 客户端 children watcher | 不阻塞 MVP | 多 Worker 压测前 |
| 健康检查 | 阻塞 Worker 故障治理，不阻塞 proto 骨架 | Worker 注册心跳阶段 |
| 服务对象所有权治理 | 不阻塞 MVP | RPC 小重构阶段 |
| `done` 生命周期托管 | 不阻塞同步业务，但要写规范 | RPC 小重构阶段 |
| metrics | 不阻塞第一版业务，但阻塞最终展示 | mock 调度闭环后 |
| tracing | 不阻塞 MVP | metrics 后 |
| 命名空间整理 | 不阻塞业务 | 最后统一重构 |
| 统一 Status 类型 | 不阻塞 MVP | 错误处理继续扩展时 |
| work queue 上限 / 过载保护 | 不阻塞 proto 骨架，后续必须 | Worker 并发执行前 |
| async/future RPC | 不阻塞 MVP | Scheduler 并发优化阶段 |

### 3.2 当前最值得补的 RPC 小项

如果你想在开始业务前再做一小轮 RPC 收尾，建议只做这 3 个：

1. `deadline_ms` 服务端过期检查
2. `RpcProvider::NotifyService(std::unique_ptr<Service>)` 或明确服务对象生命周期
3. `done` 调用规范和防泄漏说明

这三个改动小，收益明确，不会拖慢进入业务的节奏。

暂时不建议现在做：

- 完整 async RPC
- 完整 tracing
- 完整熔断
- 完整 metadata/权重负载均衡
- 完整 ZK watcher 体系

这些应该等视频平台跑起来后，根据真实压测和故障测试补。

---

## 4. 结合视频转码平台后的下一步

视频平台文档里的推荐路线是：

```text
框架 P0
  -> proto
  -> Worker 注册/心跳
  -> mock 调度闭环
  -> 失败重试
  -> FFmpeg executor
  -> 资源调度
  -> metrics
  -> 压测和文档
```

现在 `框架 P0` 已经基本完成。

所以你接下来应该进入：

```text
阶段 2：项目骨架和 proto 设计
```

不是继续无边界地完善 RPC。

---

## 5. 接下来 3 天该做什么

## 第 1 天：建立 video_platform 项目骨架

目标：

> 让视频平台从文档变成代码工程。

建议目录：

```text
video_platform/
  CMakeLists.txt
  proto/
    common.proto
    job.proto
    worker.proto
    scheduler.proto
    result.proto
  include/
    video_platform/
      common/
      storage/
      job_service/
      scheduler/
      worker_manager/
      worker/
      result_collector/
  src/
    common/
    storage/
    job_service/
    scheduler/
    worker_manager/
    worker/
    result_collector/
  conf/
  test/
```

第一天不要写复杂逻辑，只做：

- CMake 能编译
- proto 能生成 C++ 代码
- 每个服务有最小 main
- 每个服务能调用 `MprpcApplication::Init()`
- 每个服务能启动并注册到 ZK

验收：

```text
能启动 JobService / WorkerManager / Scheduler / ResultCollector / 一个 Worker
ZK 上能看到这些 service/method 节点
```

---

## 第 2 天：定义业务 proto v1

目标：

> 把业务边界定下来，先不要陷入实现细节。

必须先定义这些枚举：

```text
JobStatus
ShardStatus
WorkerStatus
ErrorCode
```

必须先定义这些核心消息：

```text
JobSpec
JobInfo
ShardInfo
WorkerInfo
ResourceSnapshot
```

第一版 RPC 服务：

```protobuf
service JobService {
  rpc SubmitJob(SubmitJobRequest) returns (SubmitJobResponse);
  rpc QueryJob(QueryJobRequest) returns (QueryJobResponse);
  rpc CancelJob(CancelJobRequest) returns (CancelJobResponse);
}

service WorkerManagerService {
  rpc RegisterWorker(RegisterWorkerRequest) returns (RegisterWorkerResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc ListWorkers(ListWorkersRequest) returns (ListWorkersResponse);
}

service SchedulerService {
  rpc ScheduleJob(ScheduleJobRequest) returns (ScheduleJobResponse);
  rpc RescheduleShard(RescheduleShardRequest) returns (RescheduleShardResponse);
}

service WorkerService {
  rpc AssignShard(AssignShardRequest) returns (AssignShardResponse);
  rpc CancelShard(CancelShardRequest) returns (CancelShardResponse);
  rpc QueryShard(QueryShardRequest) returns (QueryShardResponse);
}

service ResultCollectorService {
  rpc ReportShardProgress(ReportShardProgressRequest) returns (ReportShardProgressResponse);
  rpc ReportShardResult(ReportShardResultRequest) returns (ReportShardResultResponse);
}
```

验收：

```text
proto 生成代码成功
所有 Stub 可以被创建
Client 能调用一个 SubmitJob 空实现
```

---

## 第 3 天：实现内存版 MetadataStore

目标：

> 先用内存存储把状态机跑起来，不要一上来接数据库。

先实现这些表的内存结构：

```text
jobs: job_id -> JobInfo
shards: shard_id -> ShardInfo
workers: worker_id -> WorkerInfo
```

接口建议：

```cpp
class MetadataStore {
public:
    bool CreateJob(const JobInfo& job);
    bool UpdateJobStatus(const std::string& jobId, JobStatus status);
    bool GetJob(const std::string& jobId, JobInfo* job);

    bool CreateShard(const ShardInfo& shard);
    bool UpdateShardStatus(const std::string& shardId, ShardStatus status);
    std::vector<ShardInfo> ListShardsByJob(const std::string& jobId);

    bool UpsertWorker(const WorkerInfo& worker);
    bool UpdateWorkerHeartbeat(const std::string& workerId,
                               const ResourceSnapshot& resource);
    std::vector<WorkerInfo> ListOnlineWorkers();
};
```

验收：

```text
SubmitJob 可以创建 job 记录
QueryJob 可以查到 job
RegisterWorker 可以创建 worker 记录
Heartbeat 可以更新 worker 状态
```

---

## 6. 第一周目标

第一周不要接 FFmpeg。

第一周目标是：

> 用 mock executor 跑通分布式任务调度闭环。

建议顺序：

1. 建 `video_platform` 工程骨架
2. 写 proto v1
3. 写内存 MetadataStore
4. 实现 WorkerManager 注册与心跳
5. 实现 JobService `SubmitJob/QueryJob`
6. 实现 Scheduler 按时间切 shard
7. 实现 WorkerService `AssignShard`
8. 实现 MockExecutor
9. 实现 ResultCollector 上报进度和结果
10. 跑通一个 job 拆成多个 shard，并由多个 Worker 执行完成

第一周验收：

```text
启动：
  1 个 JobService
  1 个 SchedulerService
  1 个 WorkerManagerService
  1 个 ResultCollectorService
  3 个 TranscodeWorker

提交：
  1 个 mock 视频任务

结果：
  Job 被拆成 N 个 shard
  多个 Worker 并行执行
  进度可以查询
  所有 shard 完成后 Job 变 SUCCESS
```

---

## 7. 现在不要做什么

现在不要做这些：

- 不要深入 H.264/H.265 编码原理
- 不要马上接复杂 FFmpeg
- 不要先做漂亮 CLI
- 不要先上 MySQL/Redis/Kafka
- 不要继续无限完善 RPC
- 不要一开始就做 async RPC
- 不要一开始就做复杂负载均衡

原因：

```text
当前最大风险不是某个高级能力缺失，而是业务闭环还没落地。
```

你现在最需要的是让系统尽快出现：

```text
SubmitJob -> SplitShard -> AssignWorker -> ExecuteMock -> ReportResult -> JobSuccess
```

这个链路一旦跑通，后面所有框架增强都有真实依据。

---

## 8. RPC 后续怎么跟业务一起补

后续不要把 RPC 和业务割裂开做。

建议用下面方式推进：

| 业务阶段 | 会暴露的问题 | 对应补 RPC |
|---|---|---|
| Worker 注册/心跳 | Worker 活着但不可用 | HealthService / 心跳状态 |
| 多 Worker 调度 | endpoint 选择太粗糙 | metadata / 权重 / 标签 |
| mock 高并发进度上报 | 调用量变大 | metrics / 限流 |
| Worker 执行慢 | 调度器等待风险 | 更细粒度 timeout / deadline |
| Worker 宕机恢复 | ZK 临时节点和缓存不一致 | session expired / children watcher |
| ResultCollector 重复上报 | 请求需要幂等 | request_id +业务 attempt_id |
| Scheduler 并发调度 | 同步 RPC 不够方便 | future/async RPC |

这样做的好处是：

```text
每个框架增强都有业务场景支撑，面试时也更好讲。
```

---

## 9. 推荐的当前工作清单

按优先级排序：

1. 创建 `video_platform` 目录和 CMake。
2. 写 `common.proto`，定义通用错误码、状态枚举、基础结构。
3. 写 `job.proto`、`worker.proto`、`scheduler.proto`、`result.proto`。
4. 生成 protobuf C++ 代码并接入构建。
5. 写最小 `JobService`，能接收 `SubmitJob` 并返回 `job_id`。
6. 写最小 `WorkerManagerService`，支持 `RegisterWorker` 和 `Heartbeat`。
7. 写内存版 `MetadataStore`。
8. 写最小 `SchedulerService`，能把 job 切成 shard。
9. 写最小 `TranscodeWorker`，接收 shard 后用 sleep 模拟执行。
10. 写 `ResultCollectorService`，接收 shard 完成结果并更新 job 状态。
11. 再回来补 RPC 的健康检查、metrics 和服务端过载保护。

---

## 10. 简短判断

你现在不应该继续问：

```text
RPC 还有没有 bug？
```

更应该开始问：

```text
视频平台的第一个业务闭环怎么跑起来？
```

RPC 当前已经足够支撑这个闭环。后续如果业务压测暴露出问题，再把问题沉淀回框架。

