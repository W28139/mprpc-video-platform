# 03. 系统设计

## 总体架构

```text
                     +----------------+
                     |   Client/CLI   |
                     +-------+--------+
                             |
                             | RPC: SubmitJob / QueryJob
                             v
                     +----------------+
                     |   JobService   |
                     +-------+--------+
                             |
                             | create job / shards
                             v
                    +------------------+
                    | SchedulerService |
                    +---+----------+---+
                        |          |
        query workers   |          | assign shard
                        v          v
              +----------------+   +------------------+
              | WorkerManager  |   | TranscodeWorker  |
              +----------------+   +--------+---------+
                                            |
                                            | progress/result
                                            v
                                  +---------------------+
                                  | ResultCollector     |
                                  +----------+----------+
                                             |
                                             v
                                  +---------------------+
                                  | MetadataStore       |
                                  +---------------------+
```

## 服务拆分

### 1. JobService

职责：

- 接收用户提交任务
- 校验任务参数
- 创建 job 记录
- 调用 Scheduler 创建 shard
- 查询 job 状态

主要接口：

```protobuf
service JobService {
  rpc SubmitJob(SubmitJobRequest) returns (SubmitJobResponse);
  rpc QueryJob(QueryJobRequest) returns (QueryJobResponse);
  rpc CancelJob(CancelJobRequest) returns (CancelJobResponse);
}
```

### 2. SchedulerService

职责：

- 将 job 拆成 shard
- 根据 Worker 资源调度 shard
- 维护待调度队列
- 处理 shard 超时
- 处理失败重试

主要接口：

```protobuf
service SchedulerService {
  rpc ScheduleJob(ScheduleJobRequest) returns (ScheduleJobResponse);
  rpc RescheduleShard(RescheduleShardRequest) returns (RescheduleShardResponse);
}
```

### 3. WorkerManagerService

职责：

- Worker 注册
- Worker 心跳
- Worker 资源状态维护
- Worker 健康检测
- Worker 下线处理

主要接口：

```protobuf
service WorkerManagerService {
  rpc RegisterWorker(RegisterWorkerRequest) returns (RegisterWorkerResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc ListWorkers(ListWorkersRequest) returns (ListWorkersResponse);
}
```

### 4. TranscodeWorker

职责：

- 接收 shard
- 执行 mock 或 FFmpeg
- 上报进度
- 上报结果
- 处理取消

主要接口：

```protobuf
service WorkerService {
  rpc AssignShard(AssignShardRequest) returns (AssignShardResponse);
  rpc CancelShard(CancelShardRequest) returns (CancelShardResponse);
  rpc QueryShard(QueryShardRequest) returns (QueryShardResponse);
}
```

### 5. ResultCollectorService

职责：

- 接收 shard 成功或失败结果
- 幂等更新 shard 状态
- 判断 job 是否完成
- 触发结果合并
- 更新最终任务状态

主要接口：

```protobuf
service ResultCollectorService {
  rpc ReportShardResult(ReportShardResultRequest) returns (ReportShardResultResponse);
  rpc ReportShardProgress(ReportShardProgressRequest) returns (ReportShardProgressResponse);
}
```

## 核心实体

### Job

```text
job_id
user_id
input_path
output_path
target_format
target_resolution
target_bitrate
duration_sec
priority
status
created_at
updated_at
```

### Shard

```text
shard_id
job_id
shard_index
start_time_ms
duration_ms
status
assigned_worker_id
retry_count
max_retry
input_path
output_path
created_at
updated_at
```

### Worker

```text
worker_id
ip
port
cpu_cores
memory_mb
gpu_count
current_running_shards
max_running_shards
last_heartbeat
status
```

## 任务状态机

### Job 状态

```text
PENDING
  -> SPLITTING
  -> SCHEDULING
  -> RUNNING
  -> MERGING
  -> SUCCESS

PENDING/RUNNING/MERGING
  -> FAILED

PENDING/RUNNING
  -> CANCELED
```

### Shard 状态

```text
CREATED
  -> WAITING
  -> ASSIGNED
  -> RUNNING
  -> SUCCESS

WAITING/ASSIGNED/RUNNING
  -> FAILED
  -> RETRYING
  -> WAITING

WAITING/ASSIGNED/RUNNING
  -> CANCELED
```

## 任务切片策略

第一版按时间切片：

```text
video_duration = 600s
shard_duration = 30s
shard_count = 20
```

每个 shard：

```text
shard_0: start=0s, duration=30s
shard_1: start=30s, duration=30s
...
```

第二版可以支持：

- 按固定时长切片
- 按目标 shard 数切片
- 按关键帧切片
- 按资源需求切片

MVP 不需要关键帧精确切片，先按时间切片即可。

## 调度策略

第一版调度策略：

```text
score = available_slots * 10 - current_running_shards * 5
```

选择 score 最高的 Worker。

Worker 可用条件：

- status = ONLINE
- heartbeat 未超时
- current_running_shards < max_running_shards
- memory 剩余满足任务需求

第二版可以加入：

- CPU 使用率
- 内存使用率
- GPU 标签
- Worker 历史失败率
- 任务优先级
- 数据本地性

## Worker 心跳

Worker 周期性上报：

```text
worker_id
cpu_usage
memory_usage
running_shards
finished_shards
failed_shards
timestamp
```

心跳间隔：

- MVP：3 秒
- 超时阈值：10 秒

如果 Worker 超时：

1. Worker 状态改为 `OFFLINE`
2. 找出该 Worker 上 `RUNNING` 的 shard
3. shard 状态改为 `RETRYING`
4. 如果 retry_count < max_retry，重新进入调度队列
5. 否则 shard 失败，job 失败

## 失败恢复

### Worker 执行失败

Worker 返回失败：

```text
exit_code != 0
error_msg = ffmpeg stderr 或 mock error
```

处理：

1. ResultCollector 更新 shard 为 FAILED
2. Scheduler 判断是否可重试
3. 可重试则 retry_count + 1
4. 重新入队

### Worker 宕机

心跳超时：

1. WorkerManager 标记 Worker OFFLINE
2. Scheduler 找出未完成 shard
3. 重新调度

### ResultCollector 重复收到结果

必须幂等。

规则：

- 如果 shard 已经是 SUCCESS，再收到 SUCCESS，直接返回 OK
- 如果 shard 已经是 SUCCESS，再收到 FAILED，忽略
- 如果 shard 是 RUNNING，收到 SUCCESS，更新为 SUCCESS
- 如果 shard 是 RUNNING，收到 FAILED，进入重试流程

## 幂等设计

所有关键 RPC 都必须带 ID：

- `job_id`
- `shard_id`
- `request_id`

`AssignShard` 幂等：

```text
同一个 shard_id 重复分配给同一个 Worker，Worker 返回已接收。
同一个 shard_id 被分配给不同 Worker，需要根据 shard 当前 version 判断是否合法。
```

可以给 shard 增加：

```text
attempt_id
```

每次重试生成新的 attempt。

Worker 上报结果时必须带：

```text
job_id
shard_id
attempt_id
```

ResultCollector 只接受当前 attempt 的结果。

## 数据存储设计

MVP 可以先用 SQLite 或 MySQL。

### jobs 表

```sql
CREATE TABLE jobs (
  job_id VARCHAR(64) PRIMARY KEY,
  input_path TEXT,
  output_path TEXT,
  target_format VARCHAR(32),
  target_resolution VARCHAR(32),
  priority INT,
  status VARCHAR(32),
  shard_count INT,
  created_at BIGINT,
  updated_at BIGINT
);
```

### shards 表

```sql
CREATE TABLE shards (
  shard_id VARCHAR(64) PRIMARY KEY,
  job_id VARCHAR(64),
  shard_index INT,
  start_ms BIGINT,
  duration_ms BIGINT,
  status VARCHAR(32),
  worker_id VARCHAR(64),
  attempt_id VARCHAR(64),
  retry_count INT,
  max_retry INT,
  created_at BIGINT,
  updated_at BIGINT
);
```

### workers 表

```sql
CREATE TABLE workers (
  worker_id VARCHAR(64) PRIMARY KEY,
  ip VARCHAR(64),
  port INT,
  cpu_cores INT,
  memory_mb INT,
  max_running_shards INT,
  current_running_shards INT,
  status VARCHAR(32),
  last_heartbeat BIGINT
);
```

## 消息队列设计

第一版可以用内存队列。

事件类型：

```text
JobCreated
ShardCreated
ShardAssigned
ShardStarted
ShardProgressUpdated
ShardSucceeded
ShardFailed
ShardRetrying
JobSucceeded
JobFailed
WorkerOffline
```

后续可以替换为 Redis Stream、RabbitMQ 或 Kafka。

## RPC 调用关系

```text
Client -> JobService.SubmitJob
JobService -> SchedulerService.ScheduleJob
SchedulerService -> WorkerManagerService.ListWorkers
SchedulerService -> WorkerService.AssignShard
Worker -> ResultCollectorService.ReportShardProgress
Worker -> ResultCollectorService.ReportShardResult
Worker -> WorkerManagerService.Heartbeat
```

## 关键设计取舍

### 为什么不让 `AssignShard` 阻塞到转码完成？

因为视频处理是长任务。如果 RPC 阻塞到任务完成，会导致：

- 调度器线程被占用
- 连接长期挂起
- 超时难处理
- Worker 故障恢复复杂

正确方式：

1. `AssignShard` 只表示 Worker 接收任务
2. Worker 异步执行
3. 进度和结果通过 `ReportShardProgress/ReportShardResult` 上报

### 为什么需要 attempt_id？

因为 shard 可能重试。旧 Worker 可能在超时后又完成并上报结果。如果没有 attempt_id，旧结果可能覆盖新结果。

### 为什么资源状态不全部放 ZooKeeper？

ZooKeeper 适合服务注册和低频元数据，不适合高频负载上报。CPU、内存、running shard 数应该通过心跳 RPC 上报给 WorkerManager。

