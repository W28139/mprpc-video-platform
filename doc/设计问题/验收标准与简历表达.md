# 05. 验收标准、压测目标与简历表达

## 项目验收标准

一个完整可展示版本至少要满足下面标准。

### 功能验收

- 用户可以提交视频处理任务
- Job 可以被拆分为多个 shard
- Worker 可以注册和心跳
- Scheduler 可以根据 Worker 状态分配 shard
- Worker 可以执行 mock 或 FFmpeg 任务
- Worker 可以上报进度
- ResultCollector 可以聚合结果
- Worker 失败后 shard 可以重试
- Worker 下线后 shard 可以迁移
- Job 可以查询状态和进度
- Job 最终可以 SUCCESS 或 FAILED

### 框架验收

- RPC 支持 request_id
- RPC 支持 timeout/deadline
- RPC 支持 error_code/error_msg
- RPC 客户端使用长连接池
- 服务发现有本地缓存
- Worker 节点故障能摘除
- 服务端有最大包限制
- 服务端有基本过载保护
- 日志中能按 job_id/shard_id/request_id 查链路

### 稳定性验收

- 10 个 Worker 同时运行
- 100 个 Job 并发提交
- 1000 个 Shard 调度执行
- 随机杀掉 30% Worker 后任务可恢复
- 重复上报结果不会破坏状态
- Worker 心跳超时后 10 秒内被摘除
- 超过最大重试次数后任务失败

### 性能验收

MVP 阶段目标：

- SubmitJob QPS > 1000
- ReportShardProgress QPS > 10000
- Scheduler 单机调度 shard > 5000/s
- RPC 长连接调用失败率为 0
- Worker 心跳 1000 节点级别可处理

后续目标：

- 1 万 shard 调度压测
- 10 万进度上报压测
- P99 延迟统计
- 故障恢复时间统计

## 压测设计

### 压测 1：RPC 基础吞吐

目标：

- 验证 mprpc 长连接性能
- 验证 timeout 和 error_code 不影响主路径

命令参考：

```bash
./bin/bench_rpc_stress --direct --keepalive -c 100 -m 1000 -s 256
```

### 压测 2：Worker 心跳

目标：

- 模拟大量 Worker 心跳上报

测试项：

- 100 Worker
- 1000 Worker
- 心跳间隔 3 秒
- WorkerManager CPU 和延迟

指标：

- heartbeat QPS
- heartbeat P99
- worker 状态更新延迟

### 压测 3：任务调度

目标：

- 验证 Scheduler 调度能力

测试项：

- 100 job
- 每个 job 10 shard
- 10 Worker
- mock executor

指标：

- shard 调度耗时
- pending 队列长度
- running shard 数
- job 完成时间

### 压测 4：故障恢复

目标：

- 验证 Worker 故障转移

测试项：

- job 执行中 kill Worker
- Worker 卡死不心跳
- Worker 上报失败
- Worker 重复上报结果

指标：

- 故障检测时间
- shard 重新调度时间
- job 最终成功率
- 重试次数

## 简历表达

### 简历项目名

推荐：

> 基于自研 muduo 与 mprpc 的分布式视频处理与资源调度平台

不要写成：

> FFmpeg 视频转码系统

后者会让重点跑到音视频。

### 简历项目描述

可以这样写：

> 基于自研 C++ 网络库和 RPC 框架实现分布式视频处理调度平台，支持视频任务提交、任务切片、Worker 注册与心跳、资源感知调度、失败重试、进度上报和结果聚合。系统通过 ZooKeeper 实现服务注册发现，通过长连接 RPC 完成调度器与 Worker 通信，并基于任务状态机保证 Worker 故障后的任务恢复。

### 简历亮点 bullet

可以写：

- 设计 Job/Shard/Worker 三层任务模型，将视频处理任务按时间区间拆分为可并行执行的 shard。
- 基于自研 mprpc 实现调度器、WorkerManager、TranscodeWorker 和 ResultCollector 之间的 RPC 通信。
- 为 RPC 框架补充 request_id、timeout、error_code、长连接池和服务发现缓存，解决短连接开销和故障定位问题。
- 实现 Worker 注册、心跳检测和资源上报，根据 CPU、内存、并发 slot 等信息进行资源感知调度。
- 设计 shard 状态机和 attempt_id 机制，保证 Worker 宕机、超时和重复上报场景下的任务幂等与可恢复。
- 实现 mock executor 与 FFmpeg executor 两种执行器，支持任务进度上报、失败重试和结果聚合。
- 构建压测与故障注入用例，验证高并发 RPC、Worker 故障转移和大规模 shard 调度能力。

## 面试讲法

### 1. 项目一句话

> 这是一个基于自研 C++ 网络库和 RPC 框架实现的分布式视频任务调度平台，重点解决视频处理任务在多 Worker 节点上的拆分、调度、执行、失败恢复和结果聚合。

### 2. 为什么不直接用现成框架？

回答重点：

- 项目目标是深入理解高性能网络和 RPC 框架
- 自研框架用于掌握 Reactor、Buffer、连接管理、RPC 协议、服务发现
- 业务平台用于验证框架在真实分布式场景下的可用性

不要说：

> 我觉得现成框架不好。

应该说：

> 生产环境会优先考虑成熟框架，但这个项目的目标是通过自研框架理解底层机制，并用业务系统验证框架能力。

### 3. 为什么选视频处理？

回答：

- 视频处理任务天然可以切片并行
- Worker 节点有 CPU/GPU 等资源差异
- 任务执行时间长，需要进度上报和失败恢复
- 业务自然需要调度、心跳、重试、结果聚合
- C++ 用于网络通信和执行器封装比较合理

### 4. 项目最难的点是什么？

可以说三个：

1. **任务状态一致性**
   - Worker 可能执行失败、超时、重复上报
   - 用 shard 状态机和 attempt_id 保证幂等

2. **故障恢复**
   - Worker 心跳超时后要重新调度 RUNNING shard
   - 旧 Worker 后续上报结果不能覆盖新 attempt

3. **框架可用性**
   - 原始 mprpc 只有同步短连接
   - 为业务补了长连接池、timeout、error_code、服务发现缓存

### 5. 如果面试官问音视频懂多少

回答：

> 项目不以编解码算法为重点。我掌握了视频容器、编码格式、分辨率、码率、帧率、切片、转码、合并等工程概念。实际编解码由 FFmpeg 执行，平台重点是分布式调度和执行管理。

这样回答是合理的。

### 6. 如果面试官问系统瓶颈

可以分析：

- Scheduler 单点可能成为瓶颈
- MetadataStore 写入可能成为瓶颈
- ResultCollector 高并发上报可能成为瓶颈
- Worker 资源上报过于频繁会增加控制面压力
- 大文件传输不应该走 RPC，应走对象存储或共享文件系统

### 7. 如果面试官问如何扩展

可以回答：

- Scheduler 分片或主备
- WorkerManager 分区管理 Worker
- ResultCollector 按 job_id hash 分片
- MetadataStore 分库分表
- 任务事件接入 MQ
- 大文件放对象存储，RPC 只传元数据

## 常见误区

### 误区 1：把重点放在 FFmpeg

不应该这样讲：

> 我做了很多 FFmpeg 参数调优。

应该讲：

> FFmpeg 是执行器，项目重点是分布式任务调度和故障恢复。

### 误区 2：所有数据都走 RPC

大视频文件不要走 RPC。

RPC 只传：

- input_path
- output_path
- shard 参数
- 状态
- 结果元信息

大文件应该走：

- 本地共享目录
- NFS
- 对象存储
- mock 文件系统

### 误区 3：没有状态机

如果没有清晰状态机，项目会像脚本调度。

必须明确：

- Job 状态
- Shard 状态
- Worker 状态
- 状态转换条件

### 误区 4：没有故障故事

大厂面试很关注故障。

必须准备：

- Worker 执行中宕机怎么办
- Worker 超时后又上报结果怎么办
- Scheduler 重启怎么办
- ResultCollector 重复收到结果怎么办
- ZK 短暂不可用怎么办

## 最终完成标准

当你能做到下面这些，项目就比较完整了：

- 能启动 1 个 JobService、1 个 Scheduler、1 个 WorkerManager、多个 Worker、1 个 ResultCollector
- 能提交一个视频任务并拆成多个 shard
- 多 Worker 能并行执行
- Worker 宕机后 shard 能重新调度
- 任务状态能查询
- 结果能聚合
- 有压测数据
- 有故障恢复测试
- 有清晰架构图和状态机图
- 能讲清楚哪些框架能力是被业务倒逼出来的

