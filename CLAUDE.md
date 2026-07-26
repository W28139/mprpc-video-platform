# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

muduo_im 是一个基于 Reactor 模式的 Linux C++ 网络库及其上层 RPC 框架，并基于它们构建了分布式视频处理平台。分为三个子项目：

- **wevix_muduo**：自研网络库（受 muduo 启发），提供 TCP 服务器、事件循环、IO 多路复用（epoll）、线程池、内存池、异步日志
- **mprpc**：基于 wevix_muduo 构建的 RPC 框架，使用 protobuf 序列化 + ZooKeeper 服务注册发现
- **video_platform**：分布式视频处理与资源调度平台（基于 mprpc），包含 5 个微服务

## 当前开发进度

按照 `doc/视频转码平台/04_开发路线图.md` 的 8 阶段规划：

| 阶段 | 状态 |
|------|------|
| 1. 框架 P0 能力补齐 | ✅ 完成（2026-07-25） |
| 2. 项目骨架和 proto 设计 | ✅ 完成（2026-07-26） |
| 3. Worker 注册与心跳 | ⬜ 下一步 |
| 4. mock 任务调度闭环 | ⬜ |
| 5. 失败重试和故障恢复 | ⬜ |
| 6. 接入 FFmpeg 执行器 | ⬜ |
| 7. 资源感知调度和监控 | ⬜ |
| 8. 压测、故障测试和文档整理 | ⬜ |

开发日志见 `doc/更新框架日志/`（框架）和 `doc/更新业务日志/`（业务）。

## 构建命令

```bash
# 初始配置（在项目根目录）
cmake -B build -DCMAKE_BUILD_TYPE=Release
# Debug 构建
cmake -B build -DCMAKE_BUILD_TYPE=Debug

# 编译
cmake --build build -j$(nproc)

# 编译产物
# 静态库: lib/libwevix_muduo.a, lib/libmprpc.a, lib/libvideo_common.a
# 服务: bin/job_service, bin/scheduler_service, bin/worker_manager, bin/transcode_worker, bin/result_collector
# 测试: bin/echo_server, bin/bench_echo_stress, bin/test_*, bin/rpc_*, bin/*_consumer, bin/*_provider
```

外部依赖：pthread、protobuf、ZooKeeper C 库 (`libzookeeper_mt`)。

> **注意**：`mprpc/protobuf/` 目录包含完整的 protobuf 源码分发，已直接 vendored 到项目中（非 git submodule）。`video_platform/proto/generated/` 下的 `.pb.cc/.pb.h` 由 protoc 生成，已提交仓库。修改 proto 后重新生成：
> ```bash
> cmake --build build --target gen_protos
> ```

## 运行测试与压测

所有可执行文件在 `bin/` 目录下。

### wevix_muduo 测试

```bash
# 单元测试
./bin/test_buffer
./bin/test_threadpool

# 集成测试：echo 服务器/客户端
./bin/echo_server <ip> <port>   # 启动 echo server
./bin/echo_client <ip> <port>   # 启动 echo client

# 压测
./bin/bench_echo_stress <ip> <port>  # 多连接 echo 压测
```

### mprpc 测试

```bash
# RPC 协议单元测试
./bin/test_rpc_protocol

# 启动 RPC 服务（需要 ZooKeeper 和配置文件）
./bin/rpc_echo_server -i test/rpc_test.conf

# 启动 provider / consumer 对
./bin/user_provider -i test/rpc_test.conf
./bin/user_consumer -i test/rpc_test.conf
./bin/friend_provider -i test/rpc_test.conf
./bin/friend_consumer -i test/rpc_test.conf

# RPC 压测（支持 --direct 直连和 --keepalive 长连接模式）
./bin/bench_rpc_stress --direct --keepalive -c 50 -m 200 -s 64
```

配置文件位于 `test/conf/` 目录下（`rpc_test.conf`、`user.conf`、`friend.conf`）。

### RPC 配置文件格式

```ini
rpcserverip=127.0.0.1
rpcserverport=9877
rpcserverio_threads=4
rpcserverwork_threads=4
zookeeperip=127.0.0.1
zookeeperport=2181
```

## 架构

### wevix_muduo 网络库

**One Loop Per Thread 模型**，类似 muduo 但独立实现：

```
mainLoop_ (Acceptor) ──► 分配新连接 ──► subLoops_ (Round-Robin)
                                              │
                                         IO 线程处理 read/write
                                              │
                                    ┌─────────┴──────────┐
                                    │                    │
                              直接回调处理         提交到 workThreadPool_
                              (轻量业务)           (CPU 密集/阻塞业务)
```

核心类关系：
- `TcpServer` → 持有 `EventLoop`（mainLoop + subLoops）、`Acceptor`、`ThreadPool`、`connections_` 表
- `EventLoop` → 封装 epoll + eventfd(wakeup) + timerfd(定时器)，每个线程运行一个 loop
- `Connection` → 一个 TCP 连接 = Socket + Channel + inputBuffer + outputBuffer
- `Buffer` → 三区内存模型（prependable / readable / writable），支持 readv 散射读、零拷贝 prepend
- `Channel` → fd + events + 回调（对 epoll 事件的轻量封装）
- `ThreadPool` → 支持 MODE_FIXED / MODE_CACHED，带背压队列

**内存池**（`memory_pool/`）：三层架构类似 tcmalloc —— ThreadCache → CentralCache → PageCache，通过 `MemoryPool::allocate()` / `MemoryPool::deallocate()` 统一对外。

**异步日志**（`AsyncLogger`）：业务线程格式化后入队，后台写线程负责落盘 + 控制台输出。日志按天轮转。`LOG_DEBUG` 在 Release 构建（`-DNDEBUG`）下编译为零开销。

### mprpc RPC 框架

**服务端**（Provider）：
```
RpcProvider::Run()
  ├─ 读取配置 (rpcserverip, rpcserverport, io/work threads)
  ├─ 创建 TcpServer → 注册回调 (OnConnection / OnMessage / OnClose)
  ├─ 连接 ZooKeeper → 注册服务到 /mprpc/services/{service}/{method}/instance-*
  ├─ 设置 RpcMessageCodec → 帧边界自动处理
  └─ server.start() → 进入事件循环
```

**客户端**（Consumer）：
```
MprpcChannel::CallMethod()
  ├─ 序列化请求 → [total_len] + [header_size + RpcHeader + args]
  ├─ 服务发现：Check Cache → ZooKeeper /mprpc/services/{service}/{method}/*
  ├─ 轮询选择 endpoint → GetPooledConnection(ip, port)
  ├─ SendAll → RecvAll → 校验 request_id → 反序列化响应
  └─ 重试：连接失败时失效缓存 + 重新发现 + 重试 1 次
```

**RPC 协议帧格式**（wire format）：
```
[total_len(4B)] + [magic(2B, 0x4d52 "MR")] + [version(2B)] + [payload]
  payload (请求):
    [header_size(4B)] + [RpcHeader(protobuf)] + [args(protobuf)]
  payload (响应):
    [response_header_size(4B)] + [RpcResponseHeader(protobuf)] + [response_body]
```

客户端通过 `RpcMessageCodec` (mprpccodec.h) 在 Connection 层自动拆帧，应用层回调收到完整 payload。单帧最大 64MB。

**关键设计决策**：
- Provider 不管理 service 对象生命周期（裸指针），调用方需自行保证
- Connection pool 按 endpoint（ip:port）分片，每个 endpoint 最大连接数可配置 (`mprpcclient_connections_per_endpoint`，默认 8)
- ZK 服务地址缓存 + 连接失败时失效重刷，避免每次 RPC 都读 ZK
- 同步 RPC 带超时 + deadline 传递到 Provider 端，超时请求快速拒绝
- 已同步 RPC 调用为主，异步 done 回调已有基础设施但暂未全面使用

### video_platform 视频处理平台

**5 个微服务 + 调用关系**：

```text
Client/CLI ──SubmitJob──► JobService ──ScheduleJob──► SchedulerService
                                                           │
                                              ListWorkers  │  AssignShard
                                                           ▼
                              WorkerManager ◄──────── TranscodeWorker
                                    ▲                        │
                                    │                        │ ReportProgress
                                    └── Heartbeat            │ ReportResult
                                                             ▼
                                                     ResultCollector
```

每个服务遵从统一的启动模式：
```cpp
AsyncLogger::init() → MprpcApplication::Init(argc, argv) → RpcProvider.NotifyService() → provider.Run()
```

**服务端口分配**：

| 服务 | 二进制 | 端口 |
|------|--------|------|
| JobService | `bin/job_service` | 9001 |
| SchedulerService | `bin/scheduler_service` | 9002 |
| WorkerManagerService | `bin/worker_manager` | 9003 |
| TranscodeWorker | `bin/transcode_worker` | 9004 |
| ResultCollectorService | `bin/result_collector` | 9005 |

**启动命令**（需 ZooKeeper 运行中）：
```bash
./bin/job_service -i video_platform/conf/job_service.conf &
./bin/scheduler_service -i video_platform/conf/scheduler.conf &
./bin/worker_manager -i video_platform/conf/worker_manager.conf &
./bin/transcode_worker -i video_platform/conf/transcode_worker.conf &
./bin/result_collector -i video_platform/conf/result_collector.conf &
```

**Proto 文件位置**：`video_platform/proto/`，共享类型在 `common.proto`，每个服务的 RPC 接口在各自的 `.proto` 中。

**内存存储层**：`video_platform/include/video_platform/common_store.h` — 三个单例 Store（JobStore / ShardStore / WorkerStore），内部用 `unordered_map` + `shared_mutex` 实现线程安全 CRUD。

**当前状态**：阶段 2 骨架。所有 RPC 方法只打 `LOG_INFO` 返回空响应，不通业务逻辑。记忆存储的 JobService 已能创建/查询/取消 Job；WorkerManager 已能处理注册/心跳/查询。

## 压测工具

`bench_echo_stress`（网络库压测）和 `bench_rpc_stress`（RPC 压测）支持：
- QPS 和延迟百分位（P50/P90/P99/P99.9）统计
- 多报文大小对比（`-s` 指定或自动遍历）
- 阶梯压测（逐步增加并发，观察吞吐拐点）
- CSV 导出分析结果
- `bench_rpc_stress` 支持 `--direct`（绕过 ZK 直连）和 `--keepalive`（长连接复用）

## 文档

`doc/` 目录：

| 目录 | 内容 |
|------|------|
| `设计问题/` | Buffer 设计、日志系统、RPC 框架梳理、ZK 缓存设计、mprpc 使用说明 |
| `更新框架日志/` | 框架开发日志（#1-#10），覆盖内存池→日志→mprpc→压测优化 |
| `更新业务日志/` | 业务平台开发日志（#1 起），记录阶段 2 起的每轮迭代 |
| `视频转码平台/` | 系统设计（架构/服务拆分/状态机/调度策略/数据模型）+ 开发路线图 + 验收标准 |

## 关键配置项

| 配置键 | 默认值 | 说明 |
|---|---|---|
| `rpcserverip` | 必填 | Provider 绑定 IP |
| `rpcserverport` | 必填 | Provider 绑定端口 |
| `rpcserverio_threads` | 16 | IO 工作线程数 |
| `rpcserverwork_threads` | min(8, max(2, cpu核数)) | Work 业务线程数 |
| `zookeeperip` | 必填 | ZK 地址 |
| `zookeeperport` | 必填 | ZK 端口 |
| `mprpcclient_connections_per_endpoint` | 8 | 客户端连接池单 endpoint 最大连接数 |
