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
| 3. Worker 注册与心跳 | ✅ 完成（2026-07-28） |
| 4. mock 任务调度闭环 | ✅ 完成（2026-07-28） |
| 5. 失败重试和故障恢复 | ✅ 完成（2026-07-29） |
| 6. 接入 FFmpeg 执行器 | ✅ 完成（2026-07-31） |
| 7. 资源感知调度和监控 | ✅ 完成（2026-07-31） |
| 8. 压测、故障测试和文档整理 | ⬜ 下一步 |

## 阶段 6 完成内容

### 新增文件

- `video_platform/include/video_platform/ffmpeg_executor.h` — FfmpegExecutor 头文件（全静态方法工具类）
- `video_platform/src/transcode_worker/ffmpeg_executor.cpp` — 完整实现（Probe / Slice / Transcode / Screenshot / Merge / ExecuteCommand / ParseProgress）

### 修改文件

- `video_platform/src/transcode_worker/transcode_worker.cpp` — 新增 `FfmpegExecute`（切片+转码一 pass）+ `ReportShardResult`；`AssignShard` 按 `executor_mode` 分发
- `video_platform/src/scheduler_service/scheduler_service.cpp` — `ScheduleJob` 调用 `FfmpegExecutor::Probe()` 自动探测真实视频时长，替换 mock 时长配置；最后一个 shard 时长自适应
- `video_platform/CMakeLists.txt` — ffmpeg_executor.cpp 加入 video_common 公共库
- `video_platform/conf/transcode_worker_*.conf` — 默认 `executor_mode=ffmpeg`，新增 `ffmpeg_work_dir` / `target_resolution` / `target_bitrate`；删除 `mock_execution_time_ms` / `mock_fail_ratio`
- `video_platform/conf/scheduler.conf` — `mock_job_duration_sec` → `job_duration_fallback_sec`，`mock_shard_duration_sec` → `shard_duration_sec`
- `doc/更新业务日志/6. 阶段6接入FFmpeg执行器.md` — 完整修改日志
- `doc/视频转码平台/快速启动指南.md` — 全面更新到阶段 6

### 关键设计决策

- **切片+转码合并**：`-ss`/`-t` 直接传给 `Transcode()`，不单独调 `Slice()`，一 pass 完成，帧精确
- **Slice 改为重编码**：`-c copy` → `libx264 -preset ultrafast`，精密切片
- **Scheduler 自动探测**：`ScheduleJob` 先调 `FfmpegExecutor::Probe()` 获取真实时长，失败时回退到 `job_duration_fallback_sec`
- **ffmpeg_executor.cpp 放入 video_common**：transcode_worker 和 scheduler_service 都依赖它

### 阶段 6 遗留项（5 项，全部完成 ✅ 2026-07-31）

| 优先级 | 项目 | 说明 | 状态 |
|--------|------|------|------|
| 1 | **合并集成到 ResultCollector** | `Merge()` 已就绪，ResultCollector 的 `MarkJobTerminal` 在 JOB_SUCCESS 时调用，产出完整视频 | ✅ |
| 2 | **转码参数透传** | `target_resolution`/`target_bitrate` 从 Worker 配置改为 JobInfo→ShardInfo→Worker 透传 | ✅ |
| 3 | **popen → fork+exec** | popen 不暴露 PID，升级为 fork+exec+pipe，支持 kill 子进程即时 cancel | ✅ |
| 4 | **FFmpeg 启动检查** | 启动时检测 ffmpeg/ffprobe 可用性，不可用时 executor_mode=ffmpeg 拒绝启动 | ✅ |
| 5 | **截图集成** | `Screenshot()` 已就绪，转码成功后自动截取视频中点帧，随结果上报 | ✅ |

### 遗留项实现详情

1. **Merge 集成**：`MarkJobTerminal()` 在 JOB_SUCCESS 时收集所有 shard 的 output_path，按 shard_index 排序后调用 `FfmpegExecutor::Merge()` 产出 `{job_id}_merged.mp4`，失败只打 WARN 不改变终态。
2. **参数透传**：`common.proto` ShardInfo 新增 `target_resolution`(15) / `target_bitrate`(16) 字段；`ShardRecord` 同步新增；Scheduler 创建 shard 时从 JobRecord 拷贝并在 AssignShard/NotifyJobServiceStatus/ScheduleJobResponse 中透传；Worker `FfmpegExecute()` 从 `rs->info` 读取；Worker 配置文件删除 `target_resolution`/`target_bitrate`。
3. **fork+exec**：`ExecuteCommand()` 用 `pipe()+fork()+execl("/bin/sh",...)+fdopen` 替代 popen；新增 `should_cancel` 回调参数，返回 true 时 `kill(pid, SIGTERM)` + 优雅等待 5s → `SIGKILL`；`Transcode()` 和调用方透传 `should_cancel`。
4. **启动检查**：`transcode_worker.cpp` main() 中 `executor_mode=="ffmpeg"` 时调用 `FfmpegExecutor::CheckAvailable()`，不可用则 `LOG_ERROR + EXIT_FAILURE`。
5. **截图集成**：`result.proto` ReportShardResultRequest 新增 `screenshot_path`(11)；`FfmpegExecute()` 转码成功后调用 `Screenshot()` 截取 `start_ms + duration_ms/2` 时间点，结果上报时填入；`ReportShardResult()` 将 `screenshot_path` 写入 ShardRecord。

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

外部依赖：pthread、protobuf、ZooKeeper C 库 (`libzookeeper_mt`)、ffmpeg + ffprobe（阶段 6 起）。

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

**启动命令**（需 ZooKeeper + ffmpeg 运行中）：
```bash
./bin/job_service -i video_platform/conf/job_service.conf &
./bin/scheduler_service -i video_platform/conf/scheduler.conf &
./bin/worker_manager -i video_platform/conf/worker_manager.conf &
./bin/transcode_worker -i video_platform/conf/transcode_worker_9004.conf &
./bin/result_collector -i video_platform/conf/result_collector.conf &
```

**Proto 文件位置**：`video_platform/proto/`，共享类型在 `common.proto`，每个服务的 RPC 接口在各自的 `.proto` 中。

**内存存储层**：`video_platform/include/video_platform/common_store.h` — 三个单例 Store（JobStore / ShardStore / WorkerStore），内部用 `unordered_map` + `shared_mutex` 实现线程安全 CRUD。⚠️ 进程内单例，多进程部署时各进程 Store 完全隔离，跨进程共享通过 RPC 同步。

**FfmpegExecutor**：`video_platform/include/video_platform/ffmpeg_executor.h` — FFmpeg/FFprobe 命令行封装，全静态方法，线程安全。
- `Probe(input)` → ffprobe 探测时长/分辨率/编码
- `Slice(input, start, dur, output)` → 按时间切片（重编码，帧精确）
- `Transcode(input, output, res, bitrate, start, dur, cb)` → 转码 + 进度回调（切片+转码一 pass）
- `Screenshot(input, ts, output)` → 截图
- `Merge(inputs, output)` → concat demuxer 合并

**当前状态**：阶段 6 完成。真实 ffmpeg 转码全链路跑通（30s 720p 视频 → 2 shard → 各 15s 精确输出 → JOB_SUCCESS）。Mock 模式保留作为 fallback（`executor_mode=mock`）。

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
| `更新业务日志/` | 业务平台开发日志（#1-#6），覆盖阶段 2~6 每轮迭代 |
| `视频转码平台/` | 系统设计（架构/服务拆分/状态机/调度策略/数据模型）+ 开发路线图 + 验收标准 + 快速启动指南 |

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
| `executor_mode` | ffmpeg | Worker 执行器：ffmpeg=真实转码 / mock=sleep 模拟 |
| `ffmpeg_work_dir` | /tmp/transcode_worker | FFmpeg 工作目录 |
| `target_resolution` | (空) | 目标分辨率，如 1280x720 |
| `target_bitrate` | 0 | 目标视频码率（kbps），0=保持原码率 |
| `shard_duration_sec` | 15 | 每 shard 默认时长（秒） |
| `job_duration_fallback_sec` | 60 | ffprobe 探测失败时的回退时长（秒） |

## 下一步工作

### 阶段 7（已完成 ✅ 2026-07-31）

~180 行改动，详见 `doc/更新业务日志/7. 阶段7资源感知调度和监控.md`。

核心改动：
- Worker 真实 CPU/内存采集（`/proc/stat`、`/proc/meminfo`）
- `WorkerRecord` / `WorkerInfo` 扩展 cpu_usage/memory_usage
- `UpdateHeartbeat` / `ListWorkers` 全链路负载数据流
- 调度加权评分：`score = slots*10 - cpu*0.5 - mem*0.2`
- shard 优先级队列（按 job.priority 降序）
- Worker 过载保护（running>=max || cpu>90% → reject）
- `[SchedulerMetrics]` 每 10s 输出调度快照

### 阶段 8：压测、故障测试和文档整理

系统性验证整个平台在负载下的表现。
