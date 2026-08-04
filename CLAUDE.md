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
| 8. 压测、故障测试和文档整理 | ✅ 代码审查与Bug修复（2026-08-01），压测待进行 |
| 9. 数据持久化（MySQL） | ✅ 完成（2026-08-03） |
| 10. 中间件集成（Redis+MQ） | ✅ 完成（2026-08-03，三批全部完成；2026-08-04 复验修复 MqClient 消费锁缺陷；真实 Broker 重启实测待用户 sudo） |
| 11. 可观测性升级（Prometheus+Grafana） | ✅ 完成（2026-08-03，配置产物交付 + curl/日志告警实测；Prometheus/Grafana 部署待用户按 README 安装） |
| 12. 客户端 GUI（Qt6 桌面应用） | ✅ 完成（2026-08-04，`bin/video_gui`；GUI 交互验收待用户操作） |
| 13. 容器化与 CI/CD | ✅ 完成（2026-08-04，`docker/` + `docker-compose.yml` + `.github/workflows/ci.yml`；compose 一键启动/集成测试/--scale 全部本机实测通过，CI 绿勾待 push） |

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

**当前状态**：阶段 9 完成。三个 Store 以 MySQL 为唯一数据源（jobs/shards/workers 表），服务重启数据不丢失、多进程天然共享；删除 UpdateJobStatus/NotifyJobCanceled 两个跨进程同步 RPC。Mock 模式保留作为 fallback（`executor_mode=mock`）。

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
| `更新业务日志/` | 业务平台开发日志（#1-#8），覆盖阶段 2~8 每轮迭代 |
| `视频转码平台/` | 系统设计（架构/服务拆分/状态机/调度策略/数据模型）+ 开发路线图 + 验收标准 + 快速启动指南 |

## 关键配置项

| 配置键 | 默认值 | 说明 |
|---|---|---|
| `rpcserverip` | 必填 | Provider 绑定 IP |
| `rpcserverport` | 必填 | Provider 绑定端口 |
| `rpcserverio_threads` | 2 | IO 工作线程数（当前配置文件显式配置，未配置时默认 2） |
| `rpcserverwork_threads` | 2 | Work 业务线程数（当前配置文件显式配置，未配置时默认 2） |
| `zookeeperip` | 必填 | ZK 地址 |
| `zookeeperport` | 必填 | ZK 端口 |
| `mprpcclient_connections_per_endpoint` | 8 | 客户端连接池单 endpoint 最大连接数 |
| `executor_mode` | ffmpeg | Worker 执行器：ffmpeg=真实转码 / mock=sleep 模拟 |
| `ffmpeg_work_dir` | /tmp/transcode_worker | FFmpeg 工作目录 |
| `target_resolution` | (空) | 目标分辨率，如 1280x720 |
| `target_bitrate` | 0 | 目标视频码率（kbps），0=保持原码率 |
| `shard_duration_sec` | 15 | 每 shard 默认时长（秒） |
| `job_duration_fallback_sec` | 60 | ffprobe 探测失败时的回退时长（秒） |
| `mysqlhost` | 127.0.0.1 | MySQL 地址（阶段 9 起三个 Store 的唯一数据源） |
| `mysqlport` | 3306 | MySQL 端口 |
| `mysqluser` / `mysqlpassword` | 必填 | MySQL 账号/密码 |
| `mysqldbname` | video_platform | MySQL 数据库名 |
| `mysql_pool_size` | 4 | MySQL 连接池大小（1-64，预创建+线程安全借用） |
| `metrics_port` | 0（关闭） | Prometheus metrics HTTP 端口（阶段 11，<=0 不启用；单机各服务 9091-9097） |

## 下一步工作

### 阶段 13（✅ 2026-08-04）

容器化与 CI/CD，详见 `doc/更新业务日志/15. 阶段13容器化与CI-CD.md`。

核心改动：
- **Docker 化**：`docker/Dockerfile`（多阶段：builder 编译全部二进制 + runtime 精简镜像，`-DENABLE_NATIVE_MARCH=OFF` 去掉 march=native 保可移植）；`docker/conf/` ×6 docker 专用配置（`rpcserverip=0.0.0.0` + host 指向 compose 服务名）；`docker-compose.yml` 编排 4 中间件（zookeeper:3.8 + mysql:8.0 + redis:7 + rabbitmq:3.12，healthcheck + `depends_on: condition: service_healthy`）+ 5 平台服务（共享单镜像、command 区分、worker 不映射端口支持 `--scale`）；视频经 `./data/videos`、`./data/output` 挂载
- **CI**：`.github/workflows/ci.yml` — build（apt 装依赖 + cmake + upload bin/ artifact）→ unit-test（43/43）→ integration（`docker compose up -d --build` + `scripts/integration_test.sh`：testsrc 生成 30s 视频 → exec job_client 管道提交 → `--watch` 到 SUCCESS → 校验 merged 产物，Dockerfile 的唯一真实验证点）→ benchmark（rpc_echo_server + `bench_rpc_stress --direct --keepalive -c 50 -m 200 -s 64`）
- **配套代码改动**：① `video_platform/CMakeLists.txt` Qt6 REQUIRED → QUIET+if（无 Qt6 时跳过 GUI，本机不受影响）② 根 `CMakeLists.txt` 新增 `ENABLE_NATIVE_MARCH` 选项 ③ `transcode_worker.cpp` 新增 `ResolveWorkerId()`（环境变量 WORKER_ID > 配置 > hostname，支持 scale 副本唯一 worker_id；main 启动校验同步改造）
- **实测修复（4 个真实环境缺陷 + 2 个部署问题）**：① Docker/CI 基础镜像 22.04 → 24.04（22.04 的 protobuf 3.12 与提交的 3.21 生成码不兼容）② `ZookeeperUtil.h` include zookeeper.h 前加 `#define THREADED`（noble 3.9.1 同步 API 被宏包裹，本机旧头文件掩盖）③ 新增 `mprpc/include/mprpcutil.h` `GetLocalIp()`——rpcserverip=0.0.0.0 时 ZK 注册/Worker 上报自动换容器实际 IP（否则消费者连 0.0.0.0 必失败）④ worker main 的 `LoadRequired("worker_id")` 漏改导致 docker 启动崩溃循环 ⑤ ZK healthcheck 需显式 `bash -c`（CMD-SHELL 走 dash 不支持 /dev/tcp）⑥ 宿主端口被本机部署占用 → compose 改用替代端口 21810/23306/19001 等（容器内不变）
- **实测结果**：9 容器一键启动 + 集成测试 PASS（提交→转码→合并→SUCCESS + 产物校验）+ `--scale transcode_worker=3` 唯一 worker_id；**遗留**：CI 绿勾待 push（`git push origin main`）

### 阶段 12（✅ 2026-08-04）

Qt6 桌面客户端，详见 `doc/更新业务日志/14. 阶段12客户端GUI.md`。

核心改动：
- **服务端**：`job.proto` 新增 `ListJobs` RPC（`ListJobsRequest{limit}` → `repeated JobInfo`，按 created_at 倒序）；`JobStore::ListRecent(limit)`（SQL = ListAll + `ORDER BY created_at DESC LIMIT n`）；`JobServiceImpl::ListJobs` handler（无 Redis 缓存——列表 2s 高频 + 单表 SQL <1ms，缓存无收益）
- **GUI**（`video_platform/gui/`，`bin/video_gui`，Qt6.4.2 + `CMAKE_AUTOMOC`，link `video_common`）：
  - `rpc_client.h/.cpp` — **QRunnable + QThreadPool 异步封装**（mprpc stub 同步阻塞，绝不能进 UI 线程）：Submit/Query/ListJobs/ListWorkers/Cancel 5 个 RPC，信号回 UI 线程；`JobItem/ShardItem/WorkerItem` 轻量结构跨线程
  - `main_window` — 三页签（任务/Worker/日志）+ 2s QTimer 统一刷新 + 选中行 QueryJob 详情（进度条）；双击 SUCCESS 行 → `QProcess::startDetached("ffplay", {output_path/{job_id}_merged.mp4})` 预览
  - `job_table` — 任务列表 7 列、状态彩色；`worker_panel` — Worker 负载（CPU/内存 ≥80% 红、≥50% 黄）；`log_panel` — tail `program_log/` 最新文件（偏移续读、跟随按天轮转）；`submit_dialog` — 拖拽视频自动填路径 + 下拉分辨率/格式 + 码率/优先级/shard 时长
- **环境**：`sudo apt install qt6-base-dev`；`conf/video_gui.conf` 只需 zookeeperip/port
- 启动：`./bin/video_gui -i video_platform/conf/video_gui.conf`（WSLg DISPLAY=:0）
- 遗留：GUI 交互验收（拖拽/双击预览）待用户操作；筛选/排序（QSortFilterProxyModel）可后续加

### 阶段 10 复验发现并修复（2026-08-04，详见 log 14 Bug 1）

**MqClient 消费锁占用 → mq=push 误判 + WARN 刷屏**：`ConsumeBlocking` 锁内 poll(2s) + 消费线程紧循环 → consume_mutex_ 几乎 100% 被占 → `connected()` try_lock_for(1s) 永远失败 → `consume lock busy` 每日刷 1000+ 条 + SchedulingLoop 永远 `(mq=pull)`。修复：① poll 移出锁（fd 独立于连接内部状态，消费连接只由消费线程销毁，poll 期间无并发写者）② `connected()` 改用 `std::atomic<bool> consume_alive_` 无锁读取 ③ 销毁分支持锁且先置标志。验证：`interval=5000ms (mq=push)` 首次正确识别、刷屏消失、Push 分配 24ms。

### 阶段 11（✅ 2026-08-03）

可观测性升级，详见 `doc/更新业务日志/12. 阶段11可观测性升级.md`。

核心改动：
- 新增 `mprpc/include/mprpcmetrics.h` + `src/mprpcmetrics.cc`（框架层指标库：Counter/Gauge/Histogram（固定桶+前缀和导出+线性插值分位）、标签分片、`ExportText()` text format 0.0.4、Gauge 采样器线程、AlertLoop 日志告警兜底（30s 一轮，触发/恢复打日志）、`MetricsHttpServer`（独立线程 + 自研 TcpServer 起 HTTP，GET /metrics 200 / 其余 404，可降级组件）
- 插桩：mprpcchannel CallMethod RAII 计时 → `rpc_latency_ms{method}`；job_service `job_submitted_total`；RC `MarkJobTerminal` 单入口 → `job_success/failed_total`；scheduler 4 处 `retry_count++` → `shard_retry_total` + SchedulingLoop 迭代 RAII → `schedule_loop_duration_ms` + 5s 采样器（新增 `ShardStore::CountByStatus()` 单条 GROUP BY）→ `shard_running/waiting/scheduler_queue_size(WAITING+ASSIGNED)/shard_count{status}`；worker `transcode_duration_ms`；worker_manager 5s 采样器 → `worker_online` + 每 Worker 负载
- 内置日志告警 4 条：worker_offline(WM)、scheduler_backlog(Scheduler)、job_failed_rate_high(RC, RateEstimator 5min 窗口)、rpc_latency_p99_high(全部服务)
- 配置产物 `video_platform/observability/`：prometheus.yml（9091-9097）、alerts.yml（4 条 PromQL）、grafana-dashboard.json（4 行：Job 吞吐/Shard 分布/Worker 热力图/RPC 延迟）、README.md
- 验收：5 端口 curl 200 + 404；全链路任务 SUCCESS 后各 counter/gauge/histogram 增长；故障注入实测 `ALERT [worker_offline] firing/recovered` 与 `ALERT [job_failed_rate_high] firing`；**Prometheus 2.45（apt）+ Grafana 12.0.0（/home/wevix/grafana）实测部署**：5 targets UP、4 规则加载、dashboard 导入成功、面板数据可查（datasource proxy 需 `X-Grafana-Org-Id: 1` 头）

**实测暴露并修复两个框架级缺陷**（详见日志 12 Bug 5/6）：
- **ZK 同步调用无限阻塞**（`ZookeeperUtil.cc`）：`zoo_get/zoo_get_children` 在连接会话未建立时无限等待 → 改异步 API + 3s 信号量超时 + `zoo_state()` 快速失败 + 回调 `caller_gone` 延迟回收防 UAF
- **RabbitMQ 消费锁死**（`mq_client.h/.cpp`）：`amqp_consume_message` 内部 poll 超时不可靠，消费线程持锁无限阻塞 → SchedulingLoop 的 `connected()` 永久等锁停摆 → ①消费改自控 `poll(fd)` + `amqp_consume_message({0,0})` 立即读 ②`consume_mutex_` 改 `std::timed_mutex`，`connected()/Ack()/Reconnect()` 用 `try_lock_for(1s)` 降级（打 `consume lock busy, degrade to Pull polling`）③`OpenChannel` 后 `SO_RCVTIMEO/SO_SNDTIMEO` 2s 兜底。MQ 故障 → Pull 轮询接管（实测 5/5 SUCCESS），恢复后 Push 自动切回

### 阶段 9（已完成 ✅ 2026-08-03）

### 阶段 9（已完成 ✅ 2026-08-03）

MySQL 持久化替代内存 Store，详见 `doc/更新业务日志/10. 阶段9数据持久化MySQL.md`。

核心改动：
- 新增 `mysql_pool.h/.cpp`（libmysqlclient 连接池：固定大小预创建、mutex+cv 借用归还、mysql_ping 保活、CLIENT_FOUND_ROWS、自动建表）
- 三个 Store 接口不变、内部实现全 SQL 化；新增 `UpdateIfStatus` 条件更新（状态推进防旧快照覆盖其他进程的推进）
- 删除跨进程同步：`UpdateJobStatus` / `NotifyJobCanceled` 两个 RPC（proto+handler+调用方）、JobService/RC 的"从请求构造本地副本"逻辑、RC 的跨进程 QueryJob 查 shard_count
- 配置新增 `mysqlhost/mysqlport/mysqluser/mysqlpassword/mysqldbname/mysql_pool_size`
- 验收：全链路 SUCCESS + MySQL 落盘；kill 全部服务重启数据不丢；Scheduler/Worker 崩溃恢复；Store 操作平均延迟 ~0.2ms（<2ms 验收线）

### 阶段 10（✅ 2026-08-03 三批全部完成）

三批（Redis 缓存 + RabbitMQ 事件驱动调度 + ZK 缓存改造）完成，详见 `doc/更新业务日志/11. 阶段10中间件集成.md`。

**第一批（Redis）核心改动**：
- 新增 `redis_client.h/.cpp`（hiredis 封装：单连接+mutex、失败重连 1 次、2s 超时、**可降级组件语义**——连接失败只 WARN 不拒绝启动，区别于 MySQL 的 fail-fast）
- WorkerManager Heartbeat 双写 Redis 快照（`HSET worker:load`，值内嵌 ts）
- Scheduler `LoadOnlineWorkers` helper：Redis 快照优先（20s ts 过期过滤），失败/无数据回退 ListWorkers RPC（按次降级，恢复自动切回）
- JobService QueryJob 进度缓存（`SETEX job:progress:{id}` TTL 60s，只缓存成功响应）；RC 结果落定时 DEL
- 分布式锁：AssignShard 前 `SETNX shard:lock:{id} EX 10`，值校验释放，Redis 故障降级放行（MySQL 条件更新兜底）

**第二批（RabbitMQ）核心改动**：
- 新增 `mq_client.h/.cpp`（rabbitmq-c 封装：**双连接双锁**——发布/消费严格分连接，消费阻塞不卡发布；durable exchange/queue + delivery-mode=2 持久化消息 + 手动 ACK；拓扑声明幂等；失败重连）
- 事件拓扑：`job.events`→`shard.waiting`（分配通知，消息体=shard_id）+ `shard.events`→`result.pending`（结果数据，消息体=序列化 ReportShardResultRequest）
- 发布端在 **Scheduler**（shard 切分在 ScheduleJob，4 个 WAITING 触发点：新建/重试/离线/超时重置）
- 分配逻辑抽取 `TryAssignShard`（轮询与 MQ 消费线程共用）；MqConsumeLoop 消费即时分配 + 幂等跳过 + 无条件 ACK
- SchedulingLoop 共存：MQ 在线降频 5s 兜底（超时重扫改按时间 30s），掉线自动恢复 2s 轮询
- Worker 结果上报 MQ 优先回退直连 RPC；RC 消费 result.pending 复用公共 `HandleReportShardResult`
- 修复 Bug：初版单连接单锁导致 ConsumeBlocking 持锁卡住 ScheduleJob 发布 → RPC 超时（双连接修复）

**验收**：Push 调度延迟实测 33ms（<100ms ✅）；Redis/MQ 正常/故障（端口不可达模拟）/恢复三态全链路 SUCCESS；MQ 故障自动回退 Pull 轮询 + 直连 RPC。真实 Broker 重启测试（消息不丢）待用户 sudo 操作（log11 遗留说明）。

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

### 阶段 8：压测、故障测试和文档整理（2026-08-01 进行中）

系统性验证整个平台在负载下的表现。

**压测结果**：

| 测试项 | 结果 |
|--------|------|
| 单元测试（buffer/threadpool/rpc_protocol） | 43/43 通过 |
| echo 压测（100 并发阶梯） | 峰值 17 万 QPS，P50 ~60us，0 失败 |
| echo 64KB 大报文 | 修复 UAF 前崩溃，修复后 0 失败 |
| RPC 压测（直连长连接） | 12.3 万 QPS，P50 403us |
| RPC 阶梯压测 | 50 并发达峰 ~5 万 QPS；默认 work_threads=2 在 100 并发饱和（P99 28ms），调至 16 后 2.9ms |
| 平台全链路（真实 ffmpeg） | 30s 视频 → 2 shard → 合并 30.02s 精确，JOB_SUCCESS |
| 平台压测 | 10 任务并发全部 SUCCESS，队列峰值 10+，worker 满载保护生效 |
| 故障测试 | worker 转码中被杀 → 重调度恢复 SUCCESS；ffmpeg 失败 → 3 次重试 → JOB_FAILED |

**压测发现并修复的 Bug（3 项）**：

1. **（严重）Channel::handleEvent UAF 崩溃**（`wevix_muduo/src/Channel.cpp`）——64KB 大报文触发 SIGSEGV。epoll 同批次 `IN|RDHUP|OUT` 事件时，read 回调内 `handleClose` 同步销毁 Connection/channel_，返回后 `handleEvent` 继续访问已析构的 this。修复：每个回调执行后立即 `return`，禁止再访问 this（ET 模式下 `enableWriting` 的 `EPOLL_CTL_MOD` 保证 EPOLLOUT 重新触发不丢失）。
2. **已完成任务的 shard 被重复调度**——ResultCollector 任务终态只通知 JobService，Scheduler 不知道任务已完成；Worker 死亡时 `NotifyWorkerOffline` 会把终态任务的残留 ASSIGNED/RUNNING shard 重置为 WAITING 重新分配。修复：① `MarkJobTerminal` 终态后通知 Scheduler（复用 `CancelJobShards`，reason=JOB_TERMINAL）；② `CancelJobShards` 先把本地非终态 shard 标记 CANCELED 再通知 Worker；③ `NotifyWorkerOffline`/超时扫描/分配循环均增加 job 终态（SUCCESS/FAILED/CANCELED）检查。
3. **查询接口 retry=N/0 显示不一致**——Scheduler/ResultCollector 的 `UpdateJobStatus` 通知 shard 快照漏填 `max_retry`，JobService 幂等覆盖把 max_retry 清零。修复：两处快照补 `set_max_retry`。

**已记录文档**：`doc/更新业务日志/8. 阶段8代码审查与Bug修复.md`（代码审查部分）；压测与故障测试部分见 `doc/更新业务日志/` 下 2026-08-01 相关日志。
