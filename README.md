# mprpc-video-platform

一个从零实现的 Linux C++ 网络库、RPC 框架，以及构建在两者之上的分布式视频转码平台。

三个子项目逐层递进，每一层都是前一层的直接用户：

```
wevix_muduo（网络库）→ mprpc（RPC 框架）→ video_platform（分布式视频平台）
```

这个项目不是为了演示某个单一技术点。它覆盖的是一条完整的纵深链路：epoll 事件循环、Reactor 网络编程、内存池、异步日志、protobuf 序列化、ZooKeeper 服务发现、分布式任务调度、MySQL 持久化、Redis 缓存、RabbitMQ 事件驱动、Prometheus 可观测性，以及 FFmpeg 真实转码。网络库和 RPC 框架是自研的，平台层集成的中间件全部是真实可用的，不是 mock。整个项目在本地和 Docker 环境都跑通了全链路验证：提交一段视频，经过任务切分、多 Worker 并行转码、结果合并，最终产出完整视频。

## 项目结构

```
wevix_muduo/        自研网络库：Reactor 模型、线程池、内存池、异步日志
mprpc/              自研 RPC 框架：protobuf 序列化 + ZooKeeper 服务注册发现
video_platform/     分布式视频处理平台：5 个微服务
docker/             Docker 化部署（docker-compose 一键启动）
doc/                系统设计、开发日志、快速启动指南
scripts/            启动/停止/集成测试脚本
```

## 一、wevix_muduo：网络库

### 整体模型

采用经典的 One Loop Per Thread 模型，受 muduo 启发但完全独立实现：

```
mainLoop_ (Acceptor) ──► 新连接按 Round-Robin 分配 ──► subLoops_ × N
                                                             │
                                                        IO 线程处理 read/write
                                                             │
                                              ┌──────────────┴──────────────┐
                                        直接回调处理                   提交到 workThreadPool_
                                        （轻量业务）                 （CPU 密集/阻塞业务）
```

- `EventLoop` 封装 epoll + eventfd（唤醒）+ timerfd（定时器），每个线程跑一个 loop；
- `Acceptor` 只负责 accept，连接建立后按轮询分配给 subLoop，避免单线程成为瓶颈；
- `Connection` 持有 Socket + Channel + 双 Buffer，读写事件在所属 loop 线程内处理；
- 重活提交到 `ThreadPool`（FIXED / CACHED 两种模式，带背压队列），IO 线程不做阻塞操作。

### 值得说的几个设计

**Buffer 三区内存模型**。`Buffer` 分 prependable / readable / writable 三个区，读用 `readv` 散射读（一次系统调用把内核缓冲读完），写方向支持零拷贝 prepend（写响应头前不必把已有数据搬来搬去）。这些细节在高并发下对吞吐的影响是实打实的——echo 压测峰值 17 万 QPS、P50 约 60us，大报文场景下的表现也依赖它。

**三层内存池**。仿 tcmalloc 做了 ThreadCache → CentralCache → PageCache 三层结构，线程优先从自己的 ThreadCache 取内存，不命中才向 CentralCache 申请，中央缓存对内存块按大小分类管理，PageCache 负责向系统批量要页。小对象高频分配场景避免了大量系统调用和锁竞争。

**异步日志**。业务线程只做格式化后入队，后台专门线程负责落盘和终端输出，按天轮转。`LOG_DEBUG` 在 Release 构建（`-DNDEBUG`）下直接编译为零开销，线上不开调试日志时完全无成本。

**一个踩过的坑**。调试 64KB 大报文时遇到过 Connection 的 UAF 崩溃：epoll 同一批次同时返回 IN|RDHUP|OUT 事件时，read 回调内部触发了连接销毁，返回后 `handleEvent` 继续访问已析构的 channel。修复方式是每个事件回调执行后立即 return，之后不再访问 this；ET 模式下依赖 EPOLL_CTL_MOD 重新触发 EPOLLOUT 来保证写事件不丢失。这类问题只在真实压测下才会暴露，也是当时反复调压测工具的收获之一。

## 二、mprpc：RPC 框架

mprpc 基于 wevix_muduo 构建，protobuf 定义接口，ZooKeeper 做服务注册发现。

### 协议设计

自定义二进制帧协议，客户端按长度前缀自动拆帧：

```
[total_len(4B)] + [magic(2B, "MR")] + [version(2B)] + [payload]
  payload 请求:   [header_size(4B)] + [RpcHeader(protobuf)] + [args(protobuf)]
  payload 响应:   [response_header_size(4B)] + [RpcResponseHeader(protobuf)] + [response_body]
```

拆帧由 `RpcMessageCodec` 挂在 Connection 层完成，应用层拿到的永远是完整消息。单帧上限 64MB。

### 服务发现与调用链

```
MprpcChannel::CallMethod()
  ├─ 序列化请求 → [total_len + header + args]
  ├─ 服务发现：查本地缓存 → ZooKeeper /mprpc/services/{service}/{method}/*
  ├─ 轮询选择 endpoint → 连接池取连接
  ├─ SendAll → RecvAll → 校验 request_id → 反序列化响应
  └─ 连接失败：失效缓存 + 重新发现 + 重试 1 次
```

几个关键决策：

- **ZK 地址缓存**：不是每次调用都读 ZooKeeper，缓存命中直接走；连接失败时主动失效缓存并重刷，兼顾性能和一致性；
- **连接池按 endpoint 分片**：每个服务地址独立维护连接池，单 endpoint 最大连接数可配置，避免某对端异常时连接互相挤占；
- **同步调用 + 超时 + deadline 透传**：调用方超时后，deadline 会随请求传到 Provider 端，Provider 对已超时的请求快速拒绝，不浪费处理资源；
- **框架级 metrics 插桩**：`CallMethod` 用 RAII 计时器自动记录 `rpc_latency_ms{method}` 直方图，Prometheus 格式导出，不用改业务代码就能拿到每个 RPC 方法的延迟分布。

## 三、video_platform：分布式视频转码平台

平台基于 mprpc 构建，把一段视频切成多个 shard，分发给多台 Worker 并行转码，最后合并出完整视频。

### 五个微服务

```
Client/CLI ──SubmitJob──► JobService ──ScheduleJob──► SchedulerService
                                                            │
                                               ListWorkers │  AssignShard
                                                            ▼
                               WorkerManager ◄──────── TranscodeWorker
                                     ▲                        │
                                     │                        │ ReportProgress
                                     └── Heartbeat            │ ReportResult
                                                              ▼
                                                      ResultCollector
```

| 服务 | 职责 | RPC 端口 |
|------|------|---------|
| JobService | 接收任务提交/查询，维护任务主状态 | 9001 |
| SchedulerService | 时长探测、shard 切分、调度分配、失败重试 | 9002 |
| WorkerManager | Worker 注册、心跳、负载上报 | 9003 |
| TranscodeWorker | 真实 FFmpeg 转码执行，可多实例横向扩容 | 9004+ |
| ResultCollector | 收集 shard 结果，触发合并，落终态 | 9005 |

任务状态机：`PENDING → SCHEDULING → RUNNING → SUCCESS / FAILED`，任务下挂多个 shard 各自流转（WAITING → ASSIGNED → RUNNING → SUCCESS/FAILED），只有全部 shard 成功任务才成功。JobService 和 ResultCollector 分别是状态机的入口和出口，Worker 不直接改任务状态，避免多写者竞争。

### 调度策略

- **FFmpeg 真实时长探测**：Scheduler 在 `ScheduleJob` 时先用 ffprobe 探测视频真实时长，据此切分 shard，最后一个 shard 时长自适应；探测失败回退到配置的 fallback 时长；
- **资源感知加权评分**：Worker 心跳上报 CPU/内存占用，调度时按 `score = slots*10 - cpu*0.5 - mem*0.2` 打分选 Worker，兼顾剩余容量和当前负载；
- **优先级队列 + 过载保护**：shard 按任务优先级出队；Worker running 数达到上限或 CPU 超过 90% 时拒绝新分配；
- **多 Worker 自动均衡**：`docker compose up -d --scale transcode_worker=3` 一行命令扩容，worker_id 由环境变量 > 配置 > hostname 逐级解析，副本之间天然唯一，无需手动改配置。

### FFmpeg 执行器

`FfmpegExecutor` 封装 FFmpeg/FFprobe 命令行，全部静态方法，线程安全：

- **切片 + 转码一 pass**：`-ss` / `-t` 直接传给转码命令，避免先切完再转的多轮 IO，帧精确；
- **重编码切片**：切片不采用 `-c copy`，而是用 `libx264 ultrafast` 重编码，保证切点精确到帧；
- **fork+exec 替代 popen**：popen 拿不到子进程 PID 无法取消，改成 `pipe + fork + exec + fdopen`，通过 `should_cancel` 回调支持 kill 子进程即时取消，优雅等待 5s 再 SIGKILL；
- **进度解析**：解析 ffmpeg 的 `out_time_ms` 输出实时上报进度；
- **截图 + 合并**：转码成功后自动截取视频中点帧随结果上报；全部 shard 完成后用 concat demuxer 合并出完整视频。
- **启动自检**：`executor_mode=ffmpeg` 时启动即检测 ffmpeg/ffprobe 可用性，缺失直接拒绝启动，避免运行到一半才发现工具链不完整。

### 故障恢复

- **失败重试**：shard 失败自动重试（上限可配），worker 转码中被杀 → 重调度恢复，实测最终 SUCCESS；
- **Worker 离线重分配**：WorkerManager 检测心跳超时，把该 Worker 上未完成的 shard 重置为 WAITING 重新分配；
- **终态任务不被重复调度**：任务进入 SUCCESS/FAILED 后，残留的 ASSIGNED/RUNNING shard 不允许再被重置分配——这个问题当时真实出现过：Worker 死亡时离线处理会把已完成任务的残留 shard 重新分配出去，导致任务被重复执行。修复方式是在任务终态时通知 Scheduler 标记取消，同时离线处理、超时扫描、分配循环三处都加终态检查。

### 中间件集成

三个阶段依次接入，每一层都保留了降级路径：

- **MySQL（持久化）**：三个 Store 以 MySQL 为唯一数据源，服务重启数据不丢、多进程天然共享。连接池固定大小预创建、`mysql_ping` 保活。关键设计是 `UpdateIfStatus` 条件更新——状态推进必须带旧状态条件，防止旧快照覆盖其他进程的推进；
- **Redis（缓存 + 锁）**：WorkerManager 心跳双写 Redis 快照，Scheduler 优先读快照（20s 过期过滤），失败回退 ListWorkers RPC；分配 shard 前用 `SETNX shard:lock:{id} EX 10` 加分布式锁。Redis 被设计成**可降级组件**——连不上只 WARN 不拒启，MySQL 才是 fail-fast，职责划分明确；
- **RabbitMQ（事件驱动调度）**：事件拓扑 `job.events → shard.waiting`（分配通知）+ `shard.events → result.pending`（结果数据），durable exchange/queue + 消息持久化 + 手动 ACK，消息不丢。Scheduler 的分配逻辑抽成 `TryAssignShard` 供轮询和 MQ 消费线程共用；MQ 在线时调度延迟实测 33ms，故障时自动回退 2s Pull 轮询，恢复后自动切回。

### 可观测性

框架层提供了自研的 metrics 库（Counter/Gauge/Histogram，固定桶 + 前缀和导出 + 线性插值算分位），5 个服务各起一个 HTTP 端口暴露 Prometheus 文本格式指标：

- 任务计数（提交/成功/失败）、shard 分布、调度循环耗时；
- Worker 在线数、负载采样（Gauge 采样器线程 5s 一轮）；
- RPC 延迟直方图、转码耗时。

配套交付了 `prometheus.yml` + 4 条 PromQL 告警规则（worker 离线、调度积压、任务失败率、RPC P99 长尾）+ Grafana 面板（任务吞吐、shard 分布、Worker 负载热力图、RPC 延迟四行）。即使不部署 Prometheus，每个服务进程内还有告警兜底循环，触发和恢复都会直接打日志 `ALERT [xxx] firing/recovered`——可观测性是平台自身的一部分，不依赖外部组件就位。

## 四、测试与验证

不止是单元测试，这个项目的大量验证发生在真实运行环境：

- 单元测试 43/43 通过（buffer / threadpool / RPC 协议）；
- echo 压测：100 并发阶梯 17 万 QPS，P50 约 60us，64KB 大报文 0 失败；
- RPC 压测：直连长连接 12.3 万 QPS，P50 403us；也验证了 work_threads 从 2 调到 16 后 P99 从 28ms 降到 2.9ms 的饱和拐点；
- 平台全链路：30s 视频 → 2 shard 并行转码 → 合并 30.02s 时长精确 → JOB_SUCCESS；10 任务并发全部成功，Worker 满载保护生效；
- 故障注入：worker 转码中被 kill → 自动重调度恢复；ffmpeg 失败 → 重试 3 次 → JOB_FAILED；Redis/MQ 故障 → 自动降级 → 恢复切回；
- 数据持久化：kill 全部服务重启，任务和 shard 状态不丢。

CI 里还跑了一条真实的集成链路：Docker 构建 → 生成测试视频 → 管道提交任务 → 轮询到 SUCCESS → 校验合并产物。

## 快速启动

完整步骤（清场、两种部署方式、常见问题、监控面板）见 [doc/快速启动指南.md](doc/快速启动指南.md)。这里给最简路径：

**方式一：手工部署**（需要本机 ZooKeeper + MySQL + ffmpeg）

```bash
# 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# 启动 5 个服务（配置文件在 video_platform/conf/）
./bin/job_service       -i video_platform/conf/job_service.conf &
./bin/scheduler_service -i video_platform/conf/scheduler.conf &
./bin/worker_manager    -i video_platform/conf/worker_manager.conf &
./bin/result_collector  -i video_platform/conf/result_collector.conf &
./bin/transcode_worker  -i video_platform/conf/transcode_worker_9004.conf &

# 提交任务（管道输入：user → input → output → format → resolution → bitrate → priority → shard_duration）
printf 'test_user\n/tmp/test_video.mp4\n/tmp/output\nmp4\n720p\n2000\n0\n15\n' \
    | ./bin/job_client -i video_platform/conf/job_client.conf

# 轮询结果
./bin/job_client -i video_platform/conf/job_client.conf --query <job_id> --watch
```

**方式二：Docker 一键启动**（只需要 Docker，9 个容器：4 中间件 + 5 服务）

```bash
docker compose up -d --build
docker compose exec job_service sh -c \
  'printf "%s\n" "test_user" "/data/videos/sample.mp4" "/data/output" "mp4" "720p" "0" "0" "0" \
   | ./bin/job_client -i /app/conf/job_client.conf'
```

视频放 `data/videos/`，合并产物在 `data/output/{job_id}_merged.mp4`。扩容 Worker 只需 `docker compose up -d --scale transcode_worker=3`。
