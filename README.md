# mprpc-video-platform

从网络库到业务平台全部自研的分布式视频转码系统。

项目分三层，每一层都是独立可用的：

```
wevix_muduo   Reactor 模式网络库（epoll / 线程池 / 内存池 / 异步日志）
   └─► mprpc  基于 wevix_muduo 的 RPC 框架（protobuf 序列化 + ZooKeeper 注册发现）
          └─► video_platform  分布式视频转码平台（5 个微服务）
```

视频转码平台做的事：提交一个视频，系统按时间切成多个 shard，分给多个 Worker 并行转码，最后合并回完整视频。

## 平台功能

- **任务切分与合并**：ffprobe 探测真实时长，按 shard_duration 切分，最后一个 shard 时长自适应；全部成功后自动合并
- **资源感知调度**：Worker 心跳上报真实 CPU/内存（/proc），调度按加权评分（slots×10 - cpu×0.5 - mem×0.2）择优分配，高负载 Worker 自动避让；支持优先级
- **容错**：转码失败自动重试 3 次；Worker 崩溃（心跳超时）后其 shard 自动重调度；Scheduler 重启后扫描残留状态恢复
- **真实转码**：fork+exec 拉起 ffmpeg（支持 cancel 时 kill 子进程），进度实时解析上报，转码成功后自动截图
- **数据持久化**：MySQL 为唯一数据源，服务重启不丢数据；Redis 缓存 Worker 负载快照（可降级）；RabbitMQ 事件驱动调度（Push 分配，故障自动回退 Pull 轮询）
- **可观测性**：Prometheus 指标 + Grafana 面板 + 4 条告警（进程内日志告警兜底，不装监控也能收到）
- **部署**：Docker 一键起 9 个容器（4 中间件 + 5 服务），支持 `--scale` 水平扩容 Worker；GitHub Actions CI 自动跑单元测试 + 容器集成测试 + RPC 压测

## 快速开始

两种方式，任选其一（详细步骤见 [doc/快速启动指南.md](doc/快速启动指南.md)）：

```bash
# 方式 1：Docker 一键启动（推荐）
docker compose up -d --build

# 方式 2：手工逐个启动（需本机装好 ZooKeeper / MySQL / ffmpeg）
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
./bin/job_service -i video_platform/conf/job_service.conf &
# ... 其余 4 个服务见快速启动指南
```

提交一个任务：

```bash
printf 'test_user\n/home/wevix/muduo_im/path/test_video.mp4\n/home/wevix/muduo_im/path/temp_out\nmp4\n720p\n2000\n0\n15\n' \
  | ./bin/job_client -i video_platform/conf/job_client.conf

# 查询
./bin/job_client -i video_platform/conf/job_client.conf --query <job_id> --watch
```

## 目录结构

```
wevix_muduo/      网络库（TcpServer / EventLoop / Buffer / 内存池 / AsyncLogger）
mprpc/            RPC 框架（RpcProvider / MprpcChannel / ZK 注册发现 / metrics 库）
video_platform/   平台（proto / 5 个微服务 / Store / ffmpeg 执行器 / observability）
docker/           Dockerfile + compose 编排
doc/              设计文档 + 开发日志
test/             单元测试与压测
```

## 测试与压测

```bash
./bin/test_buffer && ./bin/test_threadpool && ./bin/test_rpc_protocol   # 单元测试 43 个
./bin/bench_echo_stress 127.0.0.1 9000                                  # 网络库压测（峰值 17 万 QPS）
./bin/bench_rpc_stress --direct --keepalive -c 50 -m 200 -s 64          # RPC 压测（12 万 QPS）
```

压测和故障注入的详细结果在 `doc/更新业务日志/` 里。

## 文档

- [快速启动指南](doc/快速启动指南.md) — 两种部署方式 + 提交/查询/监控面板使用
- [doc/设计问题/](doc/设计问题/) — 关键设计文档（调度、持久化、中间件、可观测性、容器化 CI/CD）
- [doc/更新业务日志/](doc/更新业务日志/) — 按阶段的开发日志（含踩坑记录）
- [doc/验收标准与简历表达.md](doc/验收标准与简历表达.md) — 项目验收标准

## 技术栈

Linux C++17 · epoll · protobuf 3.21 · ZooKeeper · MySQL 8 · Redis 7 · RabbitMQ 3.12 · ffmpeg · Prometheus · Grafana · Docker · GitHub Actions