# 01. 技术知识准备

## 总体学习周期

如果目标是完成这个项目，而不是成为音视频专家，建议学习周期如下：

| 模块 | 学习周期 | 目标 |
|---|---:|---|
| 视频基础概念 | 2-3 天 | 能解释转码任务是什么 |
| FFmpeg 命令行 | 3-5 天 | Worker 能调用 FFmpeg 完成基础处理 |
| 分布式任务调度 | 5-7 天 | 能设计任务状态机、调度、重试 |
| RPC 服务治理 | 持续补齐 | 能把框架改成业务可用 |
| 监控和压测 | 3-5 天 | 能证明系统稳定性和性能 |

总计：

- 最小可开工：3 天
- 能接入 FFmpeg：1 周
- 能做出像样的业务系统：4-8 周
- 做成简历强项目：8-12 周

## 1. C++ 基础要求

必须熟悉：

- RAII
- 智能指针
- move 语义
- 多线程
- mutex、condition_variable、atomic
- 线程池
- 生产者消费者模型
- 文件操作
- 进程执行和退出码
- C++ 调用外部命令

项目中会用到：

- Worker 线程执行任务
- Scheduler 并发调度
- RPC 回调处理
- 任务对象高频创建销毁
- 结果对象序列化和反序列化

## 2. Linux 系统编程

需要掌握：

- 进程和线程
- socket 基础
- epoll 基础
- 非阻塞 I/O
- signal
- 文件路径和临时目录
- 子进程执行命令
- 返回码和错误码
- 简单资源统计

Worker 执行 FFmpeg 时，至少要能做到：

- 创建任务临时目录
- 执行命令
- 获取退出码
- 设置超时时间
- 失败时清理中间文件

可以先用 `std::system()` 或 `popen()` 做 MVP，后续再换成更可控的 `fork/exec`。

## 3. 自研网络库和 RPC 框架

你需要非常清楚自己框架的这些点：

- EventLoop 线程模型
- Acceptor 和 Connection 生命周期
- Buffer 粘包/拆包处理
- TcpServer 的 IO 线程和 work 线程分工
- RPC 请求帧格式
- RPC 响应帧格式
- ZooKeeper 注册发现流程
- 客户端调用链路
- Controller 错误语义

项目会倒逼你把 `mprpc` 从 demo 框架变成真正业务框架。

## 4. 视频基础概念

只需要懂项目需要的概念：

- 容器格式：`mp4`、`mkv`、`flv`
- 视频编码：`H.264`、`H.265`
- 音频编码：`AAC`
- 分辨率：`1080p`、`720p`、`480p`
- 码率：视频清晰度和文件大小相关
- 帧率：每秒帧数
- 转码：改变编码、分辨率、码率或格式
- 转封装：只换容器，不重新编码
- 抽帧：按时间或帧数提取图片
- 截图：从视频中截取一张图
- 切片：把视频按时间段分成多个片段
- 合并：把多个片段合成一个结果

暂时不需要深入：

- H.264 宏块
- 运动估计
- 熵编码
- 音视频同步底层
- libavcodec API
- 复杂滤镜图

## 5. FFmpeg 命令行

第一阶段只需要会这些命令类型。

查看视频信息：

```bash
ffprobe input.mp4
```

转 720p：

```bash
ffmpeg -i input.mp4 -s 1280x720 output_720p.mp4
```

调整码率：

```bash
ffmpeg -i input.mp4 -b:v 1000k output.mp4
```

截图：

```bash
ffmpeg -ss 00:00:05 -i input.mp4 -frames:v 1 cover.jpg
```

按时间切片：

```bash
ffmpeg -i input.mp4 -ss 00:00:00 -t 00:00:10 part_0.mp4
```

合并片段：

```bash
ffmpeg -f concat -safe 0 -i filelist.txt -c copy output.mp4
```

MVP 阶段可以不解析 FFmpeg 进度，只根据退出码判断成功或失败。

## 6. 分布式任务调度知识

需要理解：

- Job 和 Task/Shard 的区别
- 任务状态机
- 优先级队列
- Worker 注册
- Worker 心跳
- Worker 负载上报
- 资源匹配
- 失败重试
- 超时重调度
- 幂等
- 结果聚合

核心概念：

```text
Job: 用户提交的完整视频处理任务
Shard: Job 拆出来的一个可独立执行的子任务
Worker: 实际执行 shard 的计算节点
Scheduler: 决定 shard 分配到哪个 Worker
ResultCollector: 汇总 shard 执行结果
```

## 7. 数据库和消息队列

项目可以先不用复杂中间件。

推荐 MVP：

- 元数据存储：MySQL 或 SQLite
- 消息队列：先内存队列，后续再换 Redis Stream、RabbitMQ 或 Kafka

需要掌握：

- 任务表设计
- 分片表设计
- Worker 表设计
- 状态更新
- 幂等更新
- 结果记录

消息队列主要用于：

- shard 创建事件
- shard 完成事件
- shard 失败事件
- job 完成事件
- 异步通知

## 8. 可观测性

需要会设计这些指标：

- RPC QPS
- RPC P50/P90/P99
- RPC 失败率
- 当前 Worker 数
- 当前运行中 shard 数
- 等待队列长度
- 每个 Worker CPU/内存/任务数
- Job 成功率
- Shard 重试次数
- 调度耗时
- 任务总耗时

第一版可以先写日志和控制台统计，不一定马上接 Prometheus。

## 9. 推荐学习顺序

第 1 周：

1. 学 FFmpeg 基本概念和命令
2. 梳理 RPC 框架缺口
3. 设计 Job/Shard/Worker 状态机

第 2 周：

1. 补 RPC timeout、request_id、error_code
2. 实现 Worker 注册和心跳
3. 实现 mock executor

第 3-4 周：

1. 实现 Scheduler
2. 实现任务拆分
3. 实现失败重试
4. 实现结果聚合

第 5-8 周：

1. 接入 FFmpeg 命令执行
2. 完善资源调度
3. 加监控指标
4. 做压测和故障测试

## 参考资料

- FFmpeg 官方文档：<https://www.ffmpeg.org/documentation.html>
- FFmpeg 项目介绍：<https://www.ffmpeg.org/about.html>
- FFmpeg filters 文档：<https://ffmpeg.org/ffmpeg-filters.html>
- OpenTelemetry 文档：<https://opentelemetry.io/docs/>
- gRPC deadline 设计参考：<https://grpc.io/docs/guides/deadlines/>
- gRPC health checking 设计参考：<https://grpc.io/docs/guides/health-checking/>

