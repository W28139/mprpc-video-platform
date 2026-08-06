# 吃透项目 · 学习工程

> 目标：把 **video_platform 业务代码**（5 个微服务 + 中间件封装 + proto 契约 + CLI/GUI）**一个函数一个方法**地吃透。
> wevix_muduo（网络库）和 mprpc（RPC 框架）是业务代码的依赖底座——理解到「业务怎么用它们、框架给业务什么保证」即可，不逐函数深挖。
> 方法：先宏观理顺（流程图），再按线索穿透式理解（设计决策报告）。

## 计划

| 时间 | 内容 | 产出 |
|------|------|------|
| 周三（8/5） | 整理业务项目结构，理顺全链路流程图 | `01_项目全景图.md` |
| 周四（8/6） | 业务模块级流程图细化 | `02_模块流程图集.md` |
| 周四五（8/6-8/7） | 按线索穿透式阅读每个设计 | `03_设计决策报告.md` |

## 吃透范围（业务代码清单）

```
video_platform/
├── proto/                  ★ RPC 契约（先看这个，接口即地图）
│     common.proto（实体+枚举）job.proto scheduler.proto
│     worker.proto result.proto
├── src/
│     job_service.cpp         ★ 用户入口：提交/查询/取消/列表
│     scheduler_service.cpp   ★ 核心：切分+调度+重试（2038 行，最大文件）
│     transcode_worker.cpp    ★ 核心：转码执行 + 结果上报（1469 行）
│     result_collector.cpp    ★ 核心：终态判定 + 合并
│     worker_manager.cpp      ★ Worker 注册/心跳/健康
│     common_store.cpp        ★ MySQL 唯一数据源（三 Store SQL 层）
│     mysql_pool.cpp / redis_client.cpp / mq_client.cpp
│     ffmpeg_executor.cpp     ★ FFmpeg 命令封装
│     job_client.cpp          CLI 客户端
├── gui/                     （可选）Qt6 客户端，QRunnable 异步 RPC
├── conf/                    各服务配置
└── observability/           Prometheus/Grafana 配置产物
```

## 背景知识（快速了解，不逐函数）

| 底座 | 业务代码需要知道什么 |
|------|---------------------|
| wevix_muduo | 服务怎么起（TcpServer.start 阻塞在 mainLoop）、work pool 语义（业务在 work 线程跑）、send 异步（outputBuffer）、连接回调时机 |
| mprpc | 服务端四步（Init→NotifyService→Run）、客户端 Stub 调用（CallMethod 同步阻塞）、超时/重试/错误码语义、ZK 服务发现路径、直连模式 `MprpcChannel(ip,port)` |

## 阅读路线（推荐顺序）

```
① 01_项目全景图.md         ← 业务全貌：模块地图 + 数据流 + 状态机
   （proto 文件过一遍，接口即地图）
   ↓
② 已有设计文档（作者视角，先建立正确认知）
   doc/视频转码平台/03_系统设计.md        ← 业务全景、状态机、设计取舍
   doc/视频转码平台/04_开发路线图.md       ← 8 阶段演进脉络
   doc/设计问题/框架/mprpc当前框架梳理与使用说明.md ← RPC 用法（只读业务相关）
   ↓
③ 02_模块流程图集.md       ← 每个业务模块内部流程（周四）
   ↓
④ 穿透式逐函数阅读 + 03_设计决策报告.md（周四五）
```

## 业务数据流一句话

```
提交(JobService) → 切分(Scheduler: Probe+ceil) → 调度(TryAssignShard:
评分选Worker+SETNX锁+AssignShard) → 执行(Worker: ffmpeg切片转码+进度)
→ 收集(RC: 幂等+终态判定) → 合并(Merge) → 终态 → 查询/预览
```

## 三张核心流程图（见 01，图 3 是业务主线）

1. 平台任务生命周期图（★ 核心：一条视频从提交到 SUCCESS）
2. 网络库事件循环图（背景：业务不直接碰）
3. RPC 调用链路图（背景：业务怎么被框架服务）
