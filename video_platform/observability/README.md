# 可观测性部署指南（阶段 11）

本目录是阶段 11「可观测性升级」的配置产物：每个服务暴露 Prometheus
`/metrics` endpoint，配合本目录的 Prometheus/Grafana 配置即可获得标准监控面板。

```
observability/
├── prometheus.yml        # 抓取 7 个 metrics endpoint + 引用告警规则
├── alerts.yml            # 4 条 PromQL 告警规则
├── grafana-dashboard.json# 4 行 Grafana 面板（导入即用）
└── README.md             # 本文档
```

## 1. 指标一览

| 指标 | 类型 | 标签 | 暴露进程 |
|------|------|------|----------|
| `job_submitted_total` | Counter | — | job_service |
| `job_success_total` / `job_failed_total` | Counter | — | result_collector |
| `shard_retry_total` | Counter | — | scheduler_service |
| `shard_running` / `shard_waiting` | Gauge | — | scheduler_service |
| `scheduler_queue_size` | Gauge | — | scheduler_service（WAITING+ASSIGNED） |
| `shard_count` | Gauge | `status`（枚举值 0-8） | scheduler_service |
| `worker_online` | Gauge | — | worker_manager |
| `worker_cpu_usage` / `worker_memory_usage` / `worker_running_shards` | Gauge | `worker_id` | worker_manager |
| `rpc_latency_ms` | Histogram | `method`（如 `SchedulerService.ScheduleJob`） | 全部服务（mprpc 客户端侧） |
| `transcode_duration_ms` | Histogram | — | transcode_worker |
| `schedule_loop_duration_ms` | Histogram | — | scheduler_service |

ShardStatus 枚举（`video_platform/proto/common.proto`）：0=UNKNOWN, 1=CREATED,
2=WAITING, 3=ASSIGNED, 4=RUNNING, 5=SUCCESS, 6=FAILED, 7=RETRYING, 8=CANCELED。

## 2. metrics 端口

`video_platform/conf/*.conf` 的 `metrics_port` 配置项（`<=0` 关闭，可降级组件）：

| 服务 | RPC 端口 | metrics 端口 |
|------|---------|--------------|
| job_service | 9001 | **9091** |
| scheduler_service | 9002 | **9092** |
| worker_manager | 9003 | **9093** |
| result_collector | 9004 | **9094** |
| transcode_worker (worker_001) | 9005 | **9095** |
| transcode_worker (worker_002) | 9010 | **9096** |
| transcode_worker (worker_003) | 9011 | **9097** |

> 单机多服务实例无法共用 9090，故按服务分配；若每个服务独立主机部署，
> 可全部改用 `metrics_port=9090`。

## 3. 快速验证（无需安装任何东西）

```bash
# 启动服务后
curl -s http://127.0.0.1:9091/metrics | head            # job_submitted_total 等
curl -s http://127.0.0.1:9092/metrics | grep shard_     # shard 状态分布
curl -s http://127.0.0.1:9093/metrics | grep worker_    # worker 负载
curl -s http://127.0.0.1:9094/metrics | grep job_       # 任务成功/失败计数
curl -s http://127.0.0.1:9095/metrics | grep transcode  # 转码耗时直方图
curl -s http://127.0.0.1:9091/                          # 404（非 /metrics 路径）
```

Prometheus 抓取格式为 text format 0.0.4，直方图含 `_bucket{le=...}/_sum/_count`，
可用 `promtool check metrics` 校验。

## 4. 部署 Prometheus

```bash
# Ubuntu/Debian（需 sudo；或从 https://prometheus.io/download/ 下载二进制）
sudo apt install prometheus
# 注意：apt 包默认配置是 /etc/prometheus/prometheus.yml，覆盖为本项目配置即可

prometheus --config.file=video_platform/observability/prometheus.yml
# Web UI: http://localhost:9090  →  Status/Targets 应看到 5-7 个 UP
# Rules:  http://localhost:9090/rules
```

`prometheus.yml` 抓取 7 个 endpoint（9091-9097），`alerts.yml` 定义了 4 条规则
（见下）。告警送达需要 Alertmanager：`alertmanager --config.file=...`，
alerts.yml 的规则触发后带 `severity` 标签，可按需配置邮件/webhook 路由。

## 5. 部署 Grafana

```bash
# 下载 Grafana 二进制（https://grafana.com/grafana/download）或包管理器安装
sudo apt install -y grafana   # 或下载 .deb
# 或免安装直接跑：
#   wget https://dl.grafana.com/oss/release/grafana-<ver>.linux-amd64.tar.gz
#   tar xzf grafana-*.tar.gz && ./grafana-<ver>/bin/grafana server
```

1. 登录 Grafana（默认 admin/admin）
2. **Data sources** → Add Prometheus，URL 填 `http://localhost:9090`
3. **Dashboards** → New → Import → 上传 `grafana-dashboard.json`，
   数据源下拉选择刚添加的 Prometheus
4. 面板 4 行：
   - **Row 1** Job 吞吐：提交/成功/失败速率折线
   - **Row 2** Shard 分布：当前状态饼图 + 状态堆叠时间线
   - **Row 3** Worker 负载热力图：CPU/内存/运行中 shard 数（顶部变量可筛选 Worker）
   - **Row 4** RPC 延迟：le 桶热力图 + P50/P90/P99 折线

> Grafana 9.x/10.x 兼容说明：RPC 延迟热力图用 Prometheus 直方图桶数据
> （`dataFormat: time_series_buckets`，10.x 部分版本为 `tsbuckets`，若面板
> 白屏切换该键值即可）；Worker 负载热力图由原始 gauge 系列自动分桶。

## 6. 告警规则（alerts.yml + 内置兜底）

| 告警 | PromQL | 阈值语义 |
|------|--------|----------|
| `WorkerOffline` | `sum(worker_online) < 1` | 无在线 Worker，平台不可用（critical） |
| `SchedulerQueueBacklog` | `sum(scheduler_queue_size) > 100` | 调度队列积压（WAITING+ASSIGNED） |
| `JobFailureRateHigh` | `rate(job_failed_total[5m]) / clamp_min(rate(job_submitted_total[5m]), 0.001) > 0.3` | 5 分钟失败率 > 30% |
| `RpcLatencyP99High` | `histogram_quantile(0.99, sum by (le) (rate(rpc_latency_ms_bucket[5m]))) > 1000` | RPC P99 > 1s |

**内置日志告警兜底**：即使不部署 Prometheus，每个服务进程内也有告警评估器
（`mprpc::MetricsRegistry::AlertLoop`，每 30s 一轮），触发/恢复时服务日志打印：

```
ALERT [worker_offline] firing: value=0.00, threshold=1.00, 没有在线转码 Worker
ALERT [worker_offline] recovered: value=1.00, threshold=1.00, 没有在线转码 Worker
```

各进程内置规则：worker_manager（worker_offline）、scheduler（scheduler_backlog）、
result_collector（job_failed_rate_high）、全部服务（rpc_latency_p99_high）。

> 口径差异：内置兜底按进程内评估（job_failed_rate = 失败数/终态数，RC 本地
> counter）；Prometheus 侧按集群聚合（failed/submitted 跨实例 sum）。阈值语义一致。

## 7. 故障演练建议

- `kill` 一个 transcode_worker → ~20s 心跳超时后 `worker_online` 归 0，
  约 1 分钟内 worker_manager 日志出现 `ALERT [worker_offline] firing`
- 停掉全部 Worker 持续提交任务 → RC 日志出现 `ALERT [job_failed_rate_high] firing`
- 提交不存在的输入文件 → 任务 FAILED，`job_failed_total` +1，失败率告警联动
