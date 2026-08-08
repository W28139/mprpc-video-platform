#!/usr/bin/env bash
# ============================================================================
# 启动全部平台服务（手工部署方式 A）
#
# 特性：
# - nohup 后台运行：终端关闭/退出服务不随 SIGHUP 死掉（裸 & 的坑）
# - 启动前自动清场：杀残留进程 + 清理 Docker(root) 日志文件 + 等 ZK 节点过期
# - 日志统一写到 program_log/start_all.log 的启动摘要 + 服务日志按天轮转
#
# 用法：scripts/start_all.sh
# 停止：scripts/stop_all.sh
# ============================================================================
set -e
cd "$(dirname "$0")/.."

echo "[start_all] 清场：杀掉残留服务进程..."
pkill -9 -f "job_service|scheduler_service|worker_manager|transcode_worker|result_collector" 2>/dev/null || true

echo "[start_all] 清理 Docker(root) 日志文件..."
find program_log -user root -delete 2>/dev/null || true

echo "[start_all] 等待 ZK 临时节点过期（3s）..."
sleep 3

mkdir -p program_log

# 服务 → 配置（transcode_worker 可自行追加 9010/9011 多实例）
declare -A SVCS=(
    [job_service]="video_platform/conf/job_service.conf"
    [scheduler_service]="video_platform/conf/scheduler.conf"
    [worker_manager]="video_platform/conf/worker_manager.conf"
    [result_collector]="video_platform/conf/result_collector.conf"
    [transcode_worker]="video_platform/conf/transcode_worker_9004.conf"
)

for svc in "${!SVCS[@]}"; do
    nohup ./bin/"$svc" -i "${SVCS[$svc]}" >> program_log/start_all.log 2>&1 &
    echo "[start_all] started $svc (pid $!)"
done

sleep 3

alive=$(ps aux | grep -E "job_service|scheduler_service|worker_manager|transcode_worker|result_collector" | grep -v grep | wc -l)
echo "[start_all] 运行中进程数: $alive / ${#SVCS[@]}"
[ "$alive" -ge "${#SVCS[@]}" ] && echo "[start_all] ✓ 全部启动完成" || echo "[start_all] ⚠️ 有服务未存活，查看 program_log/start_all.log"
