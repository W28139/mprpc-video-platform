#!/usr/bin/env bash
# ============================================================================
# 阶段 13：容器集成测试
#
# 前置：docker compose up -d --build 已执行（CI 由 .github/workflows/ci.yml 触发；
#       本机手动跑：docker compose up -d --build && ./scripts/integration_test.sh）
# 流程：等待服务就绪 → 生成测试视频 → 提交任务 → 轮询到 SUCCESS → 验证产物
# 运行位置：仓库根目录（脚本内部路径均相对仓库根）
# ============================================================================
set -euo pipefail

COMPOSE=${COMPOSE:-docker compose}

info() { echo "[integration] $*"; }
fail() { echo "[integration] FAIL: $*" >&2; exit 1; }

# ── 1. 等待全部服务 running（最多 ~150s） ──────────────────────────────
info "Waiting for all compose services to be running..."
services_up=false
for i in $(seq 1 30); do
    states=$($COMPOSE ps -a --format '{{.State}}' 2>/dev/null || true)
    # 全部非空且均为 running 才算就绪
    if [ -n "$states" ] && ! echo "$states" | grep -qv running; then
        services_up=true
        break
    fi
    sleep 5
done
$services_up || fail "services not running after 150s: $(docker compose ps -a 2>/dev/null | tail -n +1)"

info "Services up:"
$COMPOSE ps

# 留出 ZK 注册 + Worker 心跳 + MQ 拓扑声明窗口
sleep 8

# ── 2. 生成测试视频（testsrc 30s，挂载卷直达各容器） ───────────────────
# 注意：compose 对不存在的挂载目录会以 root 创建（宿主侧 ffmpeg 无权写入），
# 须在 docker compose up 之前预创建（CI 已处理；本机手动跑请先 mkdir 或删掉 data/ 重来）
info "Generating test video (30s testsrc)..."
mkdir -p data/videos data/output
if [ ! -w data/videos ] || [ ! -w data/output ]; then
    fail "data/videos or data/output not writable by $(id -un) (docker compose creates missing mount dirs as root); remove data/ and re-run after 'mkdir -p data/videos data/output'"
fi
ffmpeg -y -loglevel error \
    -f lavfi -i testsrc=duration=30:size=320x240:rate=10 \
    -pix_fmt yuv420p -c:v libx264 -preset ultrafast \
    data/videos/ci_sample.mp4

# ── 3. 容器内交互式提交任务（重试兜底：服务注册窗口内提交会失败） ────
SUBMIT_INPUTS='printf "%s\n" "ci_user" "/data/videos/ci_sample.mp4" "/data/output" "mp4" "720p" "0" "0" "0" | ./bin/job_client -i /app/conf/job_client.conf'

info "Submitting job (retry up to 6x / 60s)..."
job_id=""
submit_out=""
for i in $(seq 1 6); do
    submit_out=$($COMPOSE exec -T job_service sh -c "$SUBMIT_INPUTS" 2>&1 || true)
    job_id=$(echo "$submit_out" | sed -n 's/^  Job ID: //p' | head -1)
    if [ -n "$job_id" ]; then
        break
    fi
    info "  submit attempt $i/6 not successful yet, retrying in 10s..."
    sleep 10
done
if [ -z "$job_id" ]; then
    echo "$submit_out" >&2
    fail "job submission did not succeed (output above)"
fi
info "Job submitted: $job_id"

# ── 4. 轮询到终态（--watch 每 2s；超时 420s） ──────────────────────────
# 420s 兜底：RC MQ 消费偶发静默延迟（result.pending 最多 ~5 分钟，见日志记录），
# shard 结果上报可能晚到，240s 偶发不够（CI 实测 shard_1 卡 RUNNING 超时）
info "Watching job $job_id to terminal state (timeout 420s)..."
watch_out=$(timeout 420 $COMPOSE exec -T job_service sh -c \
    "./bin/job_client -i /app/conf/job_client.conf --query '$job_id' --watch" 2>&1 || true)
if ! echo "$watch_out" | grep -q "terminal state: SUCCESS"; then
    echo "$watch_out" >&2
    fail "job did not reach SUCCESS (output above)"
fi
info "Job reached SUCCESS"

# ── 5. 验证合并产物（挂载卷 data/output/ 直达宿主） ────────────────────
# 必须校验【本次提交的 job_id】的产物，避免误认历史 job 的残留文件。
# MarkJobTerminal 先置 job 状态 SUCCESS、后执行 merge（毫秒~秒级），
# 因此 SUCCESS 后需轮询等待产物出现，而非立即 test -f。
info "Waiting for merged video (merge runs after SUCCESS)..."
merged=""
for i in $(seq 1 60); do   # 最多 30s
    if [ -f "data/output/${job_id}_merged.mp4" ]; then
        merged="data/output/${job_id}_merged.mp4"
        break
    fi
    sleep 0.5
done
if [ -z "$merged" ]; then
    info "WARN: data/output/${job_id}_merged.mp4 not found after 30s; listing outputs:"
    ls -la data/output/ || true
    fail "merged video for job ${job_id} not found in data/output/"
fi
info "Merged video: $merged ($(du -h "$merged" | cut -f1))"

# ── 6. 结果 ────────────────────────────────────────────────────────────
info "PASS: full pipeline verified (submit → transcode → merge → SUCCESS)"
