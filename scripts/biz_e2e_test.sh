#!/usr/bin/env bash
# ============================================================================
# 本地（非 docker）业务功能端到端测试
#
# 与 scripts/integration_test.sh（docker 全链路）互补，覆盖它没有的路径：
#   --query 字段校验、负路径（不存在的 job）、--cancel 取消链路
# 流程：前置检查 → testsrc 10s 视频 → 提交 job1 → 查询校验 → 负路径 →
#       提交 job2 → 立即取消 → watch CANCELED → job1 watch SUCCESS → 产物校验
#
# 前置：5 个服务已由 ./scripts/start_all.sh 启动（ZK/MySQL/Redis/RabbitMQ 同机运行）
# 运行位置：仓库根目录
# ============================================================================
set -euo pipefail

info() { echo "[biz-e2e] $*"; }
fail() { echo "[biz-e2e] FAIL: $*" >&2; exit 1; }

# ── 1. 前置检查（服务缺失报错退出，不自动拉起，保持幂等） ───────────────
for svc in job_service scheduler_service worker_manager transcode_worker result_collector; do
    if ! pgrep -f "bin/$svc" >/dev/null; then
        fail "service $svc not running; run ./scripts/start_all.sh first"
    fi
done
info "All 5 services running"

if ! timeout 2 bash -c "echo > /dev/tcp/127.0.0.1/3306" 2>/dev/null; then
    fail "MySQL not reachable at 127.0.0.1:3306"
fi
command -v ffmpeg  >/dev/null || fail "ffmpeg not found in PATH"
command -v ffprobe >/dev/null || fail "ffprobe not found in PATH"
info "Prerequisites OK"

# ── 2. 生成测试视频（testsrc 10s，与 integration_test.sh 同模式） ───────
mkdir -p data/videos data/output
VIDEO_PATH=$(readlink -f data/videos/biz_e2e_sample.mp4)
OUT_DIR=$(readlink -f data/output)
if [ ! -w data/videos ] || [ ! -w data/output ]; then
    fail "data/videos or data/output not writable by $(id -un)"
fi
info "Generating test video (10s testsrc)..."
ffmpeg -y -loglevel error \
    -f lavfi -i testsrc=duration=10:size=320x240:rate=10 \
    -pix_fmt yuv420p -c:v libx264 -preset ultrafast \
    "$VIDEO_PATH"

JOB_CLIENT="./bin/job_client -i video_platform/conf/job_client.conf"

# 提交 job（stdin 8 字段），输出 job_id
submit_job() {
    printf "%s\n" "e2e_user" "$VIDEO_PATH" "$OUT_DIR" "mp4" "720p" "0" "0" "0" \
        | $JOB_CLIENT 2>&1 | sed -n 's/^  Job ID: //p' | head -1
}

# ── 3. 提交 job1 → 查询字段校验 ─────────────────────────────────────────
info "Submitting job1..."
job1=$(submit_job)
[ -n "$job1" ] || fail "job1 submission did not succeed"

info "Querying job1 for field validation..."
qout=$($JOB_CLIENT --query "$job1" 2>&1)
echo "$qout" | grep -q "Job: $job1"      || fail "query output missing job id: $qout"
echo "$qout" | grep -q "Input:"          || fail "query output missing input path"
echo "$qout" | grep -q "Status:"         || fail "query output missing status"
# 刚提交的任务不应已到终态（SUCCESS/FAILED/CANCELED）
if echo "$qout" | grep -qE "Status: +(SUCCESS|FAILED|CANCELED)"; then
    fail "job1 already in terminal state right after submit: $qout"
fi
info "job1 query OK"

# ── 4. 负路径：查询不存在的 job ─────────────────────────────────────────
info "Negative path: query nonexistent job..."
neg=$($JOB_CLIENT --query "nonexistent_job_xyz" 2>&1 || true)
echo "$neg" | grep -q "Query failed" || fail "expected 'Query failed' for nonexistent job: $neg"
info "Negative path OK"

# ── 5. 提交 job2 → 立即取消 → watch CANCELED ────────────────────────────
# 用独立 job 测取消，避免与 job1 的 SUCCESS 竞争
info "Submitting job2 and cancelling immediately..."
job2=$(submit_job)
[ -n "$job2" ] || fail "job2 submission did not succeed"
cout=$($JOB_CLIENT --cancel "$job2" 2>&1)
echo "$cout" | grep -q "Job canceled" || fail "cancel failed: $cout"
info "job2 canceled: $job2"

info "Watching job2 to CANCELED (timeout 60s)..."
wout=$(timeout 60 $JOB_CLIENT --query "$job2" --watch 2>&1 || true)
echo "$wout" | grep -q "terminal state: CANCELED" || fail "job2 did not reach CANCELED: $wout"
info "job2 CANCELED OK"

# ── 6. job1 正常链路 → watch SUCCESS ────────────────────────────────────
info "Watching job1 to SUCCESS (timeout 240s)..."
wout=$(timeout 240 $JOB_CLIENT --query "$job1" --watch 2>&1 || true)
echo "$wout" | grep -q "terminal state: SUCCESS" || fail "job1 did not reach SUCCESS: $wout"
info "job1 SUCCESS OK"

# ── 7. 产物校验（merge 在 SUCCESS 之后，轮询等待；ffprobe 校验时长） ───
info "Waiting for merged video (merge runs after SUCCESS)..."
merged=""
for i in $(seq 1 60); do   # 最多 30s
    if [ -f "$OUT_DIR/${job1}_merged.mp4" ]; then
        merged="$OUT_DIR/${job1}_merged.mp4"
        break
    fi
    sleep 0.5
done
[ -n "$merged" ] || fail "merged video for job ${job1} not found in $OUT_DIR"
info "Merged video: $merged ($(du -h "$merged" | cut -f1))"

dur=$(ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "$merged" 2>/dev/null || true)
if ! awk -v d="$dur" 'BEGIN { if (d >= 8 && d <= 12) exit 0; exit 1 }'; then
    fail "merged video duration $dur out of expected [8, 12]s (10s input)"
fi
info "Merged duration ${dur}s OK"

# ── 8. 结果 ────────────────────────────────────────────────────────────
info "PASS: local e2e verified (submit → query → cancel → SUCCESS → merged product)"
