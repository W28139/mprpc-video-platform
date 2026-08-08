#!/usr/bin/env bash
# ============================================================================
# 停止全部平台服务（手工部署方式 A）
# ============================================================================
pkill -9 -f "job_service|scheduler_service|worker_manager|transcode_worker|result_collector" 2>/dev/null || true
sleep 1
left=$(ps aux | grep -E "job_service|scheduler_service|worker_manager|transcode_worker|result_collector" | grep -v grep | wc -l)
[ "$left" -eq 0 ] && echo "[stop_all] ✓ 已全部停止" || echo "[stop_all] ⚠️ 还有 $left 个残留，稍后再试"
