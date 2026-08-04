# ============================================================================
# 阶段 13：容器化 — 多阶段构建
#
# 单镜像方案（对比"每服务独立 Dockerfile"）：
#   本项目一个 CMake 工程编译一次产出全部二进制（5 服务 + 客户端 + 测试），
#   5 个独立 Dockerfile 内容 90% 重复且构建 5 次；单镜像 + docker-compose
#   按 command 区分服务，--scale transcode_worker=N 天然支持。
#
# stage 1 — builder：安装编译依赖，构建全部二进制
# stage 2 — runtime：ffmpeg + 运行时库 + 二进制 + 配置，无编译工具链
# ============================================================================

# ── Stage 1: builder ─────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

# 编译依赖（Ubuntu 24.04 apt）
#
# ⚠️ 基础镜像必须与开发机同为 24.04：仓库提交的 proto/generated/*.pb.h 由
# protoc 3.21（24.04 的 libprotobuf-dev）生成，22.04 的 protobuf 3.12 头文件
# 无法编译它（Message::New() 纯虚接口不兼容）。这是 2026-08-04 容器实测
# 暴露的真实缺陷（详见 doc/更新业务日志/15. 阶段13容器化与CI-CD.md）。
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        libprotobuf-dev \
        protobuf-compiler \
        libzookeeper-mt-dev \
        libmysqlclient-dev \
        libhiredis-dev \
        librabbitmq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# ENABLE_NATIVE_MARCH=OFF：去掉 -march=native，镜像可在不同 CPU 架构的机器运行
# （builder 与 runtime 可能在不同宿主构建/运行，如 CI runner 构建 → 服务器运行）
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_NATIVE_MARCH=OFF \
    && cmake --build build -j$(nproc)

# ── Stage 2: runtime ─────────────────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

# 运行时库（24.04 具体版本包）+ ffmpeg/ffprobe（阶段 6 起 Worker 依赖）
# 包名与 builder 的 dev 包一一对应（libprotobuf32t64 是 noble 的 t64 包名）
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ffmpeg \
        libprotobuf32t64 \
        libzookeeper-mt2 \
        libmysqlclient21 \
        libhiredis1.1.0 \
        librabbitmq4 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 全部二进制（服务 + job_client + 测试/压测工具）
COPY --from=builder /src/bin/ /app/bin/
# docker 专用配置（rpcserverip=0.0.0.0、host=compose 服务名）
COPY docker/conf/ /app/conf/

# 日志目录（服务日志写相对路径 ./program_log）、视频输入/输出挂载点、FFmpeg 工作目录兜底
RUN mkdir -p /app/program_log /data/videos /data/output /tmp/transcode_worker

# 无 ENTRYPOINT/CMD：docker-compose 通过 command 区分要启动的服务
