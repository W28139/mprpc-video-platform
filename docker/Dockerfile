# 阶段1：编译
FROM ubuntu:24.04 AS builder

# 安装编译依赖
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

# Release 编译，ENABLE_NATIVE_MARCH=OFF 保证镜像跨机器可移植
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_NATIVE_MARCH=OFF \
    && cmake --build build -j$(nproc)

# 阶段2：运行镜像
FROM ubuntu:24.04 AS runtime

# 安装运行时库 + ffmpeg
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

# 从 builder 拷贝二进制和配置
COPY --from=builder /src/bin/ /app/bin/
COPY docker/conf/ /app/conf/

# 创建日志、数据、工作目录
RUN mkdir -p /app/program_log /data/videos /data/output /tmp/transcode_worker