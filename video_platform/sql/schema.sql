-- ============================================================================
-- 阶段 9：数据持久化 — MySQL 表结构
--
-- 3 张核心表：jobs / shards / workers
-- 对应 common_store.h 中的 JobRecord / ShardRecord / WorkerRecord。
--
-- 使用方法：
--   1. 手动执行（需要 DDL 权限的账号）：
--        mysql -uvideo_platform -p < schema.sql
--   2. 或依赖服务启动时的自动建表（CREATE TABLE IF NOT EXISTS，幂等）。
--      本文件保留作为参考与手动管理入口。
-- ============================================================================

CREATE DATABASE IF NOT EXISTS video_platform
    DEFAULT CHARACTER SET utf8mb4;

USE video_platform;

-- ----------------------------------------------------------------------------
-- jobs — 任务元信息表
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    job_id             VARCHAR(64)  NOT NULL,               -- 全局唯一任务 ID
    user_id            VARCHAR(64)  NOT NULL DEFAULT '',    -- 提交用户标识
    input_path         VARCHAR(512) NOT NULL DEFAULT '',    -- 输入视频文件路径
    output_path        VARCHAR(512) NOT NULL DEFAULT '',    -- 期望输出目录路径
    target_format      VARCHAR(16)  NOT NULL DEFAULT '',    -- 目标容器格式（mp4/mkv/flv）
    target_resolution  VARCHAR(16)  NOT NULL DEFAULT '',    -- 目标分辨率（720p/1080p/4k）
    target_bitrate     INT          NOT NULL DEFAULT 0,     -- 目标码率（kbps），0=不限制
    duration_sec       BIGINT       NOT NULL DEFAULT 0,     -- 视频总时长（秒），Scheduler 探测后回填
    priority           INT          NOT NULL DEFAULT 0,     -- 调度优先级，越大越优先
    status             INT          NOT NULL DEFAULT 0,     -- 当前状态（JobStatus 枚举）
    shard_count        INT          NOT NULL DEFAULT 0,     -- 拆分出的 shard 总数
    shard_duration_sec INT          NOT NULL DEFAULT 0,     -- 每 shard 时长（秒），0=默认值
    created_at         BIGINT       NOT NULL DEFAULT 0,     -- 创建时间戳（毫秒）
    updated_at         BIGINT       NOT NULL DEFAULT 0,     -- 最后更新时间戳（毫秒）
    PRIMARY KEY (job_id),
    INDEX idx_status (status),
    INDEX idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- shards — 子任务表
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shards (
    shard_id          VARCHAR(128) NOT NULL,               -- 全局唯一 shard ID
    job_id            VARCHAR(64)  NOT NULL DEFAULT '',    -- 所属 job ID
    shard_index       INT          NOT NULL DEFAULT 0,     -- 在 job 中的序号（0,1,2,...）
    start_ms          BIGINT       NOT NULL DEFAULT 0,     -- 视频起始偏移（毫秒）
    duration_ms       BIGINT       NOT NULL DEFAULT 0,     -- 视频片段时长（毫秒）
    status            INT          NOT NULL DEFAULT 0,     -- 当前状态（ShardStatus 枚举）
    assigned_worker   VARCHAR(64)  NOT NULL DEFAULT '',    -- 被分配的 Worker ID，未分配为空
    attempt_id        VARCHAR(128) NOT NULL DEFAULT '',    -- 当前执行尝试 ID，每次重试递增
    retry_count       INT          NOT NULL DEFAULT 0,     -- 已重试次数
    max_retry         INT          NOT NULL DEFAULT 3,     -- 最大重试次数
    input_path        VARCHAR(512) NOT NULL DEFAULT '',    -- 输入文件路径
    output_path       VARCHAR(512) NOT NULL DEFAULT '',    -- 输出文件路径
    screenshot_path   VARCHAR(512) NOT NULL DEFAULT '',    -- 截图文件路径
    target_resolution VARCHAR(16)  NOT NULL DEFAULT '',    -- 目标分辨率，从 JobInfo 透传
    target_bitrate    INT          NOT NULL DEFAULT 0,     -- 目标码率（kbps），从 JobInfo 透传
    created_at        BIGINT       NOT NULL DEFAULT 0,     -- 创建时间戳（毫秒）
    updated_at        BIGINT       NOT NULL DEFAULT 0,     -- 最后更新时间戳（毫秒）
    PRIMARY KEY (shard_id),
    INDEX idx_job (job_id),
    INDEX idx_worker (assigned_worker),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- workers — Worker 注册与负载表
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS workers (
    worker_id              VARCHAR(64) NOT NULL,            -- 全局唯一 Worker ID
    ip                     VARCHAR(45) NOT NULL DEFAULT '', -- Worker 所在机器 IP
    port                   INT         NOT NULL DEFAULT 0,  -- Worker RPC 监听端口
    cpu_cores              INT         NOT NULL DEFAULT 0,  -- 机器 CPU 核心数
    memory_mb              INT         NOT NULL DEFAULT 0,  -- 机器总内存（MB）
    gpu_count              INT         NOT NULL DEFAULT 0,  -- GPU 数量，0=无 GPU（阶段 10 预留）
    current_running_shards INT         NOT NULL DEFAULT 0,  -- 当前执行的 shard 数
    max_running_shards     INT         NOT NULL DEFAULT 0,  -- 最大并发 shard 数
    cpu_usage              INT         NOT NULL DEFAULT 0,  -- CPU 使用率（0-100）
    memory_usage           INT         NOT NULL DEFAULT 0,  -- 内存使用率（0-100）
    status                 INT         NOT NULL DEFAULT 0,  -- 当前状态（WorkerStatus 枚举）
    last_heartbeat         BIGINT      NOT NULL DEFAULT 0,  -- 最后一次心跳时间戳（毫秒）
    PRIMARY KEY (worker_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
