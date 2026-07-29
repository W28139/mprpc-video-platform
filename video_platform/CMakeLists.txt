# ============================================================================
# video_platform/ — 分布式视频处理与资源调度平台
# ============================================================================
project(video_platform VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Proto 生成的 C++ 文件 ──────────────────────────────────────────────
set(PROTO_GEN_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto/generated)

set(PROTO_SRCS
    ${PROTO_GEN_DIR}/common.pb.cc
    ${PROTO_GEN_DIR}/job.pb.cc
    ${PROTO_GEN_DIR}/scheduler.pb.cc
    ${PROTO_GEN_DIR}/worker.pb.cc
    ${PROTO_GEN_DIR}/result.pb.cc
)

# 自定义目标：重新生成 proto（需要手动运行 protoc）
add_custom_target(gen_protos
    COMMAND protoc
        --cpp_out=${PROTO_GEN_DIR}
        -I${CMAKE_CURRENT_SOURCE_DIR}/proto
        ${CMAKE_CURRENT_SOURCE_DIR}/proto/common.proto
        ${CMAKE_CURRENT_SOURCE_DIR}/proto/job.proto
        ${CMAKE_CURRENT_SOURCE_DIR}/proto/scheduler.proto
        ${CMAKE_CURRENT_SOURCE_DIR}/proto/worker.proto
        ${CMAKE_CURRENT_SOURCE_DIR}/proto/result.proto
    COMMENT "Generating C++ from proto files..."
)

# ── 公共库：存储层 ──────────────────────────────────────────────────
add_library(video_common
    ${CMAKE_CURRENT_SOURCE_DIR}/src/common_store.cpp
    ${PROTO_SRCS}
)
target_include_directories(video_common PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${PROTO_GEN_DIR}
)
target_link_libraries(video_common PUBLIC mprpc protobuf)

# ── 服务可执行文件 ──────────────────────────────────────────────────────
# 每个服务独立可执行文件，通过 -i 指定配置文件

# JobService (port 9001)
add_executable(job_service
    ${CMAKE_CURRENT_SOURCE_DIR}/src/result_collector/job_service/job_service.cpp
)
target_link_libraries(job_service video_common)

# SchedulerService (port 9002)
add_executable(scheduler_service
    ${CMAKE_CURRENT_SOURCE_DIR}/src/scheduler_service/scheduler_service.cpp
)
target_link_libraries(scheduler_service video_common)

# WorkerManagerService (port 9003)
add_executable(worker_manager
    ${CMAKE_CURRENT_SOURCE_DIR}/src/worker_manager/worker_manager.cpp
)
target_link_libraries(worker_manager video_common)

# TranscodeWorker (port 9004)
add_executable(transcode_worker
    ${CMAKE_CURRENT_SOURCE_DIR}/src/transcode_worker/transcode_worker.cpp
)
target_link_libraries(transcode_worker video_common)

# ResultCollectorService (port 9005)
add_executable(result_collector
    ${CMAKE_CURRENT_SOURCE_DIR}/src/result_collector/result_collector.cpp
)
target_link_libraries(result_collector video_common)

# JobClient — 提交任务的命令行客户端工具
add_executable(job_client
    ${CMAKE_CURRENT_SOURCE_DIR}/src/result_collector/job_service/job_client.cpp
)
target_link_libraries(job_client video_common)
