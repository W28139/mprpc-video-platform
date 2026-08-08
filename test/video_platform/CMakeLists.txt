# ============================================================================
# test/video_platform/ — video_platform 业务代码测试与压测
# ============================================================================

# --- 单元测试 ---

# FfmpegExecutor 功能测试：ffmpeg_executor.cpp 仅依赖 wevix_muduo（AsyncLogger），
# 直接编译源码，绕开 video_common 的 mprpc/mysql/hiredis/rabbitmq 依赖链。
# ffmpeg/ffprobe 不在 PATH 时测试自报 SKIP 退出 0。
add_executable(test_ffmpeg_executor
    test_ffmpeg_executor.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../../video_platform/src/transcode_worker/ffmpeg_executor.cpp
)
target_include_directories(test_ffmpeg_executor PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../video_platform/include
)
target_link_libraries(test_ffmpeg_executor wevix_muduo pthread)

# Store 原子 SQL 功能测试：需 MySQL（不可达时 SKIP 退出 0，CI 安全跳过）。
# video_common 的 include 与 mysqlclient/hiredis/rabbitmq 依赖自动传递。
add_executable(test_store test_store.cpp)
target_link_libraries(test_store video_common)

# --- 压测 ---

# 业务 RPC 压测：pb 生成码直接编译（job/worker 均 import common），
# DirectRpcChannel 只需 mprpc 头，避开 video_common 重依赖
add_executable(bench_biz_stress
    bench_biz_stress.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../../video_platform/proto/generated/job.pb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../video_platform/proto/generated/worker.pb.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../video_platform/proto/generated/common.pb.cc
)
target_include_directories(bench_biz_stress PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../video_platform/proto/generated
)
target_link_libraries(bench_biz_stress mprpc protobuf pthread)
