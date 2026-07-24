# ============================================================================
# test/ 测试与压测 CMake 构建
# ============================================================================

# 引入 proto 目录（.pb.h / .pb.cc）
include_directories(proto)

# --- 被测目标：Echo 服务器（raw TCP 层） ---
add_executable(echo_server echo_server.cpp)
target_link_libraries(echo_server wevix_muduo)

# --- 单元测试 ---

# Buffer 单元测试
add_executable(test_buffer test_buffer.cpp)
target_link_libraries(test_buffer wevix_muduo)

# ThreadPool 单元测试
add_executable(test_threadpool test_threadpool.cpp)
target_link_libraries(test_threadpool wevix_muduo)

# RPC 协议/Controller 单元测试
add_executable(test_rpc_protocol test_rpc_protocol.cpp)
target_link_libraries(test_rpc_protocol mprpc protobuf)

# --- 端到端压测 (raw TCP) ---

# 原有 echo_client (保留兼容)
add_executable(echo_client echo_client.cpp)
target_link_libraries(echo_client pthread)

# 增强版 echo 压测
add_executable(bench_echo_stress bench_echo_stress.cpp)
target_link_libraries(bench_echo_stress pthread)

# ============================================================================
# --- mprpc RPC 层压测 ---
# ============================================================================

# RPC Echo 服务端（依赖 mprpc 框架 + protobuf）
add_executable(rpc_echo_server
    rpc_echo_server.cpp
    proto/rpc_echo.pb.cc
)
target_link_libraries(rpc_echo_server mprpc protobuf)

# RPC Echo 压测客户端
# - ZK 模式：需要 mprpc 框架 + protobuf
# - Direct 模式：仅用原生 socket + protobuf，不依赖 ZK
add_executable(bench_rpc_stress
    bench_rpc_stress.cpp
    proto/rpc_echo.pb.cc
)
target_link_libraries(bench_rpc_stress mprpc protobuf pthread)
