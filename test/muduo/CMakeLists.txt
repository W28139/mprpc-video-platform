# ============================================================================
# test/muduo/ — wevix_muduo 网络库测试与压测
# ============================================================================

include_directories(../proto)

# --- 单元测试 ---

add_executable(test_buffer test_buffer.cpp)
target_link_libraries(test_buffer wevix_muduo)

add_executable(test_threadpool test_threadpool.cpp)
target_link_libraries(test_threadpool wevix_muduo)

# --- 集成测试 ---

add_executable(echo_server echo_server.cpp)
target_link_libraries(echo_server wevix_muduo)

add_executable(echo_client echo_client.cpp)
target_link_libraries(echo_client pthread)

# --- 压测 ---

add_executable(bench_echo_stress bench_echo_stress.cpp)
target_link_libraries(bench_echo_stress pthread)
