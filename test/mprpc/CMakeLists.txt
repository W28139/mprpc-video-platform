# ============================================================================
# test/mprpc/ — mprpc 框架测试、集成与压测
# ============================================================================

include_directories(../proto)

# --- 单元测试 ---

add_executable(test_rpc_protocol test_rpc_protocol.cpp)
target_link_libraries(test_rpc_protocol mprpc protobuf)

# --- 集成测试 ---

# RPC Echo 服务端
add_executable(rpc_echo_server
    rpc_echo_server.cpp
    ../proto/rpc_echo.pb.cc
)
target_link_libraries(rpc_echo_server mprpc protobuf)

# UserService provider / consumer
add_executable(user_provider user_provider.cc ../proto/user.pb.cc)
target_link_libraries(user_provider mprpc protobuf)

add_executable(user_consumer user_consumer.cc ../proto/user.pb.cc)
target_link_libraries(user_consumer mprpc protobuf)

# FriendService provider / consumer
add_executable(friend_provider friend_provider.cc ../proto/friend.pb.cc)
target_link_libraries(friend_provider mprpc protobuf)

add_executable(friend_consumer friend_consumer.cc ../proto/friend.pb.cc)
target_link_libraries(friend_consumer mprpc protobuf)

# --- 压测 ---

add_executable(bench_rpc_stress
    bench_rpc_stress.cpp
    ../proto/rpc_echo.pb.cc
)
target_link_libraries(bench_rpc_stress mprpc protobuf pthread)
