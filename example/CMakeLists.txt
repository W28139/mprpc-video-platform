include_directories(proto)

add_subdirectory(callee)
add_subdirectory(caller)

# Echo 服务器 (端到端压测)
add_executable(echo_server echo_server.cpp)
target_link_libraries(echo_server wevix_muduo)
