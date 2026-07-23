add_executable(alloc_bench alloc_bench.cpp)
target_link_libraries(alloc_bench wevix_muduo)

add_executable(echo_client echo_client.cpp)
target_link_libraries(echo_client pthread)

add_executable(pool_alloc_test pool_allocator_test.cpp)
target_link_libraries(pool_alloc_test wevix_muduo)
