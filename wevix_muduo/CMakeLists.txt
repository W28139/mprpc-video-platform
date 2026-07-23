project(wevix_muduo VERSION 1.0.0 LANGUAGES CXX)

# 查找依赖
find_package(Threads REQUIRED)

# 搜集源文件（递归,包含 memory_pool 子目录）
file(GLOB_RECURSE LIB_SOURCES "src/*.cpp")

# 生成库
add_library(${PROJECT_NAME} STATIC ${LIB_SOURCES})

# 指定头文件
target_include_directories(${PROJECT_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

# 链接
target_link_libraries(${PROJECT_NAME} PUBLIC Threads::Threads)
