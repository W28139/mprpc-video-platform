cmake_minimum_required(VERSION 3.10)
project(muduo_im VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 构建模式 (可通过 -DCMAKE_BUILD_TYPE 覆盖)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# 统一输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_SOURCE_DIR}/lib)

# Release 优化参数 (来自 wevix_muduo)
# ENABLE_NATIVE_MARCH：Docker 镜像/CI 构建需关闭（-march=native 的镜像不可跨 CPU 移植），
# 本机构建默认开启。
option(ENABLE_NATIVE_MARCH "Use -march=native for Release builds" ON)
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -DNDEBUG -flto=auto")
    if(ENABLE_NATIVE_MARCH)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    endif()
endif()

# 子项目
add_subdirectory(wevix_muduo)
add_subdirectory(mprpc)
add_subdirectory(video_platform)
add_subdirectory(test)
