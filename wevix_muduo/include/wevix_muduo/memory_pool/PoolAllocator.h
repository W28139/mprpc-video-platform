#pragma once
#include "wevix_muduo/memory_pool/MemoryPool.h"
#include <cstddef>
#include <new>
#include <type_traits>

// ============================================================================
// PoolAllocator —— 对接 mymuduo 内存池的 STL 兼容分配器
//
// 用法：
//   std::vector<char, PoolAllocator<char>>      buf;
//   std::basic_string<char, ..., PoolAllocator<char>> str;
//   std::map<int, std::string, std::less<int>,
//            PoolAllocator<std::pair<const int, std::string>>> mymap;
//
// 注意：
//   1. >256KB 的分配自动 fallback 到 malloc，对 STL 容器透明
//   2. 对齐固定为 8 字节，不适用于需要 16/32 字节对齐的 SIMD 类型
//   3. 所有实例等价（stateless），容器 move 不涉及 allocator 拷贝开销
// ============================================================================

namespace wevix_muduo {
namespace memory_pool {

template <typename T>
class PoolAllocator {
public:
    // ---- STL Allocator 必须的类型定义 ----
    using value_type      = T;
    using size_type       = std::size_t;
    using difference_type = std::ptrdiff_t;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::true_type;

    // ---- 构造/析构 ----
    PoolAllocator() = default;
    ~PoolAllocator() = default;

    // 允许不同类型之间的拷贝（如 rebind）
    template <typename U>
    PoolAllocator(const PoolAllocator<U>&) noexcept {}

    // ---- 核心：分配 ----
    [[nodiscard]] T* allocate(std::size_t n)
    {
        if (n == 0) return nullptr;

        // 溢出检查
        if (n > max_size())
            throw std::bad_array_new_length();

        std::size_t bytes = n * sizeof(T);
        void* ptr = MemoryPool::allocate(bytes);
        if (!ptr)
            throw std::bad_alloc();
        return static_cast<T*>(ptr);
    }

    // ---- 核心：释放 ----
    void deallocate(T* ptr, std::size_t n) noexcept
    {
        if (!ptr || n == 0) return;
        MemoryPool::deallocate(ptr, n * sizeof(T));
    }

    // ---- 最大可分配数量 ----
    std::size_t max_size() const noexcept
    {
        // MemoryPool 支持最大 MAX_BYTES (256KB)，更大的走 malloc
        // 这里返回一个合理的上限
        return static_cast<std::size_t>(-1) / sizeof(T);
    }

};

// ---- 相等比较（所有实例等价，stateless allocator）----
template <typename T, typename U>
bool operator==(const PoolAllocator<T>&, const PoolAllocator<U>&) noexcept
{
    return true;
}
template <typename T, typename U>
bool operator!=(const PoolAllocator<T>&, const PoolAllocator<U>&) noexcept
{
    return false;
}

} // namespace memory_pool
} // namespace wevix_muduo
