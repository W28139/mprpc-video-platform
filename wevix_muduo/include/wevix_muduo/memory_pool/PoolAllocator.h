#pragma once
#include "wevix_muduo/memory_pool/MemoryPool.h"
#include <cstddef>
#include <new>
#include <type_traits>

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
