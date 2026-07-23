#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <cstdlib>

// ============================================================================
// ThreadCache —— 线程本地缓存（三层架构的第一层）
// ============================================================================

namespace wevix_muduo {
namespace memory_pool {

class ThreadCache
{
public:
    static ThreadCache* getInstance()
    {
        static thread_local ThreadCache instance;
        return &instance;
    }

    void* allocate(size_t size);
    void deallocate(void* ptr, size_t size);

private:
    ThreadCache()
    {
        free_list_.fill(nullptr);
        free_list_size_.fill(0);
    }

    void* fetchFromCentralCache(size_t index);
    void returnToCentralCache(void* start, size_t size);
    bool shouldReturnToCentralCache(size_t index);

private:
    std::array<void*, FREE_LIST_SIZE>  free_list_;
    std::array<size_t, FREE_LIST_SIZE> free_list_size_;
};

} // namespace memory_pool
} // namespace wevix_muduo
