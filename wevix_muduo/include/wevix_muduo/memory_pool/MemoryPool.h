#pragma once
#include "wevix_muduo/memory_pool/ThreadCache.h"

// ============================================================================
// MemoryPool —— 对外统一 API
// ============================================================================

namespace wevix_muduo {
namespace memory_pool {

class MemoryPool
{
public:
    static void* allocate(size_t size)
    {
        return ThreadCache::getInstance()->allocate(size);
    }

    static void deallocate(void* ptr, size_t size)
    {
        ThreadCache::getInstance()->deallocate(ptr, size);
    }
};

} // namespace memory_pool
} // namespace wevix_muduo
