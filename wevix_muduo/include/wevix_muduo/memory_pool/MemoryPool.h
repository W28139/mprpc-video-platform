#pragma once
#include "wevix_muduo/memory_pool/ThreadCache.h"

// ============================================================================
// MemoryPool —— 对外统一 API
// ============================================================================
//
// 用户只与这个类交互，无需知道内部三层架构。
// 本质是 ThreadCache 单例的静态代理。
//
// 用法：
//   void* p = MemoryPool::allocate(128);
//   MemoryPool::deallocate(p, 128);
//
// 内部流程：
//   allocate   → ThreadCache::allocate   → (命中) 直接返回
//                                        → (不命中) fetchFromCentralCache
//                                           → CentralCache::fetchRange
//                                              → PageCache::allocateSpan
//   deallocate → ThreadCache::deallocate → 放入本地 freeList_
//                                        → 超阈值 → returnToCentralCache
//                                           → CentralCache::returnRange

namespace wevix_muduo
{
namespace memory_pool
{

class MemoryPool
{
public:
    // 分配 size 字节的内存，返回 8 字节对齐的指针
    static void* allocate(size_t size)
    {
        return ThreadCache::getInstance()->allocate(size);
    }

    // 释放 ptr 指向的内存，size 必须与分配时一致
    // （内存池不存储块大小元数据，依赖调用者传递正确的 size）
    static void deallocate(void* ptr, size_t size)
    {
        ThreadCache::getInstance()->deallocate(ptr, size);
    }
};

} // namespace memory_pool
} // namespace wevix_muduo
