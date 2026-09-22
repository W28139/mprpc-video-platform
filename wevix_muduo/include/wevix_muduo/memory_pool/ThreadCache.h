#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <cstdlib>
#include <cstdint>   // uint16_t（优化 #24/#25）

// ============================================================================
// ThreadCache —— 线程本地缓存（三层架构的第一层）
// ============================================================================
//
// 每个线程一个独立实例（thread_local 单例），完全无锁。
// 这是内存池高性能的核心 —— 大多数分配/释放只在本层完成。
//
// 数据结构：
//   freeList_[32768]     — 每个大小类别一个单向链表，存空闲块
//   freeListSize_[32768] — 每个链表当前长度，用于触发回收阈值
//
// 设计要点：
//   1. 分配时先查 freeList_[index]，命中则 O(1) 取链表头
//   2. 不命中则调用 CentralCache::fetchRange 批量取 BATCH_SIZE(32) 块
//   3. 释放时插入 freeList_[index] 头部，计数超 256 则归还 3/4 给 CentralCache
//   4. thread_local 保证无锁 —— 每个线程只操作自己的 freeList_

namespace wevix_muduo
{
namespace memory_pool
{

class ThreadCache
{
public:
    // thread_local 单例：每个线程首次调用时自动构造
    static ThreadCache* getInstance()
    {
        static thread_local ThreadCache instance;
        return &instance;
    }

    // 分配 size 字节内存
    void* allocate(size_t size);

    // 释放 ptr 指向的 size 字节内存
    void deallocate(void* ptr, size_t size);

private:
    // 构造函数：清零所有链表头和计数器
    ThreadCache()
    {
        freeList_.fill(nullptr);
        freeListSize_.fill(0);
    }

    // 从 CentralCache 批量获取内存块
    void* fetchFromCentralCache(size_t index);

    // 将超出阈值的内存块归还 CentralCache
    void returnToCentralCache(void* start, size_t size);

    // 判断是否需要归还（freeListSize_[index] > 1024，位掩码化）
    bool shouldReturnToCentralCache(size_t index);

private:
    // 每个大小类别一个自由链表头指针
    // freeList_[i] 指向该大小类别第一个空闲块
    // 空闲块的前 8 字节存 next 指针（复用用户数据区）
    std::array<void*, FREE_LIST_SIZE>  freeList_;

    // 每个自由链表当前的空闲块数量
    // 超过 256 触发 returnToCentralCache，保留 1/4 继续本地使用
    // 优化 #25：size_t → uint16_t（缓存足迹 256KB → 128KB，热路径命中率提升）。
    // 上限边界：归还触发点 2048（位掩码后首次 1024 整数倍）+ 批量 31 = 2079 < 65535 ✓
    std::array<uint16_t, FREE_LIST_SIZE> freeListSize_;
};

} // namespace memory_pool
} // namespace wevix_muduo
