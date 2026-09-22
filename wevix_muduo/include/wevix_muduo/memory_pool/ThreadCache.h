#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <cstdlib>
#include <cstdint>   // uint16_t（优化 #24/#25）

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
