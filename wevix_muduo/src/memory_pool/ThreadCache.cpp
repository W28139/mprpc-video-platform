#include "wevix_muduo/memory_pool/ThreadCache.h"
#include "wevix_muduo/memory_pool/CentralCache.h"

namespace wevix_muduo
{
namespace memory_pool
{
// allocate —— 线程本地分配入口
void* ThreadCache::allocate(size_t size)
{
    // 0 字节请求 → 至少分配一个对齐单位
    if (size == 0)
    {
        size = ALIGNMENT; // 8 字节
    }

    // 超大对象（>256KB）直接走系统 malloc，不进内存池
    if (size > MAX_BYTES)
    {
        return malloc(size);
    }

    // 计算大小类别索引：8B→0, 16B→1, 32B→2, ...
    size_t index = SizeClass::getIndex(size);

    // 先检查本地是否有现成的空闲块
    if (void* ptr = freeList_[index])
    {
        freeList_[index] = *reinterpret_cast<void**>(ptr);
        freeListSize_[index]--;
        return ptr;
    }
    // 慢速路，从 CentralCache 批量获取
    return fetchFromCentralCache(index);
}

// deallocate —— 线程本地释放入口
void ThreadCache::deallocate(void* ptr, size_t size)
{
    // 大对象直接 free
    if (size > MAX_BYTES)
    {
        free(ptr);
        return;
    }

    size_t index = SizeClass::getIndex(size);

    *reinterpret_cast<void**>(ptr) = freeList_[index];  // 改变ptr的指针指向
    freeList_[index] = ptr;                             // 改变freeList_的存储内容

    freeListSize_[index]++;

    if (shouldReturnToCentralCache(index))
    {
        returnToCentralCache(freeList_[index], size);
    }
}

bool ThreadCache::shouldReturnToCentralCache(size_t index)
{
    size_t size = freeListSize_[index];
    return ((size & 1023) == 0) && (size > 1024);
}

void* ThreadCache::fetchFromCentralCache(size_t index)
{
    // 从 CentralCache 批量取货
    size_t batchNum = 0;
    void* start = CentralCache::getInstance().fetchRange(index, batchNum);
    if (!start) return nullptr; 

    // start 是链表头，第一块返回给用户
    void* result = start;
    // 剩余块放入本地 freeList_
    freeList_[index] = *reinterpret_cast<void**>(start);
    freeListSize_[index] += batchNum - 1;
    return result;
}

void ThreadCache::returnToCentralCache(void* start, size_t size)
{
    size_t index = SizeClass::getIndex(size);
    size_t alignedSize = SizeClass::roundUp(size);

    size_t batchNum = freeListSize_[index];
    if (batchNum <= 1) return;

    // 预期保留 1/4，归还 3/4
    size_t keepNum = std::max(batchNum / 4, size_t(1));

    char* splitNode = static_cast<char*>(start);
    size_t actualKeep = 1;  // start 算第 1 个
    for (size_t i = 0; i < keepNum - 1; ++i)
    {
        void* next = *reinterpret_cast<void**>(splitNode);
        if (next == nullptr)
            break;  // 链表比计数器短，以实际为准
        splitNode = static_cast<char*>(next);
        actualKeep++;
    }

    // 在分割点断开链表
    void* nextNode = *reinterpret_cast<void**>(splitNode);
    *reinterpret_cast<void**>(splitNode) = nullptr;

    // 归还部分的实际长度 = 总计数器 - 实际保留数
    size_t actualReturn = (batchNum > actualKeep) ? (batchNum - actualKeep) : 0;

    freeList_[index] = start;
    freeListSize_[index] = actualKeep;

    // 归还给 CentralCache
    if (actualReturn > 0 && nextNode != nullptr)
    {
        CentralCache::getInstance().returnRange(nextNode, actualReturn * alignedSize, index);
    }
}

} // namespace memory_pool
} // namespace wevix_muduo
