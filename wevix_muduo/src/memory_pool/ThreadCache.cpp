#include "wevix_muduo/memory_pool/ThreadCache.h"
#include "wevix_muduo/memory_pool/CentralCache.h"

// ============================================================================
// ThreadCache —— 线程本地缓存实现
// ============================================================================
//
// 每个线程独立的 TLS 缓存。绝大多数 allocate/deallocate 只在本层完成。
//
// ── 数据流 ──
//
//   allocate(size)
//     → getIndex → freeListSize_[i]-- → 查 freeList_[i]
//       → 命中：取链表头返回（O(1)，无锁）
//       → 不命中：fetchFromCentralCache(i) → 批量拿 8 块，1 块返回，7 块进本地
//
//   deallocate(ptr, size)
//     → getIndex → 插入 freeList_[i] 头部 → freeListSize_[i]++
//       → > 1024 时：returnToCentralCache → 保留 1/4，归还 3/4 给 CentralCache
//
// ── 关键修复 #02 ──
//   freeListSize_[i]-- 从无条件执行改为只在成功取块后执行（防止无符号下溢）
//   fetchFromCentralCache 中 freeListSize_ 加 batchNum-1 而非 batchNum
//   （原代码两个错误互相抵消：allocate 多减 1 + fetch 多加 1 = 巧合正确）

namespace wevix_muduo
{
namespace memory_pool
{

// =========================================================================
// allocate —— 线程本地分配入口
// =========================================================================
//
// 快速路径：freeList_[index] 命中 → 取链表头，O(1) 返回
// 慢速路径：freeList_[index] 为空 → fetchFromCentralCache → CentralCache 取批次
//
// 注意：freeListSize_[index] 只在真正从 freeList_ 取块时才减 1
// （修复 #02：之前无条件递减，链表空时 size_t 下溢到 SIZE_MAX）
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

    // 先检查本地是否有现成的空闲块（快速路径，无锁）
    if (void* ptr = freeList_[index])
    {
        // 取链表头，更新链表头为 next
        // freeList_[index] → [块A] → [块B] → ...
        //          ptr = 块A
        // 取走后：freeList_[index] → [块B] → ...
        freeList_[index] = *reinterpret_cast<void**>(ptr);
        // 计数减 1（只在成功取块时才减，避免无符号下溢）
        freeListSize_[index]--;
        return ptr;
    }

    // 慢速路径：本地无货，从 CentralCache 批量获取
    return fetchFromCentralCache(index);
}

// =========================================================================
// deallocate —— 线程本地释放入口
// =========================================================================
//
// 插入 freeList_[index] 头部（O(1)），计数 +1。
// 超过 1024 个空闲块时触发 returnToCentralCache，归还 3/4 给 CentralCache。
void ThreadCache::deallocate(void* ptr, size_t size)
{
    // 大对象直接 free
    if (size > MAX_BYTES)
    {
        free(ptr);
        return;
    }

    size_t index = SizeClass::getIndex(size);

    // 头插法插入自由链表：新块.next = 旧链表头
    // 插入前：freeList_[index] → [旧链表...]
    // 插入后：freeList_[index] → [ptr] → [旧链表...]
    *reinterpret_cast<void**>(ptr) = freeList_[index];  // 改变ptr的指针指向
    freeList_[index] = ptr;                             // 改变freeList_的存储内容

    // 计数 +1
    freeListSize_[index]++;

    // 超过阈值（1024）→ 归还 3/4 给 CentralCache
    if (shouldReturnToCentralCache(index))
    {
        returnToCentralCache(freeList_[index], size);
    }
}

// =========================================================================
// shouldReturnToCentralCache —— 判断是否触发归还
// =========================================================================
bool ThreadCache::shouldReturnToCentralCache(size_t index)
{
    // size_t threshold = 1024;
    // return (freeListSize_[index] > threshold);
    size_t size = freeListSize_[index];
    return ((size & 1023) == 0) && (size > 1024);
}

// =========================================================================
// fetchFromCentralCache —— 从 CentralCache 批量获取内存块
// =========================================================================
//
// 调用 CentralCache::fetchRange 拿到最多 BATCH_SIZE(32) 块的链表，
// 第一块返回给调用者，剩余 31 块放入本地 freeList_ 供后续快速命中。
//
// 修复 #02：freeListSize_ += batchNum - 1（batchNum 含返回给用户的那块）
void* ThreadCache::fetchFromCentralCache(size_t index)
{
    // 从 CentralCache 批量取货（最多 32 块的链表）
    // 优化 #26：batchNum 由 fetchRange 返回，省去本地链表遍历计数
    size_t batchNum = 0;
    void* start = CentralCache::getInstance().fetchRange(index, batchNum);
    if (!start) return nullptr; // 系统内存耗尽

    // start 是链表头，第一块返回给用户
    void* result = start;
    // 剩余块放入本地 freeList_
    freeList_[index] = *reinterpret_cast<void**>(start);

    // batchNum 包含返回给用户的 result，实际进本地的是 batchNum-1 块
    // 修复 #02：从 += batchNum 改为 += batchNum-1
    freeListSize_[index] += batchNum - 1;

    return result;
}

// =========================================================================
// returnToCentralCache —— 归还多余块给 CentralCache
// =========================================================================
//
// 当 freeListSize_[index] > 1024 时触发。
// 策略：保留 1/4（至少 1 块），归还 3/4。
// 遍历链表找到保留/归还的分割点，断开后调用 CentralCache::returnRange。
void ThreadCache::returnToCentralCache(void* start, size_t size)
{
    size_t index = SizeClass::getIndex(size);
    size_t alignedSize = SizeClass::roundUp(size);

    size_t batchNum = freeListSize_[index];
    if (batchNum <= 1) return;

    // 预期保留 1/4，归还 3/4
    size_t keepNum = std::max(batchNum / 4, size_t(1));

    // 遍历链表找到保留部分的尾节点，同时统计实际遍历到的节点数
    // 如果链表实际长度 < freeListSize_（计数器出现偏差），
    // 用实际遍历到的节点数修正，而非静默跳过导致死循环
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
    // 注意：如果计数器有偏差，这里用 batchNum 而不是遍历统计（归还部分可能也不完整）
    size_t actualReturn = (batchNum > actualKeep) ? (batchNum - actualKeep) : 0;

    // 使用实际保留数更新计数器（修正可能的偏差，防止死循环）
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
