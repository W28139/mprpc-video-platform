#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <mutex>
#include <deque>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>   // uint16_t（优化 #25）

namespace wevix_muduo
{
namespace memory_pool
{

// Span 追踪器 —— 每个从 PageCache 分配的 Span 对应一个
// 所有字段为 atomic，支持无锁读取（getSpanTracker）和原子更新
struct SpanTracker {
    std::atomic<void*> spanAddr{nullptr};   // Span 起始地址（页对齐）
    std::atomic<size_t> numPages{0};        // 包含的页数
    std::atomic<size_t> blockCount{0};      // 切分成的总块数
    std::atomic<size_t> freeCount{0};       // 当前在 centralFreeList_ 中的空闲块数
    std::atomic<size_t> scanCount{0};       // performDelayedReturn 临时计数
};

class CentralCache
{
public:
    static CentralCache& getInstance()
    {
        static CentralCache instance;
        return instance;
    }

    // 向 ThreadCache 批量分发内存块（返回 BATCH_SIZE 块的链表头）
    // 优化 #26：outBatchNum 返回实际传输的块数（省 ThreadCache 侧链表遍历计数）
    void* fetchRange(size_t index, size_t& outBatchNum);

    // 接收 ThreadCache 归还的内存块链表
    void returnRange(void* start, size_t size, size_t index);

private:
    CentralCache();

    // 从 PageCache 申请新 Span
    void* fetchFromPageCache(size_t size);

    // 给定块地址，查找所属 Span 的追踪器（已排序部分二分查找 O(log n)）
    SpanTracker* getSpanTracker(void* blockAddr);

    // 更新 Span 空闲计数，全空闲则归还 PageCache
    void updateSpanFreeCount(SpanTracker* tracker, size_t freeBlocksInList, size_t index);

    // 扩容 SpanTracker 数组（每次 +256 槽位）
    void expandTrackerArray(size_t requiredIndex);

    // 重排序数组以支持二分查找（未排序条目超 64 个触发）
    void ensureSorted();

    // 延迟归还判断与执行
    bool shouldPerformDelayedReturn(size_t index, size_t currentCount,
        std::chrono::steady_clock::time_point currentTime);
    void performDelayedReturn(size_t index);

private:
    // ---- 自由链表与锁 ----
    // centralFreeList_[i] → 大小类别 i 的空闲块链表头（单向链表，next 存于块前 8 字节）
    std::array<std::atomic<void*>, FREE_LIST_SIZE> centralFreeList_;
    // locks_[i] 保护 centralFreeList_[i] 的并发访问（桶级自旋锁）
    std::array<std::atomic_flag, FREE_LIST_SIZE> locks_;

    // ---- SpanTracker 无锁存储 ----
    // 两层结构：
    //   1. trackerStorage_ (deque)    — 实际持有 SpanTracker 对象（deque 地址稳定）
    //   2. trackerArray_  (atomic)    — 扁平指针数组，扩容/重排时原子替换
    std::deque<SpanTracker> trackerStorage_;
    std::atomic<SpanTracker**> trackerArray_{nullptr};
    std::atomic<size_t> trackerCount_{0};          // 数组容量（总槽位数）
    std::atomic<size_t> spanCount_{0};             // 已使用的 SpanTracker 数量
    std::atomic<size_t> sortedCount_{0};           // 已按 spanAddr 排序的条目数
    std::mutex trackerExpandMutex_;                // 保护扩容和重排序（极低频）

    // ---- 配置常量 ----
    static const size_t TRACKER_INITIAL_CAPACITY = 1024;  // 初始 1024 个槽位
    static const size_t TRACKER_EXPAND_SIZE = 256;        // 每次扩容 +256
    // 优化 #20：BATCH_SIZE 8 → 32（tcmalloc 经验值），跨层调用与桶锁获取次数降 4 倍
    static const size_t BATCH_SIZE = 32;                  // fetchRange 批量传输 32 块
    static const size_t SORT_THRESHOLD = 64;              // 未排序超 64 触发重排

    // ---- 延迟归还 ----
    static const size_t MAX_DELAY_COUNT = 48;    // 累计归还操作 48 次后触发检查
    // 优化 #25：size_t → uint16_t（缓存足迹 256KB → 64KB）。
    // 上限边界：MAX_DELAY_COUNT(48)+1 = 49 < 65535 ✓
    std::array<std::atomic<uint16_t>, FREE_LIST_SIZE> delayCounts_;
    std::array<std::chrono::steady_clock::time_point, FREE_LIST_SIZE> lastReturnTimes_;
    static const std::chrono::milliseconds DELAY_INTERVAL; // 1 秒
};

} // namespace memory_pool
} // namespace wevix_muduo
