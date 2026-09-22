#include "wevix_muduo/memory_pool/CentralCache.h"
#include "wevix_muduo/memory_pool/PageCache.h"
#include "wevix_muduo/AsyncLogger.h"
#include <cassert>
#include <thread>     // std::this_thread::yield（自旋锁退避）
#include <chrono>
#include <algorithm>  // std::sort

namespace wevix_muduo
{
namespace memory_pool
{

// 一个大小类别至少隔 1 秒才触发一次 Span 归还检查，防止频繁归还/申请抖动
const std::chrono::milliseconds CentralCache::DELAY_INTERVAL{1000};

// ≤32KB 的大小类别统一用 8 页，减少 PageCache 碎片
static const size_t SPAN_PAGES = 8;

CentralCache::CentralCache()
{
    // 初始化所有自由链表为空
    for (auto& ptr : centralFreeList_)
    {
        ptr.store(nullptr, std::memory_order_relaxed);
    }

    // 初始化所有自旋锁为未锁定
    for (auto& lock : locks_)
    {
        lock.clear();
    }

    // 初始化延迟归还：计数归零，时间戳置为现在
    for (auto& count : delayCounts_)
    {
        count.store(0, std::memory_order_relaxed);
    }
    for (auto& time : lastReturnTimes_)
    {
        time = std::chrono::steady_clock::now();
    }
    // 初始化已被使用的也Span个数为0，内存序用最弱的relaxed,因为构造是单线程，不存在竞争
    spanCount_.store(0, std::memory_order_relaxed);

    // 共 1024 个槽位，每个槽位代表一个 Span，一个 Span 包含多页
    SpanTracker** initialArray = new SpanTracker*[TRACKER_INITIAL_CAPACITY];
    for (size_t i = 0; i < TRACKER_INITIAL_CAPACITY; ++i)
    {
        // 在双端队列尾部原地构造一个新元素，调用无参构造
        trackerStorage_.emplace_back(); 
        // .back() 返回最后一个元素的引用,取 & 得到的是指向该对象的指针,把该对象指针存入initialArray里
        initialArray[i] = &trackerStorage_.back(); 
    }
    // 把局部变量 initialArray（堆上的指针数组）原子写入成员 trackerArray_
    trackerArray_.store(initialArray, std::memory_order_release);
    // 容量 1024 原子写入成员 trackerCount_
    trackerCount_.store(TRACKER_INITIAL_CAPACITY, std::memory_order_release);
}

// fetchRange —— 向 ThreadCache 批量分发内存块
void* CentralCache::fetchRange(size_t index, size_t& outBatchNum)
{
    if (index >= FREE_LIST_SIZE)
    {
        outBatchNum = 0;
        return nullptr;
    }

    // 获取桶级自旋锁
    while (locks_[index].test_and_set(std::memory_order_acquire))
    {
        std::this_thread::yield();
    }
    void* result = nullptr;
    size_t batchCount = 0;   // 实际返回的块数

    try
    {
        result = centralFreeList_[index].load(std::memory_order_relaxed);
        if (!result)
        {
            // 分支 A：CentralCache 为空 → 从 PageCache 申请新 Span
            // 计算这个类别每块实际的字节数
            size_t size = (index + 1) * ALIGNMENT;

            // 向 PageCache 申请 Span（返回页对齐的连续内存起始地址）
            result = fetchFromPageCache(size);
            if (!result)
            {
                LOG_ERROR("CentralCache::fetchRange fetchFromPageCache failed, size=%zu, index=%zu", size, index);
                locks_[index].clear(std::memory_order_release);
                outBatchNum = 0;
                return nullptr;
            }
            
            // 转为char*类型，方便进行指针运算
            char* start = static_cast<char*>(result);

            // 计算页数和可切分块数
            size_t numPages = (size <= SPAN_PAGES * PageCache::PAGE_SIZE) ?
                                     SPAN_PAGES :
                                     (size + PageCache::PAGE_SIZE - 1) / PageCache::PAGE_SIZE;

            size_t blockNum = (numPages * PageCache::PAGE_SIZE) / size;

            if (blockNum > 1)
            {
                // ---- 切分 Span 为 blockNum 个 size 大小的小块，串成单向链表 ----
                //
                // 内存布局（size=32B）：
                //   [块0: 0x1000] [块1: 0x1020] [块2: 0x1040] ...
                // 链表：块0.next=块1, 块1.next=块2, ..., 块N.next=nullptr
                for (size_t i = 1; i < blockNum; ++i)
                {
                    void* current = start + (i - 1) * size;
                    void* next    = start + i * size;
                    *reinterpret_cast<void**>(current) = next;
                }
                *reinterpret_cast<void**>(start + (blockNum - 1) * size) = nullptr;

                // ---- 批量传输：取前 BATCH_SIZE 块返回，剩余留在 CentralCache ----
                // 传输的块数
                batchCount = 1;
                // 传输的尾指针
                void* batchTail = result;
                while (batchCount < BATCH_SIZE && batchCount < blockNum &&
                       *reinterpret_cast<void**>(batchTail) != nullptr)
                {
                    batchTail = *reinterpret_cast<void**>(batchTail);
                    batchCount++;
                }
                // rest为断开后的第一个块地址，用 ntralFreeList_
                void* rest = *reinterpret_cast<void**>(batchTail);
                *reinterpret_cast<void**>(batchTail) = nullptr; // 在批次尾断开

                // 把传入上层后剩余的块放入空闲列表
                centralFreeList_[index].store(rest, std::memory_order_release);

                // 下面就是维护内容了
                // 1. 创建 SpanTracker（登记新领取的内存块）
                // 拿到型假span的编号
                size_t trackerIndex = spanCount_++;
                
                // 2. 检查索引表（目录）够不够大。
                // trackerCount_ 是当前指针数组（目录表）的最大容量（初始1024）。
                // 如果领到的号超过了目录大小，就得给目录扩容。
                if (trackerIndex >= trackerCount_.load(std::memory_order_relaxed))
                    expandTrackerArray(trackerIndex);

                // 3. 获取当前的目录表指针,array
                // 使用 acquire 语义确保能看到 expandTrackerArray 中最新分配的内存。    
                SpanTracker** array = trackerArray_.load(std::memory_order_acquire);

                // 4. 从目录中通过索引找到对应的“空档案袋”对象（SpanTracker）。
                // 这些对象是在初始化或扩容时就 new 好的，现在直接拿地址用。
                SpanTracker* tracker = array[trackerIndex];

                // 5. 将这块内存的具体信息（Span 的属性）填入档案袋中。
                // 使用 store(..., release) 保证其他线程在 getSpanTracker 查到这些值时，数据已完整写入。
                tracker->spanAddr.store(start, std::memory_order_release);
                tracker->numPages.store(numPages, std::memory_order_release);
                tracker->blockCount.store(blockNum, std::memory_order_release);

                // 6. 初始化剩余块数（freeCount）。
                // blockNum 是总数，batchCount 是刚才已经分给 ThreadCache 的块数。
                tracker->freeCount.store(blockNum - batchCount, std::memory_order_release);

                // 性能优化：维护索引的有序性
                // 7. 获取当前页总数和已经排好序的数量。
                size_t total = spanCount_.load(std::memory_order_relaxed);
                size_t sorted = sortedCount_.load(std::memory_order_relaxed);
                // 8. 核心策略：懒排序触发器
                // 如果（总数 - 已排序数） > 64，说明末尾乱序的部分太长了，会拖慢 getSpanTracker 的线性扫描速度。
                // 这时调用 ensureSorted() 将所有 Span 按地址排序，以便后续能用二分查找（O(log n)）。
                if (total > sorted && (total - sorted) > SORT_THRESHOLD)
                    ensureSorted();
            }
            // 只有一个块（size>=16kb的操作）
            else
            {
                // blockNum == 1：大对象场景
                // 虽然只有一个块无需链表，但必须创建 SpanTracker
                // 否则该 Span 永远无法归还 PageCache
                // 直接维护trackerArray_即可，逻辑与上一致
                batchCount = 1;   // 单块整段返回
                size_t trackerIndex = spanCount_++;

                if (trackerIndex >= trackerCount_.load(std::memory_order_relaxed))
                    expandTrackerArray(trackerIndex);

                SpanTracker** array = trackerArray_.load(std::memory_order_acquire);
                SpanTracker* tracker = array[trackerIndex];

                tracker->spanAddr.store(start, std::memory_order_release);
                tracker->numPages.store(numPages, std::memory_order_release);
                tracker->blockCount.store(1, std::memory_order_release);
                // freeCount=0：唯一一块已返回给 ThreadCache
                tracker->freeCount.store(0, std::memory_order_release);

                size_t total = spanCount_.load(std::memory_order_relaxed);
                size_t sorted = sortedCount_.load(std::memory_order_relaxed);
                if (total > sorted && (total - sorted) > SORT_THRESHOLD)
                    ensureSorted();
            }
        }

        else
        {
            // 分支 B：CentralCache 有现成空闲块 → 批量取 BATCH_SIZE 块

            // 找出批次尾节点（第 BATCH_SIZE 块或链表末尾）
            batchCount = 1;
            void* batchTail = result;
            while (batchCount < BATCH_SIZE &&
                   *reinterpret_cast<void**>(batchTail) != nullptr)
            {
                batchTail = *reinterpret_cast<void**>(batchTail);
                batchCount++;
            }

            // 保存批次后的剩余链表，在批次尾断开
            void* rest = *reinterpret_cast<void**>(batchTail);
            *reinterpret_cast<void**>(batchTail) = nullptr;
            centralFreeList_[index].store(rest, std::memory_order_release);

            SpanTracker* cur = nullptr;
            void* current = result;
            for (size_t i = 0; i < batchCount; ++i)
            {
                if (!cur ||
                    current < cur->spanAddr.load(std::memory_order_relaxed) ||
                    current >= static_cast<char*>(cur->spanAddr.load(std::memory_order_relaxed)) +
                                 cur->numPages.load(std::memory_order_relaxed) * PageCache::PAGE_SIZE)
                {
                    cur = getSpanTracker(current);   // 典型情况整个批次只进 1 次
                }
                if (cur)
                    cur->freeCount.fetch_sub(1, std::memory_order_release);
                current = *reinterpret_cast<void**>(current);
            }
        }
    }
    catch (...)
    {
        locks_[index].clear(std::memory_order_release);
        outBatchNum = batchCount;   // 异常路径：已统计的块数（通常为 0）
        throw;
    }

    locks_[index].clear(std::memory_order_release);
    outBatchNum = batchCount;
    return result;
}

void CentralCache::returnRange(void* start, size_t size, size_t index)
{
    if (!start || index >= FREE_LIST_SIZE)
        return;

    size_t blockSize = (index + 1) * ALIGNMENT;
    size_t blockCount = size / blockSize;

    // 获取桶级自旋锁
    while (locks_[index].test_and_set(std::memory_order_acquire))
    {
        std::this_thread::yield();
    }

    try
    {
        // ---- 找到归还链表的尾节点 ----
        void* end = start;
        size_t count = 1;
        while (*reinterpret_cast<void**>(end) != nullptr && count < blockCount)
        {
            end = *reinterpret_cast<void**>(end);
            count++;
        }

        // ---- 头插法：CentralCache 现有链表接在归还链表后面 ----
        // 合并前：归还链表 start→...→end→nullptr  |  CentralCache oldHead→...
        // 合并后：start→...→end→oldHead→...
        void* current = centralFreeList_[index].load(std::memory_order_relaxed);
        *reinterpret_cast<void**>(end) = current;
        centralFreeList_[index].store(start, std::memory_order_release);

        size_t currentCount = delayCounts_[index].fetch_add(1, std::memory_order_relaxed) + 1;
        auto currentTime = std::chrono::steady_clock::now();

        // 两个条件满足其一就触发归还检查：
        //   A. 累计归还 ≥ 48 次（高频归还）
        //   B. 距上次检查 ≥ 1 秒（定时检查，防止 Span 长期空闲）
        if (shouldPerformDelayedReturn(index, currentCount, currentTime))
        {
            performDelayedReturn(index);
        }
    }
    catch (...)
    {
        locks_[index].clear(std::memory_order_release);
        throw;
    }

    locks_[index].clear(std::memory_order_release);
}

bool CentralCache::shouldPerformDelayedReturn(size_t index, size_t currentCount,
    std::chrono::steady_clock::time_point currentTime)
{
    if (currentCount >= MAX_DELAY_COUNT)   // 累计 ≥ 48 次
        return true;

    auto lastTime = lastReturnTimes_[index];
    return (currentTime - lastTime) >= DELAY_INTERVAL; // 距上次 ≥ 1 秒
}

void CentralCache::performDelayedReturn(size_t index)
{
    // 重置计数器与时间戳
    delayCounts_[index].store(0, std::memory_order_relaxed);
    lastReturnTimes_[index] = std::chrono::steady_clock::now();

    // 遍历空闲链表，在 SpanTracker 上累加计数
    // 遍历链表centralFreeList_，弄清楚这些乱序的内存块分别属于哪几个 Span
    void* currentBlock = centralFreeList_[index].load(std::memory_order_relaxed);
    while (currentBlock)
    {
        // 根据内存块的地址，找到它所属的SpanTracker
        SpanTracker* tracker = getSpanTracker(currentBlock);
        if (tracker)
        {
            // 找到所属 Span 后，在它内部的临时计数器 scanCount 上加 1
            // 使用 memory_order_relaxed 因为这里只保证原子增减，暂不涉及复杂的内存屏障
            tracker->scanCount.fetch_add(1, std::memory_order_relaxed);
        }
        currentBlock = *reinterpret_cast<void**>(currentBlock);
    }

    // 遍历所有 SpanTracker（span)，处理有空闲块的
    SpanTracker** array;
    size_t capacity;
    for (;;)
    {
        array = trackerArray_.load(std::memory_order_acquire);
        capacity = trackerCount_.load(std::memory_order_acquire);
        if (trackerArray_.load(std::memory_order_acquire) == array)
            break;
    }
    // 获取当前总共有多少个 Span
    // spanCount_ 先于扩容递增，读"旧数组 + 新 total"会越界遍历，上界取 min(total, capacity) 防越界；
    size_t total = spanCount_.load(std::memory_order_relaxed);
    size_t scanEnd = std::min(total, capacity);

    for (size_t i = 0; i < scanEnd; ++i)
    {
        SpanTracker* tracker = array[i];
        // 使用 exchange(0) 一次性完成两个动作：
        // a. 取出刚才在 while 循环里累加的总数,让count = scanCount(也就是该span的空闲块数)
        // b. 将 scanCount 重置为 0，以便迎接下一次 performDelayedReturn
        size_t count = tracker->scanCount.exchange(0, std::memory_order_relaxed);
        // 如果这个 Span 在刚才的扫描中有贡献（即 count > 0）
        if (count > 0)
        {
            // 调用核心更新逻辑：将这 count 个块真正还给这个 Span
            // 这个函数内部会处理：增加 Span 的 freeCount，判断是否要把全空的 Span 还给 PageCache 等
            updateSpanFreeCount(tracker, count, index);
        }
    }
}

void CentralCache::updateSpanFreeCount(SpanTracker* tracker, size_t freeBlocksInList, size_t index)
{
    // 直接设置为本次扫描统计到的空闲块数（修复 #01：从累加改为直接设置）
    tracker->freeCount.store(freeBlocksInList, std::memory_order_release);

    // 全部空闲（freeCount == blockCount）→ 归还 Span
    if (freeBlocksInList == tracker->blockCount.load(std::memory_order_relaxed))
    {
        void* spanAddr = tracker->spanAddr.load(std::memory_order_relaxed);
        size_t numPages = tracker->numPages.load(std::memory_order_relaxed);

        // ---- 从 centralFreeList_ 摘除属于该 Span 的所有块 ----
        void* head = centralFreeList_[index].load(std::memory_order_relaxed);
        void* newHead = nullptr;
        void* prev = nullptr;
        void* current = head;

        while (current)
        {
            void* next = *reinterpret_cast<void**>(current);

            // 判断 current 是否属于要归还的 Span
            if (current >= spanAddr &&
                current < static_cast<char*>(spanAddr) + numPages * PageCache::PAGE_SIZE)
            {
                // 属于 → 跳过（从链表中移除）
                if (prev)
                    *reinterpret_cast<void**>(prev) = next;
                else
                    newHead = next;
                // prev 不变（当前节点被跳过）
            }
            else
            {
                // 不属于 → 保留在新链表中
                prev = current;
            }
            current = next;
        }

        centralFreeList_[index].store(newHead, std::memory_order_release);

        // 归还 Span 给 PageCache
        PageCache::getInstance().deallocateSpan(spanAddr, numPages);

        // 归还后使 tracker 失效，防止地址复用时二分误命中僵尸档案。
        tracker->spanAddr.store(nullptr, std::memory_order_release);
        tracker->freeCount.store(0, std::memory_order_release);
    }
}


// 请求 ≤ 32KB → 固定申请 8 页（减少 PageCache 碎片）
// 请求 > 32KB → 按实际需要向上取整

void* CentralCache::fetchFromPageCache(size_t size)
{
    size_t numPages = (size + PageCache::PAGE_SIZE - 1) / PageCache::PAGE_SIZE;

    if (size <= SPAN_PAGES * PageCache::PAGE_SIZE)
    {
        return PageCache::getInstance().allocateSpan(SPAN_PAGES); // 8 页
    }
    else
    {
        return PageCache::getInstance().allocateSpan(numPages);
    }
}

SpanTracker* CentralCache::getSpanTracker(void* blockAddr)
{
    SpanTracker** array;
    size_t capacity;
    size_t sorted;
    for (;;)
    {
        array = trackerArray_.load(std::memory_order_acquire);
        capacity = trackerCount_.load(std::memory_order_acquire);
        sorted = sortedCount_.load(std::memory_order_acquire);
        if (trackerArray_.load(std::memory_order_acquire) == array)
            break;
    }
    size_t total = spanCount_.load(std::memory_order_relaxed);
    size_t scanEnd = std::min(total, capacity);

    // ---- 阶段 1：在已排序部分二分查找 O(log n) ----
    size_t left = 0, right = std::min(sorted, scanEnd);
    while (left < right)
    {
        size_t mid = left + (right - left) / 2;
        SpanTracker* t = array[mid];
        void* addr = t->spanAddr.load(std::memory_order_relaxed);

        if (blockAddr < addr)
        {
            right = mid;
        }
        else if (blockAddr >= static_cast<char*>(addr) +
                 t->numPages.load(std::memory_order_relaxed) * PageCache::PAGE_SIZE)
        {
            left = mid + 1;
        }
        else
        {
            return t; // 命中
        }
    }

    // ---- 阶段 2：未排序尾部线性扫描 O(SORT_THRESHOLD) ----
    for (size_t i = sorted; i < scanEnd; ++i)
    {
        SpanTracker* t = array[i];
        void* addr = t->spanAddr.load(std::memory_order_relaxed);
        size_t pages = t->numPages.load(std::memory_order_relaxed);

        if (blockAddr >= addr &&
            blockAddr < static_cast<char*>(addr) + pages * PageCache::PAGE_SIZE)
        {
            return t;
        }
    }
    return nullptr;
}

void CentralCache::expandTrackerArray(size_t requiredIndex)
{
    std::lock_guard<std::mutex> lock(trackerExpandMutex_);

    // 双重检查：可能其他线程已经扩容过了
    if (requiredIndex < trackerCount_.load(std::memory_order_relaxed))
        return;

    size_t oldSize = trackerCount_.load(std::memory_order_relaxed);
    size_t newSize = std::max(oldSize + TRACKER_EXPAND_SIZE, requiredIndex + 1);

    // 分配新指针数组
    SpanTracker** newArray = new SpanTracker*[newSize];

    // 复制旧指针
    SpanTracker** oldArray = trackerArray_.load(std::memory_order_acquire);
    for (size_t i = 0; i < oldSize; ++i)
        newArray[i] = oldArray[i];

    // 构造新的 SpanTracker
    for (size_t i = oldSize; i < newSize; ++i)
    {
        trackerStorage_.emplace_back();
        newArray[i] = &trackerStorage_.back();
    }

    trackerArray_.store(newArray, std::memory_order_release);
    trackerCount_.store(newSize, std::memory_order_release);
    // sortedCount_ 不更新：新槽位尚未排序，仍属于"未排序尾部"
}

void CentralCache::ensureSorted()
{
    size_t total = spanCount_.load(std::memory_order_relaxed);
    size_t sorted = sortedCount_.load(std::memory_order_relaxed);

    if (total - sorted <= SORT_THRESHOLD)
        return;

    std::lock_guard<std::mutex> lock(trackerExpandMutex_);

    // 双重检查
    total = spanCount_.load(std::memory_order_relaxed);
    sorted = sortedCount_.load(std::memory_order_relaxed);
    if (total - sorted <= SORT_THRESHOLD)
        return;

    size_t arraySize = trackerCount_.load(std::memory_order_relaxed);

    size_t used = std::min(total, arraySize);

    SpanTracker** newArray = new SpanTracker*[arraySize];

    // 复制所有指针（持锁期间 array 与 trackerCount_ 配对一致，oldArray 容量 == arraySize；
    // 两个复制循环等价于复制整个 [0, arraySize)，合并，P2-10）
    SpanTracker** oldArray = trackerArray_.load(std::memory_order_acquire);
    for (size_t i = 0; i < arraySize; ++i)
        newArray[i] = oldArray[i];

    std::sort(newArray, newArray + used,
        [](SpanTracker* a, SpanTracker* b) {
            return a->spanAddr.load(std::memory_order_relaxed) <
                   b->spanAddr.load(std::memory_order_relaxed);
        });

    // 原子交换（写序契约：先 store 数组，后 store sortedCount）
    trackerArray_.store(newArray, std::memory_order_release);
    sortedCount_.store(used, std::memory_order_release);
}

} // namespace memory_pool
} // namespace wevix_muduo
