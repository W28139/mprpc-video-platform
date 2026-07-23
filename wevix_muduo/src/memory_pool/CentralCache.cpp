#include "wevix_muduo/memory_pool/CentralCache.h"
#include "wevix_muduo/memory_pool/PageCache.h"
#include "wevix_muduo/AsyncLogger.h"
#include <cassert>
#include <thread>
#include <chrono>
#include <algorithm>

namespace wevix_muduo {
namespace memory_pool {

const std::chrono::milliseconds CentralCache::DELAY_INTERVAL{1000};
static const size_t SPAN_PAGES = 8;

CentralCache::CentralCache()
{
    for (auto& ptr : central_free_list_)
    {
        ptr.store(nullptr, std::memory_order_relaxed);
    }

    for (auto& lock : locks_)
    {
        lock.clear();
    }

    for (auto& count : delay_counts_)
    {
        count.store(0, std::memory_order_relaxed);
    }
    for (auto& time : last_return_times_)
    {
        time = std::chrono::steady_clock::now();
    }

    span_count_.store(0, std::memory_order_relaxed);
    SpanTracker** initialArray = new SpanTracker*[TRACKER_INITIAL_CAPACITY];
    for (size_t i = 0; i < TRACKER_INITIAL_CAPACITY; ++i)
    {
        tracker_storage_.emplace_back();
        initialArray[i] = &tracker_storage_.back();
    }
    tracker_array_.store(initialArray, std::memory_order_release);
    tracker_count_.store(TRACKER_INITIAL_CAPACITY, std::memory_order_release);
}

void* CentralCache::fetchRange(size_t index)
{
    if (index >= FREE_LIST_SIZE)
        return nullptr;

    while (locks_[index].test_and_set(std::memory_order_acquire))
    {
        std::this_thread::yield();
    }

    void* result = nullptr;

    try
    {
        result = central_free_list_[index].load(std::memory_order_relaxed);

        if (!result)
        {
            // Branch A: CentralCache is empty → allocate new Span from PageCache
            size_t size = (index + 1) * ALIGNMENT;

            result = fetchFromPageCache(size);
            if (!result)
            {
                LOG_ERROR("CentralCache::fetchRange fetchFromPageCache failed, size=%zu, index=%zu", size, index);
                locks_[index].clear(std::memory_order_release);
                return nullptr;
            }

            char* start = static_cast<char*>(result);

            size_t numPages = (size <= SPAN_PAGES * PageCache::PAGE_SIZE) ?
                                     SPAN_PAGES :
                                     (size + PageCache::PAGE_SIZE - 1) / PageCache::PAGE_SIZE;

            size_t blockNum = (numPages * PageCache::PAGE_SIZE) / size;

            if (blockNum > 1)
            {
                for (size_t i = 1; i < blockNum; ++i)
                {
                    void* current = start + (i - 1) * size;
                    void* next    = start + i * size;
                    *reinterpret_cast<void**>(current) = next;
                }
                *reinterpret_cast<void**>(start + (blockNum - 1) * size) = nullptr;

                size_t batchCount = 1;
                void* batchTail = result;
                while (batchCount < BATCH_SIZE && batchCount < blockNum &&
                       *reinterpret_cast<void**>(batchTail) != nullptr)
                {
                    batchTail = *reinterpret_cast<void**>(batchTail);
                    batchCount++;
                }
                void* rest = *reinterpret_cast<void**>(batchTail);
                *reinterpret_cast<void**>(batchTail) = nullptr;

                central_free_list_[index].store(rest, std::memory_order_release);

                size_t trackerIndex = span_count_++;

                if (trackerIndex >= tracker_count_.load(std::memory_order_relaxed))
                    expandTrackerArray(trackerIndex);

                SpanTracker** array = tracker_array_.load(std::memory_order_acquire);
                SpanTracker* tracker = array[trackerIndex];

                tracker->span_addr.store(start, std::memory_order_release);
                tracker->num_pages.store(numPages, std::memory_order_release);
                tracker->block_count.store(blockNum, std::memory_order_release);
                tracker->free_count.store(blockNum - batchCount, std::memory_order_release);

                size_t total = span_count_.load(std::memory_order_relaxed);
                size_t sorted = sorted_count_.load(std::memory_order_relaxed);
                if (total > sorted && (total - sorted) > SORT_THRESHOLD)
                    ensureSorted();
            }
            else
            {
                // blockNum == 1: large object, still need SpanTracker
                size_t trackerIndex = span_count_++;

                if (trackerIndex >= tracker_count_.load(std::memory_order_relaxed))
                    expandTrackerArray(trackerIndex);

                SpanTracker** array = tracker_array_.load(std::memory_order_acquire);
                SpanTracker* tracker = array[trackerIndex];

                tracker->span_addr.store(start, std::memory_order_release);
                tracker->num_pages.store(numPages, std::memory_order_release);
                tracker->block_count.store(1, std::memory_order_release);
                tracker->free_count.store(0, std::memory_order_release);

                size_t total = span_count_.load(std::memory_order_relaxed);
                size_t sorted = sorted_count_.load(std::memory_order_relaxed);
                if (total > sorted && (total - sorted) > SORT_THRESHOLD)
                    ensureSorted();
            }
        }
        else
        {
            // Branch B: CentralCache has blocks → batch fetch
            size_t batchCount = 1;
            void* batchTail = result;
            while (batchCount < BATCH_SIZE &&
                   *reinterpret_cast<void**>(batchTail) != nullptr)
            {
                batchTail = *reinterpret_cast<void**>(batchTail);
                batchCount++;
            }

            void* rest = *reinterpret_cast<void**>(batchTail);
            *reinterpret_cast<void**>(batchTail) = nullptr;
            central_free_list_[index].store(rest, std::memory_order_release);

            void* current = result;
            for (size_t i = 0; i < batchCount; ++i)
            {
                SpanTracker* tracker = getSpanTracker(current);
                if (tracker)
                    tracker->free_count.fetch_sub(1, std::memory_order_release);
                current = *reinterpret_cast<void**>(current);
            }
        }
    }
    catch (...)
    {
        locks_[index].clear(std::memory_order_release);
        throw;
    }

    locks_[index].clear(std::memory_order_release);
    return result;
}

void CentralCache::returnRange(void* start, size_t size, size_t index)
{
    if (!start || index >= FREE_LIST_SIZE)
        return;

    size_t blockSize = (index + 1) * ALIGNMENT;
    size_t blockCount = size / blockSize;

    while (locks_[index].test_and_set(std::memory_order_acquire))
    {
        std::this_thread::yield();
    }

    try
    {
        void* end = start;
        size_t count = 1;
        while (*reinterpret_cast<void**>(end) != nullptr && count < blockCount)
        {
            end = *reinterpret_cast<void**>(end);
            count++;
        }

        void* current = central_free_list_[index].load(std::memory_order_relaxed);
        *reinterpret_cast<void**>(end) = current;
        central_free_list_[index].store(start, std::memory_order_release);

        size_t currentCount = delay_counts_[index].fetch_add(1, std::memory_order_relaxed) + 1;
        auto currentTime = std::chrono::steady_clock::now();

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
    if (currentCount >= MAX_DELAY_COUNT)
        return true;

    auto lastTime = last_return_times_[index];
    return (currentTime - lastTime) >= DELAY_INTERVAL;
}

void CentralCache::performDelayedReturn(size_t index)
{
    delay_counts_[index].store(0, std::memory_order_relaxed);
    last_return_times_[index] = std::chrono::steady_clock::now();

    // Pass 1: traverse free list, accumulate scan_count on SpanTrackers
    void* currentBlock = central_free_list_[index].load(std::memory_order_relaxed);
    while (currentBlock)
    {
        SpanTracker* tracker = getSpanTracker(currentBlock);
        if (tracker)
        {
            tracker->scan_count.fetch_add(1, std::memory_order_relaxed);
        }
        currentBlock = *reinterpret_cast<void**>(currentBlock);
    }

    // Pass 2: traverse all SpanTrackers, check full-free spans
    SpanTracker** array = tracker_array_.load(std::memory_order_acquire);
    size_t total = span_count_.load(std::memory_order_relaxed);

    for (size_t i = 0; i < total; ++i)
    {
        SpanTracker* tracker = array[i];
        size_t count = tracker->scan_count.exchange(0, std::memory_order_relaxed);
        if (count > 0)
        {
            updateSpanFreeCount(tracker, count, index);
        }
    }
}

void CentralCache::updateSpanFreeCount(SpanTracker* tracker, size_t freeBlocksInList, size_t index)
{
    tracker->free_count.store(freeBlocksInList, std::memory_order_release);

    if (freeBlocksInList == tracker->block_count.load(std::memory_order_relaxed))
    {
        void* spanAddr = tracker->span_addr.load(std::memory_order_relaxed);
        size_t numPages = tracker->num_pages.load(std::memory_order_relaxed);

        // Remove all blocks belonging to this Span from central_free_list_
        void* head = central_free_list_[index].load(std::memory_order_relaxed);
        void* newHead = nullptr;
        void* prev = nullptr;
        void* current = head;

        while (current)
        {
            void* next = *reinterpret_cast<void**>(current);

            if (current >= spanAddr &&
                current < static_cast<char*>(spanAddr) + numPages * PageCache::PAGE_SIZE)
            {
                if (prev)
                    *reinterpret_cast<void**>(prev) = next;
                else
                    newHead = next;
            }
            else
            {
                prev = current;
            }
            current = next;
        }

        central_free_list_[index].store(newHead, std::memory_order_release);

        PageCache::getInstance().deallocateSpan(spanAddr, numPages);
    }
}

void* CentralCache::fetchFromPageCache(size_t size)
{
    size_t numPages = (size + PageCache::PAGE_SIZE - 1) / PageCache::PAGE_SIZE;

    if (size <= SPAN_PAGES * PageCache::PAGE_SIZE)
    {
        return PageCache::getInstance().allocateSpan(SPAN_PAGES);
    }
    else
    {
        return PageCache::getInstance().allocateSpan(numPages);
    }
}

SpanTracker* CentralCache::getSpanTracker(void* blockAddr)
{
    SpanTracker** array = tracker_array_.load(std::memory_order_acquire);
    size_t sorted = sorted_count_.load(std::memory_order_acquire);
    size_t total = span_count_.load(std::memory_order_relaxed);

    // Phase 1: binary search in sorted portion O(log n)
    size_t left = 0, right = sorted;
    while (left < right)
    {
        size_t mid = left + (right - left) / 2;
        SpanTracker* t = array[mid];
        void* addr = t->span_addr.load(std::memory_order_relaxed);

        if (blockAddr < addr)
        {
            right = mid;
        }
        else if (blockAddr >= static_cast<char*>(addr) +
                 t->num_pages.load(std::memory_order_relaxed) * PageCache::PAGE_SIZE)
        {
            left = mid + 1;
        }
        else
        {
            return t;
        }
    }

    // Phase 2: linear scan in unsorted tail O(SORT_THRESHOLD)
    for (size_t i = sorted; i < total; ++i)
    {
        SpanTracker* t = array[i];
        void* addr = t->span_addr.load(std::memory_order_relaxed);
        size_t pages = t->num_pages.load(std::memory_order_relaxed);

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
    std::lock_guard<std::mutex> lock(tracker_expand_mutex_);

    if (requiredIndex < tracker_count_.load(std::memory_order_relaxed))
        return;

    size_t oldSize = tracker_count_.load(std::memory_order_relaxed);
    size_t newSize = std::max(oldSize + TRACKER_EXPAND_SIZE, requiredIndex + 1);

    SpanTracker** newArray = new SpanTracker*[newSize];

    SpanTracker** oldArray = tracker_array_.load(std::memory_order_acquire);
    for (size_t i = 0; i < oldSize; ++i)
        newArray[i] = oldArray[i];

    for (size_t i = oldSize; i < newSize; ++i)
    {
        tracker_storage_.emplace_back();
        newArray[i] = &tracker_storage_.back();
    }

    std::sort(newArray, newArray + newSize,
        [](SpanTracker* a, SpanTracker* b) {
            return a->span_addr.load(std::memory_order_relaxed) <
                   b->span_addr.load(std::memory_order_relaxed);
        });

    tracker_array_.store(newArray, std::memory_order_release);
    tracker_count_.store(newSize, std::memory_order_release);
    sorted_count_.store(newSize, std::memory_order_release);
}

void CentralCache::ensureSorted()
{
    size_t total = span_count_.load(std::memory_order_relaxed);
    size_t sorted = sorted_count_.load(std::memory_order_relaxed);

    if (total - sorted <= SORT_THRESHOLD)
        return;

    std::lock_guard<std::mutex> lock(tracker_expand_mutex_);

    total = span_count_.load(std::memory_order_relaxed);
    sorted = sorted_count_.load(std::memory_order_relaxed);
    if (total - sorted <= SORT_THRESHOLD)
        return;

    size_t arraySize = tracker_count_.load(std::memory_order_relaxed);

    SpanTracker** newArray = new SpanTracker*[arraySize];

    SpanTracker** oldArray = tracker_array_.load(std::memory_order_acquire);
    for (size_t i = 0; i < total; ++i)
        newArray[i] = oldArray[i];
    for (size_t i = total; i < arraySize; ++i)
        newArray[i] = oldArray[i];

    std::sort(newArray, newArray + total,
        [](SpanTracker* a, SpanTracker* b) {
            return a->span_addr.load(std::memory_order_relaxed) <
                   b->span_addr.load(std::memory_order_relaxed);
        });

    tracker_array_.store(newArray, std::memory_order_release);
    sorted_count_.store(total, std::memory_order_release);
}

} // namespace memory_pool
} // namespace wevix_muduo
