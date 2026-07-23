#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <mutex>
#include <deque>
#include <array>
#include <atomic>
#include <chrono>

namespace wevix_muduo {
namespace memory_pool {

struct SpanTracker {
    std::atomic<void*> span_addr{nullptr};
    std::atomic<size_t> num_pages{0};
    std::atomic<size_t> block_count{0};
    std::atomic<size_t> free_count{0};
    std::atomic<size_t> scan_count{0};
};

class CentralCache
{
public:
    static CentralCache& getInstance()
    {
        static CentralCache instance;
        return instance;
    }

    void* fetchRange(size_t index);
    void returnRange(void* start, size_t size, size_t index);

private:
    CentralCache();
    void* fetchFromPageCache(size_t size);
    SpanTracker* getSpanTracker(void* blockAddr);
    void updateSpanFreeCount(SpanTracker* tracker, size_t freeBlocksInList, size_t index);
    void expandTrackerArray(size_t requiredIndex);
    void ensureSorted();
    bool shouldPerformDelayedReturn(size_t index, size_t currentCount,
        std::chrono::steady_clock::time_point currentTime);
    void performDelayedReturn(size_t index);

private:
    std::array<std::atomic<void*>, FREE_LIST_SIZE> central_free_list_;
    std::array<std::atomic_flag, FREE_LIST_SIZE> locks_;

    std::deque<SpanTracker> tracker_storage_;
    std::atomic<SpanTracker**> tracker_array_{nullptr};
    std::atomic<size_t> tracker_count_{0};
    std::atomic<size_t> span_count_{0};
    std::atomic<size_t> sorted_count_{0};
    std::mutex tracker_expand_mutex_;

    static const size_t TRACKER_INITIAL_CAPACITY = 1024;
    static const size_t TRACKER_EXPAND_SIZE = 256;
    static const size_t BATCH_SIZE = 8;
    static const size_t SORT_THRESHOLD = 64;

    static const size_t MAX_DELAY_COUNT = 48;
    std::array<std::atomic<size_t>, FREE_LIST_SIZE> delay_counts_;
    std::array<std::chrono::steady_clock::time_point, FREE_LIST_SIZE> last_return_times_;
    static const std::chrono::milliseconds DELAY_INTERVAL;
};

} // namespace memory_pool
} // namespace wevix_muduo
