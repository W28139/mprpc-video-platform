#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <map>
#include <mutex>

namespace wevix_muduo {
namespace memory_pool {

class PageCache
{
public:
    static const size_t PAGE_SIZE = 4096;

    static PageCache& getInstance()
    {
        static PageCache instance;
        return instance;
    }

    void* allocateSpan(size_t numPages);
    void deallocateSpan(void* ptr, size_t numPages);

private:
    PageCache() = default;
    void* systemAlloc(size_t numPages);
    void releaseExcessSpans();

    static const size_t MAX_CACHED_PAGES = 32768; // 128MB
    size_t cached_pages_{0};

private:
    struct Span
    {
        void*  page_addr;
        size_t num_pages;
        Span*  next;
    };

    std::map<size_t, Span*> free_spans_;
    std::map<void*, Span*> span_map_;
    std::mutex mutex_;
};

} // namespace memory_pool
} // namespace wevix_muduo
