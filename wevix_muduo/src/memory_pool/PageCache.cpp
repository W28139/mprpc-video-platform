#include "wevix_muduo/memory_pool/PageCache.h"
#include "wevix_muduo/AsyncLogger.h"
#include <sys/mman.h>
#include <cstring>

namespace wevix_muduo {
namespace memory_pool {

void* PageCache::allocateSpan(size_t numPages)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = free_spans_.lower_bound(numPages);

    if (it != free_spans_.end())
    {
        Span* span = it->second;

        cached_pages_ -= span->num_pages;

        if (span->next)
        {
            free_spans_[it->first] = span->next;
        }
        else
        {
            free_spans_.erase(it);
        }

        if (span->num_pages > numPages)
        {
            Span* newSpan = new Span;
            newSpan->page_addr = static_cast<char*>(span->page_addr)
                                + numPages * PAGE_SIZE;
            newSpan->num_pages = span->num_pages - numPages;
            newSpan->next = nullptr;

            auto& list = free_spans_[newSpan->num_pages];
            newSpan->next = list;
            list = newSpan;

            cached_pages_ += newSpan->num_pages;
            span->num_pages = numPages;
        }

        span_map_[span->page_addr] = span;
        return span->page_addr;
    }

    void* memory = systemAlloc(numPages);
    if (!memory)
    {
        LOG_ERROR("PageCache::allocateSpan systemAlloc failed, numPages=%zu", numPages);
        return nullptr;
    }

    Span* span = new Span;
    span->page_addr = memory;
    span->num_pages = numPages;
    span->next = nullptr;

    span_map_[memory] = span;
    return memory;
}

void PageCache::deallocateSpan(void* ptr, size_t numPages)
{
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = span_map_.find(ptr);
    if (it == span_map_.end()) return;

    Span* span = it->second;

    // Backward merge
    void* nextAddr = static_cast<char*>(ptr) + numPages * PAGE_SIZE;
    auto nextIt = span_map_.find(nextAddr);

    if (nextIt != span_map_.end())
    {
        Span* nextSpan = nextIt->second;

        bool found = false;
        auto& nextList = free_spans_[nextSpan->num_pages];

        if (nextList == nextSpan)
        {
            nextList = nextSpan->next;
            found = true;
        }
        else if (nextList)
        {
            Span* prev = nextList;
            while (prev->next)
            {
                if (prev->next == nextSpan)
                {
                    prev->next = nextSpan->next;
                    found = true;
                    break;
                }
                prev = prev->next;
            }
        }

        if (found)
        {
            span->num_pages += nextSpan->num_pages;
            span_map_.erase(nextAddr);
            delete nextSpan;
        }
    }

    auto& list = free_spans_[span->num_pages];
    span->next = list;
    list = span;

    cached_pages_ += span->num_pages;

    if (cached_pages_ > MAX_CACHED_PAGES)
    {
        releaseExcessSpans();
    }
}

void PageCache::releaseExcessSpans()
{
    size_t targetPages = MAX_CACHED_PAGES / 2;

    for (auto it = free_spans_.rbegin();
         it != free_spans_.rend() && cached_pages_ > targetPages; )
    {
        Span* span = it->second;
        while (span && cached_pages_ > targetPages)
        {
            Span* next = span->next;

            munmap(span->page_addr, span->num_pages * PAGE_SIZE);

            span_map_.erase(span->page_addr);
            cached_pages_ -= span->num_pages;
            delete span;

            span = next;
        }

        it->second = span;

        if (span == nullptr)
        {
            it = std::map<size_t, Span*>::reverse_iterator(
                free_spans_.erase(std::next(it).base()));
        }
        else
        {
            ++it;
        }
    }
}

void* PageCache::systemAlloc(size_t numPages)
{
    size_t size = numPages * PAGE_SIZE;

    void* ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    if (ptr == MAP_FAILED)
    {
        LOG_ERROR("PageCache::systemAlloc mmap failed, size=%zu, errno=%d", size, errno);
        return nullptr;
    }

    memset(ptr, 0, size);
    return ptr;
}

} // namespace memory_pool
} // namespace wevix_muduo
