#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <map>
#include <mutex>
namespace wevix_muduo
{
namespace memory_pool
{

class PageCache
{
public:
    static const size_t PAGE_SIZE = 4096; // 4KB 页大小

    static PageCache& getInstance()
    {
        static PageCache instance;
        return instance;
    }

    // 分配 numPages 页的连续内存（页对齐，已清零）
    void* allocateSpan(size_t numPages);

    // 释放 Span（合并相邻空闲 Span，缓存复用，必要时 munmap）
    void deallocateSpan(void* ptr, size_t numPages);

private:
    PageCache() = default;

    // 通过 mmap 向 OS 申请 numPages 页
    void* systemAlloc(size_t numPages);

    // 缓存页数超过 128MB 阈值时，释放多余 Span 归还 OS
    void releaseExcessSpans();

    // ---- 内存水位线 ----
    static const size_t MAX_CACHED_PAGES = 32768;   // 128MB / 4KB
    size_t cachedPages_{0};                          // 当前 freeSpans_ 中缓存的页数

private:
    // Span 控制块 —— 描述一段连续页内存
    struct Span
    {
        void*  pageAddr; // 起始地址（mmap 返回，页对齐）
        size_t numPages; // 页数
        Span*  next;     // 同页数链表的下一个 Span
    };

    // freeSpans_[页数] → 该页数的空闲 Span 链表头
    // 例：freeSpans_[3] → Span(3页) → Span(3页) → nullptr
    std::map<size_t, Span*> freeSpans_;

    // spanMap_[起始地址] → Span 控制块指针
    // deallocateSpan 时通过地址反查 Span，获取页数等信息
    std::map<void*, Span*> spanMap_;

    // 全局互斥锁：PageCache 操作频率低，一把锁足够
    std::mutex mutex_;
};

} // namespace memory_pool
} // namespace wevix_muduo
