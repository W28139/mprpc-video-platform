#pragma once
#include "wevix_muduo/memory_pool/Common.h"
#include <map>
#include <mutex>

// ============================================================================
// PageCache —— 页缓存层（三层架构的第三层，直接与 OS 交互）
// ============================================================================
//
// 以"页"（PAGE_SIZE = 4096B）为单位管理连续内存。
// 这是整个内存池中唯一调用 mmap/munmap 的地方。
//
// 核心数据结构：
//   Span — 一段连续页内存的控制块
//     pageAddr  起始地址（页对齐）
//     numPages  包含的页数
//     next      同一页数链表的下一个 Span
//
//   freeSpans_  — map<页数, Span*>  按页数索引的空闲 Span 链表
//   spanMap_    — map<地址, Span*>   地址→Span 反查（用于释放时定位）
//
// 核心操作：
//   allocateSpan(n)    — 分配 n 页（Best-Fit + 切分 + 必要时 mmap）
//   deallocateSpan(p)  — 释放 Span（后向合并 + 缓存 + 超水位 munmap）
//
// 设计要点：
//   1. 使用 std::mutex 而非自旋锁 —— PageCache 操作包含 mmap（可能毫秒级），
//      让等待线程在内核态挂起比用户态空转更高效
//   2. Span 控制块用 new/delete 分配在堆上（不在 mmap 区域内），
//      数量通常 < 10000，开销可忽略
//   3. 内存水位线 MAX_CACHED_PAGES = 128MB，超出后从大 Span 开始 munmap 归还 OS

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
