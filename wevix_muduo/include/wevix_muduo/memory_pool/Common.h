#pragma once
#include <cstddef>
#include <atomic>
#include <array>

// ============================================================================
// Common.h —— 内存池全局基础定义
// ============================================================================

namespace wevix_muduo {
namespace memory_pool {

// ---- 全局常量 ----
constexpr size_t ALIGNMENT = 8;
constexpr size_t MAX_BYTES = 256 * 1024; // 256KB
constexpr size_t FREE_LIST_SIZE = MAX_BYTES / ALIGNMENT;

// ---- 内存块头部（预留，当前未使用）----
struct BlockHeader
{
    size_t size;
    bool   inUse;
    BlockHeader* next;
};

// ============================================================================
// SizeClass —— 大小类别映射
// ============================================================================
class SizeClass
{
public:
    static size_t roundUp(size_t bytes)
    {
        return (bytes + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    }

    static size_t getIndex(size_t bytes)
    {
        bytes = std::max(bytes, ALIGNMENT);
        return (bytes + ALIGNMENT - 1) / ALIGNMENT - 1;
    }
};

} // namespace memory_pool
} // namespace wevix_muduo
