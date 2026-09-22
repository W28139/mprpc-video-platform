#pragma once
#include <cstddef>
#include <atomic>
#include <array>
#include <algorithm>   // std::max（SizeClass::getIndex）

namespace wevix_muduo
{
namespace memory_pool
{

// ---- 全局常量 ----

// 对齐粒度 = sizeof(void*) = 8 字节
// 所有分配的内存地址都 8 字节对齐，且最小分配 8 字节
constexpr size_t ALIGNMENT = 8;

// 内存池管理的最大单次分配 = 256KB
// 超过此值直接走 malloc/free，不进入三层架构
constexpr size_t MAX_BYTES = 256 * 1024; // 256KB

// 自由链表槽位数 = 256KB / 8B = 32768
// 即 ThreadCache 和 CentralCache 各维护 32768 个链表头
//   槽位 0   → 8B
//   槽位 1   → 16B
//   槽位 2   → 24B (向上取整)
//   ...
//   槽位 32767 → 256KB
constexpr size_t FREE_LIST_SIZE = MAX_BYTES / ALIGNMENT;

// ---- 内存块头部（预留，当前未使用）----
// 如果将来需要块内嵌元数据（如 size、magic），可启用此结构
struct BlockHeader
{
    size_t size;       // 内存块大小
    bool   inUse;      // 使用标志
    BlockHeader* next; // 指向下一个内存块
};

// ============================================================================
// SizeClass —— 大小类别映射
// ============================================================================
//
// 将用户请求的任意字节数映射到一个"大小类别槽位"。
// 这是整个内存池 O(1) 路由的基础 —— 无论请求多大，一步算出 index，
// 直接访问对应的 freeList_。
//
// 向上取整规则：
//   roundUp(1)  = 8
//   roundUp(8)  = 8
//   roundUp(9)  = 16
//   roundUp(24) = 24 → 但仍会被 getIndex 映射到 index=2 (对应实际块大小 24B)
//
//   实际上项目内部统一按 (index+1)*ALIGNMENT 反算块大小，
//   所以 9~16B 都走 index=1 (实际分配 16B)，17~24B 走 index=2 (24B)。

class SizeClass
{
public:
    // 向上取整到 ALIGNMENT 的倍数
    // 例：1→8, 8→8, 9→16, 256K→256K
    static size_t roundUp(size_t bytes)
    {
        return (bytes + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    }

    // 获取大小类别索引 (0 ~ FREE_LIST_SIZE-1)
    // 例：bytes=8 → (8+7)/8-1 = 0
    //     bytes=9 → max(9,8)=9 → (9+7)/8-1 = 1 (16B 类)
    static size_t getIndex(size_t bytes)
    {
        bytes = std::max(bytes, ALIGNMENT); // 至少 8 字节
        return (bytes + ALIGNMENT - 1) / ALIGNMENT - 1;
    }
};

} // namespace memory_pool
} // namespace wevix_muduo
