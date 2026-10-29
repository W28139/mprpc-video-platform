#pragma once
#include <cstddef>
#include <atomic>
#include <array>
#include <algorithm>

namespace wevix_muduo {
namespace memory_pool {

constexpr size_t ALIGNMENT = 8;
constexpr size_t MAX_BYTES = 256 * 1024;      // 256KB，超过走 malloc/free
constexpr size_t FREE_LIST_SIZE = MAX_BYTES / ALIGNMENT;  // 32768 槽位

struct BlockHeader {
    size_t size;
    bool   inUse;
    BlockHeader* next;
};

// SizeClass：用户请求字节 → 大小类别索引（O(1) 路由到 freeList_）。
// roundUp(1)→8, roundUp(9)→16, roundUp(256K)→256K

class SizeClass {
public:
    static size_t roundUp(size_t bytes) {
        return (bytes + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    }

    static size_t getIndex(size_t bytes) {
        bytes = std::max(bytes, ALIGNMENT);
        return (bytes + ALIGNMENT - 1) / ALIGNMENT - 1;
    }
};

} // namespace memory_pool
} // namespace wevix_muduo