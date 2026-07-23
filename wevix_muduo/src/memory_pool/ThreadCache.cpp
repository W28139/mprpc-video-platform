#include "wevix_muduo/memory_pool/ThreadCache.h"
#include "wevix_muduo/memory_pool/CentralCache.h"
#include "wevix_muduo/AsyncLogger.h"

namespace wevix_muduo {
namespace memory_pool {

void* ThreadCache::allocate(size_t size)
{
    if (size == 0)
    {
        size = ALIGNMENT;
    }

    if (size > MAX_BYTES)
    {
        return malloc(size);
    }

    size_t index = SizeClass::getIndex(size);

    if (void* ptr = free_list_[index])
    {
        free_list_[index] = *reinterpret_cast<void**>(ptr);
        free_list_size_[index]--;
        return ptr;
    }

    return fetchFromCentralCache(index);
}

void ThreadCache::deallocate(void* ptr, size_t size)
{
    // C++ 标准允许 delete nullptr 为合法的空操作
    if (!ptr) return;

    if (size > MAX_BYTES)
    {
        free(ptr);
        return;
    }

    size_t index = SizeClass::getIndex(size);

    *reinterpret_cast<void**>(ptr) = free_list_[index];
    free_list_[index] = ptr;

    free_list_size_[index]++;

    if (shouldReturnToCentralCache(index))
    {
        returnToCentralCache(free_list_[index], size);
    }
}

bool ThreadCache::shouldReturnToCentralCache(size_t index)
{
    size_t threshold = 256;
    return (free_list_size_[index] > threshold);
}

void* ThreadCache::fetchFromCentralCache(size_t index)
{
    void* start = CentralCache::getInstance().fetchRange(index);
    if (!start)
    {
        LOG_ERROR("ThreadCache::fetchFromCentralCache failed, index=%zu", index);
        return nullptr;
    }

    void* result = start;
    free_list_[index] = *reinterpret_cast<void**>(start);

    size_t batchNum = 0;
    void* current = start;
    while (current != nullptr)
    {
        batchNum++;
        current = *reinterpret_cast<void**>(current);
    }

    free_list_size_[index] += batchNum - 1;

    return result;
}

void ThreadCache::returnToCentralCache(void* start, size_t size)
{
    size_t index = SizeClass::getIndex(size);
    size_t alignedSize = SizeClass::roundUp(size);

    size_t batchNum = free_list_size_[index];
    if (batchNum <= 1) return;

    size_t keepNum = std::max(batchNum / 4, size_t(1));

    char* splitNode = static_cast<char*>(start);
    size_t actualKeep = 1;
    for (size_t i = 0; i < keepNum - 1; ++i)
    {
        void* next = *reinterpret_cast<void**>(splitNode);
        if (next == nullptr)
            break;
        splitNode = static_cast<char*>(next);
        actualKeep++;
    }

    void* nextNode = *reinterpret_cast<void**>(splitNode);
    *reinterpret_cast<void**>(splitNode) = nullptr;

    size_t actualReturn = (batchNum > actualKeep) ? (batchNum - actualKeep) : 0;

    free_list_[index] = start;
    free_list_size_[index] = actualKeep;

    if (actualReturn > 0 && nextNode != nullptr)
    {
        CentralCache::getInstance().returnRange(nextNode, actualReturn * alignedSize, index);
    }
}

} // namespace memory_pool
} // namespace wevix_muduo
