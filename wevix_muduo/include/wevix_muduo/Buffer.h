#pragma once

#include <vector>
#include <string>
#include <algorithm>
#include <cstddef>
#include "wevix_muduo/Noncopyable.h"

namespace wevix_muduo
{

/**
 * @brief 非阻塞 IO 网络缓冲区
 *
 * ── 为什么不用 std::vector<char> 直接读写？ ──
 *
 *   非阻塞 IO 的核心问题是"数据分批到达"。一次 read 可能只读到一个消息的一半，
 *   下一次 epoll 触发才读到另一半。如果每次把碎片数据直接交给上层，上层必须
 *   自己做拼装。Buffer 就是来扛这个活的。
 *
 * ── 三区内存模型 ──
 *
 *    0              readerIndex_       writerIndex_       buffer_.size()
 *    | prependable  |   readable       |   writable       |
 *    | 可前置数据    |  可读数据(有效)   |   可写空间        |
 *
 *   - prependable：已被消费的旧数据空间。可复用来在前面插入帧头/长度前缀（O(1)）
 *   - readable：已写入但上层还没取走的数据。peek() 读取，retrieve() 消费
 *   - writable：可写入新数据的剩余空间
 *
 * ── 核心设计决策 ──
 *
 *   1. 消费 = 移指针：retrieve() 只移动 readerIndex_，不删数据、不 memmove
 *   2. 写入 = 追加：append() 在尾部追加，不够空间则先整理碎片再扩容
 *   3. readv 散射读：readFd() 用两段 iovec（Buffer + 栈上 64KB 副缓冲），
 *      一次系统调用完成，Buffer 空间足够时零额外拷贝
 *   4. 8 字节预留：kCheapPrepend 让 prepend 操作在大多数情况下是 O(1)
 *
 * ── 生命周期 ──
 *
 *   构造: [8B空][                   1024B 可写                   ]
 *   读入: [8B空][ 200B 可读 ][       824B 可写                   ]
 *   消费: [108B空][ 100B 可读 ][     824B 可写                   ]
 *   整理: [8B空][ 100B 可读 ][            大量可写               ]
 *
 * @see doc/buffer的设计.md 详细设计文档
 */
class Buffer : private Noncopyable
{
public:
    // ---- 常量 ----

    // 预留头部空间。刚好放一个 64 位长度前缀，让 prepend 不需要 memmove
    static constexpr size_t kCheapPrepend = 8;

    // 初始缓冲区大小。1024 字节是"大多数 HTTP 请求头 + 一小段 body"的大小
    static constexpr size_t kInitialSize = 1024;

    // ---- 构造 ----

    // 构造即分配 kCheapPrepend + initialSize 字节，
    // readerIndex_ 和 writerIndex_ 均指向 kCheapPrepend
    explicit Buffer(size_t initialSize = kInitialSize);

    // ---- 容量查询（全部 O(1)）----

    size_t readableBytes() const;       // writerIndex_ - readerIndex_
    size_t writableBytes() const;       // buffer_.size() - writerIndex_
    size_t prependableBytes() const;    // readerIndex_（头部空闲量）
    const char* peek() const;           // 可读区首地址，供上层安全访问

    // ---- 消费（只移游标，不删数据，O(1)）----

    void retrieve(size_t len);                             // 消费 len 字节
    void retrieveAll();                                    // 全部清空
    std::string retrieveAllAsString();                     // 取走全部 + 清空
    std::string retrieveAsString(size_t len);              // 取走 len 字节

    // ---- 写入（从尾部追加）----

    void ensureWritableBytes(size_t len);                  // 保证可写空间 ≥ len
    void append(const char* data, size_t len);             // 追加原始数据
    void append(const std::string& str);                   // 追加 string
    char* beginWrite();                                    // 可写区首地址
    const char* beginWrite() const;

    // ---- 协议辅助 ----

    // 在可读数据中查找 "\r\n"，用于 HTTP 等文本协议的行解析。
    // 返回 nullptr 表示还没收到完整行，上层继续等数据。
    const char* findCRLF() const;

    // 在可读数据**之前**插入数据，要求 prependableBytes() >= len。
    // 常用于先写 payload 再补帧头（如长度前缀）的场景。
    void prepend(const void* data, size_t len);

    // ---- 高性能核心 ----

    // 用 readv 散射读，一次系统调用把内核数据分散读入 Buffer + 栈缓冲。
    // 多数情况下数据直接落入 Buffer，零额外拷贝。
    // savedErrno 保存错误码，供上层决策。
    ssize_t readFd(int fd, int* savedErrno);

private:
    // 返回底层 vector 的 &buffer_[0]
    char* begin();
    const char* begin() const;

    // 碎片整理或扩容：优先 memmove 整理碎片（不分配新内存），
    // 空间不够才 resize 扩容
    void makeSpace(size_t len);

private:
    std::vector<char> buffer_;       // 实际的连续内存
    size_t readerIndex_;             // 可读区起始位置 = 已消费字节数
    size_t writerIndex_;             // 可写区起始位置 = 已写入字节数

    static const char kCRLF[];       // "\r\n"
};

} // namespace wevix_muduo
