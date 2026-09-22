#pragma once

#include <vector>
#include <string>
#include <cstddef>
#include "wevix_muduo/Noncopyable.h"

namespace wevix_muduo
{

class Buffer : private Noncopyable
{
public:
    // ---- 常量 ----
    // 预留头部空间。刚好放一个 64 位长度前缀
    static constexpr size_t kCheapPrepend = 8;

    // 初始缓冲区大小。1024 字节是"大多数 HTTP 请求头 + 一小段 body"的大小
    static constexpr size_t kInitialSize = 1024;

    // 构造即分配 kCheapPrepend + initialSize 字节，
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

    // ---- 头部插入 ----
    // 在可读数据之前*插入数据，要求 prependableBytes() >= len。
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
};

} // namespace wevix_muduo
