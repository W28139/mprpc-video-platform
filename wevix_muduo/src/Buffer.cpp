#include "wevix_muduo/Buffer.h"
#include "wevix_muduo/AsyncLogger.h"

#include <errno.h>
#include <sys/uio.h>    // readv, struct iovec
#include <unistd.h>      // read
#include <cstring>

namespace wevix_muduo
{

const char Buffer::kCRLF[] = "\r\n";

// =========================================================================
// 构造
// =========================================================================
//
// 初始状态：vector 实际分配 kCheapPrepend + initialSize 字节，
// reader 和 writer 都指向 kCheapPrepend，即前面预留 8 字节、后面全是可写空间。
//
//    [________8B________][____________1024B____________]
//                        ↑
//                  readerIndex_ = writerIndex_ = 8

Buffer::Buffer(size_t initialSize)
    : buffer_(kCheapPrepend + initialSize)
    , readerIndex_(kCheapPrepend)
    , writerIndex_(kCheapPrepend)
{
}

// =========================================================================
// 容量查询 —— 全部 O(1)，只做减法
// =========================================================================

size_t Buffer::readableBytes() const
{
    // 可读 = 已写入但尚未被上层消费的部分
    return writerIndex_ - readerIndex_;
}

size_t Buffer::writableBytes() const
{
    // 可写 = vector 尾部到 writerIndex_ 之间的剩余空间
    return buffer_.size() - writerIndex_;
}

size_t Buffer::prependableBytes() const
{
    // 头部空闲 = readerIndex_ 之前的所有字节（已被消费的旧数据空间）
    return readerIndex_;
}

const char* Buffer::peek() const
{
    // 可读数据的起始地址。上层不直接 &buffer_[readerIndex_]，
    // 而是通过 peek() 获取，解耦内部存储细节。
    return begin() + readerIndex_;
}

// =========================================================================
// 消费数据 —— 只移游标，不删内容
// =========================================================================
//
// 核心思想：消费 = 移动 readerIndex_，被"消费"的数据变成 prependable 空间，
// 后续 prepend 或 makeSpace 可复用。这就是为什么是非阻塞 IO 高效缓冲区的关键——
// 不会每次消费都触发 memmove。

void Buffer::retrieve(size_t len)
{
    if (len < readableBytes())
        readerIndex_ += len;    // 消费一部分，只前移 readerIndex_
    else
        retrieveAll();          // 消费量 ≥ 已有量 → 全清，重置到初始状态
}

void Buffer::retrieveAll()
{
    // 重置为构造时的状态：前面 8 字节预留，其余全部可写
    readerIndex_ = kCheapPrepend;
    writerIndex_ = kCheapPrepend;
}

// 唯一一处返回上层数据的接口，上层调用，拿到buffer_的所以可读字符串（可以不传len,代表全读完）
std::string Buffer::retrieveAllAsString()
{
    return retrieveAsString(readableBytes());
}
std::string Buffer::retrieveAsString(size_t len)
{
    // peek() 返回可读区首地址 → std::string 构造拷贝 len 字节
    // 从peek()字节开始，读取len长的数据到result
    std::string result(peek(), len);
    // 读完以后需要更新指针 readerIndex_ -》 readerIndex_+len 
    retrieve(len);
    return result;
}

// =========================================================================
// 写入数据 —— append 系列
// =========================================================================

void Buffer::ensureWritableBytes(size_t len)
{
    // 可写空间不够 → makeSpace（先整理碎片，再扩容）
    if (writableBytes() < len)
        makeSpace(len);
}

void Buffer::append(const char* data, size_t len)
{
    // 经典写入流程：确保空间 → 拷贝数据 → 前移 writerIndex_
    ensureWritableBytes(len);
    std::copy(data, data + len, beginWrite());
    writerIndex_ += len;
}

void Buffer::append(const std::string& str)
{
    append(str.data(), str.size());
}

char* Buffer::beginWrite()
{
    // 可写区的起始地址。当层用它作为 vec[0].iov_base 直接传进内核。
    return begin() + writerIndex_;
}

const char* Buffer::beginWrite() const
{
    return begin() + writerIndex_;
}

// =========================================================================
// 协议辅助
// =========================================================================

const char* Buffer::findCRLF() const
{
    // 在 [peek, beginWrite) 范围内查找子序列 "\r\n"
    // std::search 是 STL 的子序列查找算法（不是 KMP，性能足够用）
    const char* crlf = std::search(peek(), beginWrite(), kCRLF, kCRLF + 2);
    // 返回 nullptr 表示"还没收到完整行"，上层需等待下次数据到达
    return crlf == beginWrite() ? nullptr : crlf;
}

void Buffer::prepend(const void* data, size_t len)
{
    // 在可读数据**之前**塞入 len 字节。
    //
    // 例：发送自定义协议消息，先写 body，最后才知道 length：
    //   buf.append(body);
    //   buf.prepend(&lengthHeader, 4);  // 在 body 前面插入 4 字节长度头
    //
    // 前提：prependableBytes() >= len，即前面的空位够用。
    // 如果不够（比如连续 prepend 多次），需要上层先确保空间。
    readerIndex_ -= len;
    const char* d = static_cast<const char*>(data);
    std::copy(d, d + len, begin() + readerIndex_);
}

// ========================================================================================================
// readFd —— readv 散射读，整个 Buffer 的性能核心———— 上层调用，用于读取客户端的消息（读取到buffer_里
// ========================================================================================================
//
// 为什么不用 read()？
//   read(fd, buf, N) 只能写一块连续内存。如果 Buffer 只剩 100 字节可写，
//   你只能读到 100 字节——即使内核缓冲区里有 4KB 数据。
//   而一次 epoll_wait 触发后如果不读完，边缘触发下不会再触发。
//
// 为什么是 readv？
//   readv 一次系统调用可以把数据"散射"到多块不连续内存：
//     vec[0] → Buffer 的可写区（尽可能利用）
//     vec[1] → 栈上 65536 字节 extrabuf（兜底，防止 Buffer 空间不够时丢数据）
//
//   大多数情况下 Buffer 可写区已经 ≥64KB（随着读写持续增长），
//   此时只用 vec[0] 一个段，readv 退化为 read，零额外开销。

ssize_t Buffer::readFd(int fd, int* savedErrno)
{
    // 栈上 64KB 副缓冲。不 new、不 malloc，完全在栈上，函数返回自动回收。
    char extrabuf[65536];
    struct iovec vec[2];

    // 能写入的空闲区
    const size_t writable = writableBytes();

    // 第 1 段：Buffer 自身可写区
    vec[0].iov_base = beginWrite();
    vec[0].iov_len  = writable;

    // 第 2 段：栈上缓冲，兜底用。只有 Buffer 可写区不够 64KB 时才启用。
    vec[1].iov_base = extrabuf;
    vec[1].iov_len  = sizeof(extrabuf);

    // Buffer 可写区 ≥64KB 时，只用第 1 段：
    //   数据全部直接落入 Buffer，连栈拷贝都不需要。
    // Buffer 可写区 <64KB 时，用两段：
    //   前 writable 字节进 Buffer，溢出部分暂存 extrabuf。
    const int iovcnt = (writable < sizeof(extrabuf)) ? 2 : 1;
    // 这开始真正的写入（覆盖原数据）
    const ssize_t n = ::readv(fd, vec, iovcnt);

    if (n < 0)
    {
        // 读出错。不在此处理，保存 errno 让上层决策（重试 or 关闭连接）。
        *savedErrno = errno;
    }
    else if (static_cast<size_t>(n) <= writable)
    {
        // 情况 A：数据全部落入 Buffer 的第 1 段
        // 路径：内核 → Buffer（零额外拷贝）
        writerIndex_ += n;
    }
    else
    {
        // 情况 B：Buffer 满了，剩余数据溢出到了 extrabuf
        //
        // 数据分布：
        //   Buffer 可写区 → 前 writable 字节（Buffer 满了）
        //   extrabuf      → 后 (n - writable) 字节
        //
        // 处理步骤：
        //   1. writerIndex_ 推到末尾（标记可写区已满）
        //   2. append 溢出的数据 → 自动触发 Buffer 扩容 + 拷贝
        //
        writerIndex_ = buffer_.size();
        // 溢出后只拷贝 (n - writable) 字节，而不是整个 n 字节。
        append(extrabuf, n - writable);
    }

    return n;
}

// =========================================================================
// makeSpace —— 碎片整理与扩容
// =========================================================================
//
// 触发时机：append / readFd 发现 writableBytes() < len
//
// 策略选择（二选一）：
//
//   A. 整理碎片（优先）
//      条件：prependable + writable ≥ len + kCheapPrepend
//      含义：总空闲空间够用，只是碎片化了（前面有被消费的空洞）。
//      做法：把可读数据 memmove 到 kCheapPrepend 位置，后面就空出来了。
//      代价：一次 memmove（通常是几十~几百字节），比 resize 快得多。
//
//   B. 扩容
//      条件：总空闲空间不够
//      做法：直接 vector.resize(writerIndex_ + len)，触发内存重分配。
//      代价：可能需要重新 malloc + 拷贝全部数据。
//
// 为什么始终保留 kCheapPrepend 字节？
//   即使不 prepend，也要留出这 8 字节。这保证了上层随时可以做 prepend 而不需要
//   额外整理空间。8 字节刚好放一个 int64_t 长度前缀。

void Buffer::makeSpace(size_t len)
{
    // 总空闲 = 头部空洞 + 尾部剩余
    if (writableBytes() + prependableBytes() < len + kCheapPrepend)
    {
        // ── 分支 A：扩容 ──
        // 即便把碎片整理到最前面也不够 len + 8，只能扩容。
        // resize 之后 vector 可能在内存中迁移，但 begin()/end() 会自动更新。
        size_t oldSize = buffer_.size();
        buffer_.resize(writerIndex_ + len);
        LOG_WARN("Buffer expanded: %zu -> %zu bytes, readable=%zu", oldSize, buffer_.size(), readableBytes());
    }
    else
    {
        // ── 分支 B：碎片整理（不扩容）──
        // 当前状态示例：
        //   [____100B空洞____][___200B可读___][___50B可写___]
        //   整理后：
        //   [8B][___200B可读___][__________大量可写__________]
        //
        // 只搬可读数据，空洞里的旧数据（已被消费）丢弃不管。
        // 从begin() + kCheapPrepend)开始，把begin() + readerIndex_到begin() + writerIndex_内容拷贝进来
        size_t readable = readableBytes();
        std::copy(begin() + readerIndex_,       // 源：可读区起点
                  begin() + writerIndex_,       // 源：可读区终点
                  begin() + kCheapPrepend);     // 目标：预留 8 字节后面

        // 重置游标到整理后的位置
        readerIndex_ = kCheapPrepend;
        writerIndex_ = readerIndex_ + readable;
    }
}

// =========================================================================
// begin —— 获取 vector 内部数据起始指针
// =========================================================================
//
// 使用 &*buffer_.begin() 而不是 buffer_.data()，语义相同。
// 注意：空 vector 时 begin() == end()，对空容器 &*begin() 理论上是 UB，
// 但 Buffer 构造时就分配了至少 kCheapPrepend + kInitialSize 字节，
// 整个生命周期都不会变成空 vector，所以安全。

char* Buffer::begin()
{
    return &*buffer_.begin();
}

const char* Buffer::begin() const
{
    return &*buffer_.begin();
}

} // namespace wevix_muduo
