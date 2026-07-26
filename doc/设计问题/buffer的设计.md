# Buffer 设计详解

## 1. 为什么需要 Buffer？

非阻塞 IO 的核心矛盾：**内核有数据可读≠你能一次读到完整消息**。

```
场景：客户端发来 "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"

第 1 次 epoll 触发：读到 "GET / HT"
第 2 次 epoll 触发：读到 "TP/1.1\r\nHost: exa"
第 3 次 epoll 触发：读到 "mple.com\r\n\r\n"
```

如果不用 Buffer，每次把零散数据直接交给业务层，业务层必须自己拼装消息。Buffer 的角色就是**替上层扛下粘包和半包的拼接工作**——数据到达后先存进 Buffer，攒够了再通知上层。

---

## 2. 三区内存模型

Buffer 的核心就是一个 `std::vector<char>` 加上两个游标：

```
  0              readerIndex_       writerIndex_       buffer_.size()
  | prependable  |   readable       |   writable       |
  |     8 字节    |     可读数据      |     可写空间      |
```

### 三个区的含义

| 区 | 范围 | 含义 |
|----|------|------|
| prependable | `[0, readerIndex_)` | 已读数据前面的空位，用来往前插入帧头/长度前缀 |
| readable | `[readerIndex_, writerIndex_)` | 已写入、等待上层消费的**有效数据** |
| writable | `[writerIndex_, buffer_.size())` | 可以往里写新数据的空闲空间 |

### 为什么要有 prependable 区？

假如你要给数据前面加一个 4 字节的长度头：

```
方式 A（无 prependable）：data = [可读数据...]
  → 需要把可读数据整体后移 4 字节 → memmove → 慢

方式 B（有 prependable）：data = [4B空位][可读数据...]
  → readerIndex_ -= 4，然后往空位写 → 不拷贝 → O(1)
```

这就是 prependable 区存在的意义——给协议头让路，避免 memmove。

### 和后端 buffer 的关键设计差异

| 方案 | 优缺点 |
|------|--------|
| 双 buffer（接收 + 发送分开） | 简单，但无法在一个 buffer 中做 prepend |
| 环形 buffer | 零内存碎片，但不能用 vector 扩容，也不能 prepend |
| **三区 buffer（本方案）** | 有 prepend 能力，能扩容，碎片可整理，是折中最优解 |

---

## 3. readFd —— 最核心的函数

这是整个 Buffer 中最重要的函数，也是 muduo 网络 IO 高性能的基石。

### 问题：`read()` 为什么不够好？

```cpp
// 朴素做法：read() 到栈缓冲，再 append 进 Buffer
char buf[65536];
int n = ::read(fd, buf, sizeof(buf));  // 第 1 次系统调用
if (n > 0) buffer.append(buf, n);
```

这样需要**两次拷贝**：内核→栈缓冲→Buffer。

### 方案：`readv()` 散射读

```cpp
ssize_t Buffer::readFd(int fd, int* savedErrno)
{
    char extrabuf[65536];           // 栈上副缓冲 64KB
    struct iovec vec[2];

    vec[0].iov_base = beginWrite();     // 第 1 段 → Buffer 可写区
    vec[0].iov_len  = writableBytes();
    vec[1].iov_base = extrabuf;         // 第 2 段 → 栈缓冲（备用）
    vec[1].iov_len  = sizeof(extrabuf);

    int iovcnt = (writable < sizeof(extrabuf)) ? 2 : 1;
    ssize_t n = ::readv(fd, vec, iovcnt);
    // ...
}
```

`readv()` 一次系统调用把数据散射到多块不连续内存。流程：

```
情况 A：读到数据 ≤ Buffer 可写空间
  ┌──────────┬──────────┬──────────┐
  │ prepend  │ readable │ 写入数据  │  ← 直接落在 Buffer 里
  └──────────┴──────────┴──────────┘
  零额外拷贝。只需 writerIndex_ += n

情况 B：读到数据 > Buffer 可写空间
  ┌──────────┬──────────┬──────┬──────────────┐
  │ prepend  │ readable │ 写满  │  extrabuf    │
  └──────────┴──────────┴──────┴──────────────┘
                                 ↑ 溢出到栈缓冲
  Buffer 满了 → 剩下在 extrabuf → append(extrabuf, 溢出部分)
  只拷贝溢出部分，且只发生一次。
```

### 对比总结

| 方案 | 系统调用 | 数据拷贝 |
|------|---------|---------|
| `read` + `append` | 1 次 | 2 次（内核→栈→Buffer） |
| **`readv` 散射读** | 1 次 | 0~1 次（多数情况 0 次） |

Buffer 可写空间≥64KB 时（初始 1KB+持续扩容后的常态），所有数据直接落盘，**零拷贝**。

---

## 4. 接口详解

### 4.1 容量查询

```cpp
size_t readableBytes() const;     // writerIndex_ - readerIndex_
size_t writableBytes() const;     // buffer_.size() - writerIndex_
size_t prependableBytes() const;  // readerIndex_
const char* peek() const;         // begin() + readerIndex_
```

纯查询，不修改状态。`peek()` 是对外唯一的数据读取入口——上层通过它拿到可读数据的首指针。

### 4.2 消费数据

```cpp
void retrieve(size_t len);              // 消费 len 字节（只移 readerIndex_，不删数据）
void retrieveAll();                     // 重置为初始状态
std::string retrieveAsString(size_t);
std::string retrieveAllAsString();
```

**消费 ≠ 删除**。只移动 `readerIndex_`，数据仍在 vector 里，只是标记为"已读"。好处：
- O(1) 消费，不触发任何内存操作
- 被消费的空间自动变成 prependable 区，后续可复用

### 4.3 写入数据

```cpp
void ensureWritableBytes(size_t len);   // 不够就 makeSpace
void append(const char* data, size_t len);
void append(const std::string& str);
char* beginWrite();                     // 可写区首指针
```

`append` 的典型调用链：`append → ensureWritableBytes → makeSpace`。

### 4.4 碎片整理 —— makeSpace

```
场景：读完部分数据后
  [读过的(空洞)] [可读数据] [可写空间(太小)]
  
makeSpace 策略：
  A. 总空间够 → 把可读数据平移到 kCheapPrepend 位置
     [8B空] [可读数据] [大量可写空间]  ← 碎片消失
  B. 总空间不够 → buffer_.resize() 扩容
```

平移操作是原地 memmove，不分配新内存，远快于 resize。

### 4.5 协议辅助

```cpp
const char* findCRLF() const;           // 查找 \r\n（HTTP 协议解析核心）
void prepend(const void*, size_t);      // 从头部插入（做帧头/长度前缀）
```

`findCRLF` 用 `std::search` 在 readable 区扫描 `\r\n`。HTTP 协议每行都以 `\r\n` 结尾，这个函数是 HTTP 解析的基础。

`prepend` 不经过 append 路径，直接从前面插。readerIndex_ 前移 + 数据拷贝，挪出 prependable 区的位置。

---

## 5. 上层如何调用

### 5.1 典型读路径（Connection）

```cpp
void Connection::handleRead()
{
    int savedErrno = 0;
    ssize_t n = inputBuffer_.readFd(fd(), &savedErrno);
    //                ↑ 一次 readv 把所有数据读到 Buffer

    if (n > 0)
    {
        // Buffer 已经有完整数据，通知业务层消费
        std::string msg = inputBuffer_.retrieveAllAsString();
        onMessageCallback_(shared_from_this(), msg);
    }
    else if (n == 0)
    {
        handleClose();   // 对端关闭
    }
    else
    {
        // n < 0，处理错误
    }
}
```

### 5.2 HTTP 逐行解析

```cpp
// 上层 HTTP 解析器
void HttpParser::parse(Buffer* buf)
{
    // 找到一行
    const char* crlf = buf->findCRLF();
    if (!crlf) return;   // 还没凑够一行，等下次数据

    // 取出这一行
    std::string line = buf->retrieveAsString(crlf - buf->peek());

    // 跳过 \r\n
    buf->retrieve(2);

    // 解析 "GET / HTTP/1.1"...
}
```

### 5.3 加帧头再发送

```cpp
// 上层应用：给消息加长度前缀
void sendMessage(Connection* conn, const std::string& payload)
{
    int32_t len = htonl(payload.size());  // 网络字节序

    Buffer& out = conn->outputBuffer();   // 假设有获取 outputBuffer 的接口
    out.prepend(&len, sizeof(len));        // 前面插入 4 字节长度头
    // 发送...
}
```

### 5.4 写路径

```cpp
void Connection::send(const std::string& data)
{
    // 直接调 append，内部自动处理扩容
    outputBuffer_.append(data);

    // 尝试立即发送
    if (loop_->isInLoopThread())
        sendInLoop(data);
    else
        loop_->runInLoop(std::bind(&Connection::sendInLoop, shared_from_this(), data));
}

void Connection::sendInLoop(const std::string& data)
{
    ssize_t n = ::send(fd(), outputBuffer_.peek(),
                       outputBuffer_.readableBytes(), 0);
    if (n > 0)
        outputBuffer_.retrieve(n);  // 发多少，消费多少
    // 没发完的留在 Buffer，下次 epollOUT 触发时继续发
}
```

---

## 6. 内存视角：一次连接的生命周期

```
新连接建立，Buffer 初始化:
  vector size = 8 + 1024 = 1032
  [8B 空][                         1024B 可写空间                       ]
  readerIndex_=8   writerIndex_=8

第一波数据到达 (200 字节):
  [8B 空][ 200B 可读 ][               824B 可写空间                     ]
  readerIndex_=8   writerIndex_=208

上层消费 100 字节:
  [108B 空（可prepend）][ 100B 可读 ][          824B 可写空间            ]
  readerIndex_=108  writerIndex_=208

上层 prepend 4 字节帧头:
  [104B 空][4B帧头][ 100B 可读 ][          824B 可写空间                ]
  readerIndex_=104  writerIndex_=208

持续读写后空间不足 → makeSpace:
  [8B 空] [ 所有可读数据 ] [              大量可写空间                  ]
  ↑ 碎片整理到最前面
```

关键：整个生命周期中，**数据始终在同一块 vector 内存中**，没有多余的 malloc/free。

---

## 7. 为什么不直接用 std::string / std::vector<char>？

| 需求 | string | vector | Buffer |
|------|--------|--------|--------|
| 在数据前插入帧头 | 需要 `insert(begin())` → O(n) | 同左 | `prepend()` → O(1)（空间够时） |
| 消费部分数据 | 需要 `erase()` → O(n) | 同左 | `retrieve()` → O(1) 只移指针 |
| 扩容时避免数据搬迁 | 不做不到 | 做不到 | `makeSpace` 先整理再扩容 |
| readv 散射读 | 需手动分配栈缓冲 + 拼接 | 同左 | 内置 readv，一次调用完成 |
| 作为输入缓冲 → 立刻转输出缓冲 | 需拷贝 | 需拷贝 | 三个区共存，可同时读+写 |

**Buffer 做的不是"比 vector 更快的容器"，而是"把网络 IO 中反复出现的操作（找行、加帧头、readv）封装成原语，让上层用一行代码完成原本需要几十行的工作"。**

---

## 8. 总结

```
            ┌────────────────────────────────────┐
            │          Connection                 │
            │   handleRead → 读入 inputBuffer_    │
            │   send      → 写入 outputBuffer_    │
            └──────────────┬─────────────────────┘
                           │  append / retrieve / readFd
            ┌──────────────▼─────────────────────┐
            │             Buffer                  │
            │                                     │
            │  prependable │ readable │ writable  │
            │                                     │
            │  readFd  ← 一次 readv，零拷贝       │
            │  prepend ← 往前加帧头，不搬迁数据    │
            │  retrieve ← O(1) 消费，只移指针     │
            └─────────────────────────────────────┘
```

Buffer 是连接底层 IO（`readv`/`send`）和上层协议（HTTP/自定义协议）的**翻译层**——向下对接 Linux 内核的 scatter-gather IO，向上提供"行解析"、"帧头组装"等协议友好的原语。它不做花哨的事，但把网络编程中最繁琐的数据拼接工作压缩到了最少。
