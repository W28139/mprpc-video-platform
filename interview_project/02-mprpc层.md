# mprpc 框架 · 面试拷打题库

---

## 一、整体设计

### Q1.1 介绍一下 mprpc 框架

**答（30 秒口述版）**

`mprpc` 是构建在 `wevix_muduo` 之上的 RPC 框架，用 **protobuf 做接口定义和序列化**，用 **ZooKeeper 做服务注册发现**。

**服务端**（Provider）：

```text
RpcProvider::Run()
  ├─ 读配置（rpcserverip / rpcserverport / io线程数 / work线程数）
  ├─ 建 TcpServer → 挂三个回调（连接 / 消息 / 关闭）
  ├─ 连 ZooKeeper，把服务注册到 /mprpc/services/{service}/{method}/instance-*
  ├─ 给 TcpServer 装 RpcMessageCodec（帧边界由网络层处理）
  └─ server.start()   ← 阻塞在这里
```

**客户端**（Consumer）：

```text
MprpcChannel::CallMethod()
  ├─ 序列化请求 → [total_len][header_size][RpcHeader][args]
  ├─ 服务发现：本地缓存 → Redis 集中缓存 → ZooKeeper
  ├─ 轮询选 endpoint → 从连接池取连接
  ├─ SendAll → RecvAll → 校验 request_id → 反序列化响应
  └─ 失败：清池 + 失效缓存 + 重新发现 + 重试 1 次
```

**设计上最核心的两个点**：

1. **64MB 帧上限 + 长度合法性校验**——坏包在协议层就被拒绝，不会交给 protobuf 去解析然后崩溃；
2. **帧边界下沉到网络层**——`RpcMessageCodec` 挂在 `Connection` 上，应用层的 `OnMessage` 永远只收到**完整的一帧**，不用自己处理粘包。

#### Q1.1.1 为什么要自己写 RPC 框架，不用 gRPC / brpc？

**答**：**这个项目的目标就是「从零理解 RPC 的每一层」，用现成框架就完全跳过这个过程了。**

具体说，自己实现让我必须回答一系列「用框架时不需要想」的问题：

| 问题 | 用 gRPC 时 | 自己实现时 |
|---|---|---|
| 怎么区分消息边界？ | HTTP/2 帧层做掉了 | 得自己想帧格式、处理粘包半包 |
| 服务怎么找到？ | DNS / xDS | 得设计 ZK 路径、缓存、失效策略 |
| 连接怎么复用？ | 内置多路复用 | 得设计连接池、处理死连接 |
| 超时怎么处理？ | deadline 内置 | 得设计 deadline 传递 + 服务端快速拒绝 |
| 失败重试的语义？ | 框架定义 | 得自己想清 at-least-once 还是 exactly-once |

**每个问题都不是「实现一下」，而是「做一次架构决策」**——这些决策才是这个项目真正的产出。

**而且 `mprpc` 和 `wevix_muduo` 是配套设计的**——这带来一个 gRPC 给不了的优势：**我可以让网络层知道上层协议长什么样**。比如把「长度前缀帧」的认知下沉到 `Connection` 层（`MessageCodec`），应用层就不用写拆包代码了。用通用框架的话，这一层是黑盒，我只能被动适配。

---

### Q1.2 框架层的整体分层是什么样的？

**答**：分四层，从下往上：

```text
┌─────────────────────────────────────────────┐
│  业务层：video_platform 的 5 个微服务        │  ← 只写 service 实现
├─────────────────────────────────────────────┤
│  RPC 语义层：RpcProvider / MprpcChannel      │  ← 分发、序列化、发现、重试
│              MprpcController（错误码/超时）   │
├─────────────────────────────────────────────┤
│  协议层：mprpccodec.h（帧格式 + 拆帧）        │  ← 粘包/半包、长度合法性校验
│          rpcheader.proto（15 个错误码）       │
├─────────────────────────────────────────────┤
│  传输层：wevix_muduo（TcpServer/EventLoop/…）│  ← epoll、IO 线程、Buffer
└─────────────────────────────────────────────┘
```

**关键设计是「协议层独立于传输层」**（`mprpccodec.h` 是纯 header-only 的）：

```cpp
// mprpc/include/mprpccodec.h —— 只依赖 Buffer / CodecResult 两个网络库类型
inline std::string BuildRpcFrame(const std::string& payload);
inline bool ReadNetworkUint32(const char* data, size_t len, uint32_t* value);
```

它只做「字节串 ↔ 字节串」的转换，和 muduo 的耦合点只有 `RpcMessageCodec(Buffer*, std::string&)` 这个适配函数——`Buffer` 通过裸指针接触、`CodecResult` 是网络库定义的编解码结果枚举，都不是继承关系。

**这样的好处**：如果哪天换掉网络库（比如换成 `boost::asio`），**协议层一行都不用改**，只需要重新写一个 `Buffer` 的适配。

#### Q1.2.1 那 15 个错误码是怎么分的？

**答**：按**故障层次**分三组（`mprpc/src/rpcheader.proto`）：

**第一组：网络/传输层（4 个）**——「连接没打通」

```proto
RPC_CONNECT_FAILED  = 1;   // TCP 连接建立失败
RPC_SEND_FAILED     = 2;   // 发送失败
RPC_RECV_FAILED     = 3;   // 接收失败
RPC_TIMEOUT         = 4;   // 超时（连接、发送、接收、或服务端 deadline 拒绝）
```

**第二组：协议层（5 个）**——「连接通了但数据不对」

```proto
RPC_BAD_REQUEST         = 5;   // 请求格式非法（header_size 不合法、args_size 不匹配）
RPC_FRAME_TOO_LARGE     = 6;   // 帧超过 64MB
RPC_RESPONSE_PARSE_FAILED = 7; // 响应解析失败
RPC_INVALID_RESPONSE    = 8;   // request_id 不匹配（长连接串包）
RPC_REQUEST_PARSE_FAILED = 9;  // 请求体 protobuf 解析失败
```

**第三组：服务治理层（5 个）**——「协议没问题，但服务找不到」

```proto
RPC_SERVICE_NOT_FOUND       = 10;  // ZK 里没有这个 service
RPC_METHOD_NOT_FOUND        = 11;  // service 里没有这个方法
RPC_SERVICE_DISCOVERY_FAILED = 14; // 服务发现流程失败（ZK 不可用等）
```

（完整枚举见 `rpcheader.proto:6-27`，`RPC_SUCCESS = 0`。）

**为什么要这么分**：因为**不同层的错误，处理方式完全不同**：

| 层次 | 客户端应该做什么 |
|---|---|
| 网络层 | **重试**（换个连接/endpoint 可能就好了） |
| 协议层 | **不重试**（重试还是同样的坏包，说明是代码 bug） |
| 治理层 | **失效缓存 + 重新发现**（服务可能扩容/下线了） |

我的重试逻辑就依赖这个分类（`mprpcchannel.cc:953-957`）：

```cpp
if (!callOk &&
    (callErrorCode == mprpc::RPC_CONNECT_FAILED ||
     callErrorCode == mprpc::RPC_TIMEOUT ||
     callErrorCode == mprpc::RPC_SEND_FAILED ||
     callErrorCode == mprpc::RPC_RECV_FAILED))
```

**只有第一组的 4 个错误码会触发重试**——协议错误（第二组）和服务治理错误（第三组）都不会傻乎乎地重试。

---

## 二、协议设计

### Q2.1 你的 RPC 协议帧格式是什么样的？

**答（30 秒口述版）**

```text
[total_len(4B, 网络序)] [payload]
 ←──────── total_len 覆盖的范围（不含自身 4 字节）────────→
```

`total_len == payload.size()`（`mprpccodec.h:43-50`）：

```cpp
AppendNetworkUint32(&frame, static_cast<uint32_t>(payload.size()));
```

**外层帧头只有 4 字节**——够用就好：`total_len` 唯一的职责是让接收方知道「这一帧到哪里为止」，再多的字段都是它不需要的。

**payload 内部再分两种**：

```text
请求 payload：[header_size(4B, 网络序)] + [RpcHeader(protobuf)] + [args(protobuf)]
响应 payload：[response_header_size(4B)] + [RpcResponseHeader(protobuf)] + [response_body]
```

**所以线上真实字节序列是**：

```text
total_len | header_size | RpcHeader | args
```

**关键设计点**：

**① 单帧上限 64MB**（`mprpccodec.h:13`）：

```cpp
// 单帧最大 64MB：防止异常长度字段导致 Buffer 无限扩容或内存被打爆。
constexpr uint32_t kRpcMaxFrameSize = 64 * 1024 * 1024;
```

**② 全部用网络序（大端）**，集中在 2 个 inline 函数里（`mprpccodec.h:20-39`）：

```cpp
// RPC 协议里的 total_len/header_size 都走这个函数，避免客户端和服务端各自处理字节序。
```

读侧还带长度保护（`mprpccodec.h:30-33`）：`len < sizeof(uint32_t)` 直接返回 false，注释写着 `避免坏包触发越界读取`。

**面试官想听什么**

- 能**画出完整的字节布局**（而不是只说「有个长度前缀」）
- 知道 `total_len` **不包含自己那 4 字节**——这是最容易搞错的地方
- 知道长度字段非法时为什么**直接关连接**而不是清缓冲硬撑（见 Q3.1）

**可能追问**

- Q2.1.1 为什么是「两层帧头」（外层 total_len + 内层 header_size）？

#### Q2.1.1 为什么是「两层帧头」？一层不够吗？

**答**：**两层帧头由两个不同的层消费，服务不同的目的。**

```text
[total_len] ← 网络层（RpcMessageCodec）消费：只管「这一帧有多长」
    [header_size] ← 应用层（RpcProvider::OnMessage）消费：解析出 service/method/args
```

**外层 `total_len` 归网络层**：

`RpcMessageCodec` 挂在 `Connection` 上，它**只知道长度、不解析内容**：

```cpp
inline wevix_muduo::CodecResult RpcMessageCodec(wevix_muduo::Buffer* buf, std::string& message)
{
    if (buf->readableBytes() < 4) return kNeedMoreData;       // 半包：连长度都没收全
    uint32_t total_len = 0;
    if (!mprpc::ReadNetworkUint32(buf->peek(), buf->readableBytes(), &total_len))
        return kNeedMoreData;
    if (total_len < kRpcMinFrameSize || total_len > kRpcMaxFrameSize)
        return kFatal;                                        // 长度非法 → 交给上层关连接
    if (buf->readableBytes() - 4 < total_len) return kNeedMoreData;  // 半包：帧体未收全
    buf->retrieve(4);
    message = buf->retrieveAsString(total_len);
    return kFrameReady;
}
```

它的职责边界是「**把字节流切成帧**」——**帧里面是什么它不关心**。

**内层 `header_size` 归应用层**：

服务端拿到完整的一帧后，才知道「前 4 字节是 header 长度、接下来是 RpcHeader、剩下的是 args」：

```cpp
// rpcprovider.cc:62-106 DecodeRequestHeader
// 校验 header_size == 0 || message.size()-4 < headerSize
// 校验 args_size == 实际剩余长度
```

**为什么要这么分**——**因为协议演进的需求不同**：

- **网络层只关心「帧有多长」**。即使我换了整个应用层协议（比如把 protobuf 换成 JSON、把 RpcHeader 结构全改了），**`total_len` 这一层完全不用动**——`RpcMessageCodec` 的逻辑一行都不用改。
- **应用层只关心「帧里是什么」**。它可以自由改变 payload 的内部结构（加字段、换序列化方式），不影响网络层的拆帧。

**如果只有一层会怎样**：假设只有 `total_len`，那网络层就得知道「payload 前 4 字节是 header 长度」——**网络层就绑定了应用层协议**。将来 payload 结构一变，网络层也得跟着改，而网络层是被所有连接共用的。

**这实际上是「关注点分离」在协议设计上的体现**：

| 层 | 只关心 | 变更频率 |
|---|---|---|
| 外层 `total_len` | 帧边界 | **极低**（协议大版本升级才动） |
| 内层 `header_size` + RpcHeader | 请求语义 | 中（加字段、加错误码） |
| `args` | 业务数据 | 高（业务变化） |

**分层之后，每层的变更影响范围被限制住了。**

---

### Q2.2 `RpcHeader` 里都有什么？为什么要这些字段？

**答**：6 个字段（`rpcheader.proto:30-49`）：

```proto
message RpcHeader {
    bytes  service_name = 1;   // 要调哪个服务
    bytes  method_name  = 2;   // 要调哪个方法
    uint32 args_size    = 3;   // 参数序列化后的字节数（用于校验）
    uint64 request_id   = 4;   // 请求 ID，用于匹配响应
    bytes  trace_id     = 5;   // 全局链路追踪 ID（预留）
    uint64 deadline_ms  = 6;   // 请求的失效时刻（绝对时间戳）
}
```

**逐个说用途**：

| 字段 | 用途 | 不用它会怎样 |
|---|---|---|
| `service_name` + `method_name` | 服务端据此**查找 method descriptor** | 没法分发 |
| `args_size` | **内容自校验**——和实际剩余长度比对 | 坏包会在 `ParseFromString` 时才失败，错误信息模糊 |
| `request_id` | 客户端**校验响应归属** | 长连接上无法区分「这是哪个请求的响应」（串包） |
| `deadline_ms` | 服务端**快速拒绝过期请求** | 客户端已超时，服务端还在傻算 |
| `trace_id` | 链路追踪 | **目前是预留字段**，框架层没有生成和传播 |

**重点说三个**：

**① `args_size` 是「内容自校验」**——服务端会比对（`rpcprovider.cc`）：

```cpp
// args_size 必须等于实际剩余的字节数
if (rpcHeader.args_size() != remainder)  → 回 RPC_BAD_REQUEST
```

这能挡住「header 解析对了但 body 长度不对」的坏包。**光靠外层 `total_len` 是不够的**——`total_len` 只保证「帧的字节数对」，不保证「帧内部结构自洽」。

**② `request_id` 是长连接的必需品**：

```cpp
// mprpcchannel.cc:71-75
uint64_t NextRequestId()
{
    static std::atomic<uint64_t> nextRequestId{1};
    return nextRequestId.fetch_add(1, std::memory_order_relaxed);
}
```

客户端发请求前生成，收到响应后**必须校验**（`mprpcchannel.cc:1012-1022`）：

```cpp
if (responseHeader.request_id() != requestId)
{
    // RPC_INVALID_RESPONSE
}
```

**没有这个校验会怎样**：连接池里的连接是复用的，如果上一次请求超时了、响应又姗姗来迟，客户端在同一个连接上发起新请求时，可能**读到上一个请求的迟到响应**——数据全错，而且很难查。`request_id` 让这种错配立刻暴露。

**注意 `request_id` 的作用域**：它是**进程内**递增的（`static` 变量），不是全局唯一。但因为校验只发生在「同一个连接、同一个进程」内，**够用**。

**③ `deadline_ms` 是「绝对时刻」而不是「超时时长」**：

```cpp
// mprpcchannel.cc:857-861
if (timeoutMs > 0)
{
    rpcHeader.set_deadline_ms(NowMs() + static_cast<uint64_t>(timeoutMs));
}
```

传绝对时刻（`now + timeoutMs`）而不是时长，是因为服务端**需要判断「这个请求现在还有没有意义」**——而这个判断必须在同一时间基准上做。如果传时长，服务端还得知道「客户端是什么时候发的」才行。

（⚠️ 但用绝对时刻有个前提：**客户端和服务端的时钟得基本同步**。用 `system_clock` 意味着时钟漂移会直接影响判定。这是个已知的取舍。）

**面试官想听什么**

- 能说清 `args_size` 是**内容自校验**，和外层 `total_len` 的职责不同
- 能说清 `request_id` 解决的是**长连接串包**问题
- 知道 `deadline_ms` 传的是**绝对时刻**，以及为什么

---

## 三、拆帧：RpcMessageCodec

### Q3.1 粘包和半包是怎么处理的？

**答（30 秒口述版）**

**用长度前缀法，不用状态机。**

核心函数就 27 行（`mprpccodec.h:60-86`）：

```cpp
inline wevix_muduo::CodecResult RpcMessageCodec(wevix_muduo::Buffer* buf, std::string& message)
{
    if (buf->readableBytes() < 4) return kNeedMoreData;       // ① 半包：连长度都没收全

    uint32_t total_len = 0;
    if (!mprpc::ReadNetworkUint32(buf->peek(), buf->readableBytes(), &total_len))
        return kNeedMoreData;                                 // ② 读长度失败

    if (total_len < mprpc::kRpcMinFrameSize || total_len > mprpc::kRpcMaxFrameSize)
        return kFatal;                                        // ③ 长度非法 → 关连接

    if (buf->readableBytes() - 4 < total_len) return kNeedMoreData;  // ④ 半包：帧体未收全

    buf->retrieve(4);                                         // ⑤ 消费长度头
    message = buf->retrieveAsString(total_len);               // ⑥ 取出 payload
    return kFrameReady;
}
```

**三种情况分开处理**：

**① 半包（数据不够）**：**返回 `kNeedMoreData`，一个字节都不消费**。数据留在 `Buffer` 的 readable 区，等下次 `handleRead` 追加数据后再试。这是关键——**不能消费**，否则长度头就丢了。

**② 粘包（一次读到多帧）**：由**调用方循环**处理（`Connection::handleRead`）：

```cpp
while (true)
{
    CodecResult result = messageCodec_(&inputBuffer_, message);
    if (result == CodecResult::kNeedMoreData) break;
    if (result == CodecResult::kFatal) { handleClose(); return; }
    if (disconnected_) break;
    if (onMessageCallback_) { onMessageCallback_(self, message); }
    if (disconnected_) break;
}
```

一次 `read` 可能读到 `[帧1][帧2][帧3的一半]`，`while` 循环把前两帧都吐出来，第三帧留在 Buffer 里等下次。

**③ 长度非法：返回 `kFatal`，由 `Connection` 关闭连接。**

这里我改过一版设计。最初是 `retrieveAll()` 清空缓冲接着读（注释写着「避免这个连接后续一直卡在坏帧上」），**但那个想法是错的**：长度非法意味着字节流已经错位，而长度前缀协议**没有可扫描的同步点**——清缓冲并不能重新对齐，只是在赌发送方恰好走到帧边界，赌赢的代价是中间那些帧被静默丢弃、对应请求各自干等 5s 超时。

关连接则是让错误如实暴露：对端立刻收到 `RPC_RECV_FAILED`（而不是等超时），该错误码在客户端自动重试清单里，会清池重连重发——**恢复从「5 秒超时」变成「一次重连」**。

**零拷贝的细节**：用 `peek()` **只读长度不消费**（`Buffer.h:70`），确认帧完整后才 `retrieve(4)` + `retrieveAsString(total_len)`。**先看后取**，避免读了半个帧就破坏了缓冲状态。

**面试官想听什么**

- 能说出「半包时**一个字节都不能消费**」——这是最容易写错的地方
- 知道「粘包靠调用方循环、半包靠返回值」，职责划分清楚
- 认得出「先 `peek` 后 `retrieve`」这个零拷贝模式
- 知道长度非法为什么要**关连接**而不是清缓冲（错位后不可恢复）

**可能追问**

- Q3.1.1 为什么用长度前缀而不是状态机（比如 `\r\n` 分隔）？

#### Q3.1.1 为什么用长度前缀，不用状态机或分隔符？

**答**：**因为长度前缀是「一次判断」就够，而分隔符/状态机需要「逐字节扫描」。**

**三种方案的对比**：

| 方案 | 判断帧边界的方式 | 复杂度 | 适用场景 |
|---|---|---|---|
| **长度前缀** | 读前 4 字节，直接算出帧长 | **O(1)** 判断 | **二进制协议**（长度已知） |
| 分隔符（如 `\r\n`） | 逐字节扫描找分隔符 | **O(N)** 扫描 | 文本协议（HTTP header、Redis） |
| 状态机 | 逐字节喂入状态转移 | O(N) 扫描 | 转义字符/嵌套结构 |

**为什么长度前缀适合 RPC**：

1. **RPC 的 payload 是 protobuf**——protobuf **本来就是长度前缀的**（每个字段有 tag + length）。所以外层再加一个长度是**自然的**，而且长度**在序列化时就知道了**（`payload.size()`），不需要扫描。
2. **二进制数据里可能有任意字节**。如果用 `\r\n` 做分隔，而 protobuf 序列化后的字节里**恰好出现了 `\r\n`**（完全可能），就会**提前切断帧**。必须引入转义机制——那就更复杂了。
3. **O(1) 判断 vs O(N) 扫描**。长度前缀只需要读 4 个字节就能判断「够不够一帧」；分隔符方案必须扫描到分隔符才能判断。在高 QPS 下，这个差别是实打实的。

**什么时候该用状态机**：协议里有**嵌套结构**（比如 JSON 的 `{}` 配对）、或者**转义序列**（比如某些协议用 `0x7E` 做帧边界，数据里的 `0x7E` 要转义成 `0x7D 0x5E`）。这时候需要状态机记住「我是不是在转义态」。

**我的协议**：定长头部 + 长度前缀，**没有嵌套、没有转义**——所以一个 `if` 就够了，不需要状态机。

---

### Q3.2 codec 是怎么挂到网络层上的？

**答（30 秒口述版）**

**三层传递：`TcpServer` 保存 → `TcpServer` 分配给每个新 `Connection` → `Connection::handleRead` 循环调用。**

**① 注册**（`rpcprovider.cc`）：

```cpp
// 设置帧编解码器：让 muduo 在 Connection 层自动处理粘包/拆包
server.setMessageCodec(RpcMessageCodec);
```

**② 分发给新连接**（`TcpServer.cpp:97`）：

```cpp
if (messageCodec_) conn->setMessageCodec(messageCodec_);
```

**③ 在 `handleRead` 里循环调用**（`Connection.cpp:96-135`）：

```cpp
if (messageCodec_)
{
    // 有帧编解码器：循环提取完整帧，每帧回调一次 onMessage
    std::string message;
    while (true)
    {
        CodecResult result = messageCodec_(&inputBuffer_, message);
        if (result == CodecResult::kNeedMoreData) { break; }
        if (result == CodecResult::kFatal)
        {
            LOG_WARN("handleRead fd=%d: fatal codec error, closing connection", fd());
            handleClose();
            return;                                 // handleClose 后禁止再访问 this
        }
        if (disconnected_) { break; }
        if (onMessageCallback_) { onMessageCallback_(self, message); }
        if (disconnected_) { break; }
    }
}
else
{
    // 无编解码器：保留旧行为，一次性提取所有数据透传给上层
    std::string message = inputBuffer_.retrieveAllAsString();
    if (onMessageCallback_) { onMessageCallback_(self, message); }
}
```

**`handleClose()` 后面那行 `return` 不是可有可无的**：关闭会经由 `closeCallback_` → `TcpServer::handleClose` → `removeConnection` 把连接从 `connections_` 里摘掉，之后再访问 `this` 就可能踩空。之所以安全，是因为 `handleRead` 开头有个 `ConnectionPtr self(shared_from_this())` 兜住了生命周期——**这是之前一次 UAF 崩溃换来的教训**。

**关键设计：这是可选的**。

`messageCodec_` 是个 `std::function`，默认是空的（`Connection.h:56`）：

```cpp
// 设置帧编解码器：若设置，handleRead 中循环提取完整帧再回调 onMessage
// 若未设置，行为不变（每次读到多少就回调多少）
void setMessageCodec(MessageCodec cb) { messageCodec_ = std::move(cb); }
```

**不设置时，行为完全退化回 muduo 式**——`retrieveAllAsString` 把读到的全部数据交给上层。

**这个设计的意义**：

1. **向后兼容**——`wevix_muduo` 的其他使用者（`echo_server`、`bench_echo_stress`）不需要 codec，行为不变；
2. **证明「下沉」没有牺牲通用性**——网络库还是通用的，只是**多提供了一条更省事的路径**；
3. **`MessageCodec` 的类型是通用的**（`Connection.h:21-37`）：

```cpp
// 帧编解码器的三态结果
enum class CodecResult
{
    kNeedMoreData,  // 数据不足，保留 Buffer 等下次追加
    kFrameReady,    // 成功提取一帧，message 有效
    kFatal,         // 数据流已损坏且不可恢复，调用方必须关闭连接
};

using MessageCodec = std::function<CodecResult(Buffer*, std::string&)>;
```

**为什么是三态而不是 `bool`**：`bool` 只能表达「成帧 / 没成帧」，**没法区分「数据不够，等等再来」和「流坏了，别等了」**——而这两者的处理完全相反，前者必须原样保留 Buffer，后者必须关连接。把「要不要关连接」的判断权交给 codec，是因为**只有 codec 懂协议**：网络库不该知道「多长的帧算非法」。

**任何协议都能实现这个签名**——HTTP、Redis、WebSocket 都可以。所以这不是「为 mprpc 硬编码」，而是「提供了一个可插拔的扩展点」。

**面试官想听什么**

- 能说清「为什么要把拆帧下沉到网络层」（因为 muduo 和 mprpc 是一起设计的，RPC 是唯一上层协议）
- 知道**不设置 codec 时行为完全不变**——这是「不破坏通用性」的证据

**可能追问**

- Q3.2.1 那这个 codec 在哪个线程执行？

#### Q3.2.1 codec 在哪个线程执行？业务处理又在哪个线程？

**答**：**拆帧在 IO 线程，业务处理在 work 线程。这是刻意的分层。**

**完整链路**：

```text
TcpServer::setMessageCodec(RpcMessageCodec)              // rpcprovider.cc:284
  └─ TcpServer::handleNewConnection()                    // TcpServer.cpp:85
       if (messageCodec_) conn->setMessageCodec(messageCodec_);   // TcpServer.cpp:100
  └─ Connection::handleRead()                            // Connection.cpp:96-135   ← IO 线程
       CodecResult result = messageCodec_(&inputBuffer_, message);
  └─ TcpServer::handleMessage()                          // TcpServer.cpp:148
       if (workThreadPool_) {
           auto msg = std::make_shared<std::string>(std::move(message));   // ← move 到堆上
           workThreadPool_->addTask([this, conn, msg]() { onMessageCallback_(conn, *msg); });
       } else { onMessageCallback_(conn, message); }      // ← 无 work 池则同步执行
  └─ RpcProvider::OnMessage()                            // work 线程，真正执行业务分发
```

**为什么这么分**：

**IO 线程只做「拆帧」**——这是纯内存操作（移动 Buffer 指针 + 构造 `std::string`），快且不会阻塞。

**业务处理放 work 线程**——因为 `RpcProvider::OnMessage` 会：
1. 反序列化 protobuf（可能很慢，大消息尤其）；
2. **调用用户的 service 方法**——这个方法可能做任何事（查数据库、调外部服务、甚至同步等待）。

**如果业务在 IO 线程跑会怎样**：一个慢的 RPC handler 会**阻塞这个 subLoop 上所有连接的收发**。而且 `video_platform` 的 `SchedulerService::ScheduleJob` 会调 `ffprobe` 探测视频时长（最长 15 秒）——那 15 秒里，同一 loop 上的连接全部卡死。

**work 线程数量可配**（`rpcprovider.cc:197-201`）：

```cpp
if (workThreads > 0)
{
    // 业务 protobuf service 放到 work pool 执行，避免慢业务阻塞 IO 线程。
    server.enableWorkPool(workThreads, wevix_muduo::PoolMode::MODE_FIXED);
}
```

`rpcserverwork_threads` 默认 2，范围 0~256，**0 表示禁用 work pool**（退化成全在 IO 线程跑）。

**这里有个我实测过的拐点**：默认 2 个 work 线程在 100 并发下会饱和（P99 28ms），调到 16 之后 P99 降到 **2.9ms**。因为 work 线程数不够时，请求在 `ThreadPool` 队列里排队，延迟全部堆在队列上。

**⚠️ 但 work 线程数也是把双刃剑**：`ScheduleJob` 里有 `ffprobe`（最长 15 秒），**2 个并发就能占满默认的 2 个 work 线程**，整个 Scheduler 就瘫痪了。这个风险在业务层是靠「给 `Probe` 加 15 秒超时」来兜底的。

**面试官想听什么**

- 能说清「拆帧在 IO 线程、业务在 work 线程」的分工
- 知道 `std::move(message)` + `shared_ptr` 是**跨线程传递数据的正确姿势**（避免拷贝 + 保证生命周期）
- 能说出 work 线程数不足的**实际症状**（P99 因排队而升高）

---

## 四、客户端：MprpcChannel

### Q4.1 `MprpcChannel::CallMethod` 的完整流程是什么？

**答（30 秒口述版）**

按步骤编号（`mprpcchannel.cc:799-1048`）：

| 步 | 做什么 |
|---|---|
| 0 | 从 `method` 取 service 名和方法名 |
| 0.5 | 起一个 **RAII 计时守卫**（覆盖所有 return 路径） |
| 0.6 | 生成 `requestId`；读取超时配置 |
| 1 | `request->SerializeToString(&args_str)` + 两次 64MB 校验 |
| 2 | 组 `RpcHeader`（service/method/args_size/request_id/deadline_ms），序列化 |
| 3 | 拼 payload：`[header_size] + RpcHeader + args`，再套外层帧 |
| 4 | **服务发现**（direct 直连分支 / ZK 三级缓存分支） |
| 5 | 从连接池取连接 → `SendRequestAndReadResponse` |
| 5.1 | 连接级失败 → **清池 + 重试一次** |
| 6 | 失败 → 设置 controller 错误码 |
| 7 | 解析响应帧 |
| 8 | **校验 `request_id`** |
| 9 | 检查 `error_code` |
| 10 | `response->ParseFromString(...)` → 成功则 `done->Run()` |

**第 3 步的拼装代码**（面试要求能背）：

```cpp
// 帧格式：[total_len(4B, network order)] + [header_size(4B, network order) + RpcHeader + args]
std::string request_payload;
request_payload.reserve(sizeof(uint32_t) + rpc_header_str.size() + args_str.size());
mprpc::AppendNetworkUint32(&request_payload, header_size);
request_payload += rpc_header_str;
request_payload += args_str;
// ... 64MB 校验
// 外层 total_len 由 BuildRpcFrame 写入，服务端 Connection 的 codec 用它做粘包/拆包。
std::string send_rpc_str = mprpc::BuildRpcFrame(request_payload);
```

**面试官想听什么**

- 能按顺序说出「先序列化参数、再组 header、再拼 payload、最后套帧」这个**自底向上的组装过程**
- 知道 `request_id` 校验（第 8 步）是**必做的**，不是可选的

**可能追问**

- Q4.1.1 客户端是同步还是异步的？

#### Q4.1.1 客户端是同步还是异步的？为什么？

**答**：**当前是同步阻塞的，而且实现方式很特别——根本没走 Reactor。**

`MprpcChannel` 用的是**裸 socket**：`socket()` / `connect()` / `send()` / `recv()`，配上 `SO_SNDTIMEO` / `SO_RCVTIMEO` 做超时（`mprpcchannel.cc:525-619`）：

```cpp
// 非阻塞 connect 的真实错误需要从 SO_ERROR 读取
int ConnectToEndpoint(...)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    // 设置非阻塞
    int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    int ret = ::connect(fd, ...);
    if (ret < 0 && errno == EINPROGRESS)
    {
        struct pollfd pfd{fd, POLLOUT, 0};
        int pr = ::poll(&pfd, 1, timeoutMs);      // ← 可超时的握手
        if (pr == 0) { savedErrno = ETIMEDOUT; ... }
        int err = 0; socklen_t len = sizeof(err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);   // ← 取真实错误
        ...
    }
    // 恢复阻塞 + 设置收发超时
    ::fcntl(fd, F_SETFL, flags);
    SetSocketTimeout(fd, timeoutMs, savedErrno);
}
```

**这个实现方式值得解释，因为它是刻意的**：

**① 为什么不用「非阻塞 connect + 交给 epoll」**——因为框架的**调用方式是同步的**：

```cpp
// protobuf 生成的 stub 就是这么调的
stub.ScheduleJob(&controller, &request, &response, nullptr);
//                                                  ↑ done 传 nullptr
```

`done` 传 `nullptr` 意味着**调用方要等返回**。既然语义是同步的，就没必要引入「发起 connect → 注册 EPOLLOUT → 等待 → 回调里继续」这一整套异步状态机。

**② 非阻塞 connect 是为了「可控的超时」**。直接用阻塞 `connect` 的话，超时由**内核决定**——Linux 默认的 TCP 连接超时是 **75 秒**（`tcp_syn_retries=6` 的指数退避）。对 RPC 来说这完全不可接受。

所以我用「非阻塞 connect + `poll(POLLOUT, timeoutMs)`」，**超时时间完全由我控制**。`poll` 返回后还要 `getsockopt(SO_ERROR)` 取真实错误——**这是非阻塞 connect 的标准流程**，因为 `connect` 返回 `EINPROGRESS` 时不知道成功还是失败，必须查 `SO_ERROR`。

**③ 收发超时交给内核**：

```cpp
static bool SetSocketTimeout(int fd, int64_t timeoutMs, int& savedErrno)
{
    struct timeval tv;
    tv.tv_sec = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ...
}
```

用 `SO_RCVTIMEO` 的**好处**是「等响应的时候不需要我自己做超时轮询」——`recv` 到时间没数据就返回 `EAGAIN`，我把它映射成 `RPC_TIMEOUT`：

```cpp
static int IoErrorCode(int savedErrno, int defaultCode)
{
    if (savedErrno == ETIMEDOUT || savedErrno == EAGAIN || savedErrno == EWOULDBLOCK)
        return mprpc::RPC_TIMEOUT;
    return defaultCode;
}
```

**这个设计的不对称性**：服务端是纯 Reactor（`TcpServer` + epoll），客户端却是裸 socket 同步阻塞。**两边完全不对称**。

**为什么这样是可以接受的**：
- 客户端的通信模式是「一问一答」，没有并发多路复用需求；
- 裸 socket 的代码路径短、容易推理、不需要处理回调重入；
- 每次调用都在调用方自己的线程里阻塞，**不需要跨线程唤醒**。

**代价我也清楚**：
- **`PooledConnection::mutex` 覆盖整次 send+recv**，所以单个 endpoint 的**并发 RPC 上限 = 连接池大小**（默认 8）。第 9 个并发调用者会在锁上排队。
- 没有真正的异步客户端。虽然 protobuf 的 `done` 回调机制支持异步，但框架没实现（`done` 总是同步调用的）。

---

### Q4.2 `CallMethod` 的 RAII 计时是怎么做的？为什么要 RAII？

**答**：**因为 `CallMethod` 有大约 10 个提前返回路径，手动计时必然漏掉。**

```cpp
// mprpcchannel.cc:809-823
// RPC 延迟观测。RAII 守卫覆盖函数体所有 return 路径
//（服务发现失败/超时/解析失败等 early return 同样计延迟）。
struct RpcLatencyGuard
{
    std::string method;
    std::chrono::steady_clock::time_point t0;
    explicit RpcLatencyGuard(std::string m)
        : method(std::move(m)), t0(std::chrono::steady_clock::now()) {}
    ~RpcLatencyGuard()
    {
        double ms = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - t0).count();
        mprpc::RpcLatencyHistogram(method).Observe(ms);
    }
} rpc_latency_guard(service_name + "." + method_name);
```

**`CallMethod` 的提前返回路径有哪些**（这是关键论据）：

1. 参数序列化失败
2. args 超过 64MB
3. RpcHeader 序列化失败
4. RpcHeader 超过 64MB
5. payload 超过 64MB
6. 服务发现失败（endpoint 为空）
7. `ParseHostData` 失败
8. 取连接失败
9. 发送/接收失败（超时、连接断开）
10. 响应帧解析失败
11. `request_id` 不匹配
12. 响应体反序列化失败

**如果手动计时**，就得在 **12 个地方**各写一遍 `Observe()`。**只要漏一个，那个路径的延迟就永远采集不到。**

**而漏掉的最可能是什么路径**？——**异常路径**。因为正常路径（成功返回）写起来最自然，异常路径分散在十几个 `if` 里，每加一个检查点就多一个可能漏掉的地方。

**这就是致命的地方**：告警规则 `rpc_latency_p99_high` 关心的是 **P99**——**P99 恰恰是异常路径**。如果漏掉了超时路径的计时，P99 会显示得很好看，而实际上服务正在大面积超时。

**RAII 的价值**：`rpc_latency_guard` 声明在**函数体顶部**，析构函数在**所有 return 路径上都必然执行**（包括正常返回、`return false`、异常抛出）。**"插桩正确性靠作用域，不靠调用点纪律"**——这是我从这次经历里总结的。

**一个细节：为什么用 `steady_clock`**：

```cpp
std::chrono::steady_clock::time_point t0;
```

`steady_clock` 是**单调时钟**，不受系统时间调整（NTP 校时、手动改时间）影响。而 `deadline_ms` 用的是 `system_clock`（因为是绝对时刻，要和别的机器比较）。**两个地方用不同的时钟，是有意为之**：

| 用途 | 时钟 | 原因 |
|---|---|---|
| 测量耗时（metrics） | `steady_clock` | 只需要时间**间隔**，不能被校时干扰 |
| 表达 deadline | `system_clock` | 需要**绝对时刻**，要跨机器比较 |

**面试官想听什么**

- 能**列举出提前返回路径的数量和种类**——这是「为什么需要 RAII」的实证
- 能指出「漏掉的一定是异常路径，而 P99 恰恰关心异常路径」——这一层推理很关键
- 知道 `steady_clock` 和 `system_clock` 的区别及各自的适用场景

**可能追问**

- Q4.2.1 指标采集本身会不会影响性能？

#### Q4.2.1 指标采集本身会不会影响性能？

**答**：**设计上做了三层优化，让热路径的写入只花一个原子操作。**

**① 直方图只「命中桶 +1」，不做前缀和**。

朴素实现（Prometheus 的 `_bucket` 是**累计**语义）会这样写：

```cpp
// 反例：每次 Observe 都要更新 N 个桶
for (size_t i = idx; i < buckets_.size(); ++i)
    bucketCounts_[i].fetch_add(1);      // ← 11 个原子操作
```

我的实现（`mprpcmetrics.cc:185-193`）：

```cpp
void MetricHistogram::Observe(double v)
{
    // upper_bound 返回第一个 > v 的桶上界位置；越界即 +Inf 尾桶
    size_t idx = static_cast<size_t>(
        std::upper_bound(buckets_.begin(), buckets_.end(), v) - buckets_.begin());
    bucketCounts_[idx].fetch_add(1, std::memory_order_relaxed);   // ← 只有 1 个原子操作
    count_.fetch_add(1, std::memory_order_relaxed);
    sum_.add(v);
}
```

**前缀和在「导出」时才计算**（`CumulativeCounts()`），**热路径只写一个桶**。

代价是「导出时 O(N)」——但导出只在 Prometheus 抓取时发生（**每 15 秒一次**），而 `Observe` 是**每次 RPC 都发生**。这个交换非常划算。

**② 方法名的查表结果被缓存**（`mprpcmetrics.cc:670-684`）：

```cpp
MetricHistogram& RpcLatencyHistogram(const std::string& method)
{
    // 首次调用注册进 registry 并缓存指针（注册后 series 不再增删，指针稳定）
    static std::mutex cacheMutex;
    static std::unordered_map<std::string, MetricHistogram*> cache;
    std::lock_guard<std::mutex> lock(cacheMutex);
    auto it = cache.find(method);
    if (it != cache.end()) return *it->second;
    ...
}
```

**这里头文件的注释和实现不一致**（注释说「此后每次调用仅一次 hash 查找，无锁」，但实现每次都要抢 `cacheMutex`）。**正确的优化应该是用 `thread_local` 缓存**——因为每个线程处理的 method 集合通常是固定的，`thread_local` 可以完全免锁。

**这个不一致我在审查里发现了**，在本机压测下 `cacheMutex` 的竞争不严重（因为 `CallMethod` 本身有更多开销），但如果要做极限优化，换成 `thread_local` 能再省一笔。

**③ 数值操作用原子 + `relaxed` 内存序**。

Counter 直接用一个 `std::atomic<uint64_t>`：

```cpp
class MetricCounter {
    void Inc(double v = 1.0);
    double Value() const;
private:
    std::atomic<uint64_t> value_{0};
};
```

Gauge 需要一个 `AtomicDouble`——因为 **C++17 不保证 `std::atomic<double>` 的算术运算**（`fetch_add` 只对整数类型定义）。所以用「位模式 CAS」：

```cpp
// mprpcmetrics.cc:129-157
void AtomicDouble::add(double v)
{
    uint64_t old = bits_.load(std::memory_order_relaxed);
    for (;;)
    {
        double cur; std::memcpy(&cur, &old, sizeof(cur));
        double nxt = cur + v;
        uint64_t nb; std::memcpy(&nb, &nxt, sizeof(nb));
        if (bits_.compare_exchange_weak(old, nb, std::memory_order_relaxed)) return;
    }
}
```

全部用 `memory_order_relaxed` —— **指标允许弱一致性**。丢一两个计数或者读到稍旧的值，对「看趋势」这个目的完全无影响，但能省掉内存屏障的开销。

**面试官想听什么**

- 能说清「命中桶 +1 vs 全桶前缀和」的**复杂度差异**（O(1) vs O(N)），以及为什么可以推迟
- 知道 `AtomicDouble` 为什么存在（C++17 无 `atomic<double>` 算术）
- 能**主动指出** `RpcLatencyHistogram` 的注释与实现不一致

---

## 五、服务端：RpcProvider

### Q5.1 `NotifyService` 做了什么？为什么存裸指针？

**答**：**用自己建的两层 map 索引「service 名 → ServiceInfo」，`ServiceInfo` 里再索引「method 名 → MethodDescriptor*」。**

```cpp
struct ServiceInfo {
    google::protobuf::Service *m_service;
    std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> m_methodMap;
};
std::unordered_map<std::string, ServiceInfo> m_serviceMap;
```

（`rpcprovider.h:32-38`）。注册逻辑（`rpcprovider.cc:123-146`）：

```cpp
void RpcProvider::NotifyService(google::protobuf::Service* service)
{
    const ServiceDescriptor* pserviceDesc = service->GetDescriptor();
    std::string service_name = pserviceDesc->name();
    ServiceInfo service_info;
    service_info.m_service = service;
    int method_count = pserviceDesc->method_count();
    for (int i = 0; i < method_count; ++i)
    {
        const MethodDescriptor* pmethodDesc = pserviceDesc->method(i);
        service_info.m_methodMap[pmethodDesc->name()] = pmethodDesc;
    }
    m_serviceMap[service_name] = std::move(service_info);
}
```

**为什么存裸指针**——`rpcprovider.h:17-20` 的注释就是答案：

```cpp
// 发布 RPC 服务到 Provider。
// Provider 仅存储裸指针用于方法分发，不接管对象所有权，不会 delete service。
// 调用方必须保证 service 对象在 Provider 生命周期内一直有效。
// 典型用法：栈对象或全局对象，不要用 new 后把生命周期管理丢给 Provider。
```

**这是刻意的所有权约定**：Provider **不管** service 的生命周期。业务层的实际用法就是 `main()` 里的栈对象：

```cpp
int main(int argc, char** argv)
{
    ...
    RpcProvider provider;
    provider.NotifyService(new SchedulerServiceImpl());   // 裸指针，Provider 不 delete
    if (!provider.Run()) { return EXIT_FAILURE; }
}
```

**为什么不用 `unique_ptr` 接管**：因为框架**不应该替业务决定对象的生命周期**。如果 Provider 接管所有权，那业务就没法：
- 用**栈对象**（最常见的用法）；
- 用**全局/静态对象**；
- 多个 Provider 共享同一个 service 实例。

**代价是「泄漏风险」**——业务如果写 `new XxxServiceImpl()` 之后就忘了，那是业务的 bug，框架管不了。但这比「框架偷偷接管所有权、导致业务没法用栈对象」要好。

**分发用的是自己建的 map，不是 protobuf 的反射查找**（`rpcprovider.cc:375`）：

```cpp
// 两次 unordered_map::find
auto sit = m_serviceMap.find(service_name);
auto mit = sit->second.m_methodMap.find(method_name);
```

**好处**：
1. **O(1)** ——protobuf 的 `FindMethodByName` 需要遍历 descriptor；
2. **服务名可以和解耦**——proto 里写 `package mprpc;`，但注册用的是 `ServiceDescriptor::name()`，两者不绑定。

**⚠️ `m_serviceMap` 是无锁的**。因为约定是「`NotifyService` 必须在 `Run()` 之前全部调完」——也就是**单线程初始化期**，运行期只读。这是个**隐式约定，没有断言保护**。

**面试官想听什么**

- 能说清「OWNERSHIP 归调用方」这个**刻意的设计**，以及它的好处（支持栈对象/全局对象）
- 知道分发用**自建 map** 而不是 protobuf 反射查找，以及为什么

**可能追问**

- Q5.1.1 如果运行期想动态加服务怎么办？

#### Q5.1.1 如果运行期想动态加服务怎么办？

**答**：**当前的实现做不到，这是个明确的边界。**

问题有两层：

**① `m_serviceMap` 无锁**。`NotifyService` 里直接 `m_serviceMap[service_name] = ...`，没有任何同步。运行期调用它会和 `OnMessage` 的读取**构成数据竞争**——`unordered_map` 在插入时可能 rehash，会让正在读的线程拿到悬垂引用。

**② ZK 注册也没做动态**。`Run()` 里注册一次 ZK 节点，之后就不再动了：

```cpp
// rpcprovider.cc:234-269
std::string service_path = "/mprpc/services/" + sp.first;
zkCli.Create(service_path.c_str(), nullptr, 0);              // 永久节点
std::string method_path = MethodRegistryPath(sp.first, mp.first);
zkCli.Create(method_path.c_str(), nullptr, 0);               // 永久节点
zkCli.Create(instance_path.c_str(), method_path_data, ..., ZOO_EPHEMERAL | ZOO_SEQUENCE, ...);
```

**要做动态注册，需要改三处**：

```cpp
// ① 加锁保护 m_serviceMap
std::shared_mutex service_map_mutex_;      // 读多写少 → shared_mutex

void NotifyService(Service* service)
{
    std::unique_lock lock(service_map_mutex_);      // 写锁
    ... // 原有的注册逻辑
}

// OnMessage 里读
std::shared_lock lock(service_map_mutex_);          // 读锁

// ② 动态注册 ZK 节点（复用 Run() 里的注册逻辑，抽成一个方法）
bool RegisterToZk(const std::string& service_name, const std::string& method_name);

// ③ 提供反注册接口（删除 ZK 临时节点 + 从 map 移除）
```

**为什么现在不做**：**没有需求**。`video_platform` 的 5 个服务都是**启动时注册、运行期不变**的——`NotifyService` 调一次、`Run()` 阻塞，之后服务集合是固定的。

**但我认为这是个合理的扩展方向**，因为「热更新服务实现」在某些场景下是有价值的（比如灰度发布新版本 handler）。**如果要做，`shared_mutex` 是正确选择**——读（`OnMessage` 分发）远多于写（`NotifyService`）。

**面试官想听什么**

- 承认做不到，并**给出具体的技术方案**（`shared_mutex` + ZK 节点管理）
- 说清**为什么现在不做**（没有需求），而不是「没想到」

---

### Q5.2 服务端收到请求后做了哪些检查？

**答（30 秒口述版）**

**七项检查，而且——任何一步失败都会回一个错误响应，绝不让客户端干等超时。**

| 序 | 检查 | 失败返回 |
|---|---|---|
| 1 | `DecodeRequestHeader`（解出 header） | `RPC_BAD_REQUEST`（request_id = 0） |
| 2 | `deadline_ms` 是否已过期 | `RPC_TIMEOUT` |
| 3 | `service_name` / `method_name` 是否为空 | `RPC_BAD_REQUEST` |
| 4 | service 是否存在 | `RPC_SERVICE_NOT_FOUND` |
| 5 | method 是否存在 | `RPC_METHOD_NOT_FOUND` |
| 6 | `request` 能否 `ParseFromString` | `RPC_REQUEST_PARSE_FAILED` |
| 7 | response 的 64MB 上限 | `RPC_FRAME_TOO_LARGE` |

（`rpcprovider.cc:297-413`）。

**第 1 项内部还包含两个内容校验**（`DecodeRequestHeader`，`rpcprovider.cc:62-106`）：

```cpp
// 校验 header_size 合法性
if (headerSize == 0 || message.size() - 4 < headerSize)  → 失败
// 校验 args_size 和实际剩余长度一致
if (rpcHeader.args_size() != remainder)                   → 失败
```

**「坏包也回包」这个原则**（`rpcprovider.cc:46-57` 的注释）：

```cpp
// 服务端任何解析失败都回错误帧，客户端不会干等超时
```

**为什么这条原则很重要**——如果服务端解析失败就静默丢弃：

```text
客户端：发出请求 → 等待……
服务端：解析失败，丢弃，打条日志
客户端：等到 SO_RCVTIMEO（5 秒）超时 → 报 RPC_TIMEOUT
```

客户端拿到的是 **`RPC_TIMEOUT`**——**归因完全错误**！明明是「服务端收到了但请求格式不对」，客户端却以为是「超时」。运维会去查网络、查负载，而真正的问题（协议不匹配/版本不一致）被掩盖了。

**回错误帧之后**：

```text
客户端：收到响应 → request_id 不匹配（服务端解析失败时填 0）→ RPC_INVALID_RESPONSE
       或者 request_id 匹配但 error_code != 0 → 透传远端错误码
```

至少客户端能**立刻**失败并拿到明确的原因（`RPC_BAD_REQUEST`），而不是等 5 秒超时。

**⚠️ 这里有个细节值得注意**：服务端解析失败时 `request_id` 填 **0**（因为拿不到）：

```cpp
SendRpcError(conn, 0 /* 未知 request_id */, RPC_BAD_REQUEST, errorMsg);
```

而客户端的校验是「`request_id` 必须和我发的一致」，所以会得到 `RPC_INVALID_RESPONSE` 而不是 `RPC_BAD_REQUEST`。**错误码在客户端看起来会有点绕**——它能看到 `error_msg` 里的具体原因，但 `error_code` 是 `RPC_INVALID_RESPONSE`。

**这是个可以改进的点**：客户端可以识别「`request_id == 0` + 有 `error_msg`」这种情况，直接透传服务端的错误码，而不是报 `RPC_INVALID_RESPONSE`。

**面试官想听什么**

- 能说出「**坏包也回包**」这个原则，以及**不做的话会导致错误归因**（超时掩盖真实原因）
- 知道 `args_size` 是**内容自校验**，和外层 `total_len` 层次不同
- 能主动指出 `request_id = 0` 带来的错误码绕路问题

**可能追问**

- Q5.2.1 deadline 检查为什么放在 work 线程里？

#### Q5.2.1 deadline 检查为什么放在 work 线程里？它能阻止什么、不能阻止什么？

**答**：**它能阻止「排队太久的请求被白算」，但不能中断已经在执行的 handler。**

**位置**（`rpcprovider.cc:328-342`）：

```cpp
// deadline_ms == 0 表示客户端未设置（不检查）。
if (rpcHeader.deadline_ms() > 0)
{
    uint64_t nowMs = ...system_clock...;
    if (nowMs > rpcHeader.deadline_ms())
    {
        LOG_DEBUG("RPC request expired: request_id=%ld, deadline=%lu, now=%lu", ...);
        SendRpcError(conn, requestId, mprpc::RPC_TIMEOUT,
                    "request deadline exceeded before processing");
        return;
    }
}
```

**关键在于它在哪个位置执行**：`OnMessage` 是 `TcpServer::handleMessage` 投进 work 池之后的回调，**也就是说 `OnMessage` 本身就跑在 work 线程里**。

所以 deadline 检查发生的时刻是：**「请求已经排完队、马上要开始处理」的那一瞬间**。它能拒绝的是：

```text
[t0] 请求到达，进 work 池队列
[t0 ~ t1] 在队列里排队（前面有慢请求堵着）
[t1] 轮到它了 → 检查 deadline → 已过期 → 回 RPC_TIMEOUT，直接返回
     ↑ 省掉了「反序列化 + 调用 handler」的开销
```

**它能阻止的**：**队列积压导致的无效计算**。这对 `video_platform` 特别重要——转码请求的 deadline 可能是 10 秒，但如果它在队列里排了 30 秒，客户端早就超时了，服务端不该再花资源去触发一次转码。

**它不能阻止的**：

1. **已经在执行的 handler**。如果 handler 在 `CallMethod` 里跑了 60 秒，deadline 检查早就过去了，没有任何机制能打断它。
2. **排在它前面的请求**。这些请求的 deadline 检查还没轮到。

**要真正「中断执行」，需要的是「取消机制」**——而框架层**没有实现取消**：

```cpp
// mprpccontroller.h:24-27
void StartCancel() override {}
bool IsCanceled() const override { return false; }
void NotifyOnCancel(Closure* /* callback */) override {}
```

注释写着「目前未实现具体的功能」。`service->CallMethod(method, nullptr, ...)` 的 **controller 参数传的是 `nullptr`**（`rpcprovider.cc:412`）——**Provider 侧根本没把 controller 传给业务**，所以业务层也没有办法感知取消。

**业务层是怎么补偿的**：`video_platform` 自己在**业务语义层面**实现了取消——`CancelShard` RPC + `should_cancel` 回调 + kill 子进程。**这是业务层的能力，不是框架层的**。

**所以完整的图景是**：

| 能力 | 层级 | 实现 |
|---|---|---|
| 过期请求不白算 | 框架层 | deadline 检查（队列出口） |
| 中断正在执行的 handler | **框架层缺失** | 需要取消机制（未实现） |
| 中断正在执行的子进程 | 业务层 | `CancelShard` + `should_cancel` + `kill` |

**还有一个值得说的细节**：deadline 比较用的是 `system_clock`（绝对墙钟时间），所以**机器之间的时钟漂移会直接影响判定**。如果客户端比服务端快 5 秒、超时设 5 秒，那所有请求到了服务端都会被判过期。

**严谨的做法**是用「相对于服务端收到请求时的时间差」——但这样就需要在协议里额外传「发送时刻」，而且同样依赖时钟同步。**绝对时间戳是业界普遍做法**（gRPC 的 deadline 也是绝对时间），前提是集群做时钟同步。

**面试官想听什么**

- 能精确说出 deadline 检查的**位置语义**（work 线程、队列出口）
- 能**区分**「不会白算」和「不会中断」——这是两个不同的保证
- 知道取消机制**在框架层是缺失的**，业务层是自己实现的
- 知道时钟漂移的影响

---

### Q5.3 `SendRpcResponse` 是怎么管理 response 对象生命周期的？

**答（30 秒口述版）**

**用一个上下文结构打包「连接 + response + request_id」，把所有权通过 `release()` 移交，然后在 `SendRpcResponse` 开头用 `unique_ptr` 接住。**

**第一步：构造上下文**（`rpcprovider.cc:392-408`）：

```cpp
// 绑定回调函数（Closure）
// 当业务层处理完业务后调用 done->Run()，实际执行 SendRpcResponse
// 传递 response 原始指针，由 SendRpcResponse 负责删除
// Closure 可能在业务实现里异步执行，所以把响应对象所有权交给 context。
auto* context = new RpcResponseContext{conn, response.release(), requestId};
google::protobuf::Closure *done =
    google::protobuf::NewCallback<RpcProvider, RpcResponseContext*>(
        this, &RpcProvider::SendRpcResponse, context);
service->CallMethod(method, nullptr, request.get(), rawResponse, done);
```

`RpcResponseContext`（`rpcprovider.cc:116-121`）：

```cpp
struct RpcResponseContext
{
    wevix_muduo::Connection::ConnectionPtr conn;
    google::protobuf::Message* response;
    uint64_t requestId;
};
```

**为什么用「一个结构」而不是「三个参数」**——`rpcprovider.h:27-28` 的注释：

```cpp
// protobuf::NewCallback 当前版本最多方便绑定两个参数
```

`NewCallback` 的模板参数展开有限制，打包成结构体是最省事的做法。

**第二步：接住所有权**（`rpcprovider.cc:415-420`）：

```cpp
void RpcProvider::SendRpcResponse(RpcResponseContext* context)
{
    std::unique_ptr<RpcResponseContext> autoReleaseContext(context);        // context 本体
    std::unique_ptr<google::protobuf::Message> autoRelease(context->response);  // 业务 response
    ...
}
```

**两个 `unique_ptr` 在最顶上声明**，之后的**所有 return 路径**（序列化失败、超过 64MB、发送失败、成功）都自动释放——**不会泄漏**。

**第三步：`request` 的处理**（注意对比）：

```cpp
std::unique_ptr<google::protobuf::Message> request(
    service->GetRequestPrototype(method).New());
request->ParseFromString(args_str);
...
service->CallMethod(method, nullptr, request.get(), rawResponse, done);
// request 是 unique_ptr，CallMethod 返回后自动析构
```

**`request` 用 `unique_ptr` 局部管理**（同步调用返回就释放），**`response` 用 `release()` 移交**（因为要活到 `done->Run()`）。

**这个不对称性体现了「同步 vs 异步」的区别**：
- `request` 只在「分发期间」需要 → 局部 `unique_ptr` 够；
- `response` 要活到「业务处理完」→ 必须移交所有权。

**面试官想听什么**

- 能说清「`release()` 移交 + `unique_ptr` 接住」这个**所有权交接模式**
- 知道「两个 `unique_ptr` 声明在最顶上」是为了**覆盖所有 return 路径**（和 `RpcLatencyGuard` 是同一个思路）
- 知道 `request` 和 `response` 生命周期管理的**不对称性**及原因

**可能追问**

- Q5.3.1 `done` 回调如果没被调用会怎样？

#### Q5.3.1 `done` 回调如果没被调用会怎样？

**答**：**请求会永久悬挂，客户端一直等到超时——而且服务端的 `context` 和 `response` 会内存泄漏。**

**机制**：`done` 是个 `google::protobuf::Closure`，它的 `Run()` 方法会调到 `SendRpcResponse(context)`。**只有 `SendRpcResponse` 被调用，`context` 和 `response` 才会被 `unique_ptr` 释放、客户端才会收到响应。**

如果 handler 里忘了调 `done->Run()`：

```text
服务端：业务处理完了 → 但没调 done->Run() → context/response 永远不释放（泄漏）
                                              → 客户端永远收不到响应
客户端：等到 SO_RCVTIMEO（默认 5 秒）→ RPC_TIMEOUT
```

**这里有个必须知道的分工**（`rpcprovider.h:44-49` 的注释写得很清楚）：

```cpp
//   - 同步方法：protobuf 生成的默认 CallMethod 在返回前自动调用 done->Run()，
//     框架无需额外处理，响应在 CallMethod 返回时已发出。
//   - 异步方法：业务覆写 CallMethod 后，必须在异步操作完成时手动调用
//     done->Run()。漏调会导致请求永久悬挂，客户端一直等到超时。
```

**关键区别**：

| 实现方式 | 谁调 `done->Run()` | 会不会漏 |
|---|---|---|
| **同步方法**（不覆写 `CallMethod`） | protobuf 生成的默认实现**自动调用** | **不会漏** |
| **异步方法**（覆写 `CallMethod`） | **业务代码自己调** | **可能漏** |

**为什么同步方法不会漏**：protobuf 生成的 service 基类的默认 `CallMethod` 长这样（示意）：

```cpp
void Service::CallMethod(const MethodDescriptor* method, RpcController* controller,
                         const Message* request, Message* response, Closure* done)
{
    // 默认实现：调业务实现的虚函数，然后自动 Run
    CallMethod(method, controller, request, response, done);
    if (done) done->Run();      // ← 自动调用
}
```

而业务实现的是 `Service` 的**业务虚函数**（比如 `void ScheduleJob(RpcController*, const ScheduleJobRequest*, ScheduleJobResponse*, Closure*)`），它不需要碰 `done`。

**`video_platform` 的 5 个服务全部是同步实现**——所以这条风险在实践中没有触发。

**如果真的要写异步方法**，正确的模式是：

```cpp
void MyServiceImpl::AsyncMethod(RpcController* ctrl, const Request* req,
                                 Response* resp, Closure* done)
{
    // 保存 done，发起异步操作
    thread_pool.Submit([this, req_copy, resp, done]() {
        // ... 异步处理 ...
        resp->set_result(...);
        if (done) done->Run();      // ← 必须记得
    });
}
```

**问题在于**：`request` 是框架的局部 `unique_ptr`，`CallMethod` 返回时就析构了——**异步场景下 `req` 会悬垂**！所以异步实现必须**拷贝 request**。这正是审计报告里指出的「**服务端 `request` 对象生命周期与异步 done 不兼容**」问题。

**所以当前框架对异步的支持是「不完整」的**——`done` 机制在，但 `request` 的生命周期管理没跟上。要用异步必须自己拷贝 request。

**面试官想听什么**

- 知道「同步自动调 / 异步手动调」的分工，以及**漏调的后果**（悬挂 + 泄漏）
- 能指出**异步场景下 `request` 生命周期不兼容**这个更深的问题

---

## 六、服务注册发现：ZooKeeper

### Q6.1 ZK 的节点结构是怎么设计的？

**答（30 秒口述版）**

**四层路径，永久节点做容器、临时顺序节点做实例。**

```text
/mprpc                                       ← 永久（根）
  /services                                  ← 永久（服务集合）
    /SchedulerService                        ← 永久（服务名）
      /ScheduleJob                           ← 永久（方法名）
        /instance-0000000001                 ← 临时顺序节点（实例，value = "ip:port"）
        /instance-0000000002
```

路径规则（`mprpcchannel.cc:222-232`）：

```cpp
std::string MethodRegistryPath(const std::string& svc, const std::string& m)
{
    return "/mprpc/services/" + svc + "/" + m;
}
// 兼容旧版注册路径 /Svc/Method（读取时兜底）
std::string LegacyMethodPath(const std::string& svc, const std::string& m)
{
    return "/" + svc + "/" + m;
}
```

注册代码（`rpcprovider.cc:234-269`）：

```cpp
// service_name 和 method_name 为永久节点，instance-* 为临时顺序节点。
std::string service_path = "/mprpc/services/" + sp.first;
zkCli.Create(service_path.c_str(), nullptr, 0);              // state=0 → 永久

std::string method_path = MethodRegistryPath(sp.first, mp.first);
zkCli.Create(method_path.c_str(), nullptr, 0);               // 永久

char method_path_data[128] = {0};
sprintf(method_path_data, "%s:%d", advertise_ip.c_str(), port);
std::string instance_path = method_path + "/instance-";
std::string actualPath;
zkCli.Create(instance_path.c_str(), method_path_data, strlen(method_path_data),
             ZOO_EPHEMERAL | ZOO_SEQUENCE, &actualPath);      // ← 临时 + 顺序
LOG_INFO("Register rpc instance: %s -> %s", actualPath.c_str(), method_path_data);
```

**四个设计决定，逐个说理由**：

**① 为什么「永久节点做容器、临时节点做实例」**：

| 节点类型 | 用在哪 | 理由 |
|---|---|---|
| **永久** | `/mprpc`、`/services`、`/服务名`、`/方法名` | 这些是**结构**，不该因为某个 Provider 下线就消失 |
| **临时** | `instance-*` | Provider **进程退出或会话超时**时自动删除——**天然的故障检测** |

**临时节点是这里最妙的设计**：Provider 崩溃（`kill -9`、OOM）时**没机会做任何清理**，但 ZK 会因为会话超时（30 秒）自动删掉它的临时节点。**客户端不需要任何额外的健康检查机制**，只要查 ZK 就能拿到存活实例。

**② 为什么是「顺序」节点**：

`ZOO_SEQUENCE` 让 ZK **服务端**自动在节点名后追加一个单调递增的序号（`instance-0000000001`）。好处是：
- **多个实例不会冲突**——两个 Provider 同时用 `instance-` 前缀创建，ZK 保证它们拿到不同的名字；
- **客户端拉取时可以排序**（`mprpcchannel.cc:286-349`）：

```cpp
std::vector<std::string> children = zk.GetChildren(method_path.c_str());
std::sort(children.begin(), children.end());   // instance-0000000001 有序 → 轮询稳定
```

排序让「轮询选实例」的行为在多个客户端之间**一致**（都从序号小的开始），避免了「每个客户端随机顺序导致负载不均」。

**③ `value` 存 `ip:port`**：

实例节点的数据就是 `"172.18.0.12:9002"` 这样的字符串。客户端拿到 children 名字后，还要 `GetData` 取每个节点的 value 才知道实际地址。

**④ 注册地址用的是 `advertise_ip` 而不是配置的 `rpcserverip`**（`rpcprovider.cc:167-176`）：

```cpp
// 阶段 13：服务发现地址（注册到 ZK）——rpcserverip 为 0.0.0.0/空
// （Docker 全接口监听）时，探测本机实际 IP，否则消费者连 0.0.0.0 必失败
std::string advertise_ip = ip;
if (advertise_ip.empty() || advertise_ip == "0.0.0.0")
{
    advertise_ip = mprpc::GetLocalIp();
    if (!advertise_ip.empty())
        LOG_INFO("rpcserverip=%s (container mode), advertise ip=%s for service discovery", ...);
}
```

**这是 Docker 部署时实测踩到的坑**：`rpcserverip=0.0.0.0` 是合法的**监听地址**（表示监听所有网卡），但**不是可路由的地址**——消费者拿到 `0.0.0.0:9002` 去连接会失败（实测 `errno 111`）。

修复是：**监听用配置的地址，注册用探测出来的真实 IP**。`GetLocalIp()` 用 `getifaddrs()` 取第一个非 loopback 的 IPv4（`mprpcutil.h:28-47`）：

```cpp
inline std::string GetLocalIp()
{
    struct ifaddrs* ifa = nullptr;
    if (getifaddrs(&ifa) != 0) return "";
    std::string result;
    for (struct ifaddrs* p = ifa; p; p = p->ifa_next)
    {
        if (!p->ifa_addr || p->ifa_addr->sa_family != AF_INET) continue;
        const auto* sa = reinterpret_cast<const struct sockaddr_in*>(p->ifa_addr);
        char buf[INET_ADDRSTRLEN] = {0};
        if (!inet_ntop(AF_INET, &sa->sin_addr, buf, sizeof(buf))) continue;
        std::string ip(buf);
        if (ip == "127.0.0.1" || ip == "0.0.0.0") continue;
        result = ip;   // 取第一个非 loopback 网卡地址
        break;
    }
    freeifaddrs(ifa);
    return result;
}
```

用 `getifaddrs()` 而不是 `ioctl(SIOCGIFCONF)`：**无状态、无 fd、可重入**，也不用管 `struct ifreq` 的大小限制。

**⚠️ 一个已知的脆弱点**：`GetLocalIp` 取的是「**第一个**非 loopback IPv4」，**不排序、不优选**。多网卡机器上（比如同时有 `eth0`、`docker0`、`eth1`）可能选到错误的网卡。**更严谨的做法**是优先选默认路由对应的网卡（可以读 `/proc/net/route` 找 `default` 那一行的接口）。

**面试官想听什么**

- 能画出**四层结构**，并说清「永久节点做容器、临时节点做实例」的理由（**临时节点 = 天然的故障检测**）
- 知道 `ZOO_SEQUENCE` 解决的是**多实例命名冲突**，以及排序带来的**轮询一致性**
- 知道 `0.0.0.0` 不能注册（Docker 实测踩的坑）以及 `GetLocalIp` 的作用

---

### Q6.2 服务发现的三级缓存是怎么设计的？

**答（30 秒口述版）**

**本地缓存（30s TTL）→ Redis 集中缓存（多进程共享）→ ZooKeeper。**

```
MprpcChannel::CallMethod
  └─ GetHostData(method_path, legacy_path)
       ├─ ① 本地 ServiceCache（unordered_map，30s TTL）  ← 命中率最高，零网络
       ├─ ② Redis HGET mprpc:endpoints {method_path}    ← 多进程共享
       └─ ③ ZooKeeper GetChildren + GetData             ← 兜底，并双写回前两级
```

**数据结构**（`mprpcchannel.cc:208-245`）：

```cpp
constexpr char kEndpointsHashKey[] = "mprpc:endpoints";
constexpr int64_t kEndpointCacheTtlSec = 30;

struct EndpointCacheEntry
{
    std::vector<std::string> endpoints;
    size_t nextIndex = 0;
    int64_t fetchedAtMs = 0;    ///< 本地缓存写入时间，超 TTL 视为过期
};
```

**三级查找**（`mprpcchannel.cc:364-414`）：

```cpp
// ① 本地缓存：持 ServiceCacheMutex，判 TTL
if (NowMs() - entry.fetchedAtMs <= kEndpointCacheTtlMs)
{
    host_data = entry.endpoints[entry.nextIndex++ % entry.endpoints.size()];
    fromCache = true;
}
// ② 本地 miss/过期 → Redis
else if (redis.enabled())
{
    redis.HGet(kEndpointsHashKey, methodPath, csv, found);   // ← 不持本地缓存锁（HGET 是网络操作）
    if (found) { ParseCsvEndpoints(csv, ...); /* 写回本地缓存 */ }
}
// ③ 兜底 → ZK
else { host_data = PickEndpoint(QueryEndpointList(methodPath, legacyMethodPath)); }
```

**三个设计要点**：

**① 本地缓存「命中即零网络」**——这是最重要的性能点。热路径上（每次 RPC）只做一次哈希查找 + 一次时间比较。

`mprpcchannel.cc:208-213` 的注释写明了 TTL 的统一性：

```cpp
// 本地缓存与 Redis 统一 30s TTL：实例变更（新 Provider 注册/下线）最多 30s 生效
```

**② Redis 是「多进程共享的发现结果」**。为什么需要它——因为每个消费者进程**各自有一份本地缓存**：

```text
进程 A 的本地缓存：{"Scheduler.ScheduleJob" → ["10.0.0.1:9002", "10.0.0.2:9002"]}
进程 B 的本地缓存：{"Scheduler.ScheduleJob" → ["10.0.0.1:9002"]}    ← 30s 前拉的旧数据
进程 C 的本地缓存：...
```

如果没有 Redis，**每个进程都要自己去 ZK 拉一次**——N 个进程就是 N 倍的 ZK 压力。有了 Redis，**同一份数据只拉一次、写进 Redis、所有进程共享**。

**③ 缓存的失效是「主动 + 被动」双轨**。

**主动失效**（`mprpcchannel.cc:418-430`）——连接失败时：

```cpp
void InvalidateHostData(const std::string& methodPath)
{
    { /* 清本地 map */ }
    redis.HDel("mprpc:endpoints", methodPath);
    // 其他进程的缓存同时失效，下次发现重新拉 ZK，
    // 避免集体轮询到已下线的 endpoint
}
```

**这里有个精妙点**：`HDEL` 清的是 **Redis 里的共享字段**，所以**所有进程的缓存会同时失效**——不需要 watch 机制，也不会产生「一个进程失效了、其他进程还在用旧地址」的时间差。

**被动失效**：30s TTL 自然过期。这是**兜底**——**即使 Redis 完全不可用**（`HDEL` 失败只是 `LOG_DEBUG`），各进程的本地缓存在 30 秒后也会自己过期，然后重新拉 ZK。**TTL 是自愈机制**。

**失效的触发条件有两个**（都在 `CallMethod` 里）：
1. `ParseHostData` 失败（`host_data` 格式非法）——说明缓存里存了坏数据；
2. **连接级调用失败**——说明这个 endpoint 可能已经挂了。

**面试官想听什么**

- 能画出**三级结构**，并说清每级存在的理由（本地=快、Redis=跨进程共享、ZK=权威）
- 知道 `HDEL` 清的是共享字段，**让所有进程同时失效**（避免失效风暴时间差）
- 知道 TTL 是**自愈兜底**——即使 Redis 挂了也能恢复

**可能追问**

- Q6.2.1 为什么不用 ZK 的 Watcher 做实时推送？

#### Q6.2.1 为什么不用 ZK 的 Watcher 做实时推送？

**答**：**因为 Watcher 是「一次性」的，维护成本远高于 TTL 拉模型。**

**我的代码里确实没注册 watcher**——发现路径的 watcher 参数全是 0（`ZookeeperUtil.cc:324`、`360`）：

```cpp
// 异步获取节点数据（watcher=0：不设置 Watcher 监听）
zoo_aget(m_zhandle, path, 0, GetDataCb, ctx);
```

**如果要用 Watcher，必须处理这些问题**：

**① Watcher 是一次性的**。ZK 的 watcher 触发一次后就失效，**必须重新注册**。所以每次收到通知，都要再调一遍 `zoo_awget_children(..., watcher, ...)`。漏了这次重注册，就**永久失去通知**。

**② 触发风暴**。如果 1000 个客户端都 watch 同一个节点，某个 Provider 下线时会**同时触发 1000 个回调**。ZK 会承受瞬时压力，而且这 1000 个客户端会同时去拉取最新列表——**又在 ZK 上制造一次洪峰**。

**③ 回调在 ZK 的 watcher 线程里执行**。要访问框架的缓存（`ServiceCache()`），需要**跨线程投递到自己的线程**（`runInLoop` 那一套）。这引入了线程安全问题（缓存的锁、生命周期）。

**④ 会话过期需要重新注册所有 watcher**。ZK 会话过期（30 秒）后，所有 watcher 都失效了，必须重新建立。

**对比 TTL 拉模型**：

| 维度 | Watcher 推送 | TTL 拉模型 |
|---|---|---|
| 实时性 | **秒级**（变更立即可见） | 最多 30s 延迟 |
| 实现复杂度 | 高（重注册、线程跳转、风暴处理） | **低**（就一个时间比较） |
| ZK 压力 | 变更时爆发 | **平稳**（每 30s 一次，且被 Redis 进一步摊薄） |
| 故障恢复 | 会话恢复后要重注册 | **自动**（TTL 到期自然重拉） |

**我选 TTL 的理由**：**「实例变更最多 30 秒生效」这个延迟，对我的业务是可以接受的。**

想一下实际场景：一个新的 `transcode_worker` 上线，**最多 30 秒后**才会被 Scheduler 调度。这个延迟相对「一次视频转码要几十秒」来说完全可以忽略。而 Provider 下线的情况更宽松——因为**即使客户端拿到了已下线的地址，连接会失败，然后触发「失效缓存 + 重新发现 + 重试一次」**（`mprpcchannel.cc:959-985`），**实际感知时间远小于 30 秒**。

**这就是「最终一致性」的取舍**：用「30 秒内可能拿到旧地址」换「实现简单 + ZK 压力平稳 + 自动恢复」。

**如果将来真的需要秒级感知**（比如有严格的容量调度需求），我会用 **Redis 的 pub/sub 或者专门的配置中心**来做推送，而不是 ZK Watcher——因为 Redis 已经在架构里了，而且 pub/sub 没有 ZK watcher 那些一次性/会话绑定的麻烦。

**面试官想听什么**

- 知道 ZK Watcher 是**一次性**的，以及重注册的负担
- 能说出 Watcher 的**三个具体麻烦**（重注册、触发风暴、线程跳转）
- 能论证「30s 延迟可接受」——而且指出**实际感知时间远小于 TTL**（因为连接失败会立刻触发重试链路）

---

### Q6.3 ZK 客户端封装有什么特别之处？

**答（30 秒口述版）**

**核心是「用异步 API + 信号量超时，替换掉会无限阻塞的同步 API」。**

`ZookeeperUtil.cc:11-28` 的文件头注释就是完整背景：

```cpp
// 背景：zoo_get / zoo_get_children 是同步 API，内部阻塞等待 watcher 线程
// 收到响应。若 ZK 连接处于「会话未建立/断线重连」状态（zookeeper_init 后
// 连接握手未完成，或重连中），同步调用**无限阻塞**——实测 SchedulingLoop
// 卡死 7+ 分钟（线程 wchan=futex_wait_queue）。
// 修复：改用 zoo_aget / zoo_aget_children（异步）+ 信号量 3s 超时等待；
// 超时后调用方按失败返回，ctx 交由回调线程延迟释放（caller_gone 标记），
// 杜绝悬垂指针。入口先查 zoo_state：非 CONNECTED 直接失败（快速路径）。
```

**三层防护**：

**① 入口快速失败**（`ZookeeperUtil.cc:79-89`）：

```cpp
static bool ZooConnected(zhandle_t* zh, const char* what)
{
    if (zh == nullptr) return false;
    int state = zoo_state(zh);
    if (state != ZOO_CONNECTED_STATE)
    {
        LOG_WARN("zookeeper not connected (state=%d), skip %s", state, what);
        return false;
    }
    return true;
}
```

**`zoo_state()` 是唯一不会阻塞的查询入口**——先查一下连接状态，不正常就立刻返回失败，**连 3 秒都不用等**。

**② 异步 API + 信号量超时**（`ZookeeperUtil.cc:62-76`）：

```cpp
static bool WaitZooAsync(ZooAsyncCtx* ctx, int64_t wait_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += wait_ms / 1000;
    ts.tv_nsec += (wait_ms % 1000) * 1000000;
    if (ts.tv_nsec >= 1000000000) { ts.tv_sec += 1; ts.tv_nsec -= 1000000000; }
    int r = 0;
    while ((r = sem_timedwait(&ctx->sem, &ts)) == -1 && errno == EINTR) {}
    return r == 0;
}
```

调用方（`ZookeeperUtil.cc:333-340`）：

```cpp
if (!WaitZooAsync(ctx, 3000))
{
    // 3s 无响应：按发现失败返回（调用方已有重试/失效缓存回退）；
    // ctx 由回调线程最终回收（caller_gone 标记），不泄漏不悬垂
    ctx->caller_gone.store(true, std::memory_order_release);
    LOG_WARN("zookeeper get data timeout (3s), path:%s", path);
    return "";
}
```

**③ `caller_gone` 延迟回收防 UAF**（`ZookeeperUtil.cc:31-42`）：

```cpp
static void GetDataCb(int rc, const char* value, int value_len,
                      const struct Stat* /*stat*/, const void* data)
{
    auto* ctx = const_cast<ZooAsyncCtx*>(static_cast<const ZooAsyncCtx*>(data));
    ctx->rc = rc;
    if (rc == ZOK && value != nullptr && value_len > 0)
        ctx->data.assign(value, value_len);
    sem_post(&ctx->sem);
    // 调用方已超时离开：回调负责回收（调用方不再碰 ctx，无竞态）
    if (ctx->caller_gone.load(std::memory_order_acquire))
        delete ctx;
}
```

**`ZooAsyncCtx` 的定义**（`ZookeeperUtil.cc:21-28`）：

```cpp
struct ZooAsyncCtx
{
    int rc = -1;
    std::string data;
    std::vector<std::string> children;
    sem_t sem;
    std::atomic<bool> caller_gone{false};
};
```

**`caller_gone` 的核心是「单向所有权」**：

| 路径 | 谁销毁 ctx |
|---|---|
| **成功**（`sem_timedwait` 返回 0） | **调用方**：`sem_destroy` + `delete` |
| **超时**（3 秒没等到） | **回调线程**：`sem_post` 后检查 `caller_gone` → `delete` |

**注意超时路径上调用方「什么都不做」**——它**不 `delete`**（因为回调可能还在用），**也不 `sem_destroy`**（因为回调可能正在 `sem_post` 一个即将销毁的信号量，那是 UB）。

**这就把「失败模式」设计成了「泄漏」而不是「UAF」**——两个失败方向里，**泄漏是可接受的，UAF 是崩溃**。

**内存序**：调用方 `store(release)`，回调 `load(acquire)`——保证「回调看到 `caller_gone == true`」时，调用方之前的所有写都可见。

**面试官想听什么**

- 知道**同步 API 在会话未建立时会无限阻塞**，以及这是实测踩出来的（`wchan=futex_wait_queue`，卡 7+ 分钟）
- 能说清 `caller_gone` 的**单向所有权**设计，以及为什么超时路径不能 `sem_destroy`
- 知道 `zoo_state()` 是**唯一不会阻塞**的查询

**可能追问**

- Q6.3.1 `caller_gone` 这个方案真的没问题吗？

#### Q6.3.1 `caller_gone` 这个方案真的没问题吗？

**答**：**有两个残余窗口，我诚实说明——而且我为此专门检查过。**

**窗口一：成功路径的理论 UAF。**

时序是这样：

```text
调用方：sem_timedwait 返回 0（成功）
调用方：sem_destroy(&ctx->sem); delete ctx;        ← 销毁
回调线程（此时还没跑完）：sem_post 已返回，接着执行 ctx->caller_gone.load()
                          ↑ 访问已释放的内存 → UAF
```

**为什么存在**：回调在 `sem_post` 之后还要读一次 `caller_gone`。如果调用方在 `sem_post` 之后、`caller_gone.load()` 之前就 `delete ctx` 了，回调就访问了已释放内存。

**这个窗口有多窄**：需要 `sem_timedwait` 返回、调用方执行 `sem_destroy + delete`、**全部发生在回调线程从 `sem_post` 返回后再执行一条 `load` 指令的时间里**。在真实调度下这几乎不可能发生，但**理论上存在**。

**窗口二：超时路径的理论泄漏。**

```text
回调线程：sem_post 返回
回调线程：ctx->caller_gone.load()  → 读到 false（因为调用方还没 store）
调用方：ctx->caller_gone.store(true)
调用方：return ""                              ← 不 delete（等回调删）
回调线程：因为没有 caller_gone 为 true，不 delete，直接返回
```

**两边都不删 → 泄漏**。

**怎么修才能彻底解决**：需要一个**原子的「谁负责销毁」的仲裁**。标准做法是用引用计数 + `fetch_sub`：

```cpp
struct ZooAsyncCtx {
    std::atomic<int> refs{2};      // 调用方 1 个，回调 1 个
    void Release() { if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this; }
};

// 调用方
if (!WaitZooAsync(ctx, 3000)) {
    LOG_WARN(...);
    ctx->Release();      // ← 放弃自己的引用，由回调负责最后一个
    return "";
}
// 成功：也调 ctx->Release()，但此时回调可能已经释放过了 → 由引用计数保证只删一次
ctx->Release();
sem_destroy 需要延后……

// 回调
sem_post(&ctx->sem);
ctx->Release();
```

**但 `sem_destroy` 的时机还是麻烦**——因为它不能在回调还在用 `sem` 的时候销毁。把它们打包进一个对象、由最后一个 `Release` 统一销毁才行。

**我为什么没这么做**：

1. **两个窗口都极窄**。窗口一是几条指令的竞态；窗口二是「回调恰好抢在 `store` 之前读」。
2. **失败模式是「泄漏几十字节」和「理论上可能 UAF」**——我判断在当前调用频率下（每次 RPC 发现失败才触发）风险可接受。
3. **修复的复杂度不低**（引用计数 + `sem` 生命周期捆绑），而且要仔细验证所有路径。

**但这是个真实的技术债，我不会说它「没问题」。** 如果要做到生产级，我会用引用计数方案，或者更彻底地——**用 `std::future`/`std::promise` 替代裸信号量**，让标准库管理同步原语的生命周期。

**另外还有个更简单的替代方案**：**ctx 不用堆分配，而是用「调用方栈上的对象 + 回调里只 `memcpy` 数据、不 delete」**——但这样回调仍然可能访问已销毁的栈对象。**所以根本问题还是「谁活得更久」**，没有捷径。

**面试官想听什么**

- 能**自己指出**两个残余窗口，并**构造出具体的时序**
- 能给出**正确的修法**（引用计数仲裁销毁责任）
- 能说清**为什么没做**（窗口极窄 + 修复复杂度），但**不说「所以没问题」**

---

## 七、连接池

### Q7.1 连接池是怎么设计的？

**答（30 秒口述版）**

**按 endpoint（`ip:port`）分片，每个 endpoint 一个 `vector<shared_ptr<PooledConnection>>`，每连接一把 mutex。**

**数据结构**（`mprpcchannel.cc:473-521`）：

```cpp
struct PooledConnection
{
    bool EnsureConnected(int64_t timeoutMs, int& savedErrno);
    void Close() { if (fd >= 0) { ::close(fd); fd = -1; } }

    std::string key;     // "ip:port"
    std::string ip;
    uint16_t port;
    int fd = -1;
    std::mutex mutex;    // ← 整次 send+recv 往返都持这把锁
};

// 外层 map 按 key 分片
std::unordered_map<std::string, std::vector<std::shared_ptr<PooledConnection>>>& ConnectionPool();
std::unordered_map<std::string, size_t>& ConnectionPoolNextIndex();
std::mutex& ConnectionPoolMutex();
```

**三条关键逻辑**：

**① 分片 key = `ip:port`**（`EndpointKey()`）：

```cpp
static std::string EndpointKey(const std::string& ip, uint16_t port)
{
    return ip + ":" + std::to_string(port);
}
```

**分片的意义**：某个对端出故障（连接全断）时，**只影响这个 endpoint 的连接池**，不会波及其他服务。如果不分片（全局一个大池子），一个坏服务可能把整个池子拖死。

**② 借用逻辑——懒建 + 轮询**（`mprpcchannel.cc:632-650`）：

```cpp
std::shared_ptr<PooledConnection> GetPooledConnection(const std::string& ip, uint16_t port)
{
    std::lock_guard<std::mutex> lock(ConnectionPoolMutex());
    auto& connections = ConnectionPool()[EndpointKey(ip, port)];
    size_t maxConnections = MaxConnectionsPerEndpoint();

    if (connections.size() < maxConnections)
    {
        // 池未满：新建一条（fd = -1，不在这里 connect）
        auto conn = std::make_shared<PooledConnection>();
        conn->key = ...; conn->ip = ip; conn->port = port;
        connections.push_back(conn);
        return conn;
    }
    // 池满了：轮询复用
    size_t& next = ConnectionPoolNextIndex()[key];
    return connections[next++ % connections.size()];
}
```

**注意「懒建连接」**：`GetPooledConnection` **只是创建一个空的 `PooledConnection` 对象**（`fd = -1`），**不发起 connect**。真正的 `connect` 发生在 `EnsureConnected`（在 `SendRequestAndReadResponse` 里）。

**为什么懒建**：**让「取连接」和「用连接」分离**。因为 `CallMethod` 里可能有多个候选 endpoint，如果取的时候就连，可能建了一堆用不上的连接。**建连成本只在真正要发数据时才付出**。

**③ 最大连接数可配**（`mprpcchannel.cc:463-471`）：

```cpp
static size_t MaxConnectionsPerEndpoint()
{
    if (!MprpcApplication::IsInitialized()) return 8;      // 未 Init 时不碰配置
    return MprpcApplication::GetConfig().LoadInt(
        "mprpcclient_connections_per_endpoint", 8, 1, 128);
}
```

默认 8，范围 1~128。

**面试官想听什么**

- 能说清**按 endpoint 分片**的意义（故障隔离）
- 知道「取连接」和「建连接」是**分离**的（懒建）
- 知道 `PooledConnection::mutex` 覆盖整次 send+recv，以及它带来的**并发上限**

**可能追问**

- Q7.1.1 那单个服务的并发 RPC 上限是多少？

#### Q7.1.1 单个服务的并发 RPC 上限是多少？

**答**：**等于连接池大小，默认 8。**

**推导过程**：`SendRequestAndReadResponse` 的第一行就是**锁住整条连接**（`mprpcchannel.cc:725`）：

```cpp
bool SendRequestAndReadResponse(std::shared_ptr<PooledConnection> conn, ...)
{
    std::lock_guard<std::mutex> lock(conn->mutex);      // ← 整次往返都持锁
    if (!conn->EnsureConnected(timeoutMs, savedErrno)) { ... }
    if (!SetSocketTimeout(conn->fd, timeoutMs, savedErrno)) { ... }
    if (!SendAll(conn->fd, requestFrame.data(), requestFrame.size(), savedErrno)) { ... }
    // 读 4 字节长度
    // 读帧体
    ...
}
// 函数返回，锁释放
```

**从 `SendAll` 到「读完整帧」全程持锁**——所以**一条连接在同一时刻只能有一个请求在途**。

而一个 endpoint 最多有 `mprpcclient_connections_per_endpoint`（默认 8）条连接。所以：

```text
单个 endpoint 的并发 RPC 上限 = 连接池大小 = 8（默认）
```

**第 9 个并发调用者会怎样**：`GetPooledConnection` 会轮询返回某条连接，然后它在 `conn->mutex` 上**阻塞排队**，直到前一个请求完成。

**这个设计是「同步模型」的必然结果**：

| 方案 | 并发能力 | 复杂度 |
|---|---|---|
| **每连接互斥（当前）** | = 池大小 | **低**——天然无响应错配 |
| 连接内多路复用（HTTP/2 式） | 无上限 | 高——需要 stream_id 映射、乱序响应处理 |
| 每请求一条连接 | 无上限 | 中——但建连开销大 |

**「每连接一把锁」的最大好处是：永远不会出现响应错配。** 因为一条连接上同时只有一个请求在途，收到的响应**必然**是它的。

**如果要做多路复用**，就必须维护「`request_id` → 等待中的调用者」的映射表，允许响应乱序到达——这正是 gRPC over HTTP/2 做的事，复杂度高一个量级。

**这个上限的实际影响**：`video_platform` 的场景里，单个消费者对单个 provider 的并发通常不大（Scheduler 调 Worker 是逐个的），所以 8 够用。**但如果要压测单个服务的极限 QPS**，8 就是天花板。

**实测数据佐证**（【记录】开发日志）：

| 模式 | QPS | 说明 |
|---|---|---|
| `--direct --keepalive -c 100 -m 1000` | **84.7 万** | 长连接复用 |
| `--direct -c 500 -m 20`（短连） | 6.2 万 | 每次新建连接 |

长连接能到 84.7 万，说明**在连接数足够时（100 并发 = 100 条连接 > 8）**，瓶颈不在池大小上——因为池会建到 8 条然后轮询。等等，这里有意思：100 并发但池只有 8 条连接，为什么还能到 84.7 万？

因为**每个请求的耗时很短**（P50 56 微秒），所以 8 条连接每秒能处理 `8 / 56µs ≈ 14 万` QPS 每条……不对，是 `8 条 / 56µs = 14.3 万`——嗯，84.7 万 / 8 条连接 = 每条连接 10.6 万 QPS，也就是每个请求约 9.4 微秒。这和 P50 56 微秒对不上。

**这说明压测时实际的 endpoint 数不止 1 个**，或者连接池的行为和我描述的略有出入。**这个数字的解读我需要谨慎**——我只能说「长连接模式下实测 84.7 万 QPS」，具体连接池怎么分布的我没有深究。

**面试官想听什么**

- 能从「`mutex` 覆盖整次往返」推导出「并发上限 = 池大小」
- 能对比三种并发模型（每连接互斥 / 多路复用 / 每请求连接）的**复杂度和能力**
- 知道「每连接互斥」的核心价值是**天然无响应错配**

---

### Q7.2 连接池的死连接问题是怎么处理的？

**答（30 秒口述版）**

**不做探活检测，而是「失败时清掉整个 endpoint 的池」——因为逐条检测的代价更高。**

**问题背景**：服务端的 `EventLoop` 会**主动回收空闲连接**（`wevix_muduo` 的 subLoop 每 5 秒扫一遍，10 秒没活跃就 `forceClose`）。所以客户端连接池里**可能积压多条「本地 fd 有效、但对端已经关闭」的死连接**。

**客户端的 `EnsureConnected` 只检查 `fd >= 0`**，不做 `getsockopt`/`ping` 探活：

```cpp
bool PooledConnection::EnsureConnected(int64_t timeoutMs, int& savedErrno)
{
    if (fd >= 0) return true;      // ← 只判 fd，不探活
    fd = ConnectToEndpoint(ip, port, timeoutMs, savedErrno);
    return fd >= 0;
}
```

**所以死连接是靠「发数据时失败」发现的**——`SendAll` 或 `RecvAll` 返回错误。

**发现失败后的处理是「整片清池」**（`mprpcchannel.cc:959-985`）：

```cpp
// 连接级失败：清掉该 endpoint 的整个连接池再重试一次。
// 背景：服务端 EventLoop 会回收空闲连接，客户端池中因此可能积压
// 多条「本地 fd 仍有效但对端已关闭」的死连接，逐条轮转失败代价高
// （池越大恢复越慢）。清池后重建连接立即恢复。
DropEndpointConnections(pooledConn->key);
if (use_direct_) { ... }
else
{
    InvalidateHostData(method_path);      // ← 同时失效服务发现缓存
    std::string retry_host = PickEndpoint(QueryEndpointList(method_path, legacy_method_path));
    // 用新 endpoint 重建连接，重试一次
}
```

**为什么是「清池」而不是「只关这一条」**：

假设池里有 8 条连接，其中 5 条是死的。如果只关当前这条：

```text
请求1 → 连接1 → 失败 → 关连接1 → 重试 → 拿到连接2 → 失败 → 关连接2 → ...
```

**最多要失败 8 次才能找到一条活连接**——而每次失败都包含一次完整的 `send`/`recv` 尝试和超时等待。

**清池的话**：

```text
请求1 → 连接1 → 失败 → 清空整个池 → 重试 → 拿到全新连接 → 成功
```

**一次失败就恢复**。代价是「把可能还活着的连接也关掉了」——但那 3 条活连接重建的成本，远低于「逐条试错」的成本。

**这个设计的洞察是**：**死连接是「批量出现」的**，因为服务端回收空闲连接是**周期性的、批量性的**（一次扫描把超时的都关掉）。所以「池里有一部分是死的」比「池里只有一条是死的」更常见。

**同时失效服务发现缓存**：连接失败可能意味着「这个 endpoint 已经下线了」，所以顺带 `InvalidateHostData` 清掉发现缓存（本地 + Redis），下次重新从 ZK 拉。

**⚠️ 但这里有个我注意到的问题**：重试的触发条件里**包含 `RPC_TIMEOUT`**：

```cpp
if (!callOk &&
    (callErrorCode == mprpc::RPC_CONNECT_FAILED ||
     callErrorCode == mprpc::RPC_TIMEOUT ||        // ← 超时也会重试
     ...))
```

**`RPC_TIMEOUT` 的语义是模糊的**：
- 可能是「连接建不上」（连接级失败，重试合理）；
- 也可能是「**请求已经发出去了、服务端可能已经执行了，只是响应没回来**」（重试会导致**重复执行**）。

**这就是 RPC 框架的 at-least-once 语义**——框架层不保证幂等（它做不到，因为无法区分「未执行」和「已执行未响应」）。**幂等必须由业务层保证**，`video_platform` 是用 `attempt_id` 做的。

**面试官想听什么**

- 能说清「清池而不是逐条关」的**理由**（死连接是批量出现的）
- 知道**不做探活**是个取舍（探活也有成本，而且 `getsockopt` 探不出来半开的连接）
- 能**主动指出** `RPC_TIMEOUT` 也进重试、意味着 **at-least-once 语义**

**可能追问**

- Q7.2.1 那重试导致重复执行怎么办？

#### Q7.2.1 那重试导致重复执行怎么办？

**答**：**框架层不解决，交给业务层用「幂等键」解决。这是分层的正确做法。**

**为什么框架层解决不了**：考虑超时场景：

```text
客户端：[t0] 发送请求 ────────────────► [t5] 5 秒超时，报 RPC_TIMEOUT
                                     │
服务端：        [t1] 收到请求 → 开始处理 → [t10] 处理完，回响应
                                     ↑
                          客户端在 t5 就放弃了，但它不知道
                          服务端到底「没收到」还是「收到了正在处理」
```

**客户端面临的是「不确定状态」**——它无法区分：

- 情况 A：请求在网络上丢了 → 重试是**安全**的；
- 情况 B：服务端收到了、正在处理，只是慢 → 重试会**导致重复执行**。

**要区分这两种情况，需要「两阶段提交」或「事务 ID 查询」**——那会让协议复杂很多，而且需要服务端配合维护状态。

**所以 RPC 框架的通用语义是 at-least-once（至少一次）**，这不是我的框架的缺陷，而是**分布式系统的本质限制**。

**`video_platform` 是怎么做到「恰好一次生效」的**——**用 `attempt_id` 做幂等键**：

```cpp
// Scheduler 分配时生成（scheduler_service.cpp:997）
fresh.attempt_id = fresh.shard_id + "_attempt_" + std::to_string(fresh.retry_count);
// 形如 "job_xxx_shard_0_attempt_2"

// ResultCollector 收到结果时校验
int stored_retry   = parseRetryFromAttempt(shard.attempt_id);
int incoming_retry = parseRetryFromAttempt(attempt_id);
if (!shard.attempt_id.empty() && incoming_retry < stored_retry)
{
    LOG_INFO("...: rejecting stale result for shard %s", ...);
    response->set_accepted(false);
    return;      // ← 旧 attempt 的结果直接丢弃
}
```

**`attempt_id` 本质上是个「执行代次（generation）」**：
- 每次重试，`retry_count` 递增，`attempt_id` 变化；
- 服务端（RC）记录「当前接受的代次」；
- **旧的代次上报结果 → 拒绝**（因为已经有更新的代次在执行了）；
- **同一代次重复上报 → 幂等接受**（不会重复推进状态）。

**三层防重**（proto 注释里写明了设计意图）：

| RPC | 参数 | 防什么 |
|---|---|---|
| `RescheduleShard` | `attempt_id`（`scheduler.proto:42`） | 「重复 FAILED 上报触发的二次 RescheduleShard 会被拒绝，防止 retry_count 双倍消耗」 |
| `CancelShard` | `attempt_id`（`worker.proto:96`） | 「非空时仅取消『当前正执行该 attempt』的 shard」——防止误杀新执行 |
| `ReportShardResult` | 结果里的 `attempt_id` | 「只接受当前 attempt 的结果」 |

**所以完整的分层是**：

| 层 | 保证 | 手段 |
|---|---|---|
| RPC 框架 | **at-least-once**（可能重复） | 重试 |
| 业务层 | **恰好一次生效** | `attempt_id` 幂等键 + 状态 CAS |

**面试官想听什么**

- 能说清「超时后客户端处于**不确定状态**」——这是 at-least-once 的**根本原因**
- 知道这不是框架缺陷，而是**分布式系统的本质限制**
- 能说出业务层是怎么补的（`attempt_id` 作为**执行代次**）

---

## 八、超时、重试与 deadline

### Q8.1 超时是怎么实现的？

**答（30 秒口述版）**

**分三个环节，每个环节用不同的机制。**

| 环节 | 机制 | 代码位置 |
|---|---|---|
| **TCP 建连** | 非阻塞 connect + `poll(POLLOUT, timeoutMs)` | `ConnectToEndpoint` |
| **发送** | `SO_SNDTIMEO` | `SetSocketTimeout` |
| **接收** | `SO_RCVTIMEO` | `SetSocketTimeout` |

**① 建连超时用的是「非阻塞 connect + poll」**（`mprpcchannel.cc:525-619`）：

```cpp
int fd = ::socket(AF_INET, SOCK_STREAM, 0);
int flags = ::fcntl(fd, F_GETFL, 0);
::fcntl(fd, F_SETFL, flags | O_NONBLOCK);              // 设非阻塞

int ret = ::connect(fd, ...);
if (ret < 0 && errno == EINPROGRESS)                    // 握手进行中
{
    struct pollfd pfd{fd, POLLOUT, 0};
    int pr = ::poll(&pfd, 1, timeoutMs);                // ← 我自己控制的超时
    if (pr == 0) { savedErrno = ETIMEDOUT; /* 清理 */ }

    int err = 0; socklen_t len = sizeof(err);
    ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len); // ← 取真实错误
    if (err != 0) { savedErrno = err; /* 清理 */ }
}
::fcntl(fd, F_SETFL, flags);                            // 恢复标志
SetSocketTimeout(fd, timeoutMs, savedErrno);
```

**为什么要非阻塞**：**因为要用自己的超时，而不是内核的。**

内核的 TCP 连接超时是 **75 秒**（`tcp_syn_retries=6` 的指数退避）。对 RPC 来说完全不可用。非阻塞 connect 让 `connect` 立刻返回 `EINPROGRESS`，然后我用 `poll` **自己等**——超时时间完全可控。

**为什么要 `getsockopt(SO_ERROR)`**：`connect` 返回 `EINPROGRESS` 时**不知道成功还是失败**（握手还没完成）。`poll` 返回可写之后，必须查 `SO_ERROR` 才知道结果。这是非阻塞 connect 的标准流程，注释里也写了：`非阻塞 connect 的真实错误需要从 SO_ERROR 读取`。

**② 收发超时交给内核**（`mprpcchannel.cc:97-122`）：

```cpp
static bool SetSocketTimeout(int fd, int64_t timeoutMs, int& savedErrno)
{
    struct timeval tv;
    tv.tv_sec = static_cast<time_t>(timeoutMs / 1000);
    tv.tv_usec = static_cast<suseconds_t>((timeoutMs % 1000) * 1000);

    if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) { ... }
    if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) { ... }
    return true;
}
```

**用 `SO_RCVTIMEO` 的好处**：**等响应的时候不需要我做超时轮询**。`recv` 到时间没数据就返回 `EAGAIN`，我把它映射成 `RPC_TIMEOUT`（`IoErrorCode`）：

```cpp
static int IoErrorCode(int savedErrno, int defaultCode)
{
    if (savedErrno == ETIMEDOUT || savedErrno == EAGAIN || savedErrno == EWOULDBLOCK)
        return mprpc::RPC_TIMEOUT;
    return defaultCode;
}
```

**③ 长连接复用时要「每次刷新超时」**（`mprpcchannel.cc:735-742`）：

```cpp
// 长连接复用时，每次调用都按当前 timeoutMs 刷新 socket 选项
if (!SetSocketTimeout(conn->fd, timeoutMs, savedErrno)) { ... }
```

**因为 `SO_RCVTIMEO` 是 socket 级别的属性**，会被后续的 `setsockopt` 覆盖。不同请求可能配了不同的超时（比如 `SetTimeoutMs(3000)` 和默认 5000），所以每次调用都得重设。

**面试官想听什么**

- 能说清「为什么建连要非阻塞」——**因为内核默认 75 秒超时不可接受**
- 知道 `SO_ERROR` 是必须查的（非阻塞 connect 的标准流程）
- 知道长连接要**每次刷新超时**，因为 `SO_RCVTIMEO` 是 socket 属性

**可能追问**

- Q8.1.1 这些 `setsockopt` 调用不会有性能问题吗？

#### Q8.1.1 每次调用都做 `setsockopt`，不会有性能问题吗？

**答**：**有，这是我项目里一个明确的可优化点。**

**开销分析**：每次 `CallMethod` 的发送前，会调用 `SetSocketTimeout`，里面是**两次 `setsockopt`**（`SO_SNDTIMEO` + `SO_RCVTIMEO`）。加上非阻塞 connect 时的两次 `fcntl`（`F_GETFL` + `F_SETFL`），**每次 RPC 有 4 次左右的额外系统调用**。

系统调用在 Linux 上是**几百纳秒到 1 微秒**量级（取决于是否命中 vDSO、是否有 Spectre/Meltdown 缓解）。而我的 RPC P50 是 **56 微秒**（【记录】长连接压测）。

**4 次 `setsockopt` 大约占 P50 的 3~8%**——所以**不是主要瓶颈，但确实是可以省掉的**。

**为什么现在没省**：因为「不同请求可能配不同的超时」——最简单的正确做法就是每次都设。**要优化的话，有两种方案**：

**方案一：缓存「上次设置的值」，只在变化时才调**：

```cpp
struct PooledConnection {
    ...
    int64_t currentTimeoutMs = -1;      // 记录当前 socket 上的超时值
};

// 只在变化时设置
if (conn->currentTimeoutMs != timeoutMs) {
    SetSocketTimeout(conn->fd, timeoutMs, savedErrno);
    conn->currentTimeoutMs = timeoutMs;
}
```

**但这样有个问题**：如果连接被重建（`fd` 变了），缓存就失效了。所以 `Close()` 和 `ConnectToEndpoint` 里都要重置 `currentTimeoutMs = -1`。

**方案二：干脆用固定超时**。如果业务上所有调用都用同一个超时（比如统一 5 秒），那只需要在 `ConnectToEndpoint` 里设一次——**后续完全不用设**。

**但 `video_platform` 里的超时是多样的**：
- `Scheduler` 调 `Worker.QueryShard`：`SetTimeoutMs(3000)`
- `Scheduler` 调 `Worker.AssignShard`：5 秒
- `JobClient` 调 `JobService`：`SetTimeoutMs(5000)`
- `Scheduler` 调 `WorkerManager.ListWorkers`：`SetTimeoutMs(3000)`

所以方案二不适用。

**我倾向于方案一**——它保留了灵活性，同时消除了「值没变还重复设置」的浪费。预估收益 **P50 降 3~8%、QPS 提升 5~10%**（这是审计报告里的估算，不是我实测的）。

**面试官想听什么**

- 能**量化**这个开销（4 次 syscall vs P50 56 微秒）
- 给出**具体的优化方案**，并指出各自的适用条件
- 说清「为什么现在没做」（超时值多样 + 需要处理连接重建）

---

### Q8.2 重试是怎么做的？

**答（30 秒口述版）**

**只重试一次，而且两条分支走不同的恢复路径。**

**触发条件**（`mprpcchannel.cc:953-957`）：

```cpp
if (!callOk &&
    (callErrorCode == mprpc::RPC_CONNECT_FAILED ||
     callErrorCode == mprpc::RPC_TIMEOUT ||
     callErrorCode == mprpc::RPC_SEND_FAILED ||
     callErrorCode == mprpc::RPC_RECV_FAILED))
```

**只有网络层的 4 个错误码会触发重试**（见 Q1.2.1 的分层）。

**两条恢复路径**（`mprpcchannel.cc:959-985`）：

```cpp
// 连接级失败：清掉该 endpoint 的整个连接池再重试一次。
DropEndpointConnections(pooledConn->key);

if (use_direct_)
{
    // 直连模式：没有别的 endpoint 可选，重建连接
    pooledConn = GetPooledConnection(ip, port);
    callOk = SendRequestAndReadResponse(...);
}
else
{
    // 服务发现模式：失效缓存 + 重新发现 + 用新 endpoint 重试
    InvalidateHostData(method_path);
    std::string retry_host = PickEndpoint(QueryEndpointList(method_path, legacy_method_path));
    // ... ParseHostData 成功后重建连接重试
}
```

**两条路径的区别**：

| 模式 | 重试时的动作 | 为什么 |
|---|---|---|
| **direct 直连** | 清池 + 重建连接 | 目标地址是**固定的**（`MprpcChannel(ip, port)` 构造），没有别的 endpoint 可选 |
| **ZK 发现** | 清池 + **失效缓存** + **重新发现** + 可能换 endpoint | 目标可能已经下线了，需要拉最新列表 |

**`direct` 直连模式的用途**（`mprpcchannel.h:11-14` 注释）：

```cpp
// Scheduler 需要调用特定 Worker 的 AssignShard，而非随机轮询
```

**这是个重要的场景**：`Scheduler` 要把 shard 分配给**指定的** Worker（`AssignShard` 是定向调用），不能走服务发现（那会随机选一个 Worker，可能不是它想分配的那个）。所以需要「绕过 ZK、直连指定地址」的能力。

**为什么只重试一次**（`zookeeper缓存系统.md` 的设计说明）：

> 避免在 ZK 故障或全集群宕机时进入死循环

如果无限重试：ZK 完全挂了 → 每次调用都重试 → 线程全部卡在重试循环里 → **雪崩**。重试一次是「给瞬时抖动一次机会」，但不是「无限等待」。

**面试官想听什么**

- 能说清「只有网络层错误进重试白名单」
- 能说清 **direct 模式和 ZK 模式的重试路径不同**（因为可选的 endpoint 集合不同）
- 知道 `direct` 模式的存在理由是「定向调用」（`AssignShard` 必须发给指定的 Worker）
- 知道「只重试一次」是为了**避免雪崩**

---

## 九、配置与初始化

### Q9.1 框架初始化是怎么设计的？

**答（30 秒口述版）**

**`MprpcApplication::Init(argc, argv)` 解析命令行和配置文件，失败返回 `bool` 而不是 `exit()`。**

```cpp
// mprpcapplication.cc:20-87（大意）
bool MprpcApplication::Init(int argc, char** argv)
{
    // getopt 使用全局状态，测试或同进程多次 Init 时需要重置。
    optind = 1;
    m_initialized = false;

    if (argc < 2) { ShowArgsHelp(); return false; }

    while ((c = getopt(argc, argv, "i:")) != -1)
    {
        case 'i': config_file = optarg; break;
        case '?': case ':': ShowArgsHelp(); return false;
    }

    if (!m_config.LoadConfigFile(config_file.c_str())) return false;

    // 必填项校验（fail-fast）
    if (!m_config.LoadRequired("zookeeperip", value, error)) { LOG_ERROR(...); return false; }
    if (m_config.LoadInt("zookeeperport", -1, 1, 65535) == -1) { LOG_ERROR(...); return false; }

    m_initialized = true;
    return true;
}
```

**关键设计：返回 `bool` 而不是 `exit()`**。

这是**从教训里改出来的**。最初的版本在初始化失败时直接 `exit(EXIT_FAILURE)`——问题是：

1. **框架替业务做了决策**。业务可能想「ZK 连不上就降级运行」（虽然 `mprpc` 目前不支持，但框架不该阻断这个可能）；
2. **排查困难**。多服务启动脚本里，一个服务静默退出，脚本看到的是「进程没了」但不知道为什么；
3. **不好测试**。单元测试里没法测「初始化失败」的路径（一测进程就退出了）。

改成返回 `bool` 之后，**业务层自己决定怎么办**：

```cpp
if (!MprpcApplication::Init(argc, argv)) { return EXIT_FAILURE; }
```

（`video_platform` 的 5 个服务都是这个模式——**决定「失败就退出」的是业务，不是框架**。）

**同样的改动也应用到了 `RpcProvider::Run()`**。

**配置校验的两个层次**：

```cpp
// LoadRequired：必填，空了就失败
bool MprpcConfig::LoadRequired(const std::string& key, std::string& value, std::string& error);

// LoadInt：带范围校验，非法就返回默认值（不 fail-fast）
int MprpcConfig::LoadInt(const std::string& key, int default_value, int min, int max);
```

**注意 `LoadInt` 的行为**：非法时**打 WARN 并返回默认值**，不 fail-fast：

```cpp
LOG_WARN("config key %s invalid (%s), use default=%d", ...);
```

**为什么两种行为不同**：
- `zookeeperip` **必填**——没有它根本不知道连哪，必须失败；
- `rpcserverwork_threads` **有默认值**——配置错了用默认值就好，没必要让服务起不来。

**面试官想听什么**

- 知道「框架返回 `bool` 而不是 `exit()`」这个**设计原则**（框架不替业务做决策），以及它的三个好处
- 能区分 `LoadRequired` 和 `LoadInt` 的**不同失败语义**（fail-fast vs 用默认值）

**可能追问**

- Q9.1.1 配置文件解析有什么坑？

#### Q9.1.1 配置文件解析有什么坑？

**答**：**踩过两个坑，都是「过度处理」导致的。**

**坑一：`#` 注释不能无条件截断。**

朴素写法是 `value.find('#')`，然后截掉后面所有内容。**但这样会破坏合法值**：

```ini
some_url = http://example.com/path#fragment       # URL 的 fragment 被截掉了！
```

**我的修复**是「只有 `#` 前面有空格才视为注释」（`mprpcconfig.cc`）：

```cpp
// 去掉行内注释（" #" 及之后的内容），如 "8080 # 端口号" → "8080"
// 仅在 # 前有空格时才视为注释，避免截断 URL fragment 等含 # 的合法值
size_t comment_pos = value.find(" #");
```

**这个改动的洞察是**：`#` 在**行首**（整行注释）和**空格后**（行内注释）是注释，但在**中间无空格**时可能是**值的一部分**。

**坑二：`operator[]` 会偷偷插入。**

```cpp
// 反例
std::string& MprpcConfig::Load(const std::string& key)
{
    return m_configMap[key];      // ← key 不存在时，会插入一个空字符串！
}
```

`std::map::operator[]` 的语义是「**不存在就默认构造并插入**」。所以每次查一个不存在的 key，`m_configMap` 就**多一个空条目**——**内存泄漏 + 污染**。而且 `Load` 返回的是引用，调用方看不出「这个 key 其实不存在」。

**我的修复**（`mprpcconfig.cc:101-111`）——注释里明确记录了这个坑：

```cpp
// return m_configMap[key]; 如果这样写，如果key不存在，他会自己向map里增加内容
```

改成用 `find`，未命中打 `LOG_WARN("config key not found: %s", ...)`。

**顺带说两个小的**：

**① `LoadConfigFile` 开头 `m_configMap.clear()`** —— 支持重复 Load 覆盖（测试里有用）。

**② 行尾 `\r` 要处理**：Windows 格式的配置文件（CRLF 行尾）会让 value 末尾多一个 `\r`。这会导致端口号解析失败之类的诡异问题。

**面试官想听什么**

- 知道 `operator[]` 会插入这个坑（**C++ 基础功**）
- 知道 `#` 截断要区分「行内注释」和「值的一部分」
- 能主动提到 CRLF 的坑

---

## 十、可观测性：自研 metrics 库

### Q10.1 为什么要自己写 metrics 库？

**答（30 秒口述版）**

**三个原因：项目哲学、依赖控制、以及「统一组件」的便利。**

**① 项目哲学是自研**。整个项目（网络库、RPC 框架、内存池、日志、metrics）都是从头实现的——用 `prometheus-cpp` 就破坏了这条主线。

**② 依赖控制**。`prometheus-cpp` 会引入额外的依赖（它依赖 `curl` 或 `civetweb` 做 HTTP 服务）。而我的项目已经有 `wevix_muduo` 了——**HTTP 服务可以用自己的 `TcpServer` 起**：

```cpp
// mprpcmetrics.cc:576-592
void MetricsHttpServer::RunHttpLoop()
{
    // 在线程内构造并 start：start() 阻塞在 mainLoop_->run() 直到 stop()
    server_ = std::make_unique<wevix_muduo::TcpServer>("127.0.0.1", (uint16_t)port_, 1);
    server_->setConnectionCallback(...);
    server_->setOnMessageCallback([this](const auto& conn, std::string& msg) { OnMessage(conn, msg); });
    server_->start();
}
```

**这同时也是一个「吃自己的狗粮」的验证**——用自研网络库提供 HTTP 服务，跑通了就说明网络库可用。

**③ 统一组件**。`prometheus-cpp` 只提供「指标注册 + 导出」，**不带采样器和告警**。而我需要的是一整套：

```text
指标库（Counter/Gauge/Histogram）
  + HTTP 抓取端点（/metrics）
  + Gauge 采样器线程（定期查数据库/系统状态）
  + 进程内告警循环（不依赖 Prometheus 也能发现异常）
```

自研的话，这四块可以在**一个组件里统一设计**——比如采样器和告警循环可以共享一个「单线程轮询多个不同周期任务」的调度器。

**规模也就 ~700 行**（`mprpcmetrics.h` 273 行 + `.cc` 686 行），而且 `text format 0.0.4` 是**公开规范**，不需要逆向。

**面试官想听什么**

- 能说出「用自家 `TcpServer` 起 HTTP」这个**自举**的点
- 知道自研的理由里「需要采样器和告警、现成库没有」这条最实际
- 知道 text format 是公开规范，不需要猜

**可能追问**

- Q10.1.1 那输出格式和 Prometheus 兼容吗？

#### Q10.1.1 输出格式和 Prometheus 兼容吗？

**答**：**兼容，输出的是标准 text format 0.0.4。**

**格式**（`mprpcmetrics.cc:326-396`）：

```cpp
os << "# HELP " << f->name << " " << f->help << "\n";
os << "# TYPE " << f->name << " " << f->type << "\n";
```

**Counter / Gauge 系列**：`name{labels} value`

**Histogram 系列**（最复杂的一块）：

```cpp
const std::string& labels = FormatLabels(kv.first);
const MetricHistogram* h = kv.second;
auto cum = h->CumulativeCounts();
// labels 含完整 "{...}"（可能为空串）。直方图系列需要附加
// le 标签：空 → "{le=...}"；非空 → 去掉 labels 结尾 '}'，
// 拼 ",le=..." 再闭合，得到 "{k=v,le=...}"
std::string base = labels.empty() ? "" : labels.substr(0, labels.size() - 1);
for (size_t i = 0; i < f->buckets.size(); ++i)
{
    os << f->name << "_bucket" << base
       << (labels.empty() ? "{le=\"" : ",le=\"")
       << FormatLe(f->buckets[i]) << "\"} "
       << cum[i] << "\n";
}
os << f->name << "_bucket" << base << ... << "+Inf\"} " << cum.back() << "\n";
os << f->name << "_sum" << labels << " " << FormatValue(h->Sum()) << "\n";
os << f->name << "_count" << labels << " " << h->Count() << "\n";
```

**实际输出长这样**：

```text
# HELP rpc_latency_ms RPC 调用延迟（毫秒）
# TYPE rpc_latency_ms histogram
rpc_latency_ms_bucket{method="SchedulerService.ScheduleJob",le="1.000000000"} 12
rpc_latency_ms_bucket{method="SchedulerService.ScheduleJob",le="5.000000000"} 30
...
rpc_latency_ms_bucket{method="SchedulerService.ScheduleJob",le="+Inf"} 42
rpc_latency_ms_sum{method="SchedulerService.ScheduleJob"} 1234.5
rpc_latency_ms_count{method="SchedulerService.ScheduleJob"} 42
```

**三个格式细节**：

**① `le` 输出 9 位小数**（`FormatLe` = `snprintf("%.9f")`，`mprpcmetrics.cc:96-101`）——注释写着 `le 桶上界输出为固定 9 位小数（Prometheus 约定）`。

**② 按 name 字典序输出**（`mprpcmetrics.cc:334-336`）：

```cpp
// 按 name 字典序输出，保证抓取结果确定性
```

**为什么要保证确定性**：如果不排序，`unordered_map` 的遍历顺序是不确定的，每次抓取输出的行顺序都不一样。这会让「对比两次抓取的结果」变得困难（diff 出来全是顺序变化），而且可能触发某些监控系统的误判。

**③ HTTP 响应的 `Content-Type` 带版本声明**（`mprpcmetrics.cc:658`）：

```cpp
resp += "Content-Type: text/plain; version=0.0.4\r\n";
```

**这个 `version=0.0.4` 就是 Prometheus text format 的版本号**——Prometheus 抓取时靠它识别格式。

**④ 同名不同类型会打 WARN 但继续**（`mprpcmetrics.cc:220-241`）：

```cpp
// 同名不同类型属于编程错误：警告但继续（返回已存在的族）
```

**这是「容错优先」的设计**——指标冲突不该让服务挂掉。

**实测验证**【记录】：`curl` 5 个服务的端口（9091-9097）全部返回 200，非法路径返回 404；Prometheus 实测 5 个 targets 全部 UP，4 条告警规则全部加载。

**面试官想听什么**

- 知道 `le` 要输出 9 位小数、`Content-Type` 要带 `version=0.0.4` 这些**格式细节**
- 知道「按字典序输出」是为了**抓取确定性**
- 知道 Prometheus 是**实际部署验证过**的（不只是格式对）

---

### Q10.2 直方图的分位数是怎么算的？

**答（30 秒口述版）**

**固定桶 + 导出时前缀和 + 线性插值。**

**桶边界**（`mprpcmetrics.cc:680`，用在 `rpc_latency_ms` 上）：

```cpp
std::vector<double>{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000}   // 单位 ms
```

**核心算法**（`mprpcmetrics.cc:103-124`）：

```cpp
double InterpolateQuantile(double q, const std::vector<double>& bounds,
                           const std::vector<uint64_t>& cum)
{
    if (bounds.empty() || cum.empty()) return 0;
    uint64_t total = cum.back();
    if (total == 0) return 0;

    double target = q * static_cast<double>(total);      // 目标位次
    if (target <= 0) return bounds.front();

    size_t i = 0;
    while (i < cum.size() && static_cast<double>(cum[i]) < target) ++i;   // 找第一个 ≥ target 的桶
    if (i >= cum.size()) return bounds.back();

    // 命中 +Inf 尾桶时上界取最后一个有限桶，插值限于末桶
    size_t hiIdx = std::min(i, bounds.size() - 1);
    if (i == 0) return bounds[0];

    double prevCum   = static_cast<double>(cum[i - 1]);
    double prevBound = (i == 1) ? 0.0 : bounds[i - 1];   // 首桶下界按 0 处理
    double frac = (target - prevCum) / static_cast<double>(cum[i] - prevCum);
    return prevBound + frac * (bounds[hiIdx] - prevBound);
}
```

**逐句解释**：

**① `cum` 是累计数组**。`cum[i]` = 「小于等于 `bounds[i]` 的观测数」。长度是 `buckets.size() + 1`（含 `+Inf` 尾桶）。所以 `cum.back()` 就是**总观测数**。

**② 为什么要前缀和**——因为**热路径上不做**（Q4.2.1 说过）：

```cpp
void MetricHistogram::Observe(double v)
{
    size_t idx = upper_bound(buckets_, v) - buckets_.begin();
    bucketCounts_[idx].fetch_add(1, memory_order_relaxed);   // ← 只写一个桶
    ...
}
```

**存的是「非累计」的命中数**，前缀和只在 `CumulativeCounts()` 里算（每次导出 O(N)，但导出只有 15 秒一次）。

**③ `target = q * total`** —— 要的第 q 分位对应的**位次**。比如 99 分位、1000 个观测，`target = 990`，即「第 990 小的那个值」。

**④ 找桶**：`while (cum[i] < target) ++i`——线性扫描找到第一个「累计数 ≥ target」的桶。**O(N)，但 N = 11 个桶**，无所谓。

**⑤ 线性插值**——**这一步是关键**：只知道「第 990 个落在 `[50, 100]` 这个桶里」是不够的，得**估计具体是多少**。

```cpp
double prevBound = (i == 1) ? 0.0 : bounds[i - 1];
double frac = (target - prevCum) / (cum[i] - prevCum);       // 桶内位置比例
return prevBound + frac * (bounds[hiIdx] - prevBound);       // 线性插值
```

**举例**：假设桶边界是 `[..., 50, 100, ...]`，`cum[i-1] = 980`（≤50ms 的有 980 个），`cum[i] = 995`（≤100ms 的有 995 个），`target = 990`。

- 第 990 个落在「50~100ms」这个桶里；
- 桶内有 `995 - 980 = 15` 个观测，第 990 个是桶内的第 `990 - 980 = 10` 个；
- `frac = 10 / 15 = 0.667`；
- 插值结果 = `50 + 0.667 × (100 - 50) = 83.3ms`。

**这就是「分布均匀假设下的线性插值」**——我们假设这个桶内的 15 个观测是均匀分布的。这个假设当然不完全准确，但**对于「看趋势、设告警阈值」这个目的足够了**。

**两种边界情况**：

- **`i == 0`**（target 落在第一个桶）：直接返回 `bounds[0]`——因为第一个桶的下界是 0（`prevBound` 逻辑里 `i == 1 → 0.0`，但 `i == 0` 时没有意义），不做插值。
- **`hiIdx = min(i, bounds.size()-1)`**：如果 `i` 落在 `+Inf` 尾桶，`bounds[i]` 会越界。所以把上界**钳制到最后一个有限桶**——也就是「超过最大桶的值，最多按最大桶估」。注释写着：`命中 +Inf 尾桶时上界取最后一个有限桶，插值限于末桶`。

**⑥ 还有一个「跨标签聚合」的版本**（`mprpcmetrics.cc:286-307`）：

```cpp
double MetricsRegistry::HistogramQuantile(const std::string& name, double q)
{
    // 把同一 name 下所有标签系列的累计数组逐位相加再插值（告警 p99 用）
}
```

**为什么需要它**：告警规则关心的是「**所有方法的整体 P99**」，而不是「某个方法的 P99」。所以要先把所有 label 的 `cum` 数组**逐位相加**，再做一次插值。

**这和 Prometheus 的 `histogram_quantile(sum by (le) ...)` 语义是一致的**——先按 `le` 聚合，再算分位。

**面试官想听什么**

- 能说清「热路径只写一个桶、前缀和推迟到导出」这个 **O(1) 写 vs O(N) 读**的交换
- 能**手算一遍**线性插值的过程——这说明你真的理解而不是背下来的
- 知道 `+Inf` 尾桶要钳制上界
- 知道「跨标签聚合要先逐位相加」——这是告警正确性的前提
