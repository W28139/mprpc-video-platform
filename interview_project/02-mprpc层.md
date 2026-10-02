# mprpc 框架 · 面试拷打题库

---

## 一、整体设计

### Q1.1 介绍一下 mprpc 框架

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
│  业务层：video_platform 的 5 个微服务          │  ← 只写 service 实现
├─────────────────────────────────────────────┤
│  RPC 语义层：RpcProvider / MprpcChannel       │  ← 分发、序列化、发现、重试
│              MprpcController（错误码/超时）    │
├─────────────────────────────────────────────┤
│  协议层：mprpccodec.h（帧格式 + 拆帧）          │  ← 粘包/半包、长度合法性校验
│          rpcheader.proto（15 个错误码）       │
├────────────────────────────────────────── ──┤
│  传输层：wevix_muduo（TcpServer/EventLoop/…） │  ← epoll、IO 线程、Buffer
└─────────────────────────────────────────────┘
```

**关键设计是「协议层独立于传输层」**（`mprpccodec.h` 是纯 header-only 的）：

它只做「字节串 ↔ 字节串」的转换，和 muduo 的耦合点只有 `RpcMessageCodec(Buffer*, std::string&)` 这个适配函数——`Buffer` 通过裸指针接触、`CodecResult` 是网络库定义的编解码结果枚举，都不是继承关系。

**这样的好处**：如果哪天换掉网络库（比如换成 `boost::asio`），**协议层一行都不用改**，只需要重新写一个 `Buffer` 的适配。

#### Q1.2.1 那 15 个错误码是怎么分的？

**答**：按**故障层次**分三组（`mprpc/src/rpcheader.proto`）：

**第一组：网络/传输层（4 个）**——「连接没打通」

**第二组：协议层（5 个）**——「连接通了但数据不对」

**第三组：服务治理层（5 个）**——「协议没问题，但服务找不到」

（完整枚举见 `rpcheader.proto:6-27`，`RPC_SUCCESS = 0`。）

**为什么要这么分**：因为**不同层的错误，处理方式完全不同**：

| 层次 | 客户端应该做什么 |
|---|---|
| 网络层 | **重试**（换个连接/endpoint 可能就好了） |
| 协议层 | **不重试**（重试还是同样的坏包，说明是代码 bug） |
| 治理层 | **失效缓存 + 重新发现**（服务可能扩容/下线了） |

---

## 二、协议设计

### Q2.1  RPC 协议帧格式是什么样的？

```text
[total_len(4B, 网络序)] [payload]
 ←──────── total_len 覆盖的范围（不含自身 4 字节）────────→
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

**② 全部用网络序（大端）**，集中在 2 个 inline 函数里（`mprpccodec.h:20-39`）：

读侧还带长度保护（`mprpccodec.h:30-33`）：`len < sizeof(uint32_t)` 直接返回 false，注释写着 `避免坏包触发越界读取`。

#### Q2.1.1 为什么是「两层帧头」？一层不够吗？

**答**：**两层帧头由两个不同的层消费，服务不同的目的。**

```text
[total_len] ← 网络层（RpcMessageCodec）消费：只管「这一帧有多长」
    [header_size] ← 应用层（RpcProvider::OnMessage）消费：解析出 service/method/args
```

**外层 `total_len` 归网络层**：

`RpcMessageCodec` 挂在 `Connection` 上，它**只知道长度、不解析内容**，它的职责边界是「**把字节流切成帧**」——**帧里面是什么它不关心**。

**内层 `header_size` 归应用层**：

服务端拿到完整的一帧后，才知道「前 4 字节是 header 长度、接下来是 RpcHeader、剩下的是 args」：

**为什么要这么分**——**因为协议演进的需求不同**：

- **网络层只关心「帧有多长」**。即使我换了整个应用层协议（比如把 protobuf 换成 JSON、把 RpcHeader 结构全改了），**`total_len` 这一层完全不用动**——`RpcMessageCodec` 的逻辑一行都不用改。
- **应用层只关心「帧里是什么」**。它可以自由改变 payload 的内部结构（加字段、换序列化方式），不影响网络层的拆帧。

**如果只有一层会怎样**：假设只有 `total_len`，那网络层就得知道「payload 前 4 字节是 header 长度」——**网络层就绑定了应用层协议**。将来 payload 结构一变，网络层也得跟着改，而网络层是被所有连接共用的。

---

### Q2.2 `RpcHeader` 里都有什么？为什么要这些字段？

4 个字段（`rpcheader.proto:29-37`）：

```proto
message RpcHeader {
    bytes  service_name = 1;   // 要调哪个服务
    bytes  method_name  = 2;   // 要调哪个方法
    uint64 request_id   = 4;   // 请求 ID，用于匹配响应
    uint64 deadline_ms  = 6;   // 请求的失效时刻（绝对时间戳）
}
```

**逐个说用途**：

| 字段 | 用途 | 不用它会怎样 |
|---|---|---|
| `service_name` + `method_name` | 服务端据此**查找 method descriptor** | 没法分发 |
| `request_id` | 客户端**校验响应归属** | 长连接上无法区分「这是哪个请求的响应」（串包） |
| `deadline_ms` | 服务端**快速拒绝过期请求** | 客户端已超时，服务端还在傻算 |

**重点说两个**：（后面回头看）

**① `request_id` 是长连接的必需品**：

```cpp
// mprpcchannel.cc:69-73
uint64_t NextRequestId()
{
    static std::atomic<uint64_t> nextRequestId{1};
    return nextRequestId.fetch_add(1, std::memory_order_relaxed);
}
```

客户端发请求前生成，收到响应后**必须校验**（`mprpcchannel.cc:945-955`）：

```cpp
if (responseHeader.request_id() != requestId)
{
    // RPC_INVALID_RESPONSE
}
```

**没有这个校验会怎样**：连接池里的连接是复用的，如果上一次请求超时了、响应又姗姗来迟，客户端在同一个连接上发起新请求时，可能**读到上一个请求的迟到响应**——数据全错，而且很难查。`request_id` 让这种错配立刻暴露。

**注意 `request_id` 的作用域**：它是**进程内**递增的（`static` 变量），不是全局唯一。但因为校验只发生在「同一个连接、同一个进程」内，**够用**。

**② `deadline_ms` 是「绝对时刻」而不是「超时时长」**：

```cpp
// mprpcchannel.cc:807
if (timeoutMs > 0)
{
    rpcHeader.set_deadline_ms(NowMs() + static_cast<uint64_t>(timeoutMs));
}
```

传绝对时刻（`now + timeoutMs`）而不是时长，是因为服务端在进行message操作时，会不断**需要判断「这个请求现在还有没有意义」**——而这个判断必须在同一时间基准上做。如果传时长，服务端还得知道「客户端是什么时候发的」才行

---

## 三、拆帧：RpcMessageCodec

### Q3.1 粘包和半包是怎么处理的？

**用长度前缀法。**核心函数就 27 行（`mprpccodec.h:60-86`）：

**三种情况分开处理**：

**① 半包（数据不够）**：**返回 `kNeedMoreData`，一个字节都不消费**。数据留在 `Buffer` 的 readable 区，等下次 `handleRead` 追加数据后再试。

**② 粘包（一次读到多帧）**：由**调用方循环**处理（`Connection::handleRead`）

**③ 长度非法：返回 `kFatal`，由 `Connection` 关闭连接。**

**零拷贝的细节**：用 `peek()` **只读长度不消费**（`Buffer.h:28`），确认帧完整后才 `retrieve(4)` + `retrieveAsString(total_len)`。**先看后取**，避免读了半个帧就破坏了缓冲状态。

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
2. **二进制数据里可能有任意字节**。如果用 `\r\n` 做分隔，而 protobuf 序列化后的字节里**恰好出现了 `\r\n`**（完全可能），就会**提前切断帧**。
3. **O(1) 判断 vs O(N) 扫描**。长度前缀只需要读 4 个字节就能判断「够不够一帧」；分隔符方案必须扫描到分隔符才能判断。

**什么时候该用状态机**：协议里有**嵌套结构**（比如 JSON 的 `{}` 配对）、或者**转义序列**（比如某些协议用 `0x7E` 做帧边界，数据里的 `0x7E` 要转义成 `0x7D 0x5E`）。这时候需要状态机记住「我是不是在转义态」。

**我的协议**：定长头部 + 长度前缀，**没有嵌套、没有转义**——所以一个 `if` 就够了，不需要状态机。

---

### Q3.2 codec 是怎么挂到网络层上的？

**三层传递：`TcpServer` 保存 → `TcpServer` 分配给每个新 `Connection` → `Connection::handleRead` 循环调用。**

**① 注册**（`rpcprovider.cc`）：

```cpp
server.setMessageCodec(RpcMessageCodec);
```

**② 分发给新连接**（`TcpServer.cpp:97`）：

```cpp
if (messageCodec_) conn->setMessageCodec(messageCodec_);
```

**③ 在 `handleRead` 里循环调用**（`Connection.cpp:96-135`）：

**`handleClose()` 后面那行 `return` 不是可有可无的**：关闭会经由 `closeCallback_` → `TcpServer::handleClose` → `removeConnection` 把连接从 `connections_` 里摘掉，之后再访问 `this` 就可能踩空。之所以安全，是因为 `handleRead` 开头有个 `ConnectionPtr self(shared_from_this())` 兜住了生命周期——**不然会发生 UAF 崩溃**。

**关键设计：这是可选的**。

`messageCodec_` 是个 `std::function`，默认是空的（`Connection.h:56`）：

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
```

**为什么是三态而不是 `bool`**：`bool` 只能表达「成帧 / 没成帧」，**没法区分「数据不够」和「流坏了」**——而这两者的处理完全相反，前者必须原样保留 Buffer，后者必须关连接。

#### Q3.2.1 codec 在哪个线程执行？业务处理又在哪个线程？

**拆帧在 IO 线程，业务处理在 work 线程。**

**IO 线程只做「拆帧」**——这是纯内存操作（移动 Buffer 指针 + 构造 `std::string`），快且不会阻塞。

**业务处理放 work 线程**——因为 `RpcProvider::OnMessage` 会：
1. 反序列化 protobuf（可能很慢，大消息尤其）；
2. **调用用户的 service 方法**——这个方法可能做任何事（查数据库、调外部服务、甚至同步等待）。

**如果业务在 IO 线程跑会怎样**：一个慢的 RPC handler 会**阻塞这个 subLoop 上所有连接的收发**。而且 `video_platform` 的 `SchedulerService::ScheduleJob` 会调 `ffprobe` 探测视频时长（最长 15 秒）——那 15 秒里，同一 loop 上的连接全部卡死。

---

## 四、客户端：MprpcChannel

### Q4.1 `MprpcChannel::CallMethod` 的完整流程是什么？

按步骤编号（`mprpcchannel.cc:760-981`）：

| 步 | 做什么 |
|---|---|
| 0 | 从 `method` 取 service 名和方法名；生成 `requestId`；读取超时配置 |
| 1 | `request->SerializeToString(&args_str)`（args 不单独校验长度，见下） |
| 2 | 组 `RpcHeader`（service/method/request_id/deadline_ms），序列化 + 64MB 校验 |
| 3 | 拼 payload：`[header_size] + RpcHeader + args`，**总长** 64MB 校验，再套外层帧 |
| 4 | **服务发现**（direct 直连分支 / ZK 三级缓存分支） |
| 5 | 从连接池取连接 → `SendRequestAndReadResponse` |
| 5.1 | 连接级失败 → **清池 + 重试一次** |
| 6 | 失败 → 设置 controller 错误码 |
| 7 | 解析响应帧 |
| 8 | **校验 `request_id`** |
| 9 | 检查 `error_code` |
| 10 | `response->ParseFromString(...)` → 成功则 `done->Run()` |

#### Q4.1.1 客户端是同步还是异步的？为什么？（逻辑不是很清晰）

调用mprpcchannel端为客户端，他会利用连接池获取连接，直接调用connect

**答**：**当前是同步阻塞的，而且实现方式没走 Reactor。**

`MprpcChannel` 用的是**裸 socket**：`socket()` / `connect()` / `send()` / `recv()`，配上 `SO_SNDTIMEO` / `SO_RCVTIMEO` 做超时（`mprpcchannel.cc:494-588`）：

**这个实现方式值得解释，因为它是刻意的**：

**① 为什么不用「非阻塞 connect + 交给 epoll」**——因为框架的**调用方式是同步的**：

```cpp
// protobuf 生成的 stub 就是这么调的
stub.ScheduleJob(&controller, &request, &response, nullptr);
//                                                  ↑ done 传 nullptr
```

`done` 传 `nullptr` 意味着**调用方要等返回**。既然语义是同步的，就没必要引入「发起 connect → 注册 EPOLLOUT → 等待 → 回调里继续」这一整套异步状态机。

**② 非阻塞不是为了并发，非阻塞 connect 是为了「可控的超时」**。直接用阻塞 `connect` 的话，超时由**内核决定**——Linux 默认的 TCP 连接超时是 **75 秒**。

所以我用「非阻塞 connect + `poll(POLLOUT, timeoutMs)`」，**超时时间完全由我控制**。`poll` 返回后还要 `getsockopt(SO_ERROR)` 取真实错误——**这是非阻塞 connect 的标准流程**，因为 `connect` 返回 `EINPROGRESS` 时不知道成功还是失败，必须查 `SO_ERROR`。

**这个设计的不对称性**：服务端是纯 Reactor（`TcpServer` + epoll），客户端却是裸 socket 同步阻塞。**两边完全不对称**。

**为什么这样是可以接受的**：
- 客户端的通信模式是「一问一答」，没有并发多路复用需求；
- 裸 socket 的代码路径短、容易推理、不需要处理回调重入；
- 每次调用都在调用方自己的线程里阻塞，**不需要跨线程唤醒**。

---

## 五、服务端：RpcProvider

### Q5.1 ` ` 做了什么？为什么存裸指针？

**答**：**用自己建的两层 map 索引「service 名 → ServiceInfo」，`ServiceInfo` 里再索引「method 名 → MethodDescriptor」。**

**为什么存裸指针**

Provider 仅存储裸指针用于方法分发，不接管对象所有权，不会 delete service。
调用方必须保证 service 对象在 Provider 生命周期内一直有效。

**这是刻意的所有权约定**：Provider **不管** service 的生命周期。

业务层的实际用法就是 `main()` 里的栈对象：

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

**分发用的是自己建的 map，不是 protobuf 的反射查找**（`rpcprovider.cc:353-363`）：

```cpp
// 两次 unordered_map::find
auto sit = m_serviceMap.find(service_name);
auto mit = sit->second.m_methodMap.find(method_name);
```

**⚠️ `m_serviceMap` 是无锁的**。因为约定是「`NotifyService` 必须在 `Run()` 之前全部调完」——也就是**单线程初始化期**，运行期只读。这是个**隐式约定，没有断言保护**。

#### Q5.1.1 如果运行期想动态加服务怎么办？

**答**：**当前的实现做不到，这是个明确的边界。**

问题有两层：

**① `m_serviceMap` 无锁**。`NotifyService` 里直接 `m_serviceMap[service_name] = ...`，没有任何同步。运行期调用它会和 `OnMessage` 的读取**构成数据竞争**——`unordered_map` 在插入时可能 rehash，会让正在读的线程拿到悬垂引用。

**② ZK 注册也没做动态**。`Run()` 里注册一次 ZK 节点，之后就不再动了：

```cpp
// rpcprovider.cc:233-267
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

---

### Q5.2 服务端收到请求后做了哪些检查？

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

（`rpcprovider.cc:295-411`）

#### Q5.2.1 deadline 检查为什么放在 work 线程里？它能阻止什么、不能阻止什么？

**答**：**它能阻止「排队太久的请求被白算」，但不能中断已经在执行的 handler。**

**位置**（`rpcprovider.cc:326-340`）：

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

注释写着「目前未实现具体的功能」。`service->CallMethod(method, nullptr, ...)` 的 **controller 参数传的是 `nullptr`**（`rpcprovider.cc:410`）——**Provider 侧根本没把 controller 传给业务**，所以业务层也没有办法感知取消。

---

### Q5.3 `SendRpcResponse` 是怎么管理 response 对象生命周期的？

**用一个上下文结构打包「连接 + response + request_id」，把所有权通过 `release()` 移交，然后在 `SendRpcResponse` 开头用 `unique_ptr` 接住。**

**第一步：构造上下文**（`rpcprovider.cc:389-406`）：

```cpp
auto* context = new RpcResponseContext{conn, response.release(), requestId};
google::protobuf::Closure *done =
    google::protobuf::NewCallback<RpcProvider, RpcResponseContext*>(
        this, &RpcProvider::SendRpcResponse, context);
service->CallMethod(method, nullptr, request.get(), rawResponse, done);
```

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

#### Q5.3.1 `done` 回调如果没被调用会怎样？

done 是框架交给业务层的"回执按钮"：业务填完 response 后按下它（done->Run()），等于告诉框架"响应已备好，请发送并回收资源"。**所以 done->Run() = 发送响应 + 资源回收两件事打包成一个动作。**

**答**：**请求会永久悬挂，客户端一直等到超时——而且服务端的 `context` 和 `response` 会内存泄漏。**

**机制**：`done` 是个 `google::protobuf::Closure`，它的 `Run()` 方法会调到 `SendRpcResponse(context)`。**只有 `SendRpcResponse` 被调用，`context` 和 `response` 才会被 `unique_ptr` 释放、客户端才会收到响应。**

如果 handler 里忘了调 `done->Run()`：

```text
服务端：业务处理完了 → 但没调 done->Run() → context/response 永远不释放（泄漏）
                                              → 客户端永远收不到响应
客户端：等到 SO_RCVTIMEO（默认 5 秒）→ RPC_TIMEOUT
```

---

### Q5.4 为什么目前不是异步？如何改成异步？

**先分清概念**：同步 = 发起后原地等结果，等到才走下一步；异步 = 发起后立即返回，结果好了再由回调/事件通知。判断本项目到底是哪种，只有一条标准：**`done` 有没有在业务返回之后、别的时刻才被调用。**

**为什么现在是同步**——不是「done 后直接 return」，恰恰相反，是**所有事都在 return 之前就做完了**：

```cpp
// 服务端所有 handler 的通用形态（job_service.cpp 等，全仓库 53 处一致）
void JobServiceImpl::SubmitJob(...) {
    ...
    done->Run();     // ① 发送响应 + 回收资源，此刻同步完成（job_service.cpp:173）
}                    // ② 然后才 return —— 按按钮发生在 return 之前
```

- **服务端**：53 处 `done->Run()` 全部在 handler 返回前调用，`SendRpcResponse` 里的 `conn->send()` 也是同步发送，整条链路没有一刻「让出」CPU。
- **客户端**：`MprpcChannel::CallMethod` 阻塞在 `RecvAll` 循环里直到响应回来才 return（`mprpcchannel.cc:138`），调用线程全程被占。

所以 `done` 现在等价于一个「立即执行的发送钩子」——**延迟执行的能力在，但没有任何人使用它**。

**如何改成异步**：

1. **handler 不按按钮**：拿到 `done` 后存起来先返回，业务线程立刻空出来处理下一个请求；
2. **完成后补按**：耗时操作（下游 RPC / DB / FFmpeg）在别的线程结束后，那时才调 `done->Run()`，框架此时才发送响应；
3. **框架配合改生命周期**：`request` 必须移进 `RpcResponseContext`（现在它是栈上 `unique_ptr`，`rpcprovider.cc:360`，函数返回即释放，真异步会 UAF），`conn` 裸指针也要改成保活引用。

一句话本质：**把「等待」从线程转移到回调和状态上**。同步是拿线程换简单（每在途请求占 1 线程 + 1 连接），异步是拿复杂度换规模（单线程管万级在途请求）。

**本项目要不要做？不要**：瓶颈在 FFmpeg 子进程（CPU 密集、外部进程解耦），RPC 全是毫秒级控制面调用；业务层的异步（`SubmitJob` 立即返回 job_id → 轮询 / MQ 事件驱动）已经覆盖了真正需要异步的场景。

---

## 六、服务注册发现：ZooKeeper

### Q6.1 ZK 的节点结构是怎么设计的？

**永久节点做容器、临时顺序节点做实例**

```text
/mprpc/services                      ← 永久节点
  └── UserServiceRpc                 ← 永久节点（service_name）
      └── Login                      ← 永久节点（method_name）== methodPath
          ├── instance-0000000000    ← 临时顺序节点, data = "192.168.1.5:9001"
          └── instance-0000000001    ← 临时顺序节点, data = "192.168.1.5:9002"
```

路径规则（客户端 `mprpcchannel.cc:194-198`，服务端另有一份拷贝 `rpcprovider.cc:102`）：

```cpp
std::string MethodRegistryPath(const std::string& svc, const std::string& m)
{
    return "/mprpc/services/" + svc + "/" + m;
}
```

注册（`rpcprovider.cc:223-256`）：

```cpp
zkCli.Create("/mprpc", nullptr, 0);                 // state=0 → 永久
zkCli.Create("/mprpc/services", nullptr, 0);        // 永久
zkCli.Create(service_path.c_str(), nullptr, 0);     // /mprpc/services/{服务名}    永久
zkCli.Create(method_path.c_str(), nullptr, 0);      // /mprpc/services/{服务名}/{方法名}  永久

sprintf(method_path_data, "%s:%d", advertise_ip.c_str(), port);
// ← 实例节点：临时 + 顺序，value = "ip:port"
zkCli.Create((method_path + "/instance-").c_str(), method_path_data, strlen(method_path_data),
             ZOO_EPHEMERAL | ZOO_SEQUENCE, &actualPath);
```

**三个设计决定**：

| 决定 | 理由 |
|---|---|
| 永久做容器、临时做实例 | Provider 崩溃没机会清理，ZK 会话超时（30s）自动删临时节点——客户端不需要额外健康检查 |
| `ZOO_SEQUENCE` 顺序节点 | ① 多实例命名不冲突；② 客户端 `GetChildren` 后 `std::sort`（`mprpcchannel.cc:264-265`）再轮询，**所有客户端轮询顺序一致**，避免负载不均 |
| value 存 `ip:port` | 客户端拿到 children 名后还要 `GetData` 取实际地址 |

进程被强杀时，任何清理代码都没机会执行

后果是：死掉的实例注册永远留在 ZK 里（假如用的是永久节点），客户端会一直发现并连接这个死地址，连一次失败一次，没有任何自愈机制，只能人工上 ZK 删节点。

临时节点怎么绕开这个问题？

后台线程持续给 ZK 发心跳维持 session；

会话超时时间在本项目是 30s，进程一死，心跳停止。ZK 服务端等 30s 判定会话过期，然后由 ZK 自己删除该会话创建的所有临时节点

### Q6.2 服务发现的三级缓存是怎么设计的？

**本地缓存（30s TTL）→ Redis 集中缓存（多进程共享）→ ZooKeeper。**

```text
MprpcChannel::CallMethod
  └─ GetHostData(method_path)                         ← mprpcchannel.cc:318-367
       ├─ ① 本地 ServiceCache（unordered_map + 30s TTL）  ← 热路径零网络
       ├─ ② Redis HGET mprpc:endpoints {method_path}      ← 多进程共享
       └─ ③ ZooKeeper GetChildren + GetData               ← 兜底，并双写回前两级
```

**数据结构**（`mprpcchannel.cc:180-192`）：

```cpp
constexpr char kEndpointsHashKey[] = "mprpc:endpoints";
constexpr int64_t kEndpointCacheTtlSec = 30;   // 本地与 Redis 统一 TTL：实例变更最多 30s 生效

struct EndpointCacheEntry
{
    std::vector<std::string> endpoints;
    size_t nextIndex = 0;       // 轮询游标
    int64_t fetchedAtMs = 0;    // 本地缓存写入时间，超 TTL 视为过期
};
```

**三个要点**：

**① 本地缓存「命中即零网络」**——热路径上（每次 RPC）只做一次哈希查找 + 一次时间比较。

**② Redis 是「多进程共享的发现结果」**——每个消费者进程各有一份本地缓存，没有 Redis 时 N 个进程要拉 N 次 ZK；有了它，同一份数据只拉一次写进 `mprpc:endpoints`，所有进程共享。ZK 拉取后**双写**（`mprpcchannel.cc:276-297`）：先 `HSET + EXPIRE 30s` 写 Redis（**不持本地缓存锁**——网络操作 2s 超时，持锁会卡死发现热路径），再写本地缓存。

**③ 失效是「主动 + 被动」双轨**：

- **主动**（连接级失败触发，`mprpcchannel.cc:371-383`）：`InvalidateHostData` 清本地 map + `HDEL mprpc:endpoints {field}`。**HDEL 清的是 Redis 共享字段 → 所有进程的缓存同时失效**，不会出现「A 失效了、B 还在用旧地址」的时间差；
- **被动**：30s TTL 自然过期，是**自愈兜底**——即使 Redis 完全挂掉（HDEL 失败只打 DEBUG），各进程本地缓存 30 秒后也会自己过期重拉 ZK。

#### Q6.2.1 为什么不用 ZK 的 Watcher 做实时推送？

**答**：**因为 Watcher 是「一次性」的，维护成本远高于 TTL 拉模型。**

代码里发现路径的 watcher 参数全是 0（`ZookeeperUtil.cc:317`）：

```cpp
zoo_aget(m_zhandle, path, 0, GetDataCb, ctx);   // watcher=0：不设置监听
```

要用 Watcher 必须处理四个麻烦：① **一次性**——触发后必须重新注册，漏一次就永久失去通知；② **触发风暴**——1000 个客户端 watch 同一节点，一个实例下线同时触发 1000 个回调 + 1000 次重拉，ZK 二次承压；③ **回调在 ZK 的 watcher 线程执行**——访问框架缓存要跨线程投递（`runInLoop` 那一套），引入线程安全问题；④ **会话过期后所有 watcher 失效**，要全部重注册。

**「30 秒生效」对业务可接受**：新 worker 上线最多 30s 后被调度，相对一次转码几十秒可忽略；实例下线感知更宽松——**即使拿到已下线地址，连接失败会立刻触发「失效缓存 + 重新发现 + 重试一次」，实际感知远小于 30s**。用「30s 内可能拿旧地址」换「实现简单 + ZK 压力平稳 + 自动恢复」。

---

## 七、连接池

### Q7.1 连接池是怎么设计的？

每个服务节点都有对应的连接池，统一通过ipport映射存放在pool的哈希表里

**按 endpoint（`ip:port`）分片，每个 endpoint 一个 `vector<shared_ptr<PooledConnection>>`，每连接一把 mutex。**

**数据结构**（`mprpcchannel.cc:442-490`）：

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

**② 借用逻辑——懒建 + 轮询**（`mprpcchannel.cc:601-619`）：

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

**③ 最大连接数可配**（`mprpcchannel.cc:432-440`）：

```cpp
static size_t MaxConnectionsPerEndpoint()
{
    if (!MprpcApplication::IsInitialized()) return 8;      // 未 Init 时不碰配置
    return MprpcApplication::GetConfig().LoadInt(
        "mprpcclient_connections_per_endpoint", 8, 1, 128);
}
```

默认 8，范围 1~128。

#### Q7.1.1 单个服务的并发 RPC 上限是多少？

**答**：**等于连接池大小，默认 8。**

**推导过程**：`SendRequestAndReadResponse` 的第一行就是**锁住整条连接**（`mprpcchannel.cc:694`）：

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

---

### Q7.2 连接池的死连接问题是怎么处理的？

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

**发现失败后的处理是「整片清池」**（`mprpcchannel.cc:892-918`）：

**为什么是「清池」而不是「只关这一条」**：

假设池里有 8 条连接，其中 7 条是死的。如果只关当前这条：

```text
请求1 → 连接1 → 失败 → 关连接1 → 重试 → 拿到连接2 → 失败 → 关连接2 → ...
```

**最多要失败 8 次才能找到一条活连接**——而每次失败都包含一次完整的 `send`/`recv` 尝试和超时等待。

**清池的话**：

```text
请求1 → 连接1 → 失败 → 清空整个池 → 重试 → 拿到全新连接 → 成功
```

**一次失败就恢复**。代价是「把可能还活着的连接也关掉了」——但那 几2 条活连接重建的成本，远低于「逐条试错」的成本。

---

## 八、超时、重试与 deadline

### Q8.1 超时是怎么实现的？

**分三个环节，每个环节用不同的机制。**

| 环节 | 机制 | 代码位置 |
|---|---|---|
| **TCP 建连** | 非阻塞 connect + `poll(POLLOUT, timeoutMs)` | `ConnectToEndpoint` |
| **发送** | `SO_SNDTIMEO` | `SetSocketTimeout` |
| **接收** | `SO_RCVTIMEO` | `SetSocketTimeout` |

**① 建连超时用的是「非阻塞 connect + poll」**（`mprpcchannel.cc:494-588`）：

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

**② 收发超时交给内核**（`mprpcchannel.cc:96-113`）：

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

**③ 长连接复用时要「每次刷新超时」**（`mprpcchannel.cc:704-711`）：

```cpp
// 长连接复用时，每次调用都按当前 timeoutMs 刷新 socket 选项
if (!SetSocketTimeout(conn->fd, timeoutMs, savedErrno)) { ... }
```

**因为 `SO_RCVTIMEO` 是 socket 级别的属性**，会被后续的 `setsockopt` 覆盖。不同请求可能配了不同的超时（比如 `SetTimeoutMs(3000)` 和默认 5000），所以每次调用都得重设。

### Q8.2 重试是怎么做的？

**只重试一次，而且两条分支走不同的恢复路径。**

**触发条件**（`mprpcchannel.cc:886-890`）：

```cpp
if (!callOk &&
    (callErrorCode == mprpc::RPC_CONNECT_FAILED ||
     callErrorCode == mprpc::RPC_TIMEOUT ||
     callErrorCode == mprpc::RPC_SEND_FAILED ||
     callErrorCode == mprpc::RPC_RECV_FAILED))
```

**只有网络层的 4 个错误码会触发重试**（见 Q1.2.1 的分层）。

**两条恢复路径**（`mprpcchannel.cc:892-918`）：

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
    std::string retry_host = PickEndpoint(QueryEndpointList(method_path));
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

**为什么只重试一次**：

> 避免在 ZK 故障或全集群宕机时进入死循环

如果无限重试：ZK 完全挂了 → 每次调用都重试 → 线程全部卡在重试循环里 → **雪崩**。重试一次是「给瞬时抖动一次机会」，但不是「无限等待」。
