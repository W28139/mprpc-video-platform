# 核心设计模式：线程亲和任务队列（Thread-Affinity Task Queue）

> 本文专题拆解 `runInLoop` / `queueInLoop` / `pendingTasks_` / `wakeup` 这一整套机制。
> 它是 One Loop Per Thread 模型能够「免锁」的**唯一支点**，也是面试中从「会写网络库」跨到「懂并发设计」的分水岭。
> 代码引用均基于 `wevix_muduo/`，行号可直接跳转验证。

---

## 目录

- [一、问题：为什么需要这个模式](#一问题为什么需要这个模式)
- [二、模式的正式身份：经典出处](#二模式的正式身份经典出处)
- [三、核心不变式](#三核心不变式)
- [四、机制逐层拆解](#四机制逐层拆解)
- [五、横向对照：工业界的各种化身](#五横向对照工业界的各种化身)
- [六、muduo 的实现与差异](#六muduo-的实现与差异)
- [七、设计取舍：代价与适用边界](#七设计取舍代价与适用边界)
- [八、常见误用与坑](#八常见误用与坑)
- [九、本项目中的经典模式全景映射](#九本项目中的经典模式全景映射)
- [十、面试问答](#十面试问答)

---

## 一、问题：为什么需要这个模式

### 1.1 起点：共享可变状态

多线程编程的全部痛苦来自一个事实：**多个线程访问同一份可变状态**。

一旦发生，你就必须回答一堆问题：谁在什么时候改的？读到的是中间态吗？编译器会不会把变量缓存进寄存器？CPU 会不会重排指令？——这是数据竞争（data race），在 C++ 里是 **undefined behavior**，不是「结果可能不对」，是「编译器可以做任何事」。

### 1.2 三条经典出路

让数据在多线程下安全，教科书上有且只有三条路：

| 出路 | 做法 | 代价 |
|---|---|---|
| **① 不共享** | 每个线程只碰自己的数据 | 需要「线程所有权」的划分 |
| **② 不可变** | 数据创建后只读 | 只适用于只读场景，写就得重建 |
| **③ 同步** | 加锁 / 原子操作 / 内存屏障 | 锁开销、死锁面、可扩展性差 |

大多数代码走的是 ③ —— 加锁。**加锁的问题不是慢，而是「正确性难以论证」**：

- 锁的粒度对吗？临界区里调了别人的回调吗？那个回调里会不会再抢同一把锁（死锁）？
- 加锁顺序在 A 模块是 `lock(a) → lock(b)`，B 模块反过来了吗（死锁）？
- 有人忘记加锁吗？这个 bug 只在特定时序下复现，压测跑一万次可能一次都不出。

陈硕对锁有一句很到位的评价：**用锁最大的问题是「你不知道临界区里会不会有人再调你的回调」**。一旦发生，就是死锁或重入。

### 1.3 本模式选择的路

One Loop Per Thread 选了 **① 不共享** 这条路，方式是把**「共享内存 + 锁」换成「消息传递」**：

> **一个连接的全部状态（fd / Channel / Buffer / 回调），从生到死只被一个固定的线程访问。**
> **其他线程想操作它，就把「操作」打包成一条消息，投递给拥有它的那个线程。**

其他线程**永远不直接碰**这个连接的任何字段 —— 它们只是往队列里放一个闭包。于是：

- 连接内部**一个 mutex 都不需要**
- 不是「锁用得少」，是「**锁不存在**」
- 正确性可以**论证**而非靠测试碰运气：不变式（见第三节）一旦成立，就不存在竞争

Go 语言把这套思路总结成一句格言：

> **Don't communicate by sharing memory; share memory by communicating.**
> （不要通过共享内存来通信，而要通过通信来共享内存。）

---

## 二、模式的正式身份：经典出处

这套机制在业界有正式名字，而且**每个组成部分都能对应到一个已命名的经典模式**。面试时能报出出处，分量完全不同。

### 2.1 Reactor —— 整体架构模式

> **论文**：*Reactor: An Object Behavioral Pattern for Demultiplexing and Dispatching Handles for Synchronous Events*
> **作者**：Douglas C. Schmidt，1995
> **收录**：《Pattern-Oriented Software Architecture, Vol. 2》（POSIX/ACE 模式集）

Reactor 的意图是：**用「同步事件多路分离 + 回调分发」取代「每连接一线程」**。

核心角色：

| 角色 | 本项目对应 |
|---|---|
| **Reactor**（事件循环） | `EventLoop` |
| **Demultiplexer**（多路分离器） | `Epoll` |
| **Handle**（资源句柄） | fd |
| **Event Handler**（事件处理器） | `Channel` |
| **Concrete Event Handler** | `Connection` / `Acceptor` |

Reactor 有几种**变体**，这是面试高频追问点：

| 变体 | 结构 | 典型问题 |
|---|---|---|
| **单 Reactor 单线程** | 一个线程既 accept 又 IO 又跑业务 | 一个慢请求阻塞所有人 |
| **单 Reactor 多线程** | 一个 loop 负责 IO，业务丢线程池 | 单 loop 成为 IO 瓶颈 |
| **多 Reactor（One Loop Per Thread）** | **mainLoop 只 accept + N 个 subLoop 各管一批连接** | ← **本项目采用的** |
| **主从 Reactor** | 同上的另一种叫法（nginx 的 master/worker） | |

所以「One Loop Per Thread」不是 muduo 的发明，它是 **Multi-Reactor 变体**的工程化落地；`fd % N` 或轮询分配连接是「Sub-Reactor 亲和性分配」的实现细节。

### 2.2 Active Object —— 本机制最贴近的模式

> **论文**：*Active Object: An Object Behavioral Pattern for Concurrent Programming*
> **作者**：Douglas C. Schmidt，1996
> **收录**：同上

**Active Object 的意图**：把「方法的执行」和「方法的调用」**解耦** —— 调用方调用一个方法，实际执行发生在另一个线程；两者之间靠一个**激活队列（Activation Queue）**连接。

标准角色：

| Active Object 角色 | 本项目对应 |
|---|---|
| **Proxy**（对外的调用接口） | `Connection::send()` |
| **Activation Queue**（激活队列） | `pendingTasks_` |
| **Scheduler**（调度器，把消息派发给执行者） | `EventLoop::run()` 主循环 |
| **Servant**（真正干活的对象） | `Connection`（在归属线程里被操作） |
| **Future / Method Request**（方法请求对象） | `std::function<void()>` 闭包 |

**严格说本项目是一个变体**：经典 Active Object 是「一个对象一条线程」，而这里是「**N 个连接共享一条线程（一个 loop）**」。业界把后者叫 **Serialized Execution Context** 或 **Thread-Affinity Task Queue**。但本质机制（队列 + 唤醒 + 归属线程执行）完全一致。

### 2.3 Half-Sync/Half-Async —— 解释「为什么要有一个 work 线程池」

> **论文**：*Half-Sync/Half-Async: An Architectural Pattern for Efficient and Well-structured Concurrent I/O*
> **作者**：Schmidt & Cranor，1995

意图：把系统分成**异步层**和**同步层**，中间用队列解耦。

```text
┌──────────────────────────────────────────────────┐
│ 同步层（Sync Layer）—— 业务线程，可以阻塞、可以慢  │
│   workThreadPool_（ThreadPool MODE_FIXED/CACHED） │
└────────────────────▲─────────────────────────────┘
                     │  队列（解耦点）
┌────────────────────┴─────────────────────────────┐
│ 异步层（Async Layer）—— IO 线程，绝不能阻塞        │
│   mainLoop + subLoops（epoll，ET，非阻塞）         │
└──────────────────────────────────────────────────┘
```

**这个模式直接回答了「为什么要两层线程池」**：

- 异步层（IO 线程）必须**永不阻塞** —— 阻塞它就等于阻塞它上面所有连接
- 但业务可能 CPU 密集或必须阻塞（查 MySQL、调 ffmpeg）
- 所以中间必须有队列，把「慢」隔离在同步层

如果面试官问「你为什么要搞两个线程池」，答案是 Half-Sync/Half-Async，不是「因为 muduo 有两个」。

### 2.4 Command —— 任务的本质是「命令对象」

> **出处**：GoF《设计模式》Command（命令模式）
> **意图原文**：「Encapsulate a request as an object, thereby letting you parameterize clients with different requests, **queue or log requests**, and support undoable operations.」

注意 GoF 的意图里**明确写了 queue requests** —— 「把请求封装成对象，以便排队、记录、撤销」。

`std::function<void()>` 就是一个 Command 对象：

```cpp
// Connection.cpp:221
loop_->runInLoop(std::bind(&Connection::sendInLoop, shared_from_this(), data));
//                              └── Receiver ──┘  └─ 参数 ─┘
```

- **Command**：这个 `std::function` 对象
- **Receiver**：`Connection`（真正执行动作的对象）
- **Invoker**：`EventLoop`（决定什么时候执行）
- **Client**：work 线程（构造命令并投递）

先封装、后执行、中间可以排队 —— 这就是 Command 的全部要点。

### 2.5 Producer-Consumer —— 队列本身

经典的**生产者-消费者模式**。这里：

- 生产者：任意线程（work 线程、其他服务的回调、mainLoop）
- 消费者：**唯一**的 loop 线程
- 缓冲区：`pendingTasks_`
- 同步：`mutex_`

一个特殊之处：**消费者只有一个**，所以队列天然不需要「多消费者竞争」的复杂处理（不需要 `notify_all` 惊群，也不需要多消费者唤醒协议）。

### 2.6 Thread Confinement —— 不变式的名字

> **出处**：《Java Concurrency in Practice》（Goetz 等）第 3 章 *Sharing Objects*，Confined 一节

**线程封闭**：如果一个对象只能被一个线程访问，那么它自动就是线程安全的 —— **无论它内部有没有同步**。

JCiP 把它列为「构造线程安全类」的最简单方式之一，常见的实现手段有：

- **栈封闭（Stack Confinement）**：对象只是局部变量
- **ThreadLocal**：每个线程一份副本（本项目内存池的 `ThreadCache` 就是）
- **归属线程（Thread Ownership）**：**本模式的连接属于某个 subLoop 线程**

**关键**：线程封闭本身**不能靠约定维持** —— 你必须防止别的线程拿到引用。`pendingTasks_` 就是这条防线的**唯一合法通道**：它把「跨线程的直接访问」转换成「投递」。

### 2.7 一句话串起来

```text
Reactor             → 整体架构：事件循环 + 回调分发（多 Reactor 变体 = One Loop Per Thread）
Active Object       → 跨线程通信机制：队列 + 唤醒 + 归属线程执行
Half-Sync/Half-Async→ 分层理由：异步 IO 层 + 同步业务层，中间队列解耦
Command             → 任务的本质：把请求封装成对象，以便排队
Producer-Consumer   → 队列的并发模型
Thread Confinement  → 正确性依据：状态只被一个线程碰 → 无需同步
```

---

## 三、核心不变式

> **任何连接的可变状态，只被它归属的那个 IO 线程访问。**

这是整个设计的地基。它带来三个推论：

**推论 1：连接的成员不需要保护**

`Connection` 里的 `inputBuffer_` / `outputBuffer_` / `channel_` / `lastActiveTime_` / `disconnected_` 中，只有明确标注为跨线程读写的那几个才是原子类型（如 `disconnected_` 是 `std::atomic_bool`），其余全是裸成员 —— 因为只有一个线程碰。

**推论 2：所有跨线程操作必须走投递**

这是 `loop_->runInLoop(...)` 在整个代码库里反复出现的原因。凡是「非归属线程要操作连接」，一律：

```cpp
loop_->runInLoop(std::bind(&Connection::someInLoop, shared_from_this(), ...));
```

**推论 3：所有权必须在「归属线程」里建立和销毁**

- 建立：`TcpServer::handleNewConnection` 在 mainLoop 分配 subLoop，然后 `subLoop->newConnection(conn)`（`TcpServer.cpp:106`）
- 销毁：`Connection::handleClose()` 用 `runInLoop` 包住清理动作（`Connection.cpp:172-183`），保证 `channel_->remove()` / `removeConnection()` 都发生在归属线程

**⚠️ 这里有个经典陷阱**（本项目真的踩过，见剧本⑤）：**「对象在哪个线程被构造」和「对象属于哪个线程」是两件事**。`EventLoop` 的 `threadId_` 一开始记在构造函数里就是错的 —— 因为 `subLoop` 在 mainLoop 线程里被 `new` 出来，却在 IO 线程里 `run()`。修复是把 `threadId_` 的记录**从构造函数移到 `run()` 里**（`EventLoop.cpp:84`）。

---

## 四、机制逐层拆解

### 4.1 数据结构

```cpp
// EventLoop.h:93-95
mutable std::mutex mutex_;
std::queue<Functor> pendingTasks_;
```

`Functor` 就是 `std::function<void()>`。一个 mutex + 一个队列，没有别的。

### 4.2 入队：`queueInLoop`

```cpp
// EventLoop.cpp:135-144
void EventLoop::queueInLoop(Functor cb)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pendingTasks_.push(std::move(cb));
    }                        // ← 锁在唤醒之前就释放了（重要）
    wakeup();
}
```

两个细节：

1. **`std::move(cb)`**：闭包被搬移而非拷贝。如果闭包捕获了 `std::string`，这一步避免了整块内存的拷贝。
2. **锁在 `wakeup()` 之前释放** —— 不是风格问题，是刻意为之（见 4.3）。

### 4.3 唤醒：为什么必须 `wakeup()`

循环的主体：

```cpp
// EventLoop.cpp:88-110
while (!stop_)
{
    std::vector<Channel*> activeChannels;
    epoll_->poll(-1, &activeChannels);       // ← 永久阻塞
    for (Channel* channel : activeChannels) channel->handleEvent();
    doPendingTasks();                        // ← 队列在这里被消费
}
```

`epoll_->poll(-1, ...)` 的超时是 **-1，即永久阻塞**（本项目的 timerfd 负责定时，所以不需要靠 epoll 超时兜底）。

于是问题来了：**任务已经躺在队列里，但 loop 线程睡在 `epoll_wait` 里，它不知道。**

`wakeup()` 就是解药：

```cpp
// EventLoop.cpp:146-154
void EventLoop::wakeup()
{
    uint64_t one = 1;
    ::write(wakeupFd_, &one, sizeof(one));   // 往 eventfd 写 8 字节
}
```

`wakeupFd_` 是一个 eventfd，**注册在同一个 epoll 里**（`wakeChannel_`）。写入后 `epoll_wait` 立刻返回 → 本轮循环走到 `doPendingTasks()` → 任务被执行。

**这就是经典的 self-pipe trick**（Unix 编程里的老技巧，Bernstein 提出：用一根管道把信号「转成可 select 的 fd」）。Linux 上 eventfd 是它的现代替代品：

| | pipe | **eventfd** |
|---|---|---|
| fd 数量 | 2 个（读端 + 写端） | **1 个** |
| 写满会怎样 | 缓冲区满 → `write` **阻塞**（唤醒机制自己把自己卡死） | 64 位计数器累加，**永不阻塞** |
| 语义 | 字节流 | 计数器 |
| 读侧处理 | 要处理部分读 | 一次 `read` 取走整个计数 |

**为什么是 eventfd 而不是管道**，答案就是上表第 2 行 —— 管道写满会阻塞，而那恰恰是最需要唤醒的时刻。这是个很好的面试细节。

**为什么不用条件变量？** 因为条件变量无法和 `epoll_wait` 一起阻塞。要么开一个线程专门等条件变量，要么把 `epoll_wait` 拆成带超时的轮询 —— 前者多一个线程，后者把「事件驱动」退化成「轮询」。eventfd 把唤醒信号变成了**一个普通 fd**，于是它能和其他所有 fd 一起被 epoll 管理，**不引入额外线程**。

### 4.4 消费：`doPendingTasks` 与 swap 缩锁

```cpp
// EventLoop.cpp:167-181
void EventLoop::doPendingTasks()
{
    std::queue<Functor> tasks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks.swap(pendingTasks_);      // ← 整个队列换出来
    }
    while (!tasks.empty())              // ← 锁外执行任务
    {
        tasks.front()();
        tasks.pop();
    }
}
```

**为什么必须 swap 出来，而不是持锁执行？** 两个理由，缺一不可：

**理由 1：避免自死锁。** 任务体是**用户代码**。用户代码里完全可能再次调 `queueInLoop`（比如一个任务里发消息给另一个连接）。如果此时持着 `mutex_`，同一线程二次加锁 → **死锁**。这个锁的持有者是自己，等的人也是自己。

**理由 2：避免阻塞生产者。** 用户任务可能很慢（虽然不该，但可能）。持锁执行期间，其他线程无法投递 —— 唤醒机制形同虚设。

**这是一个通用原则**：

> **持锁期间只做无副作用的操作；有副作用的动作（回调、关闭、执行用户代码）放到锁外。**

同一个项目里的第二次应用：`EventLoop::handleTimer` 的「先收集、再关闭」（`EventLoop.cpp:196-218`）—— 先在锁内把超时连接收集到 `expired` 向量，解锁后再逐个 `forceClose()`。原因完全一样：`forceClose()` 会同步回调到 `removeConnection` → 再次加锁。

**第三次应用**：`TcpServer::handleNewConnection` 里先解锁再调用户回调。

> 面试技巧：能举出同一个原则在项目里的**三处应用**，比背出原则本身有说服力得多。

### 4.5 分流：`runInLoop` 的同线程快路径

```cpp
// EventLoop.cpp:123-133
void EventLoop::runInLoop(Functor cb)
{
    if (isInLoopThread())
        cb();                      // 同线程：直接同步执行，零开销
    else
        queueInLoop(std::move(cb)); // 跨线程：入队 + 唤醒
}
```

**同线程走的是「直接执行」分支，根本不碰队列** —— 不构造唤醒、不加锁、不入队。

这个设计很重要，因为 `runInLoop` 是**最常用的接口**：调用方不需要知道自己在哪个线程，把操作丢给 `runInLoop` 即可 —— 它自动选最优路径。

**典型的调用点**：

```cpp
// Connection.cpp:205-223
void Connection::send(const std::string& data)
{
    if (disconnected_) return;
    if (loop_->isInLoopThread())
        sendInLoop(data);                    // 我在归属线程，直接写
    else
        loop_->runInLoop(std::bind(&Connection::sendInLoop,
                                   shared_from_this(), data));  // 投递回去
}
```

注意这里**先自己判了一次** `isInLoopThread()`，没直接调 `runInLoop`。这样做的收益是：同线程路径上不构造那个 `std::function`（省一次堆分配）。属于热路径上的微优化。

`runInLoop` 在项目里的其它用途，全部是「必须回到归属线程做的事」：

| 调用点 | 为什么必须回归属线程 |
|---|---|
| `Connection::handleClose`（`Connection.cpp:172`） | `channel_->remove()` 要改 epoll，必须归属线程 |
| `Connection::connectEstablished`（`Connection.cpp:290`） | `enableReading()` 要改 epoll |
| `Connection::forceClose`（`Connection.cpp:300`） | 要走到 handleClose |
| `Connection::sendInLoop` 里的 sendComplete 回调（`Connection.cpp:240`） | 用户回调需在归属线程 |
| `EventLoop::handleTimer` 里的 `forceClose`（`EventLoop.cpp:213`） | 同上（此处已在归属线程，runInLoop 直通） |

---

## 五、横向对照：工业界的各种化身

这个模式不是 muduo 的私有技巧，它是并发编程的通用解法。**能横向对照，说明你理解的是模式而不是某份代码。**

| 系统 | 对应物 | 关键语义 |
|---|---|---|
| **Android** | `Looper` + `Handler.post()` | 任意线程 post，Runnable 在 Looper 线程执行。UI 工具包「单线程模型」的基石 |
| **Chromium** | `base::SingleThreadTaskRunner` / `SequencedTaskRunner` + `PostTask` | 整个浏览器多线程模型建立在此之上（见下方辨析） |
| **boost::asio** | `io_context::post()` / `strand` | `post` 即 `queueInLoop`；`strand` 保证同 strand 上 handler 不并发 —— **同一目的** |
| **GCD / iOS** | `DispatchQueue` 串行队列 / `main.async` | 串行队列等价于「一个执行上下文」 |
| **Actor 模型** | Erlang / Akka 的 actor 邮箱 | actor 只在自己的线程处理消息，状态天然免锁 |
| **Java** | 单线程 `ExecutorService` | 「串行执行上下文」 |
| **Rust** | 所有权 + `Send`/`Sync` trait | **语言层面**强制这个不变式：不能被跨线程移动的类型直接编译报错 |

**Chromium 的两个 TaskRunner 值得辨析**（面试可能追问）：

- `SingleThreadTaskRunner` —— 保证在**特定线程**执行。这才是严格的线程亲和
- `SequencedTaskRunner` —— 保证**不并发、按序**执行，但**不承诺是哪个线程**

两者都能提供「无需加锁」的性质（因为都不并发），但只有前者提供「线程亲和」。本项目是严格的前者 —— 因为 epoll 操作本身就有线程亲和要求（`epoll_ctl` 必须和 `epoll_wait` 在同一个线程，否则事件与归属关系会乱）。

**Rust 的对照最值得说**：`wevix_muduo` 靠**纪律**维持线程封闭（谁忘了 `runInLoop` 就出 bug，编译器不拦你）；Rust 靠**类型系统**维持 —— 一个没有实现 `Send` 的类型根本不允许被移到另一个线程，编译期就报错。这是「机制保证 vs 纪律保证」在语言层面的体现。

---

## 六、muduo 的实现与差异

`muduo/net/EventLoop.{h,cc}` 里有完全对应的机制，本项目基本是平移：

| muduo | 本项目 | 差异 |
|---|---|---|
| `runInLoop(Functor cb)` | `runInLoop(Functor cb)` | 相同 |
| `queueInLoop(Functor cb)` | `queueInLoop(Functor cb)` | 相同 |
| `pendingFunctors_`（`std::vector<Functor>`） | `pendingTasks_`（`std::queue<Functor>`） | 容器不同，功能等价 |
| `doPendingFunctors()` + `swap` | `doPendingTasks()` + `swap` | 相同 |
| `wakeup()` 写 `wakeupFd_`（eventfd） | `wakeup()` 写 `wakeupFd_`（eventfd） | 相同 |
| `callingPendingFunctors_` 标志 | **无** | **实质差异** |
| `EventLoop::loop()` 末尾调 `doPendingFunctors()` | `EventLoop::run()` 末尾调 `doPendingTasks()` | 相同位置 |
| `kPollTimeMs = 10000`（poll 带 10s 超时） | `poll(-1, ...)` 永久阻塞 | 见下 |

**实质差异一：`callingPendingFunctors_` 与条件唤醒**

muduo 的 `queueInLoop` 是**条件唤醒**：

```cpp
// muduo 的写法
if (!isInLoopThread() || callingPendingFunctors_)
    wakeup();
```

本项目是**无条件唤醒**（`EventLoop.cpp:143`）。差别在哪？

**先说明为什么「无条件」不是错的** —— 有个隐蔽场景让唤醒成为必需：

```text
doPendingTasks 已把队列 swap 走
  → 正在执行的任务 A 里又调了 queueInLoop(B)
  → B 进的是 swap 后剩下的空 pendingTasks_
  → doPendingTasks 遍历的是局部变量 tasks，看不到 B
  → 本轮结束，回到 epoll_wait(-1) 阻塞
  → 如果这次没有 wakeup，B 要等到下一次无关的 fd 事件才执行，
     连接空闲时可能永远不执行
```

这正是 muduo 要引入 `callingPendingFunctors_` 的原因：**当「在 loop 线程内」且「正在执行 pending 任务」时，唤醒是必需的**；只有当「在 loop 线程内」且「不在执行 pending 任务」时才可以省（那种情况下，新任务会在本轮循环末尾的 `doPendingTasks` 里被自然消费）。

所以正确的优化是：

```cpp
if (!isInLoopThread() || callingPendingFunctors_) wakeup();
```

本项目的无条件唤醒是**安全但保守**的：绝不丢任务，代价是同线程 `queueInLoop` 时多一次 `write` 系统调用（约几百纳秒）。

**实质差异二：poll 超时**

muduo 的 `loop()` 用 `kPollTimeMs = 10000`（10 秒超时），即使没有任何事件也会每 10 秒醒一次；本项目用 `poll(-1)` 永久阻塞，完全靠 timerfd 驱动。

**⚠️ 这里要纠正一个流传较广的误解**（包括本项目文档 Q11.1 的措辞也不准确）：**muduo 的定时器底层也是 timerfd**。`muduo/net/TimerQueue.cc` 里就是 `timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)`。

两边定时器的**底层机制相同**，真正的差异在**调度策略**：

| | muduo | 本项目 |
|---|---|---|
| 底层 | timerfd | timerfd |
| 数据结构 | `std::set<Timer*>` 按到期时间排序（最小堆语义） | 无（固定周期） |
| 定时方式 | 每次把 timerfd **精确**设到「最近一个定时器的到期时刻」 | 固定重装 5s，扫全表 |
| 语义 | 任意 one-shot / repeat | 周期性巡检 |
| 精度 | 精确（「再过 3.2 秒」） | 10~15 秒（损失 50%） |

**面试被追问「muduo 定时器怎么实现的」，正确答案是「基于 timerfd + 按到期时间排序的 set，每次精确 rearm」** —— 不是时间轮，也不是红黑树。时间轮是 `libevent` / `nginx` 的路子。

---

## 七、设计取舍：代价与适用边界

### 7.1 代价清单

| 代价 | 量级 | 说明 |
|---|---|---|
| `eventfd` 系统调用 | ~几百 ns | 每次跨线程投递一次 |
| `std::function` 构造 | 可能一次堆分配 | 捕获较大的对象时 |
| 队列加锁 + 解锁 | ~几十 ns | 无竞争时很快 |
| **执行延迟** | **下一轮 loop 迭代** | 不是「立即」，是「尽快」 |

**最后一条才是最本质的代价**：`runInLoop` 的名字有误导性 —— 它**不保证立即执行**。如果目标线程正忙（比如某个回调很慢），你的任务要排队等。

### 7.2 适用边界

判断标准很简单：

> **问：这个状态的跨线程访问频率有多高？**

| 频率 | 决策 |
|---|---|
| **低频**（连接建立/关闭/发送响应、秒级） | **用任务队列**，开销可忽略 |
| **高频**（每秒百万次跨线程访问） | **不要用** —— 每次都是一次系统调用 + 队列操作，会成为新瓶颈 |

所以 muduo 自己的选择也是分裂的：

- **连接管理** → `runInLoop` / `queueInLoop`（低频，用任务队列）
- **高频数据路径** → `BlockingQueue`（有锁队列，但只在真正需要跨线程的地方用）

本项目里 `Connection::send` 就是这个判断的实例：它在 `send()` 里先判 `isInLoopThread()` 走快路径，只有真跨线程才投递。

### 7.3 还有一个决策点：有界还是无界队列

本项目 `pendingTasks_` 是**无界**的。风险：如果 loop 线程持续很忙，而其他线程疯狂投递，队列会无限增长 → 内存爆炸。

**muduo 也是无界的**（`std::vector` 不设上限）。这是有意的取舍：网络库层面无法判断「多少算太多」，需要背压的话由上层决定。

对照：本项目的 **`ThreadPool` 有背压**（`wait_for` 1 秒超时，队列满时把压力传回上游）—— 因为那里面执行的是可能很慢的业务任务，队列堆积的风险是真实的。

---

## 八、常见误用与坑

### 8.1 在任务里做阻塞操作

**最严重的一类错误。** 任务在 loop 线程执行，阻塞任务 = 阻塞该 loop 上**所有连接**。

```cpp
// ❌ 灾难
loop->runInLoop([]{ ::sleep(10); });

// ✅ 丢给 work 池
workThreadPool_->addTask([]{ /* 慢活 */ });
```

这也是 Half-Sync/Half-Async 存在的原因 —— 两层结构的**唯一目的**就是隔离阻塞。

### 8.2 在任务里再投递任务，然后期望「立即执行」

由于 `doPendingTasks` 已经 swap 过队列，任务里新投递的闭包**不会在本轮被执行**，要等下一轮（这依赖 `wakeup` 或下一次事件）。

连续投递形成链式任务时，要意识到每次投递 = 一轮 loop 迭代。

### 8.3 忘记唤醒

自己实现这套机制时，最常见的 bug 是**只入队不唤醒** —— 功能测试常常发现不了，因为一有无关的网络事件任务就被捎带执行了。只有在「连接全空闲」时才暴露，表现为**任务偶发延迟数秒甚至永久不执行**。

### 8.4 持锁执行任务

见 4.4 —— 自死锁 + 阻塞生产者。必须 swap 到锁外。

### 8.5 误以为「投递出去就安全了」

`runInLoop` 解决的是**竞争问题**，不解决**生命周期问题**。

```cpp
// ❌ 悬垂
loop->runInLoop([&]{ use(local_var); });   // local_var 可能已析构
```

所以项目里投递的闭包一律**按值捕获 `shared_ptr`**：

```cpp
// Connection.cpp:221
loop_->runInLoop(std::bind(&Connection::sendInLoop, shared_from_this(), data));
//                                              └── 保活 ──┘        └─ 按值 ─┘
```

`shared_from_this()` 让闭包持有一份引用，任务执行时对象必然存活。

**这也是 `Connection` 必须继承 `enable_shared_from_this` 的原因之一**（另一个原因是 `Channel` 回调里的 `self` 保活）。

### 8.6 顺序性假设

投递到**同一个 loop** 的任务是**按序执行**的（队列 FIFO + 单消费者），这提供了一种弱同步语义。

但**投递到不同 loop 的任务之间没有任何顺序保证** —— 连接 A 和连接 B 属于不同 subLoop 时，给它们发消息的顺序是不确定的。需要全局有序就得引入序列号或统一的排序点。

---

## 九、本项目中的经典模式全景映射

面试被问「你这个项目用了哪些设计模式」时，这张表是弹药库。**重点是每一条都能说出「不用它会怎样」**。

### 9.1 并发与架构模式

| 项目中的实现 | 经典模式 | 出处 | 不用会怎样 |
|---|---|---|---|
| `EventLoop` + `Epoll` + `Channel` + `Acceptor` + `Connection` | **Reactor**（多 Reactor 变体） | Schmidt | 每连接一线程 → C10K 崩 |
| `runInLoop`/`queueInLoop`/`pendingTasks_`/`wakeup` | **Active Object** / Thread-Affinity Task Queue | Schmidt | 连接状态要加锁，锁面爆炸 |
| IO 线程池 + work 线程池 + 队列 | **Half-Sync/Half-Async** | Schmidt | 慢业务阻塞所有连接 |
| `std::function<void()>` 任务 | **Command** | GoF | 无法排队、无法跨线程传 |
| `pendingTasks_` 队列 | **Producer-Consumer** | 经典并发 | — |
| 连接状态免锁 | **Thread Confinement** | JCiP | 必须给每个连接加锁 |
| `AsyncLogger` 前端入队 + 后端落盘 | **Half-Sync/Half-Async** + Producer-Consumer | Schmidt | 业务线程被磁盘 IO 阻塞 |
| `Channel`（fd + events + 回调的封装） | **Wrapper Facade** | Schmidt | epoll 细节泄漏到业务层 |
| `ThreadPool` 的 `MODE_FIXED` / `MODE_CACHED` | **Strategy** | GoF | 固定/弹性两种行为要写两份类 |

### 9.2 结构型与创建型

| 项目中的实现 | 经典模式 | 出处 | 备注 |
|---|---|---|---|
| `mprpc` 连接池（按 endpoint 分片） | **Object Pool** | 经典 | 避免每 RPC 建连 |
| `mysql_pool.h/.cpp` 连接池（借还 + cv） | **Object Pool** | 经典 | 同上，1-64 连接 |
| `JobStore` / `ShardStore` / `WorkerStore` 单例 | **Singleton** | GoF | ⚠️ 代价见下 |
| 回调注册（`setOnMessageCallback` 等） | **Observer** | GoF | 网络层与业务解耦 |
| `PoolAllocator<T>` | STL **Allocator** 概念 | — | 让 STL 容器走内存池，3.8× |
| `FfmpegExecutor` 全静态方法 | **Utility / Facade** | — | 屏蔽 ffmpeg 命令行细节 |
| `MetricsHttpServer` port≤0 时 Start/Stop 为 no-op | **Null Object** | GoF | 可降级组件，不影响主流程 |

**⚠️ 关于 Singleton 要主动说代价**：三个 Store 是进程内单例，导致**多进程部署时数据完全隔离**，跨进程一致性只能靠 RPC 同步或（阶段 9 之后）MySQL 兜底。这是被现实教育过的点，主动说出来比被追问出来好。

### 9.3 分布式与容错（严格说不是 GoF 模式，但属经典方法论）

| 项目中的实现 | 模式名 | 备注 |
|---|---|---|
| Redis 快照优先 → 失败回退 ListWorkers RPC | **Fallback（回退降级）** | ⚠️ **不是 Circuit Breaker** —— 见下 |
| MQ 在线 push → 掉线自动切 pull 轮询 | **Fallback** + 双模 | 恢复后自动切回 |
| `UpdateIfStatus` 条件更新（状态推进防覆盖） | **Optimistic Concurrency / CAS** | 用状态当版本号 |
| `attempt_id` 幂等去重 | **Idempotency Key** | 防「先后重复」 |
| `SETNX shard:lock:{id} EX 10` + 值校验释放 | **Distributed Lock** | 防「同时竞争」 |
| Job/Scheduler/Worker/RC 五服务拆分 | **Microservices** | 按职责边界而非技术分层 |
| MQ 事件驱动（`job.events` → `shard.waiting`） | **Publish-Subscribe** | 调度延迟 33ms |

**必须辨析的一点：Fallback ≠ Circuit Breaker。**

- **Circuit Breaker（断路器）** 有三个状态：closed / **open** / **half-open**。它会在失败后**主动拒绝**一段时间的请求（快速失败），然后进入 half-open 试探性放行。
- 本项目的降级是**按次判断**：这次失败就这次回退，下次照常尝试 —— 没有「熔断窗口」，也没有主动拒绝。

所以准确表述是「**带自动恢复的 Fallback 降级**」。面试里把 Fallback 说成 Circuit Breaker 是会被抓的 —— 主动辨析反而加分。

---

## 十、面试问答

### Q1：`runInLoop` 和 `queueInLoop` 什么区别？

**答**：
- `runInLoop` 是**智能入口**：同线程直接执行（零开销，不碰队列），跨线程才转投递
- `queueInLoop` 是**无条件投递**：必然入队 + 唤醒
- 调用方一般用 `runInLoop`，因为它自动选最优路径 —— 调用者不必关心自己在哪个线程
- `queueInLoop` 的典型直接用场：明确知道自己不在 loop 线程，或需要「保证延后执行」而非「尽快执行」

### Q2：跨线程怎么安全地操作一个连接？

**答**：**不能直接操作，只能投递**。因为 One Loop Per Thread 的核心前提是「连接状态只被归属线程访问」，这是免锁的依据。

具体做法：`loop_->runInLoop(std::bind(&Connection::xxxInLoop, shared_from_this(), args...))`。要点有三：① 必须回到归属线程 ② 用 `shared_from_this()` 保活，防悬垂 ③ 若在热路径，先自己判 `isInLoopThread()` 走快路径省一次闭包构造。

### Q3：凭什么说你的连接是线程安全的？

**答**：**因为不存在跨线程访问** —— 这叫**线程封闭（Thread Confinement）**。连接从建立（`TcpServer::handleNewConnection` 在 mainLoop 分配 subLoop）到销毁（`handleClose` 用 `runInLoop` 保归属线程）全程只被一个 IO 线程碰，所以它的成员不需要任何同步原语。

唯一的跨线程通道是 `pendingTasks_`，而它**只传递闭包、不传递引用** —— 别的线程拿不到连接的裸指针。

### Q4：`doPendingTasks` 为什么要把队列 swap 出来？

**答**：两个理由，都必须成立：
1. **避免自死锁** —— 任务体是用户代码，可能再次 `queueInLoop`，持锁执行会导致同线程二次加锁
2. **避免阻塞生产者** —— 用户任务可能很慢，持锁期间其他线程无法投递，唤醒机制失效

这是「**持锁期间只做无副作用操作**」这条原则的实例。同一原则在项目里还有两处应用：`handleTimer` 的先收集再关闭、`handleNewConnection` 的先解锁再回调。

### Q5：为什么唤醒用 eventfd 而不是管道？

**答**：管道的致命问题是**缓冲区写满时 `write` 会阻塞** —— 而那恰恰是最需要唤醒的时刻（事件密集、loop 来不及消费）。eventfd 内部是 64 位计数器，写入即累加、永不阻塞、单 fd，正好匹配「唤醒」这个只需信号不需数据的语义。这是 Unix 经典 self-pipe trick 在现代 Linux 上的替代品。

### Q6：为什么不用条件变量做唤醒？

**答**：条件变量无法和 `epoll_wait` 一起阻塞。要用它就得**额外开一个线程**专门等条件变量再唤醒 loop，或者把 `epoll_wait` 退化成带超时的轮询。eventfd 把唤醒信号变成了一个**普通 fd**，于是它能和其他所有 fd 一起被同一个 epoll 管理，**不引入额外线程、不退化事件驱动**。

### Q7：这个模式的代价是什么？

**答**：跨线程操作要付出「一次 eventfd `write` + 一次队列加锁 + 一次 `std::function` 构造」，更重要的是**执行时机从「立即」变成「下一轮 loop 迭代」**。

所以**适用边界是「低频跨线程访问」**：连接建立/关闭/发响应用它几乎免费；每秒百万次的跨线程高频访问就不适合 —— 那种场景要重新划分所有权或用专门的无锁队列。muduo 自己也是分裂的：连接管理走任务队列，高频数据路径走 `BlockingQueue`。

### Q8：这个模式在别的系统里有吗？

**答**：这是**通用模式**，不是 muduo 的发明。工业界叫法各异但机制相同：

- **Android** `Looper` / `Handler.post()`
- **Chromium** `SingleThreadTaskRunner` + `PostTask`
- **boost::asio** `io_context::post()` 和 `strand`
- **GCD** 串行队列 / `main.async`
- **Actor 模型**（Erlang / Akka 的邮箱）

学术上的正式名字是 Schmidt 的 **Active Object**（激活队列 + 代理 + 调度器 + servant），配套的还有 **Half-Sync/Half-Async** 解释两层线程池的分层理由。通用工程叫法是 **Serialized Execution Context** 或 **Thread-Affinity Task Queue**。

### Q9：这和你上一份实现（muduo）有什么区别？

**答**：机制基本一致，两处差异：

1. **唤醒策略**：muduo 有 `callingPendingFunctors_` 标志做**条件唤醒**，我这边是无条件唤醒。无条件是安全但保守的 —— 多一次系统调用，但绝不丢任务。要优化成 muduo 那样，关键是识别出「在 loop 线程内**且**正在执行 pending 任务」时唤醒**仍然必需**（因为队列已被 swap 走，新任务不会在本轮被看到）。
2. **poll 超时**：muduo 是 `kPollTimeMs = 10000`（10 秒兜底），我是 `poll(-1)` 永久阻塞，完全靠 timerfd 驱动。

### Q10：如果让你改进这套机制，你会做什么？

**答**：四点，按优先级：

1. **补 `callingPendingFunctors_` 条件唤醒** —— 最直接的开销优化
2. **队列加背压/上限** —— 目前无界，loop 持续繁忙 + 生产者疯狂投递会内存膨胀。muduo 也是无界的，但至少应该能观测（队列长度作为 metric 导出）
3. **闭环延迟可观测** —— 目前没有「任务从入队到执行」的耗时指标。这个数字直接反映 loop 是否被拖住，是排查「连接卡顿」的关键信号
4. **一个更根本的**：`runInLoop` 的语义是「尽快执行」而非「立即执行」，但接口名容易让人误以为立即。应该补文档或在调试日志里标注排队延迟 —— 目前**队积压是完全静默的**

---

## 附：自查表

| # | 问题 | 能说清的关键点 |
|---|---|---|
| 1 | 为什么需要这套机制 | 免锁的依据是线程封闭；队列是唯一合法的跨线程通道 |
| 2 | 模式的正式名字 | Reactor（架构）+ Active Object（机制）+ Half-Sync/Half-Async（分层）+ Command（任务）+ Thread Confinement（正确性） |
| 3 | 核心不变式 | **任何连接的可变状态只被归属线程访问** |
| 4 | `runInLoop` vs `queueInLoop` | 前者同线程直通、跨线程投递；后者无条件投递 |
| 5 | 为什么必须 wakeup | `epoll_wait(-1)` 永久阻塞，队列里的任务唤醒不了自己 |
| 6 | 为什么 eventfd 不用 pipe | pipe 写满会阻塞（恰好在最需要唤醒时）；eventfd 单 fd + 计数器永不阻塞 |
| 7 | 为什么不用条件变量 | 无法与 `epoll_wait` 一起阻塞，要么加线程要么退化轮询 |
| 8 | 为什么 swap 缩锁 | ① 防自死锁（任务体是用户代码，可能再入队）② 防阻塞生产者 |
| 9 | 同一条原则的三处应用 | `doPendingTasks` / `handleTimer` 先收集再关闭 / `handleNewConnection` 先解锁再回调 |
| 10 | 任务里能阻塞吗 | **绝对不能** —— 阻塞 loop = 阻塞该 loop 上所有连接 |
| 11 | 投递闭包怎么防悬垂 | 按值捕获 `shared_from_this()`（这是 `enable_shared_from_this` 的原因之一） |
| 12 | 代价与边界 | 一次 syscall + 队列锁 + 闭包构造；**执行时机是「下一轮」不是「立即」**；只适合低频跨线程 |
| 13 | muduo 的差异 | ① `callingPendingFunctors_` 条件唤醒 ② `kPollTimeMs=10000` vs `poll(-1)` |
| 14 | 为什么同线程要快路径 | 省掉闭包构造 + 加锁 + 唤醒，热路径上很值 |
| 15 | 顺序性保证到哪一步 | 同一 loop 内 FIFO 有序；**跨 loop 无序** |
| 16 | muduo 定时器怎么实现的 | **timerfd + 按到期时间排序的 set + 精确 rearm**（不是时间轮/红黑树） |
| 17 | 工业界的对应物 | Android Looper / Chromium TaskRunner / asio post+strand / GCD 串行队列 / Actor 邮箱 |
| 18 | Rust 怎么做的 | 用类型系统（`Send`/`Sync`）在**编译期**强制，而非靠纪律 |
| 19 | Fallback vs Circuit Breaker | 本项目的降级是按次回退、**无熔断窗口、无主动拒绝** —— 不是 Circuit Breaker |
