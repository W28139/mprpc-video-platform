# wevix_muduo 网络库

## 一、整体架构与线程模型

### Q1.1 介绍一下这个网络库的整体架构

它是一套 **One Loop Per Thread 的 Reactor 网络库**，受 muduo 启发但完全独立实现。整体是 **1 个 mainLoop + N 个 subLoop**：mainLoop 只负责 `accept`，**每来一个新连接就按 `fd % N` 分配给某个 subLoop；**subLoop 跑在**独立的 IO 线程里**，负责这个连接上所有的 read/write 和业务回调。**每个 EventLoop 内部封装了 epoll、用于跨线程唤醒的 eventfd、以及做连接超时扫描的 timerfd。**

在这之上还有可选的一层 **work 线程池**：如果业务处理是 CPU 密集或会阻塞的，IO 线程把消息 `move` 到堆上投进 work 池就立刻返回，不阻塞网络收发。

#### Q1.1.1 subLoop 是在哪个线程里跑起来的？

**主线程跑 mainLoop，`ioThreadPool_` 里的 N 个线程各跑一个 subLoop**。线程池被当成「线程容器」用，每个线程只跑一个永不返回的 `run()`

`TcpServer::start()` 里把每个 subLoop 的 `EventLoop::run()` 作为任务投进 `ioThreadPool_`，然后当前线程阻塞在 `mainLoop_->run()` 上（`wevix_muduo/src/TcpServer.cpp:46-58`）：

#### Q1.1.2 如果业务回调很慢会怎样？

**没开 work 池**：`TcpServer::handleMessage` 直接同步调用用户回调（`TcpServer.cpp:160-164`），慢业务会把这个 subLoop 上**所有连接**的收发都堵住。因为回调不返回，线程执行就回不到下一次 `epoll_wait`。

**开了 work 池**：IO 线程只做一次 `move` 到堆上就投递走人（`TcpServer.cpp:150-159`）：

---

### Q1.2 为什么选择 One Loop Per Thread 这个模型？

核心是为了**避免锁**。**一个连接从建立到销毁，全程只被一个固定的线程碰**——它的 fd、Buffer、回调都在那个线程里。**这样连接内部的所有状态都不需要加锁保护，省掉了锁开销**

对比另外两种常见模型：
- **单 Reactor 单线程**：连接一多就是瓶颈，而且一个慢请求阻塞所有其他请求；
- **每连接一线程**：连接数一上来**线程切换开销爆炸**，C10K 直接崩。

#### Q1.2.2 这个模型有什么缺点？

**第一，负载可能不均**。我用 **`fd % threadNum` 分配**，可能某个 subLoop 分到的连接明显更多。**muduo 用的是原子计数轮询，分配更均匀**。我选 `%` 是因为它**确定性强、便于调试**，而且实现简单，代价就是均匀性略差。

**第二，单个慢回调会拖累这个 loop 上的所有连接**。这是模型固有的，**只能靠 work 池隔离**。

**第三，跨线程操作有额外开销**。任何从其他线程发起的事情（ **work 线程要发响应**）都得 `queueInLoop` + `wakeup`，也就是一次 eventfd 系统调用。

---

## 二、EventLoop：事件循环与跨线程唤醒

### Q2.1 EventLoop 里都有什么？

1. **一个 Epoll**——封装 **epoll fd** 和**事件数组**
2. **eventfd + wakeChannel**——跨线程唤醒，其他线程往这个 fd 写 8 字节，就能把阻塞在 `epoll_wait` 里的 loop 线程叫醒
3. **timerfd + timerChannel**——定时器，用来做连接超时扫描
4. **一个任务队列 + mutex**——`pendingTasks_`，配合 `runInLoop` / `queueInLoop` 实现跨线程投递任务

5.  `conns_`（`map<int, ConnectionPtr>`）用于**超时扫描**
6.  `threadId_` 原子变量记录自己归属哪个线程

eventfd 和 timerfd **都是 fd**，所以**能统一挂到 epoll 上**——「一切皆 fd」的统一性是 Linux 事件循环设计的精髓

#### Q2.1.1 为什么定时器要用 timerfd？直接用 epoll_wait 的 timeout 不行吗？

用 **timerfd** 的好处是：定时器变成了**一个 fd**，和 socket、eventfd 一样被 epoll 统一管理。`timerfd_settime` 设置周期后，**内核负责在到期时让这个 fd 可读**。

**代价**：超时检测是**轮询式的**：每 5 秒扫一遍连接表，看谁的 `lastActiveTime_` 超时（精度 = 5 秒节拍），**不适合大量异构的精确定时任务**。

#### Q2.1.2 timerfd 会不会积压？

**默认会积压，但我的代码设置不会**。timerfd 不是给排很多个事件，而是**维护一个到期计数**。周期定时器到期后如果没人 `read`，`ticks` 就累加，fd 一直可读；下次 `read` 拿到的是错过总次数，然后清零。

但我这段代码`it_interval` 被置 0，是一次性闹钟，**内核最多 pending 一次**；而且**每轮都会 `timerfd_settime` 重装**，重装会把旧 `ticks` 清零，可读状态也清掉。

---

### Q2.2 为什么用 eventfd 唤醒？用 pipe 不行吗？（这个是用于唤醒处理其他线程的任务）

* **pipe 要两个 fd**（读端 + 写端），管理成本翻倍
* **eventfd**更方便统一管理，因为可以挂在epoll上，可以通过epoll_wait唤醒
* 唤醒逻辑：其他线程调用**wakeup()**，**epoll_wait**触发写事件，执行**handleWakeup()**读掉写进去的内容，然后执行到**doPendingTasks()**

#### Q2.2.1 write 会不会丢？多个线程同时唤醒怎么办？不会丢

`eventfd` 的 `read` 返回**累计的 write 次数**并**原子清零**。假设两个线程几乎同时调用 `wakeup()`：

- 线程 A 写 1 → 计数器 = 1
- 线程 B 写 1 → 计数器 = 2
- loop 线程 `read` → 返回 2，计数器归零

loop 线程**至少会醒一次**，醒了之后 `doPendingTasks` 会把队列里 A 和 B 投的两个任务都取走执行。

---

### Q2.3 `runInLoop` 和 `queueInLoop` 有什么区别？

`runInLoop` 会判断「我是不是 loop 线程」，是就直接执行；**`queueInLoop` 无条件入队 + 唤醒**（`EventLoop.cpp:123-144`）：

`runInLoop` 是**给业务用的**：调用方不需要知道自己是不是在 loop 线程里。

比如 `Connection::send` 中，如果调用方本来就在 IO 线程，直接发；如果在 work 线程，自动转投。

 `runInLoop` 「近路优化」意义——大多数回调场景其实已经在 loop 线程，走 `isInLoopThread()` 判断比无条件入队 + 唤醒快得多（少一次锁 + 一次系统调用）

![image-20260921144922180](C:\Users\28783\AppData\Roaming\Typora\typora-user-images\image-20260921144922180.png)

#### Q2.3.1 `isInLoopThread`为什么用原子变量？

此处 atomic 防的不是"多个写者",而是"读写之间没有 happens-before "——**store 在 IO 线程的 run() 里**,而**主线程提交完任务就直接进 mainLoop** 了、不等它,所以**主线程每次读 threadId_ 都和那次 store 构成 data race**。

`A happens-before B`,意思是：A 写的内存,保证对 B 可见，编译器/CPU 不能把 A 重排到 B 之后。

流程如下：handleNewConnection → connectEstablished→ runInLoop → isInLoopThread() 这条主线程路径,会和 IO 线程刚进 run() 时的那次 store 时间上重叠。

用 `relaxed` 原因：

① 它**不承担同步责任**，真正的同步在 `queueInLoop` 的 mutex + eventfd 上；它只需要保证happens-before即可

② **读错了也不出错**——初值 0（`getTid()` 不可能是 0），`run()` 之前谁都读到 false → 一律走 `queueInLoop` 慢路径（入队 + 唤醒），任务照样执行，只多一次锁和系统调用。

---

### Q2.4 `doPendingTasks` 为什么要把队列 swap 出来？

**为了把锁的范围缩到最小**（`EventLoop.cpp:167-181`）：

不用 swap 的朴素写法是「持锁遍历队列、逐个执行」，会消费大把时间占用锁（因为包含执行任务）

swap 方法保证持锁时间只有一次 `std::queue` 的指针交换（O(1)，几个指针赋值），任务在锁外执行。其他线程投任务几乎不会被阻塞。

---

### Q2.5 连接超时是怎么检测的？

**每个 subLoop 每 5 秒扫一遍自己管的连接表，把超过 10 秒没活跃的连接强制关闭。**

具体在 `EventLoop::handleTimer` 里（`EventLoop.cpp:183-219`）：先重装闹钟，然后**只对 subLoop 生效**（mainLoop 不参与连接管理）：

- **「扫描在锁内、回调在锁外」**——先把过期的收集到 `expired` 数组，出了作用域（释放锁）再 `forceClose()`。因为 `forceClose` → `handleClose` → `removeConnection` 会去改 `conns_`，持锁回调就是自死锁
- 10 秒超时 vs 5 秒节拍的**精度关系**：实际超时时间在 10~15 秒之间，这是节拍扫描的固有精度损失

#### Q2.5.1 这个方案和 muduo 的 TimerQueue 比怎么样？

**答**：**我的更简单，muduo 的更通用。这是一次明确的取舍。**

| 维度 | 我的 timerfd 周期扫描 | muduo TimerQueue |
|---|---|---|
| 数据结构 | 无（每次全表扫） | 小根堆（`std::set<pair<Timestamp, Timer*>>`） |
| 单次检查成本 | O(N) 全表 | O(1) 看堆顶 |
| 定时能力 | 只有「周期性节拍」 | 任意时刻、one-shot / repeat |
| 精度 | 节拍粒度（5 秒） | 精确到到期时刻 |
| 代码量 | ~30 行 | ~150 行 |

## 三、Epoll 与 Channel：事件分发

### Q3.1 Channel 是什么？为什么要有这一层？

`Channel` 是**「一个 fd + 它关心的事件 + 事件发生时的回调」**这三者的绑定。它本身不拥有 fd（fd 的生死由 `Socket` 的 RAII 管），只是把「这个 fd 上的事件该找谁处理」这件事封装起来。

它解决的是**解耦**问题：`Epoll` 只管「哪些 fd 就绪了」，不关心就绪之后干什么；`EventLoop` 只管「把这些就绪的 channel 拿去分发」。具体「读事件该调哪个函数」的知识被关在 Channel 里。

**零查找**：`epoll_wait` 返回时 `Channel*` 已经在手上了，不需要用 fd 去 map 里查。代价是**Channel 的生命周期必须严格覆盖它在 epoll 里的时间**

#### Q3.1.1 fd 和 Channel 是一对一的吗？

**答**：**逻辑上一对一，但我的实现里不止一种 Channel 类型。**

一个 fd 只会绑一个 Channel（因为 `epoll_event.data.ptr` 只能放一个指针）。项目里实际创建 Channel 的地方有三处：

1. **每个 `Connection` 一个**——`channel_(new Channel(loop_, socket_->fd()))`（`Connection.cpp:17`），管客户端连接；
2. **每个 `EventLoop` 一个 wakeChannel**——绑 eventfd（`EventLoop.cpp:51`）；
3. **每个 `EventLoop` 一个 timerChannel**——绑 timerfd（`EventLoop.cpp:53`）；
4. **`Acceptor` 一个 acceptChannel**——绑监听 socket（`Acceptor.cpp:13`）。

所以「fd ↔ Channel」是一对一，但**Channel 的用途有四种**：**数据连接、跨线程唤醒、定时、监听**。

这恰好体现了 Channel 作为抽象的价值——四种完全不同的 fd 用同一套「事件 → 回调」机制，`EventLoop::run` 里对它们一视同仁

#### Q3.1.2 Channel 为什么要重载 `operator new`？

**答**：**因为 Channel 是高频率创建/销毁的对象，我想让它走自研内存池，避开 malloc。**

每个连接建立时 new 一个 Channel，断开时 delete——高并发短连接下这就是每秒几万次的 malloc/free。

**但这里有个我实测出来的诚实结论**，Channel 大约是 152 字节的对象，**它还不足以成为性能瓶颈，因此这项改动的收益不大**

---

### Q3.2 为什么用 ET（边缘触发）而不是 LT（水平触发）

**为了减少 `epoll_wait` 的唤醒次数和事件重复通知。**

LT（水平触发）的语义是「只要 fd 可读，每次 `epoll_wait` 都会通知」。所以如果一次没读完，下一轮还会被通知——好处是**编程简单，不读也不丢**；坏处是**同一个 fd 可能被反复通知**，大量时间花在重复的事件通知上。

ET（边缘触发）的语义是「只在状态**变化**时通知一次」。一次可读事件只通知一次，**必须**循环 `read` 到 `EAGAIN` 为止，否则剩下的数据会一直躺在内核缓冲区里，**永远不再通知**。

**注意：ET 和 LT 对业务层是透明的**——上层完全感觉不到区别。真正需要配合的是「必须读到 EAGAIN」这条纪律。

#### Q3.2.1 ET 模式下 `EPOLLOUT` 有什么坑？

ET 下 `EPOLLOUT` 的语义是「从**不可写**变**可写**时通知一次」。socket 绝大多数时候都可写，所以平时**根本收不到** `EPOLLOUT`，只有发送缓冲满过又空了才会来一次。由此两条规则：

**规则一：按需注册。** `sendInLoop` 先试直发，发不完才 `enableWriting()`（`Connection.cpp:232-265`）；发完立刻 `disableWriting()`（`Connection.cpp:144-148`）。不能一上来就常驻监听——那会让 epoll 每次都报可写，等于白忙。

**规则二：同批事件里写必须先于读。** ：epoll 一次返回 `EPOLLIN | EPOLLOUT | EPOLLRDHUP`，而代码**先处理读**，读回调执行完就 `return`→ **`EPOLLOUT` 被吞掉**。此时输出缓冲里还有没发完的数据，但状态**没有任何迁移**：没有新 `append`，也没有新的 `enableWriting`→ ET 下**不会重触发** → 数据永远发不出去。

| 顺序 | 被跳过的 | 后果 |
|---|---|---|
| 先写后读 | `EPOLLIN` 延迟一轮 | **无害**——**等下一次新数据到达** |
| 先读后写 | `EPOLLOUT` 延迟一轮 | **致命**——永远不会再次触发 |

ET 下 `EPOLLIN` 不会因为“接收缓冲区还有数据”而反复报。但如果对端**又发来新数据**，协议栈会再次唤醒 epoll，产生一次新的可读通知。而`EPOLLOUT` 的 ET 触发条件是：发送缓冲区从“满”变“不满”，也就是 socket 从不可写变可写。错过一次 `EPOLLOUT` 后，发送缓冲区已经变成不满/空，socket 一直可写。这个“可写”状态会持续存在。

### Q3.4 `Channel::handleEvent` 里的事件处理顺序是什么？为什么是这个顺序？

顺序是：**`EPOLLHUP`（无 `EPOLLIN`）→ `EPOLLERR` → `EPOLLOUT` → `EPOLLIN|EPOLLPRI|EPOLLRDHUP`**，而且**每个回调执行完立刻 `return`**（`Channel.cpp:77-113`）：

**第一层：错误优先于正常事件。** `EPOLLHUP` 和 `EPOLLERR` 是「这个连接完了」的信号，先处理它们能避免在坏连接上做无用功。特别地，`EPOLLHUP` 我加了 `&& !(revents_ & EPOLLIN)` 的条件——因为对端正常关闭时（发了 FIN 但还有数据没读完），内核也会置 `EPOLLHUP`，但这时**还有数据要读**，不能直接关。

**第二层：写优先于读。** 这是 ET 模式下的硬性要求，见 Q3.2.1 的详细分析——是同批事件里「先读后写会永久卡死」逼出来的。**（稍后研究）**

**第三层：每个回调后立即 return。** 这是 UAF 防线，见 Q3.5。

（**UAF 是一种具体的错误；UB 是一个更大的类别。UAF 属于 UB 的一种**，**UAF = Use-After-Free，中文叫“释放后使用”**）

---

### Q3.5 回调之后为什么必须 `return`？

**因为回调可能把 `this` 给销毁了。**

`Channel` 是被 `Connection` 持有的（`std::unique_ptr<Channel> channel_`），而 `Connection` 被 `TcpServer::connections_` 里的 `shared_ptr` 和 `EventLoop::conns_` 里的 `shared_ptr` 共同持有。当读回调里发生「对端关闭」「错误」等事件时，会一路走到 `handleClose` → `TcpServer::removeConnection` → 最后一个 `shared_ptr` 释放 → **`Connection` 析构 → `Channel` 析构**。

而这一切**发生在 `Channel::handleEvent` 的调用栈里面**。`handleEvent` 是 `Channel` 的成员函数，`this` 已经被析构了，接下来的任何 `revents_`、`fd_` 访问都是**读已释放内存**。

#### Q3.5.1 muduo 是怎么解决的？为什么不用同样的方案？

**答**：muduo 官方靠 **`shared_ptr` 保活 + 引用计数**，具体是在 `Channel::handleEvent` 里对 `tie_`（一个 `weak_ptr`）做提升：

```cpp
// muduo 官方 Channel::handleEvent 的大意
void Channel::handleEvent(Timestamp receiveTime)
{
    if (tie_.lock())    // 提升成功 → 对象还活着，且被临时 shared_ptr 保住了
    {
        handleEventWithGuard(receiveTime);
    }
    // 提升失败 → 已经死了，什么都不做
}
```

**为什么不用 muduo 的方案：**

**性能**：`tie_.lock()` 是一个 `weak_ptr` 提升，涉及原子引用计数的 CAS。`handleEvent` 是**最热的路径**——每个 epoll 事件都要走一次。为了一个「极少数回调会同步销毁自己」的场景，在最热路径上加原子操作，我认为不划算。

**代价**：这个方案依赖于**「未来所有新增的回调分支都记得 return」**。如果哪次重构漏了，就是 UAF。muduo 的方案在这一点上更**抗腐蚀**。这是个真实的取舍——我选了我认为当下更合适的那个，但如果这个库要长期多人维护，我会倾向于 muduo 的自动保活。

#### Q3.5.2 那 `Connection::handleRead` 里的 `ConnectionPtr self(shared_from_this())` 有什么用？

这个 `self` 的作用是：**保证从函数开始到结束，`Connection` 不会被析构**。但它保护不了 `handleEvent`——因为 `self` 是 `handleRead` 的**局部变量**，`handleRead` 返回时 `self` 析构，引用计数又降回去，如果这时没有其他持有者了，析构**就发生在 `self` 析构的那一刻**，也就是 `handleRead` 即将返回时。而 `handleEvent` 还在栈上，之后访问 `revents_` 就是 UAF。

所以这两个机制是**互补的、不同层级的**：

| 机制 | 位置 | 保护对象 |
|---|---|---|
| `ConnectionPtr self(shared_from_this())` | `handleRead` 内 | 函数**执行期间**的对象存活 |
| 回调后立即 `return` | `handleEvent` 内 | 回调**返回之后**不再访问 `this` |
| `handleClose` 里的 `runInLoop` + self 捕获 | `handleClose` | 把**销毁动作推迟**到本次事件处理之外 |

**注意 `handleRead` 里的 `self` 还有一个更重要的用途**：它是**传给业务回调的参数**：

```cpp
onMessageCallback_(self, message);      // Connection.cpp:105
```

业务层（比如 mprpc 的 `RpcProvider::OnMessage`）拿到这个 `shared_ptr` 后，可能把响应异步发出去、可能投进 work 线程池——**那时 `handleRead` 早就返回了**。如果没有 `self` 保活，work 线程拿到的就是一个悬垂指针。这跟 `TcpServer::handleMessage` 里 `workThreadPool_->addTask([this, conn, msg]() {...})` 按值捕获 `conn`（一个 `shared_ptr`）是同一个道理。

---

## 四、TcpServer 与 Acceptor：连接建立

### Q4.1 `TcpServer` 的生命周期和线程模型是怎样的？

构造时干四件事（`TcpServer.cpp:11-39`）：

- `mainLoop_`（构造参数 `true`）→ **只做 accept**，不参与连接管理；
- `subLoops_` × N（构造参数 `false, 5, 10`）→ 每个管一批连接的 read/write + 5 秒超时扫描；
- `ioThreadPool_` → 它是**线程容器**，N 个线程各跑一个 `EventLoop::run()`。

**注： `mainLoop_` 和 `subLoops_` 的构造参数不同**：`EventLoop(true)` 用默认的 `(30, 100)`，`EventLoop(false, 5, 10)` 用 5 秒节拍、10 秒超时。而且 `handleTimer` 里 `if (!isMainLoop_)` 保证了**只有 subLoop 会扫超时**——mainLoop 虽然也创建了 timerfd，但它不做连接管理。

Acceptor 挂在 mainLoop 上、Connection 挂在 subLoop 上——**这是主从 Reactor 的核心分工**

#### Q4.1.1 `SIGPIPE` 到底是干什么的？不忽略会怎样？

**在对端已经关闭的 socket 上调用 `write`，内核会先发一个 `SIGPIPE` 信号，默认行为是直接终止进程。**

**两层防护**：

**第一层（更全面）：进程级忽略**（`TcpServer.cpp:19`）：

```cpp
::signal(SIGPIPE, SIG_IGN);
```

**第二层（更精确）：发送时用 `MSG_NOSIGNAL`**，让这一次 `send` 不产生信号，而是返回 `EPIPE` 错误码：

```cpp
ssize_t n = ::send(fd(), outputBuffer_.peek(), outputBuffer_.readableBytes(), MSG_NOSIGNAL);
```

**为什么要两层？** —— `SIGPIPE` 不只 `send`/`write` 会触发，`sendfile`、`splice` 等也会。**进程级忽略是「兜底所有情况」**

#### Q4.1.2 `TcpServer` 的析构为什么要调用 `stop()`？

**答**：**为了把「优雅停止」这件事做成 RAII。**

1. **先停 mainLoop** —— 不再接受新连接；
2. **再停 subLoops** —— 网络事件不再产生；
3. **然后 `ioThreadPool_.stop()`** —— 这一步是**阻塞的**，它会 `join` 所有 IO 线程（见 Q7.5），保证所有 `EventLoop::run()` 真的退出来了；
4. **最后停 work 池** —— 因为 work 池的任务可能还在用连接，要在网络层完全停下之后再收尾。

**为什么 `stop()` 要幂等？** —— 因为用户可能显式调用 `stop()`，然后 `TcpServer` 析构时又调一次。

---

### Q4.2 `Acceptor` 为什么要循环 `accept`？

**因为一次 `epoll` 唤醒时，accept 队列里可能积压了多个连接，一个一个取会让后面的连接等下一轮 `epoll_wait`，累积 Accept 延迟。**

**前提条件是被 accept 的 socket 必须是非阻塞的**，否则循环 `accept` 会永久阻塞在最后一个 `accept` 上。我用 `accept4` 一步到位（`Socket.cpp:95-108`）：

用 `accept4` 而不是 `accept` + 两次 `fcntl`，省了两次系统调用。

---

### Q4.3 `connectEstablished` 为什么延迟注册读事件？**为什么不能在 `Connection` 构造函数里就 `enableReading()`？**

因为 `Connection` 是在 **mainLoop 线程**里创建的，而它属于 **subLoop 线程**。如果构造函数里直接注册读事件，那么：
1. `enableReading()` 会调 `epoll_ctl(ADD)`——但这是在 mainLoop 线程做的，**跨线程操作了 subLoop 的 epoll fd**；
2. 如果此时 subLoop 正好在跑 `epoll_wait`，它可能**立刻**就返回这个新连接的读事件，然后调用 `handleRead` ，而此时 `messageCodec_` 这些**回调还没设置完**

**所以设计是：先在 mainLoop 线程里把「这个连接该怎么工作」全部配置好，再通过 `runInLoop` 转投到 subLoop 线程，在那里注册读事件。**确保在触发事件前，所有回调（配置）全部设置完成

---

## 五、Connection：连接生命周期

### Q5.1 `Connection` 的生命周期是怎么管理的？为什么继承 `enable_shared_from_this`？

`Connection` 用 `shared_ptr` 管理，而且有**两个持有者**：

1. **`TcpServer::connections_`**（`map<int, ConnectionPtr>`）—— 全量连接表，用于管理和遍历；
2. **`EventLoop::conns_`**（`map<int, ConnectionPtr>`）—— 每个 subLoop 自己的一份，**只用于超时扫描**。

为什么要两份？因为**超时扫描发生在 subLoop 线程里**（`timerfd` 回调），而 `TcpServer::connections_` 是跨线程共享的（有 mutex）。如果让 subLoop 每 5 秒去锁 `TcpServer::mutex_` 扫全表，会和 mainLoop 上的 accept 抢锁；分成两份之后，**subLoop 只锁自己的 `connsMutex_`**，扫描的是自己管的那些连接，锁竞争降到最低。

**继承 `enable_shared_from_this` 的原因**：需要在成员函数里**安全地获取指向自己的 `shared_ptr`**

- `handleRead` 要用 `self` 作为参数传给业务回调（回调可能异步持有）；
- `handleClose`、`forceClose` 要 `runInLoop` 一个 lambda，lambda 里**必须捕获 `self`** 来保活（否则 lambda 执行到一半对象就没了）；
- `send` 跨线程时要 `std::bind(&Connection::sendInLoop, shared_from_this(), data)`

**为什么不能直接 `ConnectionPtr(this)`？** 那会创建一个**独立的引用计数**，变成**两个控制块管理同一个对象**

#### Q5.1.1 `make_shared` 相比 `shared_ptr(new Connection(...))` 有什么好处？

**好处**：`make_shared` 把「对象本身」和「引用计数控制块」**合并成一次分配**——它申请一块内存。而 `shared_ptr(new Foo)` 是**两次分配**（一次 `new Foo`，一次控制块）。

此外 `make_shared` 还有**异常安全**的优势：`f(new Foo(), g())` 这种写法在参数求值顺序不定时可能泄漏，`make_shared` 没有这个问题。

**代价**：`make_shared` **不会调用类的 `operator new`**。

我给 `Channel` 重载了 `operator new`，但 `Channel` 是通过 `std::unique_ptr<Channel> channel_(new Channel(...))` 创建的，用的是裸 `new`，**确实走到了我的 `operator new`**。

而如果换成 `make_shared<Channel>`，它就会走**全局 `operator new`**，绕过我的内存池

---

### Q5.2 `handleClose` 为什么要用 `runInLoop`？

**为了把「销毁连接」这个动作推迟到「本次事件处理之外」，避免在 `Channel::handleEvent` 的调用栈里销毁 `Channel` 自己。**

（`Connection.cpp:161-184`）。

**几个设计点**：

**① `disconnected_` 幂等保护**。`handleClose` 可能被多次调用（`handleRead` 里读到 0、`handleError` 里、超时扫描里）。第一次进来设了标志，后面的直接返回，保证清理逻辑只跑一遍。

**② 用 `runInLoop` 而不是直接执行**，**保证「所有销毁动作都在连接归属的线程里发生」**，杜绝跨线程析构

- 从 loop 线程调用（`handleRead` 检测到对端关闭）→ 立刻清理；
- 从其他线程调用（业务线程主动 `forceClose`）→ 转投到 loop 线程清理。

**③ `self` 捕获**。lambda 被 `runInLoop` 包成一个 `std::function` 存进队列，可能要过一会儿才执行。如果捕获的是 `this`，对象可能已经析构。捕获 `self`（`shared_ptr`）**保证 lambda 存活期间对象一定活着**。

**④ `handleError` 为什么不用 `runInLoop`？**

这里是**直接执行**的。`handleError` 只在 `handleRead` 里被调用（`Connection.cpp:82`），也就是**一定在 loop 线程**，所以不需要 `runInLoop` 转投

---

### Q5.3 `send` 是怎么保证线程安全的？

**靠「自动转投」——调用方不需要关心自己在哪个线程。**（`Connection.cpp:204-223`）。

**为什么必须转投？** 因为 `sendInLoop` 会碰三样**只属于 loop 线程的东西**：

- `outputBuffer_`（`Buffer` 非线程安全）；
- `channel_->isWriting()` / `enableWriting()` / `disableWriting()`（会调 `epoll_ctl`，动的是 loop 的 epoll fd）；
- `::send(fd, ...)`（虽然 `send` 本身线程安全，但和上面两个配合起来就有竞态——比如两个线程同时判断「没有 pending 数据」然后同时直发，会把数据顺序搞乱）。

**`sendInLoop` 的两级发送策略**（`Connection.cpp:225-272`）：

**这个「先直发、发不完再缓冲」的策略就是为了追求「零缓冲」的快路径**——在负载不重的时候，绝大多数 `send` 都能一次发完，完全不走输出缓冲区、不注册 `EPOLLOUT`、不产生额外的 epoll 事件。

#### Q5.3.1 输出缓冲区无限增长怎么办？

**答**：**我这版没做上限保护，这是一个真实的缺口。** 我如实说明现状和应该怎么做。

**问题场景**：客户端连上来之后**只发请求不读响应**（或者读得极慢）。服务端拼命 `send`，socket 发送缓冲区满了之后数据全堆在 `outputBuffer_` 里。因为客户端一直不读，`EPOLLOUT` 一直不触发（缓冲区一直满），`outputBuffer_` 就**只增不减**——最终 OOM。

这实际上是一个**慢速客户端攻击**的变种。

**正确做法（没实现）**：

```cpp
// 应该在 sendInLoop 的 append 之前检查
constexpr size_t kMaxOutputBufferSize = 64 * 1024 * 1024;   // 64MB

if (outputBuffer_.readableBytes() + remaining > kMaxOutputBufferSize)
{
    LOG_ERROR("fd=%d output buffer overflow (%zu bytes), force close",
              fd(), outputBuffer_.readableBytes());
    faultError = true;      // 走 handleClose 路径
}
```

**muduo 的做法**更精细，它提供了「水位线回调」（`HighWaterMarkCallback`）：缓冲超过阈值时通知业务层，让**业务层自己决定**是断开还是降级。而不是一刀切强制关闭。

**为什么我没做**：这个库目前的主要使用者是 mprpc，而 mprpc 的交互模式是**同步一问一答**（客户端发请求 → 等服务端响应 → 才发下一个）。响应报文通常不大，且客户端一定在读。但这是「**当前使用场景下不会触发**」，不是「设计上安全」——如果哪天有人用这个库写推送服务，这就是个必须补的洞。

---

### Q5.4 `handleRead` 为什么必须循环读？

**答**：**因为 ET 模式下不读到 `EAGAIN`，剩下的数据就永远不会再通知。**完整实现（`Connection.cpp:50-132`）

**① `totalRead > 0` 才刷新 `lastActiveTime_`**。如果一次 `readFd` 就返回 `EAGAIN`（epoll 说可读但实际被别的线程读走了），不应该刷新活跃时间。

**② `EINTR` 必须重试**。信号（比如 `SIGCHLD`、`SIGALRM`）打断 `read` 时返回 `-1/EINTR`，这不是错误，要 `continue`。

**④ 错误分支返回前要 `return` 而不是 `break`**——因为 `handleError()` 已经处理了关闭逻辑，后面的 `if (peerClosed) handleClose()` 不该再跑下面逻辑

**⑤ 帧编解码器循环**（`Connection.cpp:90-115`）：在 `totalRead > 0` 之后还有一层循环：

这是**处理粘包**的地方：一次 `read` 可能读到 `[完整帧1][完整帧2][帧3的一半]`，`while` 循环把前两帧都吐出来，第三帧留在 Buffer 里等下次。

**注意两个 `disconnected_` 检查**：一个在取帧前、一个在回调后。因为**一次 read 里可能连收几个帧，中间某一帧的处理把连接关了**（比如 RPC 收到 cancel 请求），后续的帧就不该再处理了。同时 `self` 保活让对象不会崩——只是「逻辑上不再处理」。

---

### Q5.5 `shutdown()` 是干什么用的？（与mprpc相关）

**答**：**发起 TCP 半关闭（发 FIN），只关写端、保留读端。**

对比 `close()` 和 `shutdown(SHUT_WR)` 的区别：

| | `close(fd)` | `shutdown(fd, SHUT_WR)` |
|---|---|---|
| 效果 | 关闭整个 fd，**读写都不能用** | 只关写方向，**读还能继续** |
| 发 FIN | 是（当引用计数归零） | 是 |
| fd 回收 | 是 | **否**（fd 还在） |
| 能否再 `recv` | 不能 | **能** |

**在我项目里的用途**：主要是 **RPC 响应用完之后，让服务端安全地把连接关掉**。

这里有个我专门研究过的细节——**服务端回包之后不能直接 `shutdown`**。

**核心矛盾**：`conn->send()` 是**异步**的——它可能把数据塞进 `outputBuffer_` 然后立刻返回，数据还在等着 `EPOLLOUT` 才真正发出去。而 `shutdown()` 是**同步**的——你调的那一刻就把 FIN 发出去了。如果 `send()` 时数据还没发完就 `shutdown`：

1. 已经排队的 `outputBuffer_` 里的数据可能**发不出去**（连接已经关了写端）；
2. 即使发出去了，接着 `handleWrite` 里对已 `shutdown` 的 socket 调 `send` 会返回 `EPIPE`。

所以 `shutdown` 适合「还有数据要发完、但不再发新的了」，`forceClose` 适合「立刻断开，不管有没有数据」。

---

### Q5.6 `Connection` 里的 `Buffer` 是值成员还是指针？为什么？

**答**：**值成员，两个 `Buffer` 直接内嵌在 `Connection` 里**（`Connection.h:88-89`）：

```cpp
Buffer inputBuffer_;   // 接收缓冲区
Buffer outputBuffer_;  // 发送缓冲区
```

**为什么用值语义**：

1. **生命周期绑定**——`Buffer` 和 `Connection` 同生共死，不需要额外的所有权管理。用 `unique_ptr<Buffer>` 只是多一次堆分配和一次间接访问，没有任何好处。
2. **访问更快**——`inputBuffer_.readFd(...)` 直接访问栈上（其实是对象内）的成员，没有指针解引用。`handleRead` 是热路径。

**代价**：`Connection` 的大小增加了。每个 `Buffer` 内部是 `std::vector<char>` + 两个 `size_t`，构造时 `vector` 会分配 `8 + 1024` 字节。所以**每个连接一建立就分配 2KB（两个 Buffer 各 1KB）**。

**这是个可以优化的点**：如果连接数很大而大部分连接流量很小，可以考虑**延迟分配**——`outputBuffer_` 只在真正需要缓冲时才扩容（现在构造时就分配了 1KB）。我的 `Buffer` 构造函数是 `buffer_(kCheapPrepend + initialSize)`，也就是**构造即分配**。

---

## 六、Buffer：三区内存模型

相比于常规read出数据：

**相同点：内核到用户态的 `O(n)` 拷贝无法避免；应用层读取数据都是直接访问内存指针，不额外拷贝。**
**它省掉的是数据进入用户态之后，因为消费、前插、扩容而产生的反复内存搬移和重复拷贝。**

| 操作         | `std::string` 常见做法                    | muduo `Buffer`                                    | 省了什么           |
| :----------- | :---------------------------------------- | :------------------------------------------------ | :----------------- |
| 消费已读数据 | `erase(0, len)` → `O(n)` 搬移剩余数据     | `readerIndex_ += len` → `O(1)`                    | 省掉搬移           |
| 前插协议头   | `insert(0, header)` → `O(n)` 搬移全部数据 | 写 `prependable` 区，`readerIndex_` 前移 → `O(1)` | 省掉搬移           |
| 追加写入     | `append`，可能扩容搬移                    | `writerIndex_ += len`，优先内部腾挪               | 减少扩容和搬移     |
| 突发大数据   | 每个连接预分配大缓冲区，或多次系统调用    | `readv` + 栈上 `extrabuf` 一次读尽                | 省内存、省系统调用 |
| 手动管理     | 自己维护读偏移、写偏移、头部预留          | 三区模型封装好                                    | 省心、不易出错     |

### Q6.1 三区模型是什么？

`Buffer` 内部是一个连续的 `std::vector<char>`，用**两个游标**把它逻辑上分成三段（`Buffer.h:22-29`）：

```text
  0              readerIndex_       writerIndex_       buffer_.size()
  | prependable  |   readable       |   writable       |
  | 可前置数据    |  可读数据(有效)   |   可写空间           |
```

- **prependable**（`0 ~ readerIndex_`）：已经被消费掉的旧数据空间，可以复用来在前面插入数据；
- **readable**（`readerIndex_ ~ writerIndex_`）：已经收到但上层还没取走的数据，`peek()` 拿首地址、`retrieve()` 消费；
- **writable**（`writerIndex_ ~ size`）：剩余可写空间。

**核心思想是「消费 = 移指针，不搬数据」**：`retrieve` 是 **O(1)** 的——只是加一个数，不 `memmove`、不删数据。被「消费」的字节自动变成 prependable 空间，下次写到那里时可以复用。

#### Q6.1.1 那 prependable 空间会无限增长吗？

**答**：**不会，上限是「历史峰值可读数据量」，而且会被 `makeSpace` 主动回收。**

当空间不够时，`makeSpace` 会把可读数据**搬回 `kCheapPrepend` 位置**，`readerIndex_` 被重置为 8（`Buffer.cpp:258-289`）。

**所以 `prependable` 的峰值 = 一次「整理周期」内被消费掉的最大字节数**。

**真正的「无限增长」风险在 writable 那边**——就是 Q5.3.1 说的输出缓冲区问题。`prependable` 只是「不回收的空间」，但它的上界被 `makeSpace` 的整理分支管住了。

#### Q6.1.2 `retrieveAll` 为什么重置到 8 而不是 0？

**答**：**为了保住那 8 字节的 prepend 预留。**

（`Buffer.cpp:77-82`）。如果重置到 0，下次要 `prepend` 一个长度前缀时，`prependableBytes()` 是 0，就没法前插了

重置到 8 保证了**任何时刻 `prependableBytes() >= 8`**，也就是**永远可以无代价地前插 8 字节**（正好一个 `int64_t` 或两个 `uint32_t`）。

---

### Q6.2 为什么消费不删数据？这样做不会浪费内存吗？

**因为删数据需要 `memmove`，而 `memmove` 是 O(N) 的。**

假设缓冲区里有 100KB 数据，消费了头部的 1KB。如果每次都「把剩下的 99KB 往前搬」：
- 时间是 O(N)，每次消费都要搬一次；
- 而且这个搬动是**纯粹的浪费**——数据内容一样，只是位置变了。

「移指针」的代价是 O(1)——一次加法。这是**用一个「浪费一点空间」换「省掉全部搬动」**的经典时间-空间权衡。

**至于「浪费内存」**：这些"被浪费"的空间**不是白扔的**，它们变成了 prependable 区域，有两个用途：

1. **前插数据**（`prepend`）——O(1) 在头部塞长度前缀；
2. **写新数据时的复用**——`makeSpace` 优先做「碎片整理」而不是「扩容」，就是把这些空洞用起来。

**所以真实的内存占用上界是「峰值可读数据量」，不是「累计吞吐量」**。一个连接传了 1GB 数据，只要每次读进来就很快消费掉，`Buffer` 的大小始终维持在几 KB。

#### Q6.2.1 如果上层一直不消费，缓冲区会怎样？

**答**：**会持续扩容，直到 OOM。** 这是我这个 `Buffer` 的一个真实边界。

流程是这样的：上层不消费 → `readerIndex_` 不动 → `readableBytes()` 很大 → `writableBytes()` 很小 → `append` 时 `ensureWritableBytes` 触发 `makeSpace`

---

### Q6.3 `readv` 散射读是怎么省的？

**它把「先读到临时缓冲区、再拷贝到 Buffer」这两步，合并成「直接读进 Buffer」一步。**

问题背景：`read(fd, buf, N)` 只能写**一块连续内存**。如果 `Buffer` 只剩 100 字节可写，你只能读到 100 字节——即使内核缓冲区里有 4KB 数据。而 ET 模式下**不读完就丢事件**，所以这是个必须解决的问题。

`readv` 允许一次系统调用把数据**散射**到多块不连续内存（`Buffer.cpp:179-233`）：

**情况 A（绝大多数）：只用一个 iovec 段，数据 `内核 → Buffer` 直达**。这时候 `readv` 的 iovcnt = 1，它**退化成 `read`**，连"散射"的机制开销都没有。

**情况 B：Buffer 空间不够，溢出的部分先进栈上 `extrabuf`，再 `append` 进 Buffer**。但注意——**只拷贝溢出的那部分**（`n - writable`），已经直接落进 Buffer 的那部分不重复拷贝。如果没有 `extrabuf` 这个设计，你要么只能读 Buffer 能装下的量（丢事件），要么开一个大的堆缓冲区然后整个拷贝一次。

对比「朴素方案」：
```
朴素：read(fd, tmpbuf, N)  → memcpy(tmpbuf, buffer+writer, n)   ← 两次触碰数据
我的：readv(fd, {buffer, extrabuf})                             ← 一次（多数情况）
```

最后的数据是都全部写入buffer,只不过情况B下，buffer会执行`makeSpace()`

#### Q6.3.1 为什么是 64KB？为什么放栈上？（普遍性问题，读一读）

**为什么是 64KB：**

1. **它是「一次系统调用能读完的合理量」**。Linux 默认 socket 接收缓冲区通常是 128KB~256KB（`net.core.rmem_default`），单次 `readv` 内核也会限制一次返回的量。取 64KB 能覆盖绝大多数场景下「Buffer 满了但内核还有数据」的溢出量。
2. **它是「栈上分配的极限附近」**。Linux 默认线程栈 8MB，64KB 只占 0.8%——安全。再大（比如 1MB）在多线程 + 深调用栈的场景下就有爆栈风险了。
3. **它是 2 的幂**，方便和 `writable` 做比较。

**为什么放栈上：**

1. **零分配成本**——栈分配就是移动栈指针（一条指令），不涉及 `malloc`。而 `readFd` 是**最高频的调用路径**（每个读事件、可能每次循环都调）。
2. **零并发问题**——栈是线程私有的，天然线程安全。如果是成员缓冲区，那 `Buffer` 就不能被两个线程同时读了。
3. **自动回收**——函数返回自动释放，不需要 RAII，也不会泄漏。
4. **命中 CPU 缓存**——栈内存是热的（刚被上一帧用过）。

**代价**：每次调用 `readFd` 都会「占用」64KB 栈空间——但因为函数返回就释放，**峰值栈深 = 调用链上同时存活的 `readFd` 数**。我的调用链里 `readFd` 不是递归的、也不会嵌套调用，所以峰值就是 64KB。

---

### Q6.4 `makeSpace` 的策略是什么？

**先尝试「碎片整理」，实在不够才「扩容」。**

**判断条件 `writableBytes() + prependableBytes() < len + kCheapPrepend` 的含义是**：
「把头部空洞和尾部剩余加起来，够不够放下 `len` 字节 + 保留 8 字节的 prepend 余量？」

- **够** → 碎片整理：把可读数据搬到 `kCheapPrepend` 位置，后面就腾出一大片可写空间。代价是**一次 `memmove`，但要搬的只有可读数据**（几十~几百字节量级），比 `resize` 便宜得多。
- **不够** → 扩容：`buffer_.resize(writerIndex_ + len)`。可能触发 `malloc` + 全量拷贝。

---

### Q6.5 `prepend` 是干什么用的？

**答**：**往「已写入数据的前面」插数据，要求 `prependableBytes() >= len`。**（`Buffer.cpp:147-160`）。**O(1) 前插**——不需要搬动后面的数据，只是把 `readerIndex_` 往前推。

**典型场景就是「先写 payload、后才知道帧头」**

## 七、ThreadPool：双模式与背压

### Q7.1 两种模式的区别是什么？

- **`MODE_FIXED`（固定）**：线程数在 `start()` 时确定，之后不变。适合**IO 线程池**——因为每个线程要跑一个永不返回的 `EventLoop::run()`，线程本身是「被占用的」，不能动态增删。
- **`MODE_CACHED`（缓存）**：任务积压时自动扩容，线程空闲 60 秒后自动回收。适合**业务线程池**——负载有峰谷，闲时不需要养一堆线程。

扩容逻辑在 `submitTask` 里（`ThreadPool.h:91-97`）收缩逻辑在 `threadFunc` 里（`ThreadPool.cpp:133-143`）

收缩条件是 `curThreadSize_ > initThreadSize_`——**不会把线程数缩到初始值以下**。`initThreadSize_` 是「保底线程数」，扩容出来的才是临时的。

---

### Q7.2 背压是怎么做的？

**用条件变量等 1 秒，等不到就放弃这次投递**（`ThreadPool.h:74-84`）：

**为什么需要背压**：如果不限制，任务队列会无限膨胀。队列越长，任务从入队到执行的延迟越大，最终客户端全部超时——**做了一堆无效工作**。

**背压的本质是「把压力传递给上游」**：队列满了 → 投递者被阻塞 → 上游（比如 IO 线程）处理变慢 → 上游的缓冲区积压 → 最终 TCP 接收窗口收缩 → 客户端发送变慢。**压力沿着调用链反向传导，最后落到最源头**，这才是正确的行为。

---

### Q7.3 为什么用 `notify_one` 而不是 `notify_all`？

**避免惊群（thundering herd）。**

```cpp
notEmpty_.notify_one(); // 唤醒一个线程即可，避免惊群
```

如果每次入队都用 `notify_all`，会唤醒**所有**等待的线程。它们同时被唤醒、同时去抢同一把 `taskQueMtx_`，最后**只有 1 个能拿到任务**，其余全部空手回去继续等。这就是「惊群」——N-1 次唤醒 + N-1 次加锁/解锁 + N-1 次条件变量重新等待，全是无用功。

---

### Q7.4 线程池怎么保证析构安全？

**用「join 所有线程」而不是「detach」，并且用 CAS 保证 `stop()` 幂等。**（`ThreadPool.cpp:67-93`）。

**关键点一：CAS 幂等**。`TcpServer::stop()` 会调用一次，`ThreadPool` 析构又会调一次（`ThreadPool::~ThreadPool() { stop(); }`）。如果 `stop()` 不幂等，第二次会对**已经 join 过的 `std::thread` 对象**再调 `join()`——那是 UB（`joinable()` 会是 false，但 `threads_` 已经被 `clear()` 了，其实是空循环）。

**关键点二：`notify_all` **。注意这里的 `notify_all` 和 Q7.3 说「避免惊群」**不矛盾**——因为这里的目的就是「让所有线程都醒来退出」，而不是「唤醒一个来干活」。语义不同，选择就不同。

**关键点三：`join` 而不是 `detach`**。`detach` 会让线程在后台继续跑，而 `ThreadPool` 对象（以及它的 `this` 指针）已经在析构了——线程访问 `this->taskQueMtx_` 就是 UAF。`join` 保证**所有线程真正退出后**，`stop()` 才返回，析构才继续。

### Q7.5 `submitTask` 返回 `future` 是怎么实现的？

**答**：**用 `std::packaged_task` 包装可调用对象，取出 `future` 存进队列。**（`ThreadPool.h:64-100`）。

**① `shared_ptr<packaged_task>` 而不是直接存 `packaged_task`**。因为 `std::packaged_task` 是 **move-only** 的（不可拷贝），而 `std::function` 要求可拷贝。所以用 `shared_ptr` 把它包起来，lambda 捕获 `shared_ptr`（可拷贝），调用时 `(*task)()`。

**② `std::bind` + 完美转发**。`std::bind(std::forward<Func>(func), std::forward<Args>(args)...)` 把参数**绑定成值**（丢弃引用），这样参数的生命周期就跟 `packaged_task` 绑定了，不会因为调用方的栈变量销毁而悬垂。这是必要的——因为任务是**异步执行**的。

**③ 返回 `std::future` 让调用方.get()拿结果**

---

## 九、内存池：三层架构

### Q9.1 为什么要自己写内存池？

直接原因是 **`malloc`/`free` 在高并发小对象场景下有内在开销**

内存池的思路是「**用场景换性能**」：如果我知道「绝大多数分配都是小对象、8 字节对齐」，就可以把分配简化成「从一条链表上摘一个节点」

**但是这个优化在我的项目里收益很有限**

#### Q9.1.1 那为什么不直接用 tcmalloc / jemalloc？

**整个 `wevix_muduo` 是一个「从零实现」的网络库，内存池是其中的一个模块。直接链接 tcmalloc 就完全跳过了「**怎么设计一个多层缓存的分配器**」这个学习过程——而这个过程里的东西（线程本地缓存怎么免锁、批量搬运怎么减少锁争用、Span 怎么管理、延迟归还怎么减少跨层操作）**才是这个模块真正的价值。

`wevix_muduo` **没有强制**所有分配都走内存池——只有 `Channel` 显式重载了 `operator new`

---

### Q9.2 三层架构各自的职责是什么？

```text
ThreadCache（每线程私有）  →  CentralCache（全局共享）  →  PageCache（全局共享，管页）
     免锁快路径                   按 size class 分锁              按页管理，mmap/munmap
```

| 层 | 粒度 | 并发 | 职责 |
|---|---|---|---|
| **ThreadCache** | 单个对象 | **无锁**（`thread_local`） | 分配/回收的**快路径**，缓存一批空闲块 |
| **CentralCache** | 一批对象（8 个） | 每个 size class 一把自旋锁 | **批量搬运**，把 PageCache 的大块切成小对象 |
| **PageCache** | 整页（4KB） | 一把 mutex | **向系统要内存**，按页分配、切分、合并、归还 |

**一次分配的生命周期**：

```text
allocate(32B)
  └─ ThreadCache: 空闲链表有 → 直接摘一个返回（~ns 级，无锁）  ← 绝大多数命中这里
      └─ 没有 → fetchFromCentralCache(索引)
           └─ CentralCache: 中央链表有 → 摘一批（BATCH_SIZE=8）给 ThreadCache
               └─ 没有 → fetchFromPageCache → PageCache 分配一个 Span（8 页 = 32KB）
                     └─ PageCache 空闲 Span 链有 → 摘一个
                         └─ 没有 → systemAlloc → mmap 向内核要
```

**一次释放的生命周期**：

```text
deallocate(ptr, 32B)
  └─ ThreadCache: 挂回本地空闲链表，计数 +1（无锁）
      └─ 计数 > 256 → returnToCentralCache
           └─ 保留 1/4、归还 3/4 给 CentralCache
               └─ CentralCache: 挂到中央链表 + 延迟归还检查
                   └─ 整个 Span 都满了 → 还给 PageCache
                       └─ PageCache: 和后一个空闲 Span 合并
                           └─ 缓存页数 > 32768（128MB）→ munmap 还给内核
```

---

### Q9.3 ThreadCache 为什么不用锁？

**答**：**因为它是 `thread_local` 的——每个线程一份，只有自己访问。**（`ThreadCache.h:15-37`）。

**`static thread_local` 的语义**：每个线程第一次调用 `getInstance()` 时**各自构造**一个 `ThreadCache` 实例，之后返回的都是自己那份。所以两个线程调 `getInstance()` 拿到的是**两个不同的对象**——没有共享，自然不需要锁。

**为什么是「每 8 字节一条」而不是「按 2 的幂分档」**：

线性分档的碎片小得多，代价是链表数组大——`32768 × 8 字节 = 256KB`（指针数组）+ `32768 × 8 = 256KB`（计数数组）= **每个线程 512KB**！

**这是一个真实的代价**：如果程序有 100 个线程，光 ThreadCache 的空数组就要 `100 × 512KB = 50MB`。而且**不管用不用，只要线程碰过一次 `allocate` 就会构造**。

#### Q9.3.1 512KB 的数组开销是不是太大了？

**答**：**是，这是我这版设计里一个真实的浪费点。** 而且它揭示了一个更本质的问题。

**分档改粗**。这是最直接的。经典的 tcmalloc 用的是「小对象按 8 字节线性，超过某个阈值后按 2 的幂或更粗粒度」——比如：

```
1~128 字节：每 8 字节一档      →  16 档
129~1024：每 64 字节一档       →  14 档
1025~8192：每 512 字节一档     →  14 档
8193~256K：每 4KB 一档         →  62 档
                              共 ~106 档，而不是 32768 档
```
代价是内部碎片变大（比如 129 字节的分配会拿到 192 字节的块，浪费 63 字节），但**数组从 512KB 降到不到 1KB**。

---

### Q9.4 CentralCache 的并发是怎么保证的？

**每个 size class 一把 `atomic_flag` 自旋锁，粒度是「每个大小档」。**（`CentralCache.h:44-45`）加锁方式（`CentralCache.cpp:52-55`）：

**① 锁的粒度是「size class」而不是「整个 CentralCache」**。所以一个线程在申请 32 字节的块、另一个线程在申请 64 字节的块，**完全不冲突**

**② 用 `atomic_flag` 而不是 `std::mutex`**。`std::mutex` 抢不到锁时会把线程**挂起**（陷进内核等唤醒）几微秒。而 `atomic_flag` 的 `test_and_set` 就是一条 CPU 指令，抢不到就再转一圈。**临界区里只是「摘几个指针」这种纳秒级**

**③ 抢不到锁时 `std::this_thread::yield()`，并非硬转**。`yield()` 让出CPU，让持锁的那个线程赶紧把锁放了。如果换成纯自旋（`while(test_and_set);`），在**线程数比 CPU 核数还多**的机器上会把 CPU 全占着——持锁线程反而抢不到 CPU 去放锁，越转越慢。

**④ `memory_order_acquire` / `release`**：

- `acquire` 保证「拿锁之后的所有读写，不会被重排到拿锁之前」；
- `release` 保证「放锁之前的所有读写，不会被重排到放锁之后」。

---

### Q9.5 为什么是 8 字节对齐？为什么上限定在 256KB？

**为什么最小是8字节？**

**链表复用**：内存池把空闲块的前 8 字节拿来存「下一个空闲块」的指针，**这要求每个块至少有 8 字节**——所以最小档位就是 8 字节。

**256KB 上限的理由**：

1. **大对象的复用率低**。小对象（32B、64B）在高频创建销毁，缓存的价值高；大对象（1MB 的缓冲区）**通常活得很久**，回收后也很少立刻被复用。
2. **避免内存膨胀**。如果 `MAX_BYTES` 是 1GB，`FREE_LIST_SIZE` 就是 `1GB/8 = 1.34 亿`档——ThreadCache 的数组会大到荒谬

**超过 256KB 怎么办**——**直接回退到 `malloc`/`free`**

---

### Q9.6 Span 是什么？`SpanTracker` 又是什么？

**`Span` 是一段连续的物理页**——内存管理的最小单位，一个 8 页的 Span 就是 32KB 连续内存。

`CentralCache` 向 `PageCache` 要一个 Span，然后**把它切成若干个固定大小的小块**，串成链表给 ThreadCache 用。

**为什么需要 `SpanTracker`**：

核心需求是「**知道一个 Span 是否已经完全空闲了**」——只有完全空闲的 Span 才能还给 PageCache（否则里面还有块在用，还回去就崩了）。

判断「完全空闲」需要知道：**这个 Span 被切成了几块**、**现在有几块是空闲的**。当两者相等时，整个 Span 就是空闲的。

---

### Q9.7 为什么要「延迟归还」？

**因为「检查一个 Span 是否完全空闲」太贵了，不能每次归还块都做。**

**朴素方案的问题**：每次 `returnRange` 归还一批块时，都判断一下「这个 Span 是不是全空了」。但要知道 `free_count` 现在是多少，需要**遍历中央空闲链表统计这个 Span 的块数**——这是 O(链表长度) 的操作，而链表可能很长。每次归还都做一遍，代价无法接受。

**触发条件有两个**（`CentralCache.cpp:227-235`）：

| 条件 | 场景 | 目的 |
|---|---|---|
| 累计 48 次归还 | **高负载**——归还很频繁 | 攒够一批做一次，**摊薄成本** |
| 距上次超 1 秒 | **低负载**——归还很稀疏 | 保证不会因为「凑不够 48 次」而**永远不归还** |

---

## 十一、与官方 muduo 的差异

### Q11.1 你这个库和官方 muduo 有什么不同？

| # | 维度 | 官方 muduo | wevix_muduo | 理由 |
|---|---|---|---|---|
| 1 | **触发模式** | LT（水平触发） | **ET（边缘触发）** | 减少事件重复通知 |
| 2 | **事件处理顺序** | 读 → 写 | **写 → 读** | ET 下的硬性要求（Q3.2.1） |
| 3 | **UAF 防护** | `shared_ptr` 保活（`tie_`） | **回调后立即 `return`** | 省掉热路径的原子操作（Q3.5.1） |
| 4 | **拆包** | 应用层自己处理 | **Connection 层可插拔 codec** | RPC 是唯一上层协议，下沉更省事 |
| 5 | **定时器** | TimerQueue（最小堆） | **timerfd 周期扫描** | 当前只有连接超时一个需求（Q2.5.2） |
| 6 | **TcpServer 生命周期** | 外部传 loop + 手动 `loop()` | **自管理，构造声明线程数** | 调用方不用碰 EventLoop |

muduo 是**通用**网络库，它追求「协议无关」：`Buffer` 直接暴露给用户，帧解析完全交给上层。这样做的好处是任何协议都能用，代价是**每个使用者都要自己写粘包/拆包**。

而 `wevix_muduo` 和 `mprpc` 是**一起设计的**，RPC 是它的**唯一上层协议**。所以我可以把「长度前缀帧」这个认知**下沉到 `Connection` 层**：

`handleRead` 里循环拆帧，`onMessage` 回调**保证收到完整消息**：

**而且这是可选的**——`setMessageCodec` 没调用时，行为完全退化回 muduo 式的「读到多少给多少」（`Connection.cpp:116-124`）。

**这就是自研网络库相比复用通用库的核心优势**——我可以让网络层知道上层协议长什么样，从而消除掉一整层「每个使用者都要写」的样板代码。

#### Q11.1.1 那你觉得 muduo 有哪些地方比你的好？

**① UAF 防护的「抗腐蚀性」**（Q3.5.1 详细说过）。muduo 用 `shared_ptr` 保活，是**机制上的保证**——不管未来怎么改代码，只要 `tie_` 设置对了，就不会 UAF。我的「回调后立即 `return`」是**纪律上的保证**——依赖每个新增分支都记得 `return`。**在多人长期维护的项目里，muduo 的方式明显更稳健。**

**② 定时器的通用性和效率**（Q2.5.2）。muduo 的 TimerQueue 支持任意时刻的 one-shot/repeat 定时器，且每次只看堆顶 O(1)。我的周期扫描是 O(N) 全表，且只支持「固定节拍」。**如果哪天需要「30 秒后重发」这种能力，我得重写。**

**③ 连接超时的精确定时**。muduo 用 TimerQueue 可以精确到「这个连接再过 3.2 秒该超时了」，而我只能保证「10~15 秒之间」——**精度损失 50%**。对连接的主动清理够用，但如果用来做「请求超时」，这个精度就不可接受了。

### Q11.2 你这个库有哪些地方是「为自己的场景定制」的？

**答**：**有四处明显的场景定制，我列出来。**

**① 帧编解码器下沉**（Q11.1 详述）。这是最核心的一处——因为 `mprpc` 是唯一上层协议。

**② `OnConnection` / `OnClose` 拆成两个独立回调**：

muduo 的做法是：

```cpp
// muduo：一个回调，连接建立和断开都触发
void onConnection(const TcpConnectionPtr& conn) {
    if (conn->connected()) { /* 新连接 */ }
    else { /* 断开 */ }
}
```

需要在回调里判断 `connected()`。我拆成了两个：

```cpp
void setConnectionCallback(ConnectionCallback cb);    // 只在建立时触发
void setCloseCallback(ConnectionCallback cb);         // 只在关闭时触发
```

**理由**：**消除样板代码**。我看了 `mprpc` 和 `video_platform` 的用法，两者从来不需要「在同一个回调里同时处理建立和断开」——总是分开关心的。既然实际用法总是分开的，就没必要强制合在一起判断。代价是**丢失了 muduo 的灵活性**（如果将来真需要「在同一个地方处理两种事件」就做不到了）。

**③ `TcpServer` 自管理 EventLoop 生命周期**：

muduo：

```cpp
EventLoop loop;
TcpServer server(&loop, addr, "name");
server.setThreadNum(4);
server.start();
loop.loop();          // ← 调用方要显式启动 loop
```

我的：

```cpp
wevix_muduo::TcpServer server(ip, port, ioThreads);   // ← 构造时就声明线程数
server.start();                                       // ← 内部启动全部 loop 并阻塞
```

**理由**：**调用方不需要持有 `EventLoop`**。我看了 `video_platform` 的 5 个服务，它们的 `main()` 长得一模一样：

**④ `OnMessage` 传 `std::string&` 而不是 `Buffer*`**：

因为网络层已经用 codec 提取完毕（或者用 `retrieveAllAsString` 全取），应用层**不需要操作 `Buffer` 内部细节**。

**理由**：`mprpc` 的 `OnMessage` 拿到的是「一帧完整的 protobuf 数据」，它要做的就是「解析 + 分发」。给它一个 `Buffer*` 反而让它要去做 `peek`/`retrieve` 的动作，**没有意义**
