# timerfd 设计：把定时器变成一个 fd

> **一句话**：timerfd 把「时间到了」这件事**翻译成「一个 fd 可读了」**。
> 于是定时器不需要任何额外机制——直接挂到 epoll 上，和其他 fd 一起排队等通知。

本文记录 timerfd 的**全部语义**（从 `timerfd_create` 到内核行为），以及它在 wevix_muduo `EventLoop` 里的完整用法。
所有实测数据都注明了复现方式。

---

## 目录

- [〇、速查表](#〇速查表)
- [一、问题起点：定时这件事难在哪](#一问题起点定时这件事难在哪)
- [二、API 全解](#二api-全解)
- [三、内核语义：五个必须记住的细节](#三内核语义五个必须记住的细节)
- [四、本项目实现：逐层拆解](#四本项目实现逐层拆解)
- [五、十个关键坑](#五十个关键坑)
- [六、实测数据](#六实测数据)
- [七、横向对比：五种定时方案](#七横向对比五种定时方案)
- [八、面试问答](#八面试问答)
- [九、自查表](#九自查表)

---

## 〇、速查表

| 项 | 内容 |
|---|---|
| 头文件 | `<sys/timerfd.h>` |
| 创建 | `int timerfd_create(int clockid, int flags)` |
| 设置 | `int timerfd_settime(int fd, int flags, const struct itimerspec *new, struct itimerspec *old)` |
| 查询 | `int timerfd_gettime(int fd, struct itimerspec *cur)` |
| 读取 | `read(fd, &exp, 8)` → 返回**到期次数**并清零 |
| 关闭 | `close(fd)` —— 它就是个普通 fd |
| 到期表现 | fd **变可读**（poll/epoll 报 `EPOLLIN`） |
| 内核版本 | Linux 2.6.25+ / glibc 2.8+ |

**四个关键事实**（本文其余部分都在展开这四条）：

1. 它是 **fd** → 能和 socket / eventfd / 管道一起被 epoll 统一管理；
2. 它是**计数器**不是布尔量 → 到期次数会**累积**，`read` 一次取走全部；
3. `it_interval = 0` 是**一次性**闹钟，`!= 0` 是**周期**闹钟；
4. **`timerfd_settime` 会顺手把累积计数清零** —— 这是本项目「不 read 也不出事」的根本原因。

---

## 一、问题起点：定时这件事难在哪

一个事件循环（Reactor）的核心是 `epoll_wait` 阻塞等 IO 事件。现在要加一件事：**每 5 秒扫一遍连接表，清理超时连接**。

难点在于：`epoll_wait` 只能等 **fd**，而「时间」不是 fd。三条经典出路：

| 出路 | 做法 | 问题 |
|---|---|---|
| **A. 信号** | `alarm()` / `setitimer()` → `SIGALRM` | 信号处理函数里能安全调用的函数极少（async-signal-safe），写复杂逻辑必踩坑；信号还会打断系统调用 |
| **B. 超时参数** | `epoll_wait` 的 timeout 传一个相对毫秒数 | 能用，但要**自己维护「下一个到期时刻」**：每次 wait 前算 `min(到期时刻 - now)`，新加一个定时任务可能还要打断当前等待重算 |
| **C. timerfd** | 内核定时器 + 一个 fd | **时间变成了 fd**，直接挂 epoll，不用管 timeout |

muduo 选的是 B 的精细版（`TimerQueue`：最小堆 + 每次取堆顶算 timeout）；**本项目选的是 C**（`timerfd` + 固定节拍）。

C 的代价要说清楚：**它给的是一个「节拍器」，不是「每个任务各自的到期时刻」**。所以本项目的超时检测是**轮询式**的——每 5 秒扫一遍全表，而不是「在连接到期的那一刻被精确唤醒」。

---

## 二、API 全解

### 2.1 `timerfd_create`

```c
#include <sys/timerfd.h>

int tfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
//                       ↑ 用哪个时钟            ↑ 创建期 flags
```

**clockid（选哪个时钟）**：

| clockid | 含义 | 什么时候用 |
|---|---|---|
| `CLOCK_MONOTONIC` | 单调递增，从开机起算，**不受改系统时间影响** | **定时器的正确选择**（本项目用这个） |
| `CLOCK_REALTIME` | 墙钟时间，会被 NTP 校准 / 手动 `date` 改 | 需要「某年某月某日几点」的绝对时刻时 |
| `CLOCK_BOOTTIME` | 同 MONOTONIC，但**包含系统休眠时间** | 移动设备（挂起后仍要计时） |
| `CLOCK_REALTIME_ALARM` / `CLOCK_BOOTTIME_ALARM` | 能唤醒挂起的系统 | 需要闹钟唤醒，要求 `CAP_WAKE_ALARM` 权限 |

**flags（创建期）** —— 只有两个：

| flag | 作用 |
|---|---|
| `TFD_NONBLOCK` | 没到期时 `read` 返回 `EAGAIN` 而不是阻塞线程（**事件循环里必加**，否则一次误读就卡死整个 loop） |
| `TFD_CLOEXEC` | `exec` 时自动关闭 fd（**fork+exec 场景防泄漏**，本项目两个 fd 都带） |

**返回值**：一个新的 fd。失败返回 -1 + errno。

### 2.2 `struct itimerspec`：一次性还是周期，全看这两个字段

```c
struct itimerspec {
    struct timespec it_interval;   // 周期：后续每隔多久响一次
    struct timespec it_value;      // 首次到期：从现在起多久（默认）或绝对时刻（ABSTIME）
};
```

**四个组合，覆盖所有用法**：

| `it_value` | `it_interval` | 行为 | 备注 |
|---|---|---|---|
| `0` | 任意 | **解除武装（disarm）** | 定时器停止，`it_interval` 被忽略 |
| `>0` | `0` | **一次性（one-shot）** | 响一次后**自动 disarm** ← **本项目用的就是这个** |
| `>0` | `>0` | **周期** | 首次在 `it_value`，之后每 `it_interval` 自动重装 |
| `0` | `>0` | disarm | 同上，`it_interval` 无效 |

> 关键认知：**一次性闹钟到期后内核不会再响**。想要「每 5 秒一次」，要么设 `it_interval`，要么**自己在 handler 里重装**——本项目走的是后者（第 4.4 节）。

### 2.3 `timerfd_settime`：设置 / 重装

```c
::timerfd_settime(tfd, 0, &new_spec, nullptr);
//                  ↑ flags        ↑ 可选，接收上一次的设置
```

**flags（设置期）** —— 和创建期完全是两套，别混：

| flag | 作用 |
|---|---|
| `0` | `it_value` 是**相对时间**（从现在起多久） |
| `TFD_TIMER_ABSTIME` | `it_value` 是**该时钟上的绝对时刻**（如「下一个整点」），此时 `it_value = 0` 是唯一表示 disarm 的值 |
| `TFD_TIMER_CANCEL_ON_SET` | 仅配合 `ABSTIME` 使用：当 `CLOCK_REALTIME` 被**向前跳跃**时，让 fd 变为可读并返回 `ECANCELED`，用来感知系统时间被改 |

**语义要点**：

- **重复设置 = 覆盖重装**，不是叠加。对已 armed 的定时器再 `settime`，等于「把到期时间重新推到 now + it_value」；
- **重装会清零累积计数**（第 3.2 节，本项目安全性的根基）；
- `old_value` 非 NULL 时返回上一次的设置（**总是相对时间**）。

### 2.4 `read`：读到的是「次数」，不是「有没有」

```c
uint64_t expirations = 0;
ssize_t n = ::read(tfd, &expirations, sizeof(expirations));
// 成功：n == 8，expirations = 自上次 read 以来的到期总次数
// 没到期 + 非阻塞：n == -1, errno == EAGAIN
```

- **必须正好 8 字节**，否则 `EINVAL`；
- 是**累积计数**：如果到期 3 次没人读，一次 `read` 拿到 `3`；
- `read` 会**清零**计数器，fd 随即变回不可读。

### 2.5 poll / epoll 语义

```c
// 内核里的逻辑，一句话：
if (ctx->ticks != 0) events |= EPOLLIN;
```

**计数器非 0 → fd 可读**；`read` 取走后 → 变回不可读。

这就是「时间变 IO 事件」的全部魔法：内核定时器到期 → `ticks++` → 唤醒 waitqueue → epoll 报可读 → 你的 handler 被调用。

### 2.6 `timerfd_gettime`（了解即可）

`timerfd_gettime` 返回**当前剩余时间**（`it_value`）+ 当前周期（`it_interval`）；两个字段都为 0 表示已 disarm。`it_value` **永远是相对时间**，即使当初用 `ABSTIME` 设置的。

---

## 三、内核语义：五个必须记住的细节

### 3.1 累积计数会「积压」，并饱和于 `ULLONG_MAX`

到期没人读 → `ticks++`（到达 `ULLONG_MAX` 后不再增加，不会回绕）。

**后果**：长期不读，一次 read 拿到一个巨大的数字。对「节拍器」语义这是灾难——你以为会被调用 100 次补做 100 轮扫描，实际只被调 1 次、手里攥着 `ticks=100`。

### 3.2 `settime` 会清零累积计数 ⭐

内核 `do_timerfd_settime()` 里：

```c
if (timerfd_setup(ctx, flags, new))
    ctx->ticks = 0;          // ← 重装顺手把计数器清了
```

**实测验证**（`/tmp/tfd_test/t3.c`）：让定时器过期（`ticks = 1`）后**不读**、直接 `settime` 重装，再 `read` → 返回 `EAGAIN`（`ticks` 已被清成 0）。

**这条是理解本项目实现的关键**：它让「不 read 也不会忙轮询」成为可能。

### 3.3 一次性闹钟到期后自动 disarm

`it_interval = 0` 时，到期一次后 `it_value` 归 0，定时器**不会自己再响**。这一点很容易和「周期性」搞混：

```c
timerfd_settime(tfd, 0, &spec, NULL);   // spec.it_value = 5s, it_interval = 0
// 5 秒后响一次 —— 然后就没有然后了。想再来一次必须自己重装。
```

### 3.4 「不读」的代价取决于 LT 还是 ET

| 模式 | 不读会发生什么 |
|---|---|
| **LT（水平触发）** | fd 一直可读 → `epoll_wait` 立即返回 → **忙轮询 100% CPU**（实测 5 秒被唤醒 **2040 万次**） |
| **ET（边缘触发）** | 只报一次边沿，表面正常，但 `ticks` 一直在累积（到 `ULLONG_MAX` 为止） |

**ET 会把「忘了读」这个 bug 藏起来**，非常危险。

### 3.5 时钟源决定「抗时间跳变」能力

- `CLOCK_MONOTONIC` + 相对时间：系统时间被改**完全不受影响**；
- `CLOCK_REALTIME` + 绝对时间：`date` 一改，定时器行为立刻异常（向后改 → 迟迟不响；向前改 → 立刻响甚至 sweep）；
- 需要感知这种跳变时，用 `TFD_TIMER_ABSTIME | TFD_TIMER_CANCEL_ON_SET`，`read` 会返回 `ECANCELED`。

---

## 四、本项目实现：逐层拆解

### 4.1 全景

```text
EventLoop 构造（EventLoop.cpp:46-69）
  ├─ epoll_                       ← 事件分发
  ├─ eventfd + wakeChannel        ← 跨线程唤醒
  └─ timerfd + timerChannel       ← 定时（本文主角）
         │
         │  内核每 timerInterval_ 秒把 timerFd_ 变可读
         ▼
   epoll_wait(-1) 返回 → timerChannel_->handleEvent() → EventLoop::handleTimer()
         ├─ ① 无条件重装闹钟（把下次到期推到 now + interval）
         └─ ② 加锁遍历 conns_，对 isTimeout() 的连接 forceClose() + timerCallback_(fd)
                                                              │
                                                              ▼
                                                TcpServer::removeConnection(fd)
```

### 4.2 创建：`createTimerFd`（`EventLoop.cpp:26-43`）

```cpp
int createTimerFd(int sec)   // 匿名 namespace 内的辅助函数
{
    int tfd = ::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
    if (tfd < 0) { LOG_ERROR("timerfd_create failed, errno=%d", errno); return tfd; }

    struct itimerspec timeout;
    ::memset(&timeout, 0, sizeof(timeout));   // ← 关键：it_interval = 0 → 一次性
    timeout.it_value.tv_sec = sec;
    if (::timerfd_settime(tfd, 0, &timeout, nullptr) < 0)
        LOG_ERROR("timerfd_settime failed, fd=%d, errno=%d", tfd, errno);
    return tfd;
}
```

**四个决策点**，每一个都有理由：

| 决策 | 为什么 |
|---|---|
| `CLOCK_MONOTONIC` | 抗系统时间跳变（第 3.5 节） |
| `TFD_NONBLOCK` | loop 线程不能被 read 阻塞 |
| `TFD_CLOEXEC` | 后续 fork+exec 子进程（FFmpeg 就在干这个）不会继承泄漏 |
| `memset` → `it_interval = 0` | **一次性闹钟**：内核最多 pending 1 次到期，从根上杜绝累积 |

### 4.3 注册：挂到 epoll（`EventLoop.cpp:52-68`）

```cpp
, timerFd_(createTimerFd(timeout))                    // 注意：用的是 timeout，不是 timerInterval_
, timerChannel_(new Channel(this, timerFd_))
...
timerChannel_->setReadCallback(std::bind(&EventLoop::handleTimer, this));
timerChannel_->enableReading();                       // 只 EPOLLIN，没有 useET() → LT
```

**这里有个容易忽略的事实**：`useET()` 在全项目只有一处调用（`Connection.cpp:292`，只作用于连接 fd）。
**timerfd 和 eventfd 的 channel 都是默认的 LT（水平触发）**——这一点直接决定了第 5.1 节的坑。

### 4.4 触发：`handleTimer`（`EventLoop.cpp:183-220`）

```cpp
void EventLoop::handleTimer()
{
    // ① 重新设置闹钟
    struct itimerspec timeout;
    ::memset(&timeout, 0, sizeof(timeout));
    timeout.it_value.tv_sec = timerInterval_;
    ::timerfd_settime(timerFd_, 0, &timeout, nullptr);   // 重装 → 顺手清零 ticks

    // ② 扫描超时连接（主循环不干这事）
    if (!isMainLoop_)
    {
        time_t now = ::time(nullptr);
        std::vector<ConnectionPtr> expired;
        {
            std::lock_guard<std::mutex> lock(connsMutex_);
            for (auto& entry : conns_)
                if (entry.second->isTimeout(now, timeout_))
                    expired.push_back(entry.second);
        }
        for (const auto& conn : expired)          // 锁外做回调
        {
            int fd = conn->fd();
            LOG_INFO("connection timeout, removing fd=%d", fd);
            conn->forceClose();
            if (timerCallback_) timerCallback_(fd);
        }
    }
}
```

**三个细节值得说**：

1. **从没 `read` 过 `timerFd_`**——安全性靠「一次性闹钟 + 每轮重装」，见第 5.2 节；
2. **先拷贝再处理**：加锁只做「筛选」，`forceClose` / `timerCallback_` 都在**锁外**执行，避免回调里再碰 `conns_` 造成死锁；
3. `forceClose()` 内部走 `runInLoop`，但当前已在 loop 线程 → 命中同线程快路径直接执行（不会绕一圈队列）。

### 4.5 扫描依赖的两张表

| 表 | 位置 | 谁维护 |
|---|---|---|
| `conns_`（`map<int, ConnectionPtr>`） | `EventLoop` | `newConnection()` / `removeConnection()`（`EventLoop.cpp:222-232`） |
| `connections_` | `TcpServer` | `handleNewConnection` / `removeConnection` |

链路是：`TcpServer::handleNewConnection` → `subLoop->newConnection(conn)` 登记到 loop（`TcpServer.cpp:106`）。

### 4.6 接线：超时后怎么摘掉连接（`TcpServer.cpp:30-35`）

```cpp
std::unique_ptr<EventLoop> loop(new EventLoop(false, 5, 10));   // 节拍 5s，超时阈值 10s
loop->setTimerCallback(std::bind(&TcpServer::removeConnection, this, std::placeholders::_1));
```

即：**loop 只负责发现超时，摘除动作交回 TcpServer**（职责分离）。

### 4.7 为什么 `epoll_wait` 敢传 -1（`EventLoop.cpp:91`）

```cpp
epoll_->poll(-1, &activeChannels);     // 永久阻塞
```

因为**时间管理完全交给 timerfd 了**——不需要靠 timeout「顺便醒来」。代价是：真的没有 fd 事件时线程会一直睡着，只能靠 `eventfd` 唤醒（`stop()` 跨线程调用时就是这么干的，`EventLoop.cpp:113-121`）。

### 4.8 主从循环的分工

```cpp
mainLoop_(new EventLoop(true))              // 默认参数：节拍 30s，阈值 100s
std::unique_ptr<EventLoop> loop(new EventLoop(false, 5, 10));   // 从循环：节拍 5s，阈值 10s
```

| | 节拍 | 超时阈值 | `isMainLoop_` | 扫描 |
|---|---|---|---|---|
| 主循环 | 30s（首次 100s） | 100s | `true` | **跳过**（`handleTimer` 里直接 return） |
| 从循环 ×N | 5s（首次 10s） | 10s | `false` | 全表扫描该 loop 上的连接 |

> 顺带一个观察：**主循环的 timerfd 是纯空转的**——它照样创建、照样每 30 秒醒来一次，但 handler 里因为 `isMainLoop_ = true` 什么都不做。开销可忽略（30 秒一次系统调用），但确实是无用功。

### 4.9 超时判定（`Connection.cpp:282-285`）

```cpp
bool Connection::isTimeout(time_t now, int seconds) const
{ return (now - lastActiveTime_.toSeconds()) > seconds; }
```

`lastActiveTime_` 的更新点**只有一处**：`handleRead` 里真的读到数据时（`Connection.cpp:88`），构造时初始化一次（`Connection.cpp:19`）。

**实际超时时间 = 10 ~ 15 秒**：判定阈值 10s，但检查是每 5s 一次，所以最坏要等到下一个节拍。

---

## 五、十个关键坑

### 5.1 不 read 的代价：LT 下是 100% CPU 忙轮询

**实测**（周期闹钟 + 不读 + 不重装）：

| 触发模式 | 5 秒内 epoll 唤醒次数 |
|---|---|
| **LT** | **20,409,293**（基本是死循环空转） |
| **ET** | 1（bug 被完美掩盖） |

**教训**：只要 fd 可能处于「可读但没人消费」的状态，LT 就是灾难，ET 是定时炸弹。

### 5.2 本项目为什么「不 read 也没事」—— 双保险

| 保险 | 机制 | 出处 |
|---|---|---|
| ① 一次性闹钟 | `it_interval = 0`，内核最多 pending **1** 次到期，物理上累积不起来 | `EventLoop.cpp:35` |
| ② 重装清零 | `timerfd_settime` 时内核 `ctx->ticks = 0`，把可读态清掉 | 内核 `do_timerfd_settime`（第 3.2 节） |

两条**同时**生效，才让「不 read」安全。实测：不 read + 每轮重装，5 秒内稳定唤醒 5 次，没有任何空转。

### 5.3 这个安全是「借来的」—— 两处改动都会炸

| 改动 | 后果 |
|---|---|
| 把 `it_interval` 设成 5s（想「省掉 handler 里的重装」） | 周期闹钟 + 不读 + LT → **100% CPU 空转** |
| 给 `timerChannel_` 加 `useET()` | 变成每次到期一个边沿，`ticks` 无限累积到 `ULLONG_MAX`（不炸但语义已错） |

**所以「每轮必须重装」是硬约束，不是可选的优化**。代码里最好把这句话写成注释钉死。

### 5.4 时钟源不一致：定时器用 MONOTONIC，超时判定用 REALTIME

- `timerfd` 用 `CLOCK_MONOTONIC`（`EventLoop.cpp:28`）；
- `isTimeout` 比较的是 `time(nullptr)`（`Timestamp.cpp:22`，**`CLOCK_REALTIME`**）。

**后果**：`date` 把系统时间**往回拨** → 连接永不过期；**往前跳** → 一批连接被误杀。要修的话，`Timestamp` 应该改用 `clock_gettime(CLOCK_MONOTONIC)`。

### 5.5 首次 arm 用的是 `timeout` 而不是 `timerInterval_`

```cpp
, timerFd_(createTimerFd(timeout))    // EventLoop.cpp:52
```

从循环 `EventLoop(false, 5, 10)` → **首次扫描在 10 秒后**，之后才是 5 秒一拍。首次延迟取「连接超时阈值」没有语义依据，更像笔误（应传 `timerInterval_`）。影响很小（只影响启动后第一次扫描），但会让「超时检测延迟」的口径不好解释。

### 5.6 精度只到秒

`Timestamp::now()` 就是 `time(nullptr) * 1e6`（`Timestamp.cpp:22`），**微秒字段恒为 0**。所以超时判定是秒级粒度，`> seconds` 是整秒比较。

### 5.7 只有「收到数据」才算活跃

`lastActiveTime_` 只在 `handleRead` 有数据时更新。因此：

- 一个**只收不发**的客户端（比如订阅型连接），只要不往服务端写数据，10 秒后就会被判定超时踢掉；
- 反过来，服务端**发送**数据不算活跃。

对 IM / 转码平台这种「请求-响应」场景够用，对长连接推送场景需要额外的心跳或「写也更新活跃时间」。

### 5.8 扫描是 O(N) 全表

每 5 秒遍历该 loop 上的**全部**连接。连接数上万时，这是一次 O(N) 尖峰（虽然只是几次内存访问 + 时间比较）。这正是 muduo 用最小堆（只看堆顶）的理由（见第七节）。

### 5.9 两套 flags 极易搞混

```c
timerfd_create(clockid, TFD_NONBLOCK | TFD_CLOEXEC);              // 创建期只有这两个
timerfd_settime(fd, TFD_TIMER_ABSTIME, &spec, NULL);              // 设置期只有这两个
```

`<sys/timerfd.h>` 把它们都定义在同一个头文件里，**写混了照样编译通过，但行为完全不是你想的**（比如把 `TFD_TIMER_ABSTIME` 传给 create 会被忽略/报 EINVAL）。

### 5.10 忘了处理 `EAGAIN`

非阻塞 timerfd 在「没有到期」时 `read` 返回 `EAGAIN`。如果日志里对此打 `LOG_WARN`/`LOG_ERROR`，会被偶发的空读刷屏。① 正常路径应该静默处理；② 反过来，**如果收到 `EPOLLIN` 却从不 `read`，就回到 5.1 的忙轮询**。

---

## 六、实测数据

**测试程序**（`/tmp/tfd_test/t.c`，约 30 行）：

```c
int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
struct itimerspec it; memset(&it, 0, sizeof(it));
it.it_value.tv_sec = do_periodic ? 1 : 1;
it.it_interval.tv_sec = do_periodic ? 1 : 0;      // 一次性 / 周期
timerfd_settime(tfd, 0, &it, NULL);

struct epoll_event ev; ev.events = EPOLLIN | (et ? EPOLLET : 0); ev.data.fd = tfd;
epoll_ctl(ep, EPOLL_CTL_ADD, tfd, &ev);

while (elapsed < 5000ms) { epoll_wait(ep, &out, 1, 5000); wake++;
    if (do_read) read(tfd, &v, 8);
    if (do_rearm) { memset(&it,0,sizeof(it)); it.it_value.tv_sec = 1; timerfd_settime(tfd,0,&it,NULL); } }
```

**结果汇总**：

| # | 场景 | 结果 | 说明 |
|---|---|---|---|
| 1 | **本项目模式**：一次性 + handler 里重装 + 不 read（ET / LT 各测一次） | 5 秒唤醒 **5 次** | ✅ 无积压、无空转 |
| 2 | 一次性 + 重装 + **read**（ET / LT） | 5 秒唤醒 **5 次**，每次读到 1 | 读与不读等价 |
| 3 | **周期闹钟** + 不读 + 不重装 + **LT** | 5 秒唤醒 **20,409,293 次** | ❌ 忙轮询 |
| 4 | 周期闹钟 + 不读 + 不重装 + **ET** | 5 秒唤醒 **1 次** | ⚠️ bug 被掩盖，`ticks` 仍在累积 |
| 5 | 周期闹钟，停 3 秒不读，再 read | 返回 **8** | 「积压」的直接证据 |
| 6 | 过期后**不读**、直接 `settime` 重装，再 read | **EAGAIN** | 证明 `settime` 清零了 `ticks` |

**结论**：项目当前实现正确，但正确性来自「一次性 + 每轮重装」这个**隐式契约**，而不是显式设计。

---

## 七、横向对比：五种定时方案

| 方案 | 精度 | 能和 epoll 统一？ | 多定时器 | 时间跳变 | 评价 |
|---|---|---|---|---|---|
| **A. `sleep` / `usleep`** | ms | ❌ 阻塞整个线程 | ❌ | 不受影响 | 事件循环里**绝对禁止** |
| **B. `alarm` / `SIGALRM`** | s | ❌ 信号 | ❌ | 受影响 | 老代码，信号安全坑多 |
| **C. `epoll_wait` timeout** | ms | ⚠️ 本身就是 timeout | 需自己算最近到期 | 不受影响（相对） | 只适合「单一定时」 |
| **D. `timerfd`（本项目）** | ns | ✅ 就是个 fd | 每个定时器一个 fd | **MONOTONIC 免疫** | 固定节拍场景最优 |
| **E. TimerQueue（muduo）** | ns | ✅ 也是 timerfd | ✅ **一个 fd 管 N 个**（最小堆） | 同上 | **通用定时**的正解 |

**D 和 E 的关系**：E 的底层其实就是 D，差别在**上层**——

- D：一个 timerfd 一个节拍，handler 里做全表轮询（O(N)），只支持「固定节拍」；
- E：一个 timerfd + 一个按到期时间排序的最小堆，每次只取堆顶算出「下一个到期时刻」重装 timerfd（O(log N)），支持任意 one-shot / repeat 定时器。

**选择依据**：需求只有「连接超时扫描」一种，且对精度要求是秒级 → **D 的简单性 > E 的通用性**。
**什么时候必须换 E**：出现「30 秒后重发」「任务延迟重试」这类**每个任务到期时刻不同**的需求时（届时 D 的固定节拍根本表达不了）。

---

## 八、面试问答

### Q1：timerfd 是什么？一句话说清

**答**：Linux 提供的一种定时器接口，它把「定时到期」这个事件**变成一个 fd 的可读事件**。核心价值是**统一**——定时器、socket、eventfd 在 epoll 眼里全是 fd，所以事件循环不需要为「时间」单独准备一套机制。

### Q2：为什么不用 `epoll_wait` 的 timeout？

**答**：能用，但很别扭。timeout 是「一个相对时间的超时」，你得自己维护「下一个超时时刻」，每次 wait 前算一遍 `min(到期时刻 - now)`；新加一个定时任务还可能打断当前等待、重算。muduo 的 `TimerQueue` 做的就是这件事（最小堆 + 取堆顶）。

用 timerfd 之后，时间变成了一个 fd，**`epoll_wait` 可以直接传 -1 永久阻塞**（`EventLoop.cpp:91`），代码简单得多。代价是它给的是**固定节拍的节拍器**，不是每个任务各自的到期时刻。

### Q3：timerfd 会不会积压？

**答**：timerfd 本身**会**——它是累积计数语义，到期没人读就 `ticks++`，下次 `read` 拿到的是错过的总次数（实测 1 秒周期停 3 秒不读，一次读到 8）。

但**本项目的实现不会**，而且我**从不 read 它**，靠两条：① 创建时 `it_interval` 被 memset 置 0，是**一次性闹钟**，内核最多 pending 1 次；② `handleTimer` 每轮无条件 `timerfd_settime` 重装——内核 `do_timerfd_settime` 里 `if (timerfd_setup(...)) ctx->ticks = 0`，**重装顺手把计数器清零了**（实测：过期后不读、直接重装，再 read 返回 `EAGAIN`）。

⚠️ **硬约束**：`timerChannel_` 没调 `useET()`，它是 **LT**，可读态不清就是 `epoll_wait` 忙轮询（实测周期闹钟不读不重装，5 秒被唤醒 2000 万次）。所以「每轮必须重装」不能省。

### Q4：为什么不干脆 `read` 掉？

**答**：可以，而且更稳（不依赖 `settime` 的副作用）。不 read 是因为**没必要**——一次性闹钟最多积 1 次，而重装本身就清了。但代价是这份正确性依赖一个**隐式契约**：一旦有人把 `it_interval` 改成非 0、或去掉 handler 里的重装，立刻退化成 100% CPU 空转。所以我更倾向于**显式 `read` + 注释钉死约束**，把「靠副作用」变成「靠设计」。

### Q5：为什么用一次性闹钟而不是周期闹钟？

**答**：为了让**节拍可控**。周期闹钟由内核自动重装，我在 handler 里**做不了任何节奏调整**；而「handler 里手动重装」等于把节拍的主动权拿回用户态——将来想改成自适应节拍（连接多时扫描频繁些、连接少时稀疏些），只需要改一个变量。

（当前代码还没用到这个灵活性，但保留了余地。）

### Q6：系统时间被改会怎样？

**答**：分两块，**目前不一致，这是个真问题**：

- **定时器本身安全**：用 `CLOCK_MONOTONIC`，改 `date` 完全不影响；
- **超时判定不安全**：`isTimeout` 比较的是 `time(nullptr)`（`CLOCK_REALTIME`，`Timestamp.cpp:22`）。时间往**回拨** → 连接永不过期；往**前跳** → 一批连接被误杀。

修法：`Timestamp` 改用 `clock_gettime(CLOCK_MONOTONIC)`。

### Q7：这个方案和 muduo 的 `TimerQueue` 比怎么样？

**答**：muduo 的 `TimerQueue` 是「最小堆 + 一个 timerfd」，每次取堆顶算出下一个到期时刻重装——O(log N)，且支持任意 one-shot / repeat 定时器。我的方案是「一个 timerfd + 固定节拍 + 全表扫描」——O(N)，只支持固定节拍。

**在「连接超时」这个场景我的方案够用**（秒级精度、需求单一、代码量小得多）。但**通用性和效率都更差**：出现「每个任务到期时刻不同」的需求时（如延迟重试），固定节拍根本表达不了，得重写。

### Q8：连接超时检测整体是怎么做的？

**答**：三层——

1. **计时**：每个从循环一个 timerfd（节拍 5s、阈值 10s），到期 → `handleTimer`；
2. **判定**：遍历 `conns_`（fd → `ConnectionPtr`），`isTimeout(now, 10)` 即 `now - lastActiveTime_ > 10`，其中 `lastActiveTime_` 只在 `handleRead` 真正读到数据时更新；
3. **清理**：锁外 `forceClose()` + `timerCallback_(fd)` → `TcpServer::removeConnection(fd)`。

**实际超时是 10~15 秒**（阈值 10s + 节拍 5s 的量化误差）。

---

## 九、自查表

能不能不看文档答出下面每一条？答不出就回去翻对应章节。

- [ ] `timerfd_create` 的两个 flag 是什么？（2.1）
- [ ] `TFD_TIMER_ABSTIME` 是 create 还是 settime 的参数？（5.9）
- [ ] `it_value` 和 `it_interval` 各是什么？四种组合分别什么行为？（2.2）
- [ ] 怎么把已 armed 的定时器**关掉**？（2.2，`it_value = 0`）
- [ ] `read` 必须读几个字节？返回值是什么语义？（2.4）
- [ ] 为什么 `read` 返回 `EAGAIN` 是正常的？（2.4）
- [ ] 「积压」是什么意思？怎么产生的？（3.1 / 6-实验 5）
- [ ] 为什么本项目**不 read 也不会**忙轮询？（3.2 / 5.2）
- [ ] 为什么说这个安全性是「借来的」？（5.3）
- [ ] timerfd 的 channel 是 LT 还是 ET？依据是什么？（4.3）
- [ ] 为什么 `epoll_wait` 可以传 -1？（4.7）
- [ ] 连接超时实际是多少秒？为什么不是精确的 10 秒？（4.9）
- [ ] 系统时间被回拨，超时检测会怎样？（5.4 / Q6）
- [ ] 为什么用固定节拍而不是 muduo 的最小堆？（Q5 / 第七节）
- [ ] 什么时候**必须**换成 `TimerQueue`？（第七节：出现「每个任务到期时刻不同」的需求）
