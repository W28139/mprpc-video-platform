# ZooKeeper 客户端缓存系统

> 代码位置：`mprpc/src/mprpcchannel.cc`（匿名 namespace）
>
> ZK 封装层：`mprpc/include/ZookeeperUtil.h` + `mprpc/src/ZookeeperUtil.cc`

---

## 一、背景：为什么要这套体系

### 修复前的问题

`MprpcChannel::CallMethod()` 每次 RPC 调用都在栈上创建一个全新的 ZK 会话：

```text
CallMethod() {
    ZkClient zkCli;            // 栈对象
    zkCli.Start();             // → zookeeper_init（异步，创建 IO + Watcher 内部线程）
    zkCli.GetData(path);       // 同步阻塞查 ZK
}   // 析构 → zookeeper_close（关闭 TCP 连接，销毁内部线程）
```

每个 RPC 请求都经历 **ZK TCP 握手 → ZK Session 建立 → 查数据 → 拆连接** 的完整生命周期。服务发现被放在了每次 RPC 调用的热路径上。

### 修复后的目标

1. **进程只建一次 ZK 连接** — 不是每个 RPC，也不是每个线程，而是整个进程
2. **查 ZK 只在必要时发生** — 缓存命中时完全不碰 ZK
3. **TCP 连接也复用** — 查到 ip:port 后不是每次建 socket，而是从连接池取
4. **故障自动恢复** — 某个 Provider 实例挂了，自动踢缓存、换实例、重试

---

## 二、架构总览：三层结构

```
CallMethod()
  │
  │  ┌──────────────────────────────────────────────────┐
  │  │  第 1 层：ServiceCache（本地哈希缓存）             │
  │  │                                                   │
  │  │  key:  "/mprpc/services/UserServiceRpc/Login"    │
  │  │  val:  EndpointCacheEntry {                       │
  │  │          endpoints: ["10.0.0.1:8000", "10.0.0.2:8000"]  │
  │  │          nextIndex: 7                             │
  │  │        }                                          │
  │  │                                                   │
  │  │  命中 → 直接返回 endpoint，不碰 ZK，不加锁        │
  │  └──────────────────┬───────────────────────────────┘
  │                     │ cache miss
  │                     ▼
  │  ┌──────────────────────────────────────────────────┐
  │  │  第 2 层：SharedZkClient（进程级单例 ZK 连接）     │
  │  │                                                   │
  │  │  static ZkClient client;  ← 全进程唯一实例        │
  │  │  std::call_once → Start() 只执行一次               │
  │  │                                                   │
  │  │  GetChildren()  →  查 /mprpc/services/.../        │
  │  │  GetData()       →  逐个取子节点的 ip:port          │
  │  └──────────────────┬───────────────────────────────┘
  │                     │ 拿到 ip:port 列表，写入 ServiceCache
  │                     ▼
  │  ┌──────────────────────────────────────────────────┐
  │  │  第 3 层：ConnectionPool（TCP 连接池）             │
  │  │                                                   │
  │  │  key:  "10.0.0.1:8000"                           │
  │  │  val:  [PooledConnection, PooledConnection, ...]  │
  │  │        每个 endpoint 最多 8 个长连接                │
  │  │                                                   │
  │  │  轮询取一个 → EnsureConnected → Send/Recv         │
  │  └──────────────────────────────────────────────────┘
  │
  ▼
  发送 RPC 请求 → 读取响应
```

**访问频率**：第 1 层（每次 RPC） > 第 2 层（首次 + 缓存失效） > 第 3 层（首次 + 连接断开）

---

## 三、第 1 层：ServiceCache — 本地哈希缓存

### 3.1 数据结构

```cpp
struct EndpointCacheEntry
{
    std::vector<std::string> endpoints;  // 该 method 所有实例的 "ip:port" 列表
    size_t nextIndex = 0;                // 当前线程的轮询下标，每次取 endpoint 后自增
};

// key: method_path（如 "/mprpc/services/FriendServiceRpc/GetFriendsList"）
// val: EndpointCacheEntry
static std::unordered_map<std::string, EndpointCacheEntry> cache;
static std::mutex cacheMutex;
```

**为什么存列表而不是单个地址？** 为了多实例部署。同一个 RPC 方法可以注册多个 Provider 实例，客户端在实例间轮询，实现最基本的客户端负载均衡。

`nextIndex` 不是全局原子变量，而是缓存条目内的字段，受 `ServiceCacheMutex` 保护。这样每次 `GetHostData()` 返回一个 endpoint 的同时推进下标，多个调用者各自拿到不同（或轮转）的实例地址。

### 3.2 读取流程：GetHostData()

```
GetHostData(methodPath, legacyPath)
  │
  ├─ Lock(cacheMutex)
  │   ├─ 查到 EndpointCacheEntry && endpoints 非空
  │   │   → 轮询：endpoints[nextIndex % size]，nextIndex++
  │   │   → fromCache = true
  │   │   → 返回 "10.0.0.1:8000"
  │   │
  │   └─ 未查到
  │       → 释放锁
  │
  └─ 缓存未命中路径（不加锁）
      ├─ QueryEndpointList(methodPath, legacyPath)
      │   → 查 ZK，拿到 endpoints 列表
      │   → Lock(cacheMutex)，写入或清空缓存
      │   → 返回 endpoints
      │
      └─ PickEndpoint(endpoints)
          → 全局原子轮询，挑选一个 endpoint 返回
```

关键设计点：

- **缓存命中时持锁时间极短**：只做一次 map find + 数组索引 + 自增，然后立刻释放锁
- **缓存未命中时不在锁内查 ZK**：ZK I/O 可能耗时数百毫秒，绝不能持有锁期间做网络 I/O
- **未命中路径用原子轮询**：`PickEndpoint()` 用 `std::atomic<size_t>` 全局轮询，无锁

### 3.3 写入流程：QueryEndpointList()

```
QueryEndpointList(methodPath, legacyPath)
  │
  ├─ EnsureSharedZkClientStarted()
  │   → call_once 保证 ZK 客户端只启动一次
  │   → 失败则返回空列表
  │
  ├─ zk.GetChildren(methodPath)
  │   → 查 /mprpc/services/UserServiceRpc/Login/
  │   → 返回子节点名：["0000000001", "0000000002"]
  │   → 排序保证确定性
  │
  ├─ 逐个 zk.GetData(methodPath + "/" + childName)
  │   → 取每个子节点的值："10.0.0.1:8000", "10.0.0.2:8000"
  │   → 汇总到 endpoints 列表
  │
  ├─ 如果 endpoints 为空（新路径没数据）
  │   → 兜底：zk.GetData(legacyPath)
  │       → 查 /UserServiceRpc/Login → "10.0.0.1:8000"
  │       → 旧路径只支持单实例，但保证不过渡期不可用
  │
  └─ Lock(cacheMutex)
      ├─ endpoints 非空 → 写入 ServiceCache[methodPath] = {endpoints, nextIndex=0}
      └─ endpoints 为空 → ServiceCache.erase(methodPath)
```

### 3.4 ZK 路径兼容策略

Provider 端注册时使用新格式。客户端查询时兼容两种：

```
新路径（多实例）:
  /mprpc/services/FriendServiceRpc/GetFriendsList/
    ├── 0000000001  →  "10.0.0.1:8000"
    └── 0000000002  →  "10.0.0.2:8000"

旧路径（单实例兜底）:
  /FriendServiceRpc/GetFriendsList  →  "10.0.0.1:8000"
```

新路径的优势：每个实例是一个独立子节点（Ephemeral Sequential），天然支持多实例。某个实例下线 → 子节点自动删除 → 客户端下次查 ZK 时自然只看到活着的实例。

### 3.5 失效机制：InvalidateHostData()

```cpp
void InvalidateHostData(const std::string& methodPath)
{
    std::lock_guard<std::mutex> lock(ServiceCacheMutex());
    ServiceCache().erase(methodPath);
}
```

触发时机（在 `CallMethod()` 的重试逻辑中）：

```
SendRequestAndReadResponse() 失败
  && errorCode ∈ {CONNECT_FAILED, TIMEOUT, SEND_FAILED, RECV_FAILED}
  │
  ├─ ① DropEndpointConnections(endpointKey)
  │     → Lock(connectionPoolMutex)
  │     → 清空该 endpoint 的所有 PooledConnection
  │     → 避免下次取到已断开的连接
  │
  ├─ ② InvalidateHostData(methodPath)
  │     → Lock(cacheMutex)
  │     → 删除该 method 的缓存
  │     → 避免下次拿到已下线的实例地址
  │
  ├─ ③ QueryEndpointList(methodPath, legacyPath)
  │     → 从 ZK 重新拉最新实例列表
  │
  └─ ④ 用新 endpoint 重试一次 RPC
```

**重试只做一次**（不是无限循环），避免在 ZK 故障或全集群宕机时进入死循环。

---

## 四、第 2 层：SharedZkClient — 进程级单例

### 4.1 一分为二的设计

```cpp
// 函数 1：返回进程唯一的 ZkClient 实例（Meyers Singleton）
ZkClient& SharedZkClient()
{
    static ZkClient client;   // C++11 保证线程安全的静态局部变量初始化
    return client;
}

// 函数 2：保证 Start() 只执行一次（call_once 是幂等保证）
bool EnsureSharedZkClientStarted()
{
    static bool started = false;
    static std::once_flag once;
    std::call_once(once, []() {
        started = SharedZkClient().Start();  // zookeeper_init + 同步等待连接
    });
    return started;
}
```

**为什么拆成两个函数而不是合并？**

| 如果合并（不推荐） | 拆开（当前设计） |
|-------------------|-----------------|
| 每次拿实例都要检查 `call_once` | 确定已启动的代码路径（如 `QueryEndpointList` 先调 `Ensure`）跳过无谓检查 |
| 无法在不启动 ZK 的情况下获取实例（如 future 的 direct 模式想直接拿 client 做其他事） | 拿实例不触发启动，调用者自行决定是否先调 `Ensure` |
| 职责混合 | 生命周期管理（`Ensure`）与实例访问（`SharedZkClient`）分离 |

### 4.2 启动流程

`SharedZkClient().Start()` 内部做的事（`ZkClient::Start()`）：

1. `zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR)` — 抑制 ZK C SDK 的内部日志
2. 从配置文件读 `zookeeperip` 和 `zookeeperport`
3. `zookeeper_init(connstr, watcher, timeout, ..., &sem, 0)` — 异步启动
4. `sem_timedwait(&sem, 5s)` — 同步等待连接成功（watcher 回调 `sem_post` 唤醒）
5. 连接成功 → `zoo_set_context(nullptr)` + `sem_destroy`

`call_once` 保证：无论多少个线程同时首次触发 RPC，`Start()` 只执行一次。调用 `Start()` 期间其他线程在 `call_once` 上阻塞等待。

---

## 五、第 3 层：ConnectionPool — TCP 连接池

### 5.1 数据结构

```cpp
// endpoint_key  →  该 endpoint 的连接对象列表
static std::unordered_map<std::string, std::vector<std::shared_ptr<PooledConnection>>> pool;

// endpoint_key  →  该 endpoint 的连接池轮询下标
static std::unordered_map<std::string, size_t> nextIndex;

static std::mutex poolMutex;
```

`PooledConnection` 结构：

```
PooledConnection {
    key:   "10.0.0.1:8000"
    ip:    "10.0.0.1"
    port:  8000
    fd:    -1 或有效 socket fd
    mutex: 单连接互斥锁
}
```

### 5.2 取连接：GetPooledConnection()

```
GetPooledConnection("10.0.0.1", 8000)
  │
  ├─ Lock(poolMutex)
  │
  ├─ pool["10.0.0.1:8000"].size() < maxConnections（默认 8）
  │   → 新建 PooledConnection，加入池
  │   → 返回这个新连接（fd=-1，首次 EnsureConnected 时才建 TCP）
  │
  └─ pool["10.0.0.1:8000"].size() >= maxConnections
      → 轮询取已有连接：pool[key][nextIndex[key] % size]，nextIndex++
      → 返回已有连接（fd 可能仍然有效，也可能已断开）
```

### 5.3 连接生命周期

```
新建 PooledConnection
  fd = -1
  │
  ├─ EnsureConnected(timeoutMs) 首次调用
  │   → ConnectToEndpoint(ip, port, timeoutMs)
  │       ├─ socket() + fcntl(O_NONBLOCK)
  │       ├─ connect() → EINPROGRESS
  │       ├─ poll(POLLOUT, timeoutMs)
  │       ├─ getsockopt(SO_ERROR) 确认真实错误
  │       └─ fcntl(恢复原 flags) + setsockopt(SO_SNDTIMEO/SO_RCVTIMEO)
  │   → fd = 有效 socket
  │
  ├─ SendRequestAndReadResponse()
  │   ├─ Lock(conn->mutex)      ← 单连接互斥，避免两个线程同时在一个 fd 上交错读写
  │   ├─ SetSocketTimeout()      ← 每次调用刷新超时
  │   ├─ SendAll() 循环发送
  │   └─ RecvAll() 循环接收
  │
  └─ 如果 send/recv 失败
      → conn->Close()            ← 关闭 fd，置 fd=-1
      → 下次 EnsureConnected() 会重新 ConnectToEndpoint()  ← 自动重连
```

### 5.4 为什么"取连接"和"发请求"是分开的

`GetPooledConnection()` 在 `CallMethod()` 主路径上被调用，只做池操作（O(1) 查找 + 轮询）。真正的 TCP 连接建立在 `SendRequestAndReadResponse()` 内部通过 `EnsureConnected()` 延迟完成。

这样设计的好处：

- 连接失败时，`CallMethod()` 的重试逻辑中先 `DropEndpointConnections()` 清掉坏连接，再重新 `GetPooledConnection()` → 拿到新连接（或触发新建） → `EnsureConnected()` 才建 TCP。不需要额外处理"一个连接正在被使用但已断开"的中间态
- 连接建立成本只在实际发送数据时发生，不在服务发现热路径上

### 5.5 连接上限配置

```cpp
int MaxConnectionsPerEndpoint()
{
    // 配置文件可调：mprpcclient_connections_per_endpoint
    // 默认 8，范围 [1, 128]
    return MprpcApplication::GetConfig().LoadInt(
        "mprpcclient_connections_per_endpoint", 8, 1, 128);
}
```

---

## 六、Provider 端：ZK 数据从哪来

前面三章都在讲客户端怎么读缓存、查 ZK、复用连接。这一章讲另一侧：**Provider 启动时怎么把地址写到 ZK 上**。

### 6.1 注册路径设计

Provider 在 `RpcProvider::Run()` 中向 ZK 注册，路径分四层：

```
/mprpc                          ← 永久节点，项目根
  /services                     ← 永久节点，服务注册根
    /UserServiceRpc             ← 永久节点，服务名
      /Login                     ← 永久节点，方法名
        /instance-0000000001     ← 临时顺序节点 → "10.0.0.1:8000"
        /instance-0000000002     ← 临时顺序节点 → "10.0.0.2:8000"
```

| 层级 | 节点类型 | 生命周期 | 作用 |
|------|---------|---------|------|
| `/mprpc` | 永久 | 手动创建后一直存在 | 项目命名空间 |
| `/mprpc/services` | 永久 | 同上 | 服务注册根 |
| `.../{service}` | 永久 | 同上 | 一个 Service 下的所有 Method |
| `.../{method}` | 永久 | 同上 | 一个 Method 下的所有实例 |
| `.../instance-*` | **临时 + 顺序** | **随 Provider 进程存活** | 一个实例的 ip:port |

关键设计：

- **永久节点做容器**：`/mprpc` → `/services` → `{service}` → `{method}` 这四级都是永久节点，作为"目录"容纳实例
- **临时节点做实例**：`instance-*` 是 `ZOO_EPHEMERAL`，Provider 进程退出或 ZK session 超时（默认 30s）后自动删除。客户端下次 `GetChildren` 时自然看不到已下线的实例
- **顺序后缀做去重**：`ZOO_SEQUENCE` 保证同一 method 下多个 Provider 实例不会冲突，各自生成 `instance-0000000001`、`instance-0000000002` 等唯一名

### 6.2 注册流程

Provider 启动时遍历本地注册的所有 Service 和 Method，逐级创建节点：

```
RpcProvider::Run()
  │
  ├─ 1. zkCli.Start()                             ← 连接 ZK
  │
  ├─ 2. zkCli.Create("/mprpc", nullptr, 0)         ← 创建项目根（永久）
  ├─ 3. zkCli.Create("/mprpc/services", nullptr, 0) ← 创建注册根（永久）
  │
  ├─ 4. 遍历 m_serviceMap
  │     │
  │     ├─ 对每个 service:
  │     │   zkCli.Create("/mprpc/services/UserServiceRpc", ...)  ← 服务节点（永久）
  │     │   │
  │     │   └─ 对每个 method:
  │     │       zkCli.Create("/mprpc/services/UserServiceRpc/Login", ...)  ← 方法节点（永久）
  │     │       zkCli.Create("/mprpc/services/UserServiceRpc/Login/instance-",
  │     │                     "10.0.0.1:8000", ...,
  │     │                     ZOO_EPHEMERAL | ZOO_SEQUENCE)   ← ★ 实例节点（临时顺序）
  │     │
  │     └─ ... 重复
  │
  └─ 5. 进入 TcpServer 事件循环
```

### 6.3 新旧路径对比

| | 旧方案 | 新方案（当前） |
|------|--------|---------------|
| 路径 | `/{Service}/{Method}` → `ip:port` | `/mprpc/services/{Service}/{Method}/instance-*` → `ip:port` |
| 多实例 | 不支持（一个方法只能有一个节点） | 天然支持（一个方法下有多个 instance-* 子节点） |
| 节点类型 | 永久 | 实例节点为临时 |
| 实例下线 | 节点残留，客户端读到死地址 | ZK 自动删除，客户端 `GetChildren` 自然看不到 |
| 客户端查询 | `GetData(path)` 读单个值 | `GetChildren(path)` 拉全量 + 逐个 `GetData` |

客户端在 `QueryEndpointList()` 中优先查新路径，如果新路径无数据则回退到旧路径（`LegacyMethodPath`），保证过渡期兼容。

### 6.4 与客户端缓存的对应

```
Provider 端                                客户端
══════════                                ════════

RpcProvider::Run()                        CallMethod()
  │                                         │
  ├─ Create(instance-0001, "10.0.0.1:8000",    ├─ GetHostData()
  │          EPHEMERAL|SEQUENCE)               │   └─ cache miss
  │                                            │       ↓
  ├─ Create(instance-0002, "10.0.0.2:8000",    └─ QueryEndpointList()
  │          EPHEMERAL|SEQUENCE)                   ├─ GetChildren(methodPath)
  │                                                │   → ["instance-0001", "instance-0002"]
  │   Provider 宕机 → ZK session 超时              ├─ GetData(.../instance-0001) → "10.0.0.1:8000"
  │   → instance-0001 自动删除                     ├─ GetData(.../instance-0002) → "10.0.0.2:8000"
  │                                                └─ 写入 ServiceCache →
  │   Provider 恢复 → 重新 Create                     {["10.0.0.1:8000", "10.0.0.2:8000"], 0}
  │   → instance-0003 "10.0.0.1:8000"
```

**数据流向**：Provider 写 ZK → 客户端查 ZK → 写入 ServiceCache → 轮询取 endpoint → ConnectionPool 取连接。

---

## 七、完整调用链路

以一次 `GetFriendsList` RPC 调用为例，从 `CallMethod()` 到拿到响应的全路径：

```
CallMethod("FriendServiceRpc", "GetFriendsList")
  │
  │  ┌── 阶段 0：构建 RPC 请求帧 ──┐
  │  │ 序列化 RpcHeader + request body → BuildRpcFrame()
  │  └────────────────────────────┘
  │
  │  ┌── 阶段 1：服务发现 ──┐
  │  │
  │  │ methodPath  = "/mprpc/services/FriendServiceRpc/GetFriendsList"
  │  │ legacyPath  = "/FriendServiceRpc/GetFriendsList"
  │  │
  │  │ GetHostData(methodPath, legacyPath, fromCache)
  │  │   │
  │  │   ├─ [首次调用] ServiceCache 为空
  │  │   │   → QueryEndpointList()
  │  │   │       → EnsureSharedZkClientStarted()  ← call_once，ZK 客户端启动
  │  │   │       → SharedZkClient().GetChildren(methodPath)
  │  │   │           → ZK 返回子节点: ["0000000001"]
  │  │   │       → SharedZkClient().GetData(methodPath + "/0000000001")
  │  │   │           → ZK 返回: "127.0.0.1:8000"
  │  │   │       → 写入 ServiceCache[".../GetFriendsList"] = {["127.0.0.1:8000"], 0}
  │  │   │   → PickEndpoint → "127.0.0.1:8000"
  │  │   │   → fromCache = false
  │  │   │
  │  │   └─ [后续调用] ServiceCache 命中
  │  │       → 轮询返回 "127.0.0.1:8000"
  │  │       → fromCache = true
  │  │       → 不碰 ZK，不加锁（临界区极短）
  │  │
  │  └──────────────────────────┘
  │
  │  ┌── 阶段 2：解析地址 ──┐
  │  │ ParseHostData("127.0.0.1:8000")
  │  │   → ip = "127.0.0.1", port = 8000
  │  │   → 校验 port ∈ [1, 65535]
  │  └──────────────────────┘
  │
  │  ┌── 阶段 3：获取连接 ──┐
  │  │ pooledConn = GetPooledConnection("127.0.0.1", 8000)
  │  │   → Lock(poolMutex)
  │  │   → pool["127.0.0.1:8000"] 未满 → 新建 PooledConnection(fd=-1)
  │  │   → 返回 shared_ptr<PooledConnection>
  │  └──────────────────────┘
  │
  │  ┌── 阶段 4：发送请求 & 接收响应 ──┐
  │  │ SendRequestAndReadResponse(pooledConn, frame, timeoutMs)
  │  │   │
  │  │   ├─ Lock(conn->mutex)           ← 单连接互斥
  │  │   ├─ conn->EnsureConnected()     ← 首次：非阻塞 connect + poll
  │  │   ├─ SetSocketTimeout(timeoutMs)
  │  │   ├─ SendAll()                   ← 循环 send 直到全写完
  │  │   ├─ RecvAll(4B)                 ← 读响应帧长度
  │  │   ├─ RecvAll(responseFrameSize)  ← 读完整响应帧
  │  │   └─ DecodeRpcFramePayload()     ← 解帧包头 + body
  │  │
  │  └──────────────────────────────────┘
  │
  │  ┌── 阶段 5：失败重试（如触发）──┐
  │  │ 如果阶段 4 失败且 errorCode ∈ {CONNECT_FAILED, TIMEOUT, SEND/RECV_FAILED}:
  │  │   ├─ DropEndpointConnections("127.0.0.1:8000")  ← 清连接池
  │  │   ├─ InvalidateHostData(methodPath)               ← 清 ServiceCache
  │  │   ├─ QueryEndpointList()                          ← 重新查 ZK
  │  │   ├─ 用新 endpoint 重建连接
  │  │   └─ SendRequestAndReadResponse() 重试一次
  │  └──────────────────────────────────┘
  │
  │  ┌── 阶段 6：解析响应 ──┐
  │  │ DecodeRpcResponsePayload()
  │  │   → 校验 request_id 一致（防串包）
  │  │   → 校验 error_code（远端是否返回业务错误）
  │  │   → response->ParseFromString(body)
  │  └──────────────────────┘
  │
  ▼
  返回给调用方
```

---

## 八、剩余的缓存时效性问题

### 8.1 当前缺陷

三层缓存全部依赖于一个前提：**ServiceCache 里的数据是正确的**。但当前没有任何机制在 ZK 数据变化时主动通知客户端。

失效链：

```
Provider 实例下线 → ZK Ephemeral 节点删除
  → 客户端 ServiceCache 完全无感知
  → 继续用旧 ip:port 发请求
  → TCP connect/recv 失败
  → InvalidateHostData() + 重试     ← 只有到这一步才纠正
```

影响：

| ZK 变更 | 当前行为 | 影响 |
|---------|---------|------|
| 新实例上线 | 不感知 | 新实例闲置，负载不均 |
| 实例下线 | 每次请求 TCP 超时后才重试 | P99 延迟被拖垮 |
| 地址变更 | 同上 | 同上 |

### 8.2 修复方向

核心是两步：**ZK Watch**（主动通知）+ **TTL**（被动兜底）。

**ZK Watch 方案：**

```
QueryEndpointList() 查 ZK 时对 method_path 注册一次性 Children Watch

    ↓ 子节点变化

ZK watcher 回调（在 ZK 内部线程中触发）
    → 通过 watcherCtx（void* 指针）桥接到 C++ lambda
    → lambda 中 Lock(cacheMutex) + ServiceCache().erase(methodPath)

    ↓ 下次 RPC 调用

GetHostData() → cache miss → QueryEndpointList() → 重新查 ZK + 重新注册 watch
                                          ↑
                              自然完成了 watch 重注册
```

关键细节：
- ZK watch 是一次性的，触发后自动移除。但 `QueryEndpointList()` 每次执行都会重新调 `zoo_get_children(path, 1, ...)`（设 `watch=1`），所以下次 cache miss 时自然重新注册。**不需要单独的 watch 重注册逻辑**
- watch 回调在 ZK IO 线程中执行，只能用 `watcherCtx`（`void*` 透传）桥接到 C++ 的 `ServiceCache`。做法：传一个 `std::function<void()>*` 指针作为 context，回调中 `static_cast` 回来调用

**TTL 兜底：**

给 `EndpointCacheEntry` 加一个 `cachedAt` 时间戳。`GetHostData()` 中如果发现缓存超过 30 秒，视为过期，走 `QueryEndpointList()` 刷新。

TTL 的作用不是替代 watch，而是兜底：
- ZK 网络抖动导致 watch 丢失
- ZK session 断开重连后所有 watch 清空
- watch 回调因某些原因未能执行

**复杂度评估**：总计约 80 行改动（ZookeeperUtil.h/cc + mprpcchannel.cc），核心难点是 C 回调到 C++ lambda 的 `void*` 桥接。

---

## 九、与 CallMethod 整体流程的关系

```
CallMethod(method, controller, request, response, done)
  │
  ├─ 0. 序列化请求 (protobuf → 字节流)
  │
  ├─ 1. 服务发现 ← 本文档的核心
  │     ├─ GetHostData() → 缓存 / ZK
  │     ├─ ParseHostData() → "ip:port" → ip + port
  │     └─ GetPooledConnection() → 连接池
  │
  ├─ 2. 网络传输
  │     └─ SendRequestAndReadResponse()
  │         ├─ ConnectToEndpoint() (非阻塞 + poll 超时)
  │         ├─ SendAll() (循环 send)
  │         └─ RecvAll() (循环 recv)
  │
  ├─ 3. 失败重试
  │     ├─ DropEndpointConnections()
  │     ├─ InvalidateHostData()
  │     ├─ 重新 QueryEndpointList()
  │     └─ 重试一次 SendRequestAndReadResponse()
  │
  └─ 4. 解析响应
        ├─ DecodeRpcResponsePayload()
        ├─ 校验 request_id
        ├─ 校验 error_code
        └─ response->ParseFromString()
```
