# mprpc-video-platform

一个从零实现的 Linux C++ 网络库、RPC 框架，以及构建在两者之上的分布式视频转码平台。

三个子项目逐层递进，每一层都是前一层的直接用户：

```
wevix_muduo（网络库）→ mprpc（RPC 框架）→ video_platform（分布式视频平台）
```

这个项目不是为了演示某个单一技术点。它覆盖的是一条完整的纵深链路：epoll 事件循环、Reactor 网络编程、内存池、异步日志、protobuf 序列化、ZooKeeper 服务发现、分布式任务调度、MySQL 持久化、Redis 缓存、RabbitMQ 事件驱动、Prometheus 可观测性，以及 FFmpeg 真实转码。网络库和 RPC 框架是自研的，平台层集成的中间件全部是真实可用的，不是 mock。整个项目在本地和 Docker 环境都跑通了全链路验证：提交一段视频，经过任务切分、多 Worker 并行转码、结果合并，最终产出完整视频。
xu

## 项目自述
其实一开始想做高并发的IM项目，但是感觉逻辑很简单，很常见，做的收获不会太高，而且之前也做过一个demo版的聊天室，所以这次就换了一个，不过底层依然是基于我自己写的网络库和分布式框架，这俩放一块也是不断得到优化，中间也是加了很多投入自己思考的内容，我认为项目还是有很多地方值得优化的，比如，纯网络库的压测是七十万QPS,但是在分布式框架上就成了十几万，加上业务就只剩1万，虽然降低是合理的，但是视乎降的太多了，所以后面对于这个项目还是会不断进行优化的

下面是我认为一些比较经典、比较优秀的设计点：

### 网络库框架层：

- **ET 边缘触发 + 循环读满为止**：经典 muduo 用 LT，本库改用 EPOLLET，`handleRead` 循环 readv 直到 EAGAIN，一次 epoll 唤醒榨干内核缓冲，事件更少、唤醒开销更低（压测支撑 17 万 QPS）
- **写事件先于读事件处理（与 muduo 顺序相反）**：ET 模式下同批 IN|OUT 若先读后写，读回调执行后 return 会吞掉 EPOLLOUT，而输出缓冲无状态迁移不会再触发 → 连接永久悬挂。先写后读：OUT 被消费，IN 延迟一轮由 ET 持续触发，数据不丢
- **回调后立即 return 的 UAF 防线**：压测 64KB 大报文真实踩过 SIGSEGV——读回调内可能同步销毁 Connection 和 Channel。约定"每个回调执行后禁止再访问 this"；muduo 靠 shared_ptr 保活，这里靠结构性纪律，更省心
- **插件式帧编解码器**：Connection 可挂 `MessageCodec`（Buffer→完整消息），handleRead 循环拆帧，粘包/拆包下沉到网络层，应用层永远收到完整消息；长度非法自动清缓冲。经典 muduo 需要应用层自己拼包
- **timerfd 单定时器空闲扫描**：每个 subLoop 一个 5s 周期 timerfd，线性扫描超时（10s）连接并 forceClose；替代 muduo 的 TimerQueue 最小堆，连接超时场景足够且实现简单
- **Channel 走内存池分配**：operator new/delete 挂自研 MemoryPool，高频创建/销毁的对象免 malloc
- **tcmalloc 式三层内存池**：ThreadCache（thread_local 空闲链表，8B 对齐线性分类到 256KB）→ CentralCache（每 size 类自旋锁、批量 8 块获取、SpanTracker 排序+二分定位、延迟归还合并整 span）→ PageCache（按页数 best-fit + 碎片向后合并 + 128MB 上限 mmap/munmap 释放超量）
- **线程池双模式 + 背压**：FIXED（IO 循环线程）/ CACHED（work 池：积压自动扩容、空闲 60s 自动收缩）；任务队列满时 wait_for 1s 背压而非无限膨胀；submitTask 返回 future；notify_one 防惊群
- **Work 池 + 消息堆上转移**：IO 线程把消息 move 进 shared_ptr 再投递 work 池后立即返回，慢业务不阻塞网络线程；未启用 work 池时零拷贝直接回调
- **fd % threadNum 确定性分配**：新连接按 fd 取模选 subLoop，简单确定；经典 muduo 用原子计数轮询
- **connectEstablished 延迟注册读事件**：所有回调设置完成之后才 enableReading，新连接不会在配置就绪前被抢先读包
- **Acceptor 循环 accept**：一次唤醒 accept 完整个队列（EAGAIN 为止），高并发下不积压 Accept 延迟
- **跨线程安全 send**：业务线程调用 send 自动 runInLoop 转投 IO 线程；直发 + 缓冲发送两级路径；EPIPE/ECONNRESET 自动收尾；shutdown() 半关闭支持短连接 RPC 场景
- **Buffer 三区模型 + readv 散射读**：8B prepend 预留让前置协议头 O(1)；消费只移游标不 memmove；readv + 栈上 64KB 副缓冲一次系统调用读满；空间不足先碎片整理再扩容
- **handleClose 的 runInLoop 自清理模式**：关闭时把 shared_from_this 捕获进 lambda 转投所属 IO 线程统一执行清理（disableAll → remove → 移除连接表 → 用户回调），self 保活保证清理完成前 Connection 不被析构，所有销毁动作都在所属线程，杜绝跨线程析构
- **双容器连接管理**：TcpServer::connections_（全量连接表，mutex 保护）+ 每个 subLoop 自己的 conns_（仅供超时扫描）；超时剔除只发生在 subLoop 的 timerfd 周期里，mainLoop 不参与连接管理
- **EventLoop 任务队列锁内 swap**：doPendingTasks 把队列 swap 到局部变量再逐条执行，锁只保护 swap 一瞬间，执行任务期间不持锁，其他线程入队不被阻塞
- **异步日志的细节优化**：业务线程只格式化入队、后台线程落盘；时间戳秒级缓存避免每条日志调 localtime_r；LOG_DEBUG 在 Release 下编译为零开销；FATAL 同步写 stderr 再 abort 保证崩溃前消息不丢；按天轮转且保持文件打开

**压测故事与踩坑修复（每个都是真实事故）：**

- **压测断崖定位法：反证法 + 数据反推**：500 并发 QPS 从百万级掉到 825、总耗时 12s，P50 正常但 P99 爆炸（27ms）——"P50 正常证明处理不慢，问题是吞吐被外部延迟拉低"。三个嫌疑参数（backlog=128 / 单次 accept / epoll 100 事件上限）逐个修复验证：只修 backlog 断崖即消失。根因：Accept 队列满时内核直接丢 SYN，客户端 TCP 重传指数退避（1s→2s→4s→8s）决定总耗时。修复：backlog 4096 + Acceptor 循环 accept + 事件数组 4096
- **EventLoop 线程 ID 错位 → epoll EEXIST**：threadId_ 在构造函数记录的是主线程 tid，而 subLoop 实际跑在 IO 子线程——isInLoopThread() 双向误判（主线程被误判为 loop 线程、IO 线程被误判为外部线程），fd 的 DEL/ADD 不保证同线程顺序执行，短连接 fd 快速复用时 "ADD 先于 DEL 到达内核" → EEXIST 新连接建立失败。修复：threadId_ 在 run() 中记录 + atomic 读取
- **消息路径 4 次 malloc 的解剖与消除**：一条 12 字节消息被拷贝 4 次（retrieveAllAsString → handleMessage 复制 → lambda 按值捕获 → 内部再拷贝），按 4KB 消息 × 1000 QPS 推算每秒 160MB 拷贝流量。修复：IO 线程 make_shared<std::string>(std::move(message))、lambda 只捕获 shared_ptr——4 次 malloc + 3 次 memcpy → 1 次 malloc + 0 次 memcpy，零 API 变更
- **连接关闭滞留修复**：handleClose 删除了 TcpServer::connections_，但 EventLoop::conns_ 仍持有 shared_ptr（供超时扫描）→ fd 直到定时器超时才能释放，短连接高 QPS 下大量关闭连接滞留。新增 EventLoop::removeConnection(fd) 在 close/error 时立即删除
- **日志分层策略**：9 处静默忽略的错误补 LOG_ERROR、9 处关键状态变更补 INFO/WARN、高频连接日志降 DEBUG（Release 编译期裁剪，压测日志不进热路径）；accept 的 EAGAIN 曾被误打 ERROR 刷屏——错误分类在决策方（Acceptor）不在执行方（Socket）

**内存池专题（压测驱动优化的完整方法论）：**

- **make_shared 绕过类级 operator new（实测对照）**：new Foo / make_unique / shared_ptr(new Foo) 都会调用类级 operator new，唯独 make_shared 不调——它用全局 operator new 一次性分配"控制块+对象"联合内存再 placement-new。接入决策三选一：new 创建 → 类内 operator new（零成本）；make_shared 且是热点 → allocate_shared + 自定义 Allocator；非热点 → 不接入
- **内存池的诚实结论**：单线程 P99 16ns vs malloc 23ns（1.4×），多线程反而更慢（CentralCache 自旋锁竞争）；Channel 接入内存池后 E2E ±3% 零影响——152 字节对象分配在网络 syscall（微秒级）面前完全淹没。结论：优化目标选错了，真正的热点在 STL 容器分配，muduo 层先用 move 消拷贝
- **PoolAllocator：内存池打通 STL**：类级 operator new 不能用于 STL 容器（内部分配由 std::allocator 控制）且与 make_shared 不兼容；PoolAllocator<T> 把 allocate/deallocate 重定向 MemoryPool。四个关键点：is_always_equal（容器 move 直接偷指针）、泛型拷贝构造（C++17 rebind 废弃后自动推导）、allocate(0) 返回 nullptr、operator== 必须定义在类外（friend 模板会重复实例化报 redefinition）。RPC 反序列化模拟 3.8× 提升（20 万次小分配走无锁 TLS 快路径，避开 glibc arena 锁）

**与经典 muduo 的 API 差异（设计取舍）：**

- **TcpServer 自管理 EventLoop**：构造即声明线程数（TcpServer(ip, port, 4)），替代 muduo 的"外部传 loop + setThreadNum + 手动 loop()"；start() 内部启动全部 loop 并阻塞当前线程，调用方不持有任何 EventLoop
- **OnConnection/OnClose 拆分**：muduo 的 OnConnection 在连接建立和断开时都触发、需自行判断 connected()；这里拆成两个独立回调，消除样板代码
- **OnMessage 传 std::string& 而非 Buffer\***：网络层已用 codec 或 retrieveAllAsString 提取完毕，应用层不需要操作 Buffer 细节；压测论证无 work pool 时透传引用与 muduo 拷贝次数持平
- **"为自己的协议定制网络栈"**：通用 muduo 追求协议无关、把 Buffer 直接暴露给用户、帧解析全交上层；wevix_muduo 与 mprpc 一同设计、RPC 是唯一上层协议，所以把长度前缀帧认知下沉到 Connection 层（codec 可插拔、不设 codec 行为完全不变）——这是自研网络库相比复用通用库的核心优势

### RPC框架层：

- **协议帧 magic/version 演进与防护**：`[total_len(4B)] + [magic "MR"(2B)] + [version(2B)] + [payload]`，单帧上限 64MB；长度非法直接清缓冲、magic/version 错误拒绝该帧——坏包在协议层被拒，而不是交给 protobuf 解析崩溃；双端再做 args_size/response_size 内容自校验
- **ZK 节点结构**：`/mprpc/services/{service}/{method}/instance-*` 临时顺序节点，同方法多实例天然支持，Provider 崩溃节点自动消失；永久节点只创建一次
- **三级服务发现缓存**：本地 30s TTL → Redis 集中缓存（多进程共享发现结果、省 ZK 往返）→ ZK 兜底并双写回前两级；连接失败时 HDEL 让所有进程同时失效，不依赖 watch 也无失效风暴，Redis 不可用时 TTL 自愈
- **连接池按 endpoint 分片 + 每连接互斥**：同步模型下"每连接一把锁、一连接一请求在途"，天然无响应错配；连接级失败整片清池重建——专治服务端回收空闲连接导致客户端积压死连接的问题
- **同步阻塞 + 内核级超时 + deadline 透传**：等响应交给 SO_RCVTIMEO 内核超时；request_id 原子自增、响应必校验防长连接串包；deadline 随请求传到服务端，已过期请求直接拒绝，不浪费 work 线程
- **失败分级 + 一次重试**：连接级失败清池重建重试、发现级失败失效缓存重新发现，两轮重试互不干扰；15 个错误码分网络/协议/服务治理三层；服务端任何解析失败都回错误帧，客户端不会干等超时
- **protobuf 反射分发 + IO/Work 线程分离**：GetRequestPrototype().New() 动态创建 request/response，框架零代码生成支持任意 service；IO 线程只拆包投递，业务隔离在固定 work 池，慢方法不饿死连接
- **自研可观测性栈**：Counter/Gauge/Histogram（固定桶 + 导出时前缀和 + 线性插值算分位）+ 手写 HTTP /metrics 抓取端点 + 30s 日志告警循环——Prometheus 没部署也能从日志发现异常；metrics_port<=0 整体可降级
- **容器注册地址修复**：rpcserverip=0.0.0.0（Docker 全接口监听）时用 GetLocalIp() 自动探测容器真实 IP 注册 ZK，否则消费者拿到不可路由地址（实测 connect 0.0.0.0 必失败）
- **ZK 防阻塞封装**：C SDK 同步 API 实测会无限阻塞（7+ 分钟），改异步 API + sem_timedwait 3s 超时 + caller_gone 延迟回收防 UAF + zoo_state 快速失败；stale ephemeral 节点先删后建保证会话绑定
- **直连模式 + 内嵌 Redis 缓存**：MprpcChannel(ip, port) 跳过服务发现直连，支持定向调用；150 行 mprpcredis 只做 HGET/HSET/EXPIRE/HDEL 集中管理 ZK 缓存，失败自动降级回退

**问题驱动：每个能力都是真实故障逼出来的**

- **"每次 RPC 建 ZK 会话"是 QPS 杀手**：初版 CallMethod 每次在栈上创建 ZkClient（zookeeper_init 触发建内部 IO/Watcher 线程）、查数据、析构关闭——服务发现在每次调用的热路径上，ZK 日志刷屏且 QPS 极低。修复为三层缓存（本地 ServiceCache → 进程级单例 ZK 连接 → TCP 连接池），压测 ZK 模式 72.5K QPS、长连接 84.7 万 QPS
- **失败失效链路 + 只重试一次**：连接失败 → ① 清该 endpoint 连接池（避免取到已断连接）② 删服务发现缓存 ③ 重新查 ZK ④ 用新 endpoint 重试一次。重试只做一次而非无限循环（ZK 故障或全集群宕机时不进入死循环）；"取连接"与"发请求"分离，TCP 连接延迟建立只在真正发送数据时
- **非阻塞 connect 可超时握手**：connect() 返回 EINPROGRESS → poll(POLLOUT, timeoutMs) → getsockopt(SO_ERROR) 确认真实错误 → 恢复非阻塞标志 + 设置 SO_SNDTIMEO/SO_RCVTIMEO——不依赖内核默认 75s 连接超时
- **SendAll/RecvAll 循环收发**：send() 不保证一次写完（大请求/发送缓冲紧张时只写部分），统一循环发送直到写完；接收统一 RecvAll，kMaxRpcFrameSize 64MB 上限防恶意/损坏帧
- **服务端七项边界检查 + 坏包也回包**：header_size 合法 → RpcHeader 可反序列化 → args_size 与实际长度一致 → service 存在 → method 存在 → 参数可解析，任何一步失败都返回带 error_code 的响应；连 header 都解不出也回 request_id=0 的错误帧——客户端绝不干等超时
- **服务端消费 deadline**：OnMessage 解码 header 后、进 work pool 前检查 nowMs > deadline_ms 直接回 RPC_TIMEOUT——请求在队列里排队过期就丢弃，不做无效计算；对视频转码这类长耗时场景尤其重要（转码排队 30s、客户端超时 10s 时服务端不该白算）
- **框架初始化不替业务做决策**：MprpcApplication::Init()/RpcProvider::Run() 返回 bool，由业务自己决定退出/降级/重试；配置读取防御化（LoadRequired 必填、LoadInt 范围校验、修复空白行与重复初始化残留）——避免空 IP、空端口这类隐式错误
- **所有权与 done 闭包契约**：Provider 只存裸指针用于方法分发、不接管所有权（栈或全局对象，杜绝 new 完不管的泄漏写法）；done 绑定 SendRpcResponse 统一回包——同步方法由 protobuf 默认实现自动调 done->Run()、异步方法必须手动调，漏调导致请求永久悬挂
- **两层帧头各司其职**：外层 total_len 由网络层 codec 消费（只认长度不解析内容，管帧边界、半帧留在 Buffer）；内层 header_size 由应用层消费（提取 service/method/args_size）——协议演进只需换 codec 实现

### 业务层：

- **Job/Shard 双状态机 + 条件更新防回退**：Job（PENDING→SCHEDULING→RUNNING→SUCCESS/FAILED）与 Shard（WAITING→RUNNING→SUCCESS，FAILED→RETRYING→WAITING）双机流转；所有状态推进走单条 `UPDATE ... WHERE id=? AND status IN(前置)`，MySQL 行级原子性完成 CAS，旧快照写不进去，枚举值单调递增再兜底
- **资源感知加权评分调度**：`score = 空闲槽×10 − CPU×0.5 − 内存×0.2`，空闲槽 = max_running − current_running − 轮内已分配；轮内配额防一轮扫描超分配；WAITING shard 按 job 优先级降序 + 同优先级 FIFO，防饥饿
- **分片切分 + 末片时长自适应**：ffprobe 探测真实时长（失败回退配置值），ceil 分片、末片取剩余；切片+转码一 pass（-ss 前置 input seeking + 重编码帧精确），省一次中间文件 IO；Probe 带 15s 超时 SIGKILL 兜底
- **四条故障恢复路径收敛到 WAITING**：结果失败重试 / Worker 离线重分配 / 执行超时重扫（ASSIGNED 30s 先 QueryShard 二次确认、RUNNING 5min 强信号）/ Scheduler 崩溃重启恢复——全部重置 WAITING 复用同一套分配逻辑；job 已终态的残留 shard 一律 CANCELED，绝不复活
- **心跳 + 原子离线判定**：3s 心跳上报真实 CPU（聚合进程树 utime/stime ticks）/内存/running 数；离线判定是单条条件 SQL `UPDATE ... WHERE status=ONLINE AND last_heartbeat<now-20s`，与心跳更新天然互斥，消除 TOCTOU
- **MQ 双连接双锁 + Push/Pull 双模调度**：发布/消费严格分连接，消费 poll 移出锁 + atomic 无锁读 connected()，永不被锁卡死；MQ 在线 5s 兜底轮询、故障 2s 快扫、恢复自动切回，实测 Push 调度延迟 33ms
- **Redis 双层用途 + 降级语义**：worker 负载快照（调度读路径免 RPC 往返，20s 过期过滤）+ shard 分布式锁（SETNX EX 10 + GET 校验再 DEL）；Redis 是读路径加速器不是数据源，故障降级放行由 MySQL CAS 兜底
- **MySQL 连接池 + CLIENT_FOUND_ROWS**：固定池预创建 + ping 保活 + RAII 归还；FOUND_ROWS 保证全量覆盖更新能正确判命中；INSERT 1062 幂等；启动自动建表部署零步骤；字符串全转义防注入
- **FfmpegExecutor：fork+execvp 替代 popen**：不走 shell 无命令注入；poll 非阻塞读管道 + should_cancel 回调 → SIGTERM → 5s 未退 SIGKILL，取消即时生效；-threads 限核防"并发 shard × 全核"CPU 超订；进度解析双向回退
- **可降级组件 vs fail-fast 分层**：MySQL/ffmpeg 启动必检、失败拒绝服务；Redis/MQ/metrics 连不上只 WARN 自动走替代路径；结果上报三级降级链（MQ 发布 → 直连 RPC 重试 3 次 → 心跳线程兜底重试）
- **全链路幂等 attempt_id**：每次重试 attempt 递增，结果上报/重调度/取消三条路径共享同一语义——MQ 重投、RPC 重试、进程重启都不会双份执行，at-least-once 投递 + 恰好一次生效
- **后台扫描三件套互相兜底**：JobService.PendingScanLoop（10s）/ Scheduler.SchedulingLoop（启动恢复+超时重扫+轮询分配）/ RC.TerminalSweepLoop（15s 兜底终态判定+补 merge），任一 RPC 链路断裂都有周期任务收尾
- **Worker 弹性扩容**：worker_id 环境变量 > 配置 > hostname 三级解析，`--scale transcode_worker=3` 副本天然唯一；免分布式 ID 方案 prefix_时间戳_计数器_随机数，避开 /dev/urandom 在容器里可能阻塞的问题

**并发与一致性专题（全部是真实事故驱动的设计）：**

- **ScheduleJob 全局互斥 + 幂等早退**：并发来源是 SubmitJob 内联重试 × PendingScanLoop 后台扫描，两个请求可能同时通过"shard 表为空"的幂等检查 → 各自探测（ffprobe 失败还会回退 fallback 时长算出不同片数）→ 两套分片计划交错落库（主键冲突被静默吞掉 + 调用方不检查返回值是放大器）→ 下游合并/终态全乱。修复：全局互斥锁内跑完"检查→探测→切分→回填"四段；幂等判据 = shard 表已有记录而非 job 记录（只有 shard 存在才能证明切分真的做完了）
- **UpdateIfStatus 为什么不用版本号**：状态字段本身就是天然的版本号，且 expect 集合比 version 字段更精确（如启动恢复只允许重置 {ASSIGNED, RUNNING}，不允许动已是 WAITING 的）；22 处调用点全部"快照 → 改 → 条件更新"，失败哲学 = 不重试不覆盖、交给兜底循环收敛（这也是所有状态推进点都处于某个循环内的原因）；实测平均 0.166ms
- **三层防线 + 锁是防护不是依赖**：① 状态复核（入口重新 Get，非 WAITING 直接 skip，防过期快照）② SETNX 分布式锁（防同一时刻竞争）③ MySQL 条件更新兜底（Redis 故障时正确性仍成立）。SETNX 失败要区分两种原因：GET 成功 = 锁被占用 → 跳过；GET 失败 = Redis 故障 → 降级放行——不能因为 Redis 挂了让整个调度停摆
- **取消链路两层哲学**：状态保证是强保证（MySQL CAS：job→CANCELED + 全部非终态 shard→CANCELED，单条原子 UPDATE 无网络依赖）；执行保证是 best-effort（RPC 通知 + SIGTERM→5s→SIGKILL）。最坏情况 = 已启动的转码跑完、产出被丢弃——但取消后任务永远不会再被调度、被重试、被合并产出成品
- **CancelShard 只置原子标志不直接 kill**：ffmpeg 子进程由执行线程创建并 waitpid 回收，跨线程 kill 与回收有竞态；SIGKILL 写文件中途会留损坏中间产物；标志位是最小同步面。检测点双置：poll 超时（子进程安静时）+ 每行进度输出后（~0.5s 响应）
- **attempt 是"执行代次"：取消意图决定是否带 attempt_id**：超时重扫的 CancelShard 必须带 attempt_id（意图是取消卡死的旧执行、保留新执行——不带会把新 attempt 一起杀掉，新执行白分配可能再卡死，形成死循环式浪费）；用户取消不带 attempt_id（job 已取消，所有执行包括重分配后的新 attempt 都该停）。attempt_id 是手段，意图是目的
- **Merge 互斥集：mutex 管"同时"、set 管"先后"**：锁只保护同一时刻，保护不了另一线程稍后再次进入（TerminalSweepLoop 15s 后又判一次 SUCCESS 再 merge）——set 是跨轮次的持久记忆。成功即永久登记（防重复合并）、失败即撤销登记（可重试收敛闭环）；曾因无锁并发写同一 filelist.txt 真实产出过串片损坏的视频
- **TranscodeWorker 五条死锁设计纪律**：① 锁内绝不 join/detach 线程（执行线程结束时等锁 → AB-BA 死锁）② 锁内绝不 RPC/长 IO（5s 超时劫持锁）③ 检查+插入必须同一锁内（TOCTOU 双条目互相覆盖）④ 复合结构用锁、单值用 atomic ⑤ 临界区全部 O(1)。pending 结果队列 swap 整队搬走、锁外做 RPC；CPU 采样用 static 局部 mutex 而非 thread_local（后者每线程独立采样状态，第一拍恒返回 0%）

**故障恢复与调度专题：**

- **调度模式演进：Pull → Push → 双模并存**：Pull 2s 轮询的延迟被周期钳制（平均 ~1s）；Push 事件驱动在"任何 shard 变为 WAITING 的路径"发布事件（新建/重试/离线重置/超时重置/启动恢复），实测 33ms；Pull 保留 5s 兜底——因为轮询还承担 Push 替代不了的可靠性职责（超时重扫/启动恢复/卡死检测）；超时重扫改按时间 30s 驱动（interval 随 MQ 状态 2s↔5s 变化，按轮次会漂移成 75s）
- **超时重扫"先问后判" + progress_watch_ 记账本**：ASSIGNED 超时 ≠ 卡死（正常 Worker 从不向 Scheduler 报进度，慢视频 30s 没动静完全正常）——先直连原 Worker QueryShard 确认，四种结果处置（已不持有 → 等结果；进度前进 → 续期观察窗；停滞 → 连续 2 轮判死；RPC 失败 → 同停滞）。记账本让"无记忆"的 30s 周期重扫能区分"刚停"与"停了 1 分钟"；连续 2 轮才判死不误杀慢任务（转码中途短暂停顿正常）
- **NotifyWorkerOffline 待重试集合**："下一轮会重试"是错的——Worker 已标 OFFLINE 后扫描和条件更新都是 no-op，通知丢失则 ASSIGNED shard 永久卡死。修复：pending_notify 集合无限重试（at-least-once 送达，失败永不离场）；重试前复查 Worker 是否已复活（否则把存活 Worker 正在执行的 shard 重置 = 双份转码）
- **五类故障 → 恢复机制映射**：执行失败 → 重试（上限 3）；进程退出 → 心跳超时 → 离线重分配；上报超时 → 心跳线程兜底重试；重复上报 → attempt_id 幂等拒绝；Scheduler 崩溃重启 → 启动恢复扫描重置残留 shard。每类故障都有明确的兜底路径，无死角
- **心跳超时 10s → 20s**：网络抖动被当成执行失败 → 虚假重调度 + 不向旧 Worker 发取消 → 同 shard 双份转码、重试预算被抖动白白消耗。20s ≈ 两个完整心跳周期
- **Worker 重启后自动恢复注册**：WM 重启丢记录后心跳返回 alive=false → Worker 立即重新 RegisterWorker（幂等 upsert）——解决"WM 晚就绪导致 Worker 永久离线"；注册带指数退避 1s→30s 无限重试
- **CPU 采集的三次演进**：/proc/stat 是主机级指标（单机多 Worker 下繁忙邻居让空闲 Worker 报 CPU>90 拒收全部任务）→ /proc/self/stat 进程级（但 cutime/cstime 只统计已收割的已终止子进程，运行中的 ffmpeg 不计入，心跳 CPU 恒 0）→ /proc/self/task/*/children 聚合整个进程树
- **mock_fail_ratio 故障注入**：配置驱动（0-100）不重编译即可切换故障场景，保留压测/演示能力；thread_local mt19937 每线程独立生成器不共享竞争；失败时 error_msg 携带 ratio 和 roll 值便于复现

**可观测性专题（指标口径即需求）：**

- **直方图只加命中桶、导出时前缀和**：朴素实现每次 Observe 要自增 N 个桶原子量；改为命中桶 +1（+count+sum），导出/分位计算时前缀和——热路径只写 1 个原子量
- **RAII 守卫插桩覆盖所有 early return**：CallMethod 有 ~10 个提前返回路径（序列化失败/帧过大/发现失败/超时/解析失败…），手动计时必然漏掉异常路径——而告警关心的正是异常延迟；RAII 守卫声明在函数体顶部、析构覆盖全部 return。"插桩正确性靠作用域，不靠调用点纪律"
- **指标口径设计**：scheduler_queue_size = WAITING + ASSIGNED（已分配未开始也是调度积压，与 shard_waiting 严格区分）；shard_count 全枚举显式 Set 含 0（状态归零后 Prometheus 保留旧值 → 饼图出现永不消失的残留扇区）；失败率两套口径（Prometheus 用失败/提交、进程内兜底用失败/终态——单进程拿不到对方 counter）是刻意设计不是 bug
- **告警双轨**：进程内 AlertLoop（30s 一轮，触发/恢复直接打日志）+ Prometheus 集群级规则并存——不装监控也能从日志发现异常；4 条规则阈值（worker_online<1 持续 1min / queue>100 持续 2min / 失败率>30% 持续 5min / RPC P99>1s 持续 5min）；监控上线实测暴露了 2 个框架级缺陷（ZK 同步调用无限阻塞、MQ 消费锁死）


# 下面是我借助claude工具，写的一个README介绍，我认为比我写的好多了，至少能让看的人读的很清晰

## 项目结构

```
wevix_muduo/        自研网络库：Reactor 模型、线程池、内存池、异步日志
mprpc/              自研 RPC 框架：protobuf 序列化 + ZooKeeper 服务注册发现
video_platform/     分布式视频处理平台：5 个微服务
docker/             Docker 化部署（docker-compose 一键启动）
doc/                系统设计、开发日志、快速启动指南
scripts/            启动/停止/集成测试脚本
```

## 一、wevix_muduo：网络库

### 整体模型

采用经典的 One Loop Per Thread 模型，受 muduo 启发但完全独立实现：

```
mainLoop_ (Acceptor) ──► 新连接按 Round-Robin 分配 ──► subLoops_ × N
                                                             │
                                                        IO 线程处理 read/write
                                                             │
                                              ┌──────────────┴──────────────┐
                                        直接回调处理                   提交到 workThreadPool_
                                        （轻量业务）                 （CPU 密集/阻塞业务）
```

- `EventLoop` 封装 epoll + eventfd（唤醒）+ timerfd（定时器），每个线程跑一个 loop；
- `Acceptor` 只负责 accept，连接建立后按轮询分配给 subLoop，避免单线程成为瓶颈；
- `Connection` 持有 Socket + Channel + 双 Buffer，读写事件在所属 loop 线程内处理；
- 重活提交到 `ThreadPool`（FIXED / CACHED 两种模式，带背压队列），IO 线程不做阻塞操作。


## 二、mprpc：RPC 框架

mprpc 基于 wevix_muduo 构建，protobuf 定义接口，ZooKeeper 做服务注册发现。

### 协议设计

自定义二进制帧协议，客户端按长度前缀自动拆帧：

```
[total_len(4B)] + [magic(2B, "MR")] + [version(2B)] + [payload]
  payload 请求:   [header_size(4B)] + [RpcHeader(protobuf)] + [args(protobuf)]
  payload 响应:   [response_header_size(4B)] + [RpcResponseHeader(protobuf)] + [response_body]
```

拆帧由 `RpcMessageCodec` 挂在 Connection 层完成，应用层拿到的永远是完整消息。单帧上限 64MB。

### 服务发现与调用链

```
MprpcChannel::CallMethod()
  ├─ 序列化请求 → [total_len + header + args]
  ├─ 服务发现：查本地缓存 → ZooKeeper /mprpc/services/{service}/{method}/*
  ├─ 轮询选择 endpoint → 连接池取连接
  ├─ SendAll → RecvAll → 校验 request_id → 反序列化响应
  └─ 连接失败：失效缓存 + 重新发现 + 重试 1 次
```

几个关键决策：

- **ZK 地址缓存**：不是每次调用都读 ZooKeeper，缓存命中直接走；连接失败时主动失效缓存并重刷，兼顾性能和一致性；
- **连接池按 endpoint 分片**：每个服务地址独立维护连接池，单 endpoint 最大连接数可配置，避免某对端异常时连接互相挤占；
- **同步调用 + 超时 + deadline 透传**：调用方超时后，deadline 会随请求传到 Provider 端，Provider 对已超时的请求快速拒绝，不浪费处理资源；
- **框架级 metrics 插桩**：`CallMethod` 用 RAII 计时器自动记录 `rpc_latency_ms{method}` 直方图，Prometheus 格式导出，不用改业务代码就能拿到每个 RPC 方法的延迟分布。

## 三、video_platform：分布式视频转码平台

平台基于 mprpc 构建，把一段视频切成多个 shard，分发给多台 Worker 并行转码，最后合并出完整视频。

### 五个微服务

```
Client/CLI ──SubmitJob──► JobService ──ScheduleJob──► SchedulerService
                                                            │
                                               ListWorkers │  AssignShard
                                                            ▼
                               WorkerManager ◄──────── TranscodeWorker
                                     ▲                        │
                                     │                        │ ReportProgress
                                     └── Heartbeat            │ ReportResult
                                                              ▼
                                                      ResultCollector
```

| 服务 | 职责 | RPC 端口 |
|------|------|---------|
| JobService | 接收任务提交/查询，维护任务主状态 | 9001 |
| SchedulerService | 时长探测、shard 切分、调度分配、失败重试 | 9002 |
| WorkerManager | Worker 注册、心跳、负载上报 | 9003 |
| TranscodeWorker | 真实 FFmpeg 转码执行，可多实例横向扩容 | 9004+ |
| ResultCollector | 收集 shard 结果，触发合并，落终态 | 9005 |

任务状态机：`PENDING → SCHEDULING → RUNNING → SUCCESS / FAILED`，任务下挂多个 shard 各自流转（WAITING → ASSIGNED → RUNNING → SUCCESS/FAILED），只有全部 shard 成功任务才成功。JobService 和 ResultCollector 分别是状态机的入口和出口，Worker 不直接改任务状态，避免多写者竞争。

### 调度策略

- **FFmpeg 真实时长探测**：Scheduler 在 `ScheduleJob` 时先用 ffprobe 探测视频真实时长，据此切分 shard，最后一个 shard 时长自适应；探测失败回退到配置的 fallback 时长；
- **资源感知加权评分**：Worker 心跳上报 CPU/内存占用，调度时按 `score = slots*10 - cpu*0.5 - mem*0.2` 打分选 Worker，兼顾剩余容量和当前负载；
- **优先级队列 + 过载保护**：shard 按任务优先级出队；Worker running 数达到上限或 CPU 超过 90% 时拒绝新分配；
- **多 Worker 自动均衡**：`docker compose up -d --scale transcode_worker=3` 一行命令扩容，worker_id 由环境变量 > 配置 > hostname 逐级解析，副本之间天然唯一，无需手动改配置。

### FFmpeg 执行器

`FfmpegExecutor` 封装 FFmpeg/FFprobe 命令行，全部静态方法，线程安全：

- **切片 + 转码一 pass**：`-ss` / `-t` 直接传给转码命令，避免先切完再转的多轮 IO，帧精确；
- **重编码切片**：切片不采用 `-c copy`，而是用 `libx264 ultrafast` 重编码，保证切点精确到帧；
- **fork+exec 替代 popen**：popen 拿不到子进程 PID 无法取消，改成 `pipe + fork + exec + fdopen`，通过 `should_cancel` 回调支持 kill 子进程即时取消，优雅等待 5s 再 SIGKILL；
- **进度解析**：解析 ffmpeg 的 `out_time_ms` 输出实时上报进度；
- **截图 + 合并**：转码成功后自动截取视频中点帧随结果上报；全部 shard 完成后用 concat demuxer 合并出完整视频。
- **启动自检**：`executor_mode=ffmpeg` 时启动即检测 ffmpeg/ffprobe 可用性，缺失直接拒绝启动，避免运行到一半才发现工具链不完整。

### 故障恢复

- **失败重试**：shard 失败自动重试（上限可配），worker 转码中被杀 → 重调度恢复，实测最终 SUCCESS；
- **Worker 离线重分配**：WorkerManager 检测心跳超时，把该 Worker 上未完成的 shard 重置为 WAITING 重新分配；
- **终态任务不被重复调度**：任务进入 SUCCESS/FAILED 后，残留的 ASSIGNED/RUNNING shard 不允许再被重置分配——这个问题当时真实出现过：Worker 死亡时离线处理会把已完成任务的残留 shard 重新分配出去，导致任务被重复执行。修复方式是在任务终态时通知 Scheduler 标记取消，同时离线处理、超时扫描、分配循环三处都加终态检查。

### 中间件集成

三个阶段依次接入，每一层都保留了降级路径：

- **MySQL（持久化）**：三个 Store 以 MySQL 为唯一数据源，服务重启数据不丢、多进程天然共享。连接池固定大小预创建、`mysql_ping` 保活。关键设计是 `UpdateIfStatus` 条件更新——状态推进必须带旧状态条件，防止旧快照覆盖其他进程的推进；
- **Redis（缓存 + 锁）**：WorkerManager 心跳双写 Redis 快照，Scheduler 优先读快照（20s 过期过滤），失败回退 ListWorkers RPC；分配 shard 前用 `SETNX shard:lock:{id} EX 10` 加分布式锁。Redis 被设计成**可降级组件**——连不上只 WARN 不拒启，MySQL 才是 fail-fast，职责划分明确；
- **RabbitMQ（事件驱动调度）**：事件拓扑 `job.events → shard.waiting`（分配通知）+ `shard.events → result.pending`（结果数据），durable exchange/queue + 消息持久化 + 手动 ACK，消息不丢。Scheduler 的分配逻辑抽成 `TryAssignShard` 供轮询和 MQ 消费线程共用；MQ 在线时调度延迟实测 33ms，故障时自动回退 2s Pull 轮询，恢复后自动切回。

### 可观测性

框架层提供了自研的 metrics 库（Counter/Gauge/Histogram，固定桶 + 前缀和导出 + 线性插值算分位），5 个服务各起一个 HTTP 端口暴露 Prometheus 文本格式指标：

- 任务计数（提交/成功/失败）、shard 分布、调度循环耗时；
- Worker 在线数、负载采样（Gauge 采样器线程 5s 一轮）；
- RPC 延迟直方图、转码耗时。

配套交付了 `prometheus.yml` + 4 条 PromQL 告警规则（worker 离线、调度积压、任务失败率、RPC P99 长尾）+ Grafana 面板（任务吞吐、shard 分布、Worker 负载热力图、RPC 延迟四行）。即使不部署 Prometheus，每个服务进程内还有告警兜底循环，触发和恢复都会直接打日志 `ALERT [xxx] firing/recovered`——可观测性是平台自身的一部分，不依赖外部组件就位。

## 四、测试与验证

不止是单元测试，这个项目的大量验证发生在真实运行环境：

- 单元测试 43/43 通过（buffer / threadpool / RPC 协议）；
- echo 压测：100 并发阶梯 17 万 QPS，P50 约 60us，64KB 大报文 0 失败；
- RPC 压测：直连长连接 12.3 万 QPS，P50 403us；也验证了 work_threads 从 2 调到 16 后 P99 从 28ms 降到 2.9ms 的饱和拐点；
- 平台全链路：30s 视频 → 2 shard 并行转码 → 合并 30.02s 时长精确 → JOB_SUCCESS；10 任务并发全部成功，Worker 满载保护生效；
- 故障注入：worker 转码中被 kill → 自动重调度恢复；ffmpeg 失败 → 重试 3 次 → JOB_FAILED；Redis/MQ 故障 → 自动降级 → 恢复切回；
- 数据持久化：kill 全部服务重启，任务和 shard 状态不丢。

CI 里还跑了一条真实的集成链路：Docker 构建 → 生成测试视频 → 管道提交任务 → 轮询到 SUCCESS → 校验合并产物。

## 快速启动

完整步骤（清场、两种部署方式、常见问题、监控面板）见 [doc/快速启动指南.md](doc/快速启动指南.md)。这里给最简路径：

**方式一：手工部署**（需要本机 ZooKeeper + MySQL + ffmpeg）

```bash
# 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# 启动 5 个服务（配置文件在 video_platform/conf/）
./bin/job_service       -i video_platform/conf/job_service.conf &
./bin/scheduler_service -i video_platform/conf/scheduler.conf &
./bin/worker_manager    -i video_platform/conf/worker_manager.conf &
./bin/result_collector  -i video_platform/conf/result_collector.conf &
./bin/transcode_worker  -i video_platform/conf/transcode_worker_9004.conf &

# 提交任务（管道输入：user → input → output → format → resolution → bitrate → priority → shard_duration）
printf 'test_user\n/tmp/test_video.mp4\n/tmp/output\nmp4\n720p\n2000\n0\n15\n' \
    | ./bin/job_client -i video_platform/conf/job_client.conf

# 轮询结果
./bin/job_client -i video_platform/conf/job_client.conf --query <job_id> --watch
```

**方式二：Docker 一键启动**（只需要 Docker，9 个容器：4 中间件 + 5 服务）

```bash
docker compose up -d --build
docker compose exec job_service sh -c \
  'printf "%s\n" "test_user" "/data/videos/sample.mp4" "/data/output" "mp4" "720p" "0" "0" "0" \
   | ./bin/job_client -i /app/conf/job_client.conf'
```

视频放 `data/videos/`，合并产物在 `data/output/{job_id}_merged.mp4`。扩容 Worker 只需 `docker compose up -d --scale transcode_worker=3`。
