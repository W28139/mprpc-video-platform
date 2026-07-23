# 项目变更日志 (CHANGELOG)

## [V0.2.0] - 2026-07-22 —— 集成内存池底层框架

### 概述

将独立的 `memory_pool_2` 项目（基于 TCMalloc 思想的三层并发内存分配器）整合到 `wevix_muduo` 网络库框架中，作为底层内存管理组件。

---

### 重大变更

#### 1. 项目命名统一

| 变更项 | 旧值 | 新值 |
|--------|------|------|
| CMake 项目名 | `my_muduo` | `wevix_muduo` |
| 库文件名 | `libmy_muduo.a` | `libwevix_muduo.a` |
| 命名空间 | `mymuduo` | `wevix_muduo` |
| 内存池命名空间 | `wevix_memoryPool` | `wevix_muduo::memory_pool` |

#### 2. C++ 标准升级

- 从 C++11 升级到 **C++17**
- 原因：`memory_pool` 使用 C++17 特性（`std::deque` 地址稳定性保证等）
- 影响文件：`CMakeLists.txt`

#### 3. 引入内存池子系统

从 `memory_pool_2` 迁移而来，放置于以下目录：

```
include/my_muduo/memory_pool/     ← 5 个头文件
  ├── Common.h                    ← 全局常量(ALIGNMENT=8B, MAX_BYTES=256KB)、SizeClass 映射
  ├── MemoryPool.h                ← 对外统一 API (allocate / deallocate)
  ├── ThreadCache.h               ← 第一层：线程本地 TLS 缓存，无锁
  ├── CentralCache.h              ← 第二层：全局中心调度器，桶级自旋锁
  └── PageCache.h                 ← 第三层：页缓存，与 OS 交互 (mmap/munmap)

src/memory_pool/                  ← 3 个源文件
  ├── ThreadCache.cpp
  ├── CentralCache.cpp
  └── PageCache.cpp
```

**核心特性：**
- 三层架构：ThreadCache (TLS, 无锁) → CentralCache (自旋锁) → PageCache (互斥锁)
- 管理范围：8B ~ 256KB
- 对齐：8 字节
- 32768 个大小类别槽位
- PageCache 水位线：128MB (可调 `MAX_CACHED_PAGES` 宏)
- 延迟归还机制：48 次操作 / 1 秒触发一次 Span 全空检查

#### 4. 新增 Noncopyable 基础工具类

新建 `include/my_muduo/Noncopyable.h`，采用 `protected` 构造 + `= delete` 拷贝的方式。

已应用到的类：
- `Epoll`, `EventLoop`, `TcpServer`, `ThreadPool`
- `Acceptor`, `Connection`, `Socket`
- `Channel`, `Buffer`

#### 5. 高频对象接入内存池

`Connection` 和 `Channel` 新增自定义 `operator new` / `operator delete`：

```cpp
// Connection / Channel 类内
static void* operator new(size_t size) {
    return memory_pool::MemoryPool::allocate(size);
}
static void operator delete(void* ptr, size_t size) {
    memory_pool::MemoryPool::deallocate(ptr, size);
}
```

**注意**：通过 `std::make_shared<Connection>()` 创建的对象由 `std::allocator` 管理，不会触发自定义 `operator new`。推荐在需要内存池分配的场景下直接使用 `new Connection(...)`。

---

### 文件变更清单

#### 新建 (10 个文件)

| 文件 | 说明 |
|------|------|
| `include/my_muduo/Noncopyable.h` | Noncopyable 基类 |
| `include/my_muduo/memory_pool/Common.h` | 内存池全局常量与 SizeClass |
| `include/my_muduo/memory_pool/MemoryPool.h` | 内存池公共 API |
| `include/my_muduo/memory_pool/ThreadCache.h` | 线程本地缓存 |
| `include/my_muduo/memory_pool/CentralCache.h` | 中心缓存层 |
| `include/my_muduo/memory_pool/PageCache.h` | 页缓存层 |
| `src/memory_pool/ThreadCache.cpp` | 线程本地缓存实现 |
| `src/memory_pool/CentralCache.cpp` | 中心缓存层实现 |
| `src/memory_pool/PageCache.cpp` | 页缓存层实现 |
| `doc/CHANGELOG.md` | 本文件 |

#### 修改 (20 个文件)

| 文件 | 修改内容 |
|------|---------|
| `CMakeLists.txt` | 项目名 → `wevix_muduo`；C++17；递归 glob 源文件 |
| `include/my_muduo/Epoll.h` | 命名空间 + 继承 Noncopyable |
| `include/my_muduo/EventLoop.h` | 命名空间 + 继承 Noncopyable |
| `include/my_muduo/TcpServer.h` | 命名空间 + 继承 Noncopyable |
| `include/my_muduo/ThreadPool.h` | 命名空间 + 继承 Noncopyable |
| `include/my_muduo/Connection.h` | 命名空间 + 继承 Noncopyable + operator new/delete |
| `include/my_muduo/Channel.h` | 命名空间 + 继承 Noncopyable + operator new/delete |
| `include/my_muduo/Acceptor.h` | 命名空间 + 继承 Noncopyable |
| `include/my_muduo/Socket.h` | 命名空间 + 继承 Noncopyable |
| `include/my_muduo/Buffer.h` | 命名空间 + 继承 Noncopyable |
| `include/my_muduo/InetAddress.h` | 命名空间 |
| `include/my_muduo/Timestamp.h` | 命名空间 |
| `src/EventLoop.cpp` | 命名空间 |
| `src/Connection.cpp` | 命名空间 |
| `src/Channel.cpp` | 命名空间 |
| `src/Acceptor.cpp` | 命名空间 |
| `src/Buffer.cpp` | 命名空间 |
| `src/Epoll.cpp` | 命名空间 |
| `src/Socket.cpp` | 命名空间 |
| `src/ThreadPool.cpp` | 命名空间 |
| `src/TcpServer.cpp` | 命名空间 |
| `src/Timestamp.cpp` | 命名空间 |
| `src/InetAddress.cpp` | 命名空间 |

---

### 构建验证

```bash
cd my_muduo/build
cmake ..
make -j$(nproc)
# [100%] Built target wevix_muduo
# 输出：lib/libwevix_muduo.a
```

**编译器**：GCC 13.3.0
**编译结果**：全部 14 个源文件编译通过，静态库链接成功。

---

### 已知限制 & 后续工作

1. **`std::make_shared<Connection>` 不走内存池**：`make_shared` 内部使用 `std::allocator`，不会触发自定义 `operator new`。如需在 `make_shared` 中使用内存池，需传入自定义 `Allocator`。

2. **Buffer 的 `std::vector<char>` 内部仍使用 `std::allocator`**：当前未替换，后续可通过自定义 `Allocator` 模板参数接入。

3. **内存池线程安全**：已确认 `wevix_muduo::memory_pool` 的线程安全设计与 `wevix_muduo` 的 One-Loop-Per-Thread 架构兼容：
   - ThreadCache (`thread_local`) → 每个 EventLoop 线程独立一份
   - CentralCache (自旋锁) → 多线程安全竞争
   - PageCache (mutex) → 全局唯一，低频调用

4. **PageCache 的 Span 控制块使用 `new Span`**：这在 `mmap` 区域外使用堆分配，避免了循环依赖（如果 `::operator new` 被全局替换为 `MemoryPool::allocate` 时）。

5. **文档迁移**：`memory_pool_2/doc/` 中的优化文档（12 个优化记录 + QA）尚未迁移，计划放入 `doc/memory_pool/`。

---

---

## [V0.2.1] - 2026-07-22 —— Bug 修复

代码审查发现 4 个需要修复的 bug，详见 [bug_fixes.md](bug_fixes.md)。

| # | 严重度 | 描述 | 文件 |
|---|--------|------|------|
| 1 | 高 | `Connection::send` 捕获原始 `this`，改为 `shared_from_this()` | `src/Connection.cpp:156` |
| 2 | 高 | `delete nullptr` 在 ThreadCache 中崩溃，增加空指针检查 | `src/memory_pool/ThreadCache.cpp:33` |
| 3 | 中 | `operator new` 失败时返回 nullptr（应抛 `std::bad_alloc`） | `Connection.h`, `Channel.h` |
| 4 | 低 | `Noncopyable` 未 `= delete` 移动构造/赋值 | `Noncopyable.h` |

### 新增文档

- [源码阅读指南](read_guide.md) — 从底层到顶层，逐层讲解，附带 3 小时精读时间表
- [Buffer 设计详解](buffer的设计.md) — 三区模型、readv 散射读、接口详解、上层调用示例
- [事件循环层](事件循环层.md) — Channel、Epoll、EventLoop 的调用关系与每个函数的作用
- [TCP 生命周期层](TCP生命周期层.md) — Connection 的读/写/关闭路径详解
- [服务器层](服务器层.md) — Acceptor 与 TcpServer 的组装逻辑与完整请求旅程

### 下一步计划

- [ ] 迁移 `memory_pool_2/tests/` 单元测试
- [ ] 添加多线程环境下的集成压力测试
- [ ] 编写 memory_pool 使用文档
- [ ] 为 `Buffer` 接入内存池自定义 Allocator
- [ ] 性能基准对比 (MemoryPool vs 原生 new/delete)
- [ ] 为 pageCache 增加零回收配置开关
- [ ] 长连接计时内存池稳定性测试
