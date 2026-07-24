// ============================================================================
// ThreadPool 单元测试
// 测试 wevix_muduo::ThreadPool 的核心功能：基础任务提交、submitTask
// 返回值、并发安全、Fixed/Cached 模式、异常处理、stop/start 生命周期等
// ============================================================================

#include "wevix_muduo/ThreadPool.h"
#include <iostream>
#include <iomanip>
#include <atomic>
#include <thread>
#include <chrono>
#include <vector>
#include <cmath>

using namespace wevix_muduo;

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name)                                              \
    do {                                                        \
        std::cout << "  [" << std::setw(40) << std::left << name << "] "; \
    } while(0)

#define PASS() do { g_passed++; std::cout << "\033[1;32mPASS\033[0m\n"; } while(0)

#define CHECK(cond)                                             \
    do {                                                        \
        if (!(cond)) {                                          \
            g_failed++;                                         \
            std::cerr << "\033[1;31mFAIL\033[0m @" << __LINE__ \
                      << "  expected: " #cond "\n";             \
            return;                                             \
        }                                                       \
    } while(0)

// ============================================================================
// 测试 1：基础任务提交与执行
// ============================================================================
static void test_basic_task()
{
    TEST("基础任务提交与执行");

    ThreadPool pool(2, "TestPool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    std::atomic<int> counter{0};
    const int N = 100;

    for (int i = 0; i < N; ++i) {
        pool.addTask([&counter]() {
            counter.fetch_add(1, std::memory_order_relaxed);
        });
    }

    // 等待所有任务完成
    pool.stop();
    CHECK(counter.load() == N);

    PASS();
}

// ============================================================================
// 测试 2：submitTask 返回 future 获取结果
// ============================================================================
static void test_submit_future()
{
    TEST("submitTask 返回 future");

    ThreadPool pool(4, "FuturePool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    // 计算密集型任务：计算斐波那契
    auto f1 = pool.submitTask([](int n) -> long {
        long a = 0, b = 1;
        for (int i = 0; i < n; ++i) {
            long tmp = a + b;
            a = b;
            b = tmp;
        }
        return b;
    }, 40);

    // 返回值类型推断
    auto f2 = pool.submitTask([]() -> std::string {
        return "hello from thread pool";
    });

    auto f3 = pool.submitTask([](int a, int b) { return a * b; }, 7, 11);

    CHECK(f1.get() == 165580141);  // fib(40)
    CHECK(f2.get() == "hello from thread pool");
    CHECK(f3.get() == 77);

    pool.stop();
    PASS();
}

// ============================================================================
// 测试 3：多线程并发提交
// ============================================================================
static void test_concurrent_submit()
{
    TEST("多线程并发提交任务");

    ThreadPool pool(8, "ConcurrentPool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    std::atomic<long> sum{0};
    const int TASKS_PER_THREAD = 500;
    const int SUBMITTERS = 4;

    std::vector<std::thread> submitters;
    for (int s = 0; s < SUBMITTERS; ++s) {
        submitters.emplace_back([&pool, &sum, TASKS_PER_THREAD]() {
            for (int i = 0; i < TASKS_PER_THREAD; ++i) {
                pool.addTask([&sum, i]() {
                    sum.fetch_add(1, std::memory_order_relaxed);
                    // 模拟不均匀工作负载
                    volatile int dummy = 0;
                    for (int j = 0; j < 100; ++j) dummy += j;
                });
            }
        });
    }

    for (auto& th : submitters) th.join();
    pool.stop();

    int expected = SUBMITTERS * TASKS_PER_THREAD;
    CHECK(sum.load() == expected);

    PASS();
}

// ============================================================================
// 测试 4：submitTask 批量返回值汇总
// ============================================================================
static void test_batch_future()
{
    TEST("批量 submitTask 结果汇总");

    ThreadPool pool(4, "BatchPool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    const int N = 200;
    std::vector<std::future<int>> futures;

    for (int i = 0; i < N; ++i) {
        futures.push_back(pool.submitTask([](int val) {
            return val * val;
        }, i));
    }

    long total = 0;
    for (auto& f : futures) {
        total += f.get();
    }
    pool.stop();

    // 验证: sum of i^2 for i in [0, 199]
    long expected = 0;
    for (int i = 0; i < N; ++i) expected += i * i;
    CHECK(total == expected);

    PASS();
}

// ============================================================================
// 测试 5：Fixed vs Cached 模式
// ============================================================================
static void test_mode_fixed_vs_cached()
{
    TEST("Fixed vs Cached 模式行为");

    // --- Fixed 模式：线程数固定 ---
    {
        ThreadPool pool(4, "FixedPool");
        pool.setMode(PoolMode::MODE_FIXED);
        pool.start();

        // 提交大量任务，线程数应保持 4
        std::atomic<int> done{0};
        for (int i = 0; i < 100; ++i) {
            pool.addTask([&done]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                done.fetch_add(1);
            });
        }
        pool.stop();
        CHECK(done.load() == 100);
    }

    // --- Cached 模式：任务多时动态扩容 ---
    {
        ThreadPool pool(2, "CachedPool");
        pool.setMode(PoolMode::MODE_CACHED);
        pool.setThreadSizeThreshold(16);
        pool.start();

        std::atomic<int> done{0};

        // 提交大量阻塞型任务
        for (int i = 0; i < 100; ++i) {
            pool.addTask([&done]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                done.fetch_add(1);
            });
        }
        pool.stop();
        CHECK(done.load() == 100);
    }

    PASS();
}

// ============================================================================
// 测试 6：异常任务不应导致线程崩溃
// ============================================================================
static void test_exception_safety()
{
    TEST("异常任务安全性");

    ThreadPool pool(2, "ExceptionPool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    std::atomic<int> normalDone{0};

    // 提交一个会抛异常的任务
    pool.addTask([]() {
        throw std::runtime_error("task error");
    });

    // 后续正常任务应该仍能执行
    for (int i = 0; i < 50; ++i) {
        pool.addTask([&normalDone]() {
            normalDone.fetch_add(1);
        });
    }
    pool.stop();

    CHECK(normalDone.load() == 50);

    PASS();
}

// ============================================================================
// 测试 7：stop 后 stop 不影响 (重复停止安全)
// ============================================================================
static void test_double_stop()
{
    TEST("重复 stop 安全性");

    ThreadPool pool(2, "DblStop");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    std::atomic<int> done{0};
    pool.addTask([&done]() { done++; });
    pool.stop();
    pool.stop();  // 第二次 stop 不应崩溃
    CHECK(done.load() == 1);

    PASS();
}

// ============================================================================
// 测试 8：空线程池
// ============================================================================
static void test_empty_pool()
{
    TEST("空线程池 (0 线程)");

    ThreadPool pool(0, "EmptyPool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    // 提交任务到空线程池：应被拒绝或忽略
    std::atomic<int> done{0};
    pool.addTask([&done]() { done++; });

    // 给一点时间确保不崩溃
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    pool.stop();

    // 空线程池无法执行任务，done 应为 0
    CHECK(done.load() == 0);

    PASS();
}

// ============================================================================
// 测试 9：高性能计数器并发更新
// ============================================================================
static void test_high_concurrency_counter()
{
    TEST("高并发原子计数器");

    ThreadPool pool(8, "CounterPool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    const int TASKS = 10000;
    std::atomic<long> counter{0};

    for (int i = 0; i < TASKS; ++i) {
        pool.addTask([&counter]() {
            counter.fetch_add(1, std::memory_order_relaxed);
        });
    }
    pool.stop();

    CHECK(counter.load() == TASKS);

    PASS();
}

// ============================================================================
// 测试 10：线程池名称正确传递
// ============================================================================
static void test_pool_name()
{
    TEST("线程池名称");

    ThreadPool pool(1, "MyNamedPool");
    pool.setMode(PoolMode::MODE_FIXED);
    pool.start();

    std::atomic<int> done{0};
    pool.addTask([&done]() { done++; });
    pool.stop();

    CHECK(done.load() == 1);
    // 名称逻辑无法在测试中直接验证，但至少确保构造不崩溃

    PASS();
}

// ============================================================================
// main
// ============================================================================
int main()
{
    std::cout << "\n\033[1;36m========================================\033[0m\n";
    std::cout << "\033[1;36m  ThreadPool 单元测试\033[0m\n";
    std::cout << "\033[1;36m========================================\033[0m\n\n";

    test_basic_task();
    test_submit_future();
    test_concurrent_submit();
    test_batch_future();
    test_mode_fixed_vs_cached();
    test_exception_safety();
    test_double_stop();
    test_empty_pool();
    test_high_concurrency_counter();
    test_pool_name();

    std::cout << "\n\033[1;36m========================================\033[0m\n";
    std::cout << "  \033[1;32m通过: " << g_passed << "\033[0m  /  ";
    if (g_failed > 0)
        std::cout << "\033[1;31m失败: " << g_failed << "\033[0m\n";
    else
        std::cout << "\033[1;32m失败: 0\033[0m\n";
    std::cout << "\033[1;36m========================================\033[0m\n\n";

    return g_failed > 0 ? 1 : 0;
}
