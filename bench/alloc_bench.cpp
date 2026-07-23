// ============================================================================
// 内存池 vs malloc 分配性能压测
// 模拟 Channel 对象 (152 bytes) 在高并发下的分配/释放模式
// ============================================================================

#include "wevix_muduo/memory_pool/MemoryPool.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <thread>
#include <cstdlib>
#include <atomic>
#include <algorithm>
#include <numeric>
#include <mutex>
#include <sys/resource.h>
#include <cstring>

using Clock = std::chrono::high_resolution_clock;
using namespace wevix_muduo::memory_pool;

// ============================================================================
// 测试对象：模拟 Channel (152 字节)
// ============================================================================
struct PoolChannel {
    char data[152];
    static void* operator new(size_t size)   { return MemoryPool::allocate(size); }
    static void operator delete(void* p, size_t s) noexcept { MemoryPool::deallocate(p, s); }
};
struct MallocChannel {
    char data[152];
};

long getRSS() {
    struct rusage u; getrusage(RUSAGE_SELF, &u); return u.ru_maxrss;
}

// ============================================================================
// 测试 1：单线程分配/释放吞吐量
// ============================================================================
void test1_singleThread() {
    constexpr int N = 50000;  // 5 万对象
    std::cout << "\n========== 测试 1：单线程分配/释放 (" << N << " 对象) ==========\n";

    {
        auto start = Clock::now();
        for (int i = 0; i < N; ++i) {
            auto* p = new PoolChannel();
            delete p;
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  Pool:   " << std::fixed << std::setprecision(2) << ms
                  << " ms  (" << std::fixed << std::setprecision(0) << (N/(ms/1000)) << " ops/s)\n";
    }

    {
        auto start = Clock::now();
        for (int i = 0; i < N; ++i) {
            auto* p = new MallocChannel();
            delete p;
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  Malloc: " << std::fixed << std::setprecision(2) << ms
                  << " ms  (" << std::fixed << std::setprecision(0) << (N/(ms/1000)) << " ops/s)\n";
    }
}

// ============================================================================
// 测试 2：单线程延迟分布
// ============================================================================
void test2_latency() {
    constexpr int N = 20000;
    std::cout << "\n========== 测试 2：延迟分布 (" << N << " 次, ns) ==========\n";

    auto runPool = []() {
        std::vector<double> lats; lats.reserve(N);
        for (int i = 0; i < N; ++i) {
            auto t1 = Clock::now();
            auto* p = new PoolChannel();
            auto t2 = Clock::now();
            lats.push_back(std::chrono::duration<double, std::nano>(t2 - t1).count());
            delete p;
        }
        std::sort(lats.begin(), lats.end());
        return std::make_tuple(
            std::accumulate(lats.begin(), lats.end(), 0.0) / N, lats[N/2],
            lats[N*99/100], lats[N*999/1000]);
    };
    auto runMalloc = []() {
        std::vector<double> lats; lats.reserve(N);
        for (int i = 0; i < N; ++i) {
            auto t1 = Clock::now();
            auto* p = new MallocChannel();
            auto t2 = Clock::now();
            lats.push_back(std::chrono::duration<double, std::nano>(t2 - t1).count());
            delete p;
        }
        std::sort(lats.begin(), lats.end());
        return std::make_tuple(
            std::accumulate(lats.begin(), lats.end(), 0.0) / N, lats[N/2],
            lats[N*99/100], lats[N*999/1000]);
    };

    auto [pAvg, p50, p99, p999] = runPool();
    auto [mAvg, m50, m99, m999] = runMalloc();

    std::cout << std::left
              << std::setw(10) << "方案"
              << std::setw(12) << "Avg"
              << std::setw(12) << "P50"
              << std::setw(12) << "P99"
              << std::setw(14) << "P99.9"
              << "\n" << std::string(60, '-') << "\n";

    auto print = [](const char* n, double a, double p, double p9, double p99_9) {
        std::cout << std::left << std::setw(10) << n
                  << std::setw(10) << std::fixed << std::setprecision(0) << a << "ns"
                  << std::setw(10) << std::fixed << std::setprecision(0) << p << "ns"
                  << std::setw(10) << std::fixed << std::setprecision(0) << p9 << "ns"
                  << std::setw(12) << std::fixed << std::setprecision(0) << p99_9 << "ns\n";
    };
    print("Pool", pAvg, p50, p99, p999);
    print("Malloc", mAvg, m50, m99, m999);
    std::cout << "  → P99 延迟: Pool " << (m99 > p99 ? "更优" : "较差") << "\n";
}

// ============================================================================
// 测试 3：多线程并发（模拟 4 个 EventLoop 线程）
// ============================================================================
void test3_multiThread() {
    constexpr int THREADS = 4;
    constexpr int OPS_PER_THREAD = 10000;
    std::cout << "\n========== 测试 3：多线程并发 ("
              << THREADS << " 线程 × " << OPS_PER_THREAD << " = "
              << (THREADS*OPS_PER_THREAD) << " 对象) ==========\n";

    auto runMT = [](auto dummy) -> double {
        using T = decltype(dummy);
        std::vector<std::thread> threads;
        std::atomic<long> done{0};

        auto start = Clock::now();
        for (int t = 0; t < THREADS; ++t) {
            threads.emplace_back([&]() {
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    auto* p = new T();
                    delete p;
                    done.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }
        for (auto& th : threads) th.join();
        auto end = Clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    };

    // 跑 3 轮取平均
    double pSum = 0, mSum = 0;
    for (int r = 0; r < 3; ++r) {
        pSum += runMT(PoolChannel{});
        mSum += runMT(MallocChannel{});
    }
    double pMs = pSum / 3, mMs = mSum / 3;
    int total = THREADS * OPS_PER_THREAD;

    std::cout << "  Pool:   " << std::fixed << std::setprecision(2) << pMs
              << " ms  (" << std::fixed << std::setprecision(0) << (total/(pMs/1000)) << " ops/s)\n";
    std::cout << "  Malloc: " << std::fixed << std::setprecision(2) << mMs
              << " ms  (" << std::fixed << std::setprecision(0) << (total/(mMs/1000)) << " ops/s)\n";
    std::cout << "  → 多线程下 Pool " << (mMs > pMs ? "快" : "慢") << " "
              << std::fixed << std::setprecision(2)
              << (mMs > pMs ? mMs/pMs : pMs/mMs) << " 倍\n";
}

// ============================================================================
// 测试 4：混合负载（模拟真实服务器：夹杂其他大小的 malloc）
// ============================================================================
void test4_mixed() {
    constexpr int CHANNEL_N = 10000;
    constexpr int NOISE_N  = 50000;
    std::cout << "\n========== 测试 4：混合负载 (碎片化后"
              << CHANNEL_N << " Channel + " << NOISE_N << " 干扰 malloc) ==========\n";

    auto test = [](auto dummy) {
        using T = decltype(dummy);

        // 先制造内存碎片
        std::vector<void*> noise;
        for (int i = 0; i < NOISE_N; ++i) {
            noise.push_back(malloc((rand() % 2048) + 16));
        }
        // 随机释放一半，制造碎片
        for (size_t i = 0; i < noise.size(); i += 2) {
            free(noise[i]);
        }

        auto start = Clock::now();
        for (int i = 0; i < CHANNEL_N; ++i) {
            auto* p = new T();
            delete p;
        }
        auto end = Clock::now();

        // 清理
        for (size_t i = 1; i < noise.size(); i += 2) free(noise[i]);

        return std::chrono::duration<double, std::milli>(end - start).count();
    };

    double pMs = test(PoolChannel{});
    double mMs = test(MallocChannel{});
    std::cout << "  Pool:   " << std::fixed << std::setprecision(2) << pMs << " ms\n";
    std::cout << "  Malloc: " << std::fixed << std::setprecision(2) << mMs << " ms\n";
    std::cout << "  → 碎片环境下 Pool " << (mMs > pMs ? "快" : "慢") << " "
              << std::fixed << std::setprecision(2)
              << (mMs > pMs ? mMs/pMs : pMs/mMs) << " 倍\n";
}

// ============================================================================
// 测试 5：稳态性能（模拟服务器长期运行后）
// ============================================================================
void test5_steadyState() {
    constexpr int ROUNDS = 10;
    constexpr int OPS    = 10000;
    std::cout << "\n========== 测试 5：稳态性能 ("
              << ROUNDS << " 轮 × " << OPS << " 次) ==========\n";
    std::cout << std::setw(6) << "轮" << std::setw(12) << "Pool(ms)"
              << std::setw(12) << "Malloc(ms)" << std::setw(10) << "比值\n"
              << std::string(40, '-') << "\n";

    std::vector<double> pTimes, mTimes;
    for (int r = 0; r < ROUNDS; ++r) {
        auto start = Clock::now();
        for (int i = 0; i < OPS; ++i) { auto* p = new PoolChannel(); delete p; }
        auto end = Clock::now();
        pTimes.push_back(std::chrono::duration<double, std::milli>(end - start).count());

        start = Clock::now();
        for (int i = 0; i < OPS; ++i) { auto* p = new MallocChannel(); delete p; }
        end = Clock::now();
        mTimes.push_back(std::chrono::duration<double, std::milli>(end - start).count());

        double ratio = mTimes.back() / pTimes.back();
        std::cout << std::setw(6) << (r+1)
                  << std::setw(10) << std::fixed << std::setprecision(3) << pTimes.back()
                  << std::setw(10) << std::fixed << std::setprecision(3) << mTimes.back()
                  << std::setw(8) << std::fixed << std::setprecision(2) << ratio << "x\n";
    }

    double pAvg = std::accumulate(pTimes.begin(), pTimes.end(), 0.0) / ROUNDS;
    double mAvg = std::accumulate(mTimes.begin(), mTimes.end(), 0.0) / ROUNDS;
    std::cout << "\n  均值: Pool=" << std::fixed << std::setprecision(3) << pAvg
              << "ms  Malloc=" << std::fixed << std::setprecision(3) << mAvg
              << "ms  → Pool 快 " << std::fixed << std::setprecision(2)
              << (mAvg/pAvg) << " 倍\n";
}

// ============================================================================
int main() {
    std::cout << "============================================================\n";
    std::cout << "  Channel (152 bytes) 内存池 vs malloc 压测\n";
    std::cout << "============================================================\n";

    test1_singleThread();
    test2_latency();
    test3_multiThread();
    test4_mixed();
    test5_steadyState();

    std::cout << "\n============================================================\n";
    std::cout << "  压测完成\n";
    std::cout << "============================================================\n";
    return 0;
}
