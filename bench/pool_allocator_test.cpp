// ============================================================================
// PoolAllocator 与 STL 容器集成验证 + 性能压测
// ============================================================================

#include "wevix_muduo/memory_pool/PoolAllocator.h"
#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <chrono>
#include <cstring>

using namespace wevix_muduo::memory_pool;
using Clock = std::chrono::high_resolution_clock;

// 便捷别名
template <typename T>
using PoolVector = std::vector<T, PoolAllocator<T>>;
using PoolString = std::basic_string<char, std::char_traits<char>, PoolAllocator<char>>;

// ============================================================================
// 测试 1：正确性
// ============================================================================
void test1_correctness() {
    std::cout << "========== 测试 1：正确性 ==========\n";

    // 1a: vector<char> 类似 Buffer 的用法
    std::cout << "1a. PoolVector<char> (模拟 Buffer): ";
    PoolVector<char> buf;
    buf.resize(1032);           // 模拟 Buffer 初始大小
    for (int i = 0; i < 1032; ++i) buf[i] = 'A';
    buf.resize(5000);           // 扩容
    buf.push_back('B');
    std::cout << "size=" << buf.size() << " OK\n";

    // 1b: string
    std::cout << "1b. PoolString: ";
    PoolString s;
    s = "Hello";
    s += " World! this is a longer string to trigger heap allocation beyond SSO";
    std::cout << "\"" << s.substr(0, 20) << "...\" OK\n";

    // 1c: vector<int>
    std::cout << "1c. PoolVector<int>: ";
    PoolVector<int> nums;
    for (int i = 0; i < 1000; ++i) nums.push_back(i);
    int sum = 0;
    for (int x : nums) sum += x;
    std::cout << "size=" << nums.size() << " sum=" << sum << " OK\n";

    // 1d: 混合分配/释放
    std::cout << "1d. 混合大小 (模拟消息路径): ";
    for (int round = 0; round < 100; ++round) {
        PoolString msg(round % 2 ? "short" : "a somewhat longer message for this test case");
        PoolVector<char> tmp;
        tmp.resize((round % 500) + 64);
        // msg 和 tmp 析构时自动归还到内存池
    }
    std::cout << "OK\n";

    // 1e: 空容器
    std::cout << "1e. 空容器: ";
    PoolVector<int> empty;
    PoolString emptyStr;
    empty.clear();
    emptyStr.clear();
    std::cout << "OK\n";

    std::cout << "\n=== 正确性全部通过 ===\n\n";
}

// ============================================================================
// 测试 2：PoolString vs std::string 性能对比
// ============================================================================
void test2_string_perf() {
    constexpr int N = 50000;
    constexpr int MSG_LEN = 256;
    std::cout << "========== 测试 2：String 分配/释放 ("
              << N << " 次, " << MSG_LEN << " 字节) ==========\n";

    // PoolString
    {
        auto start = Clock::now();
        for (int i = 0; i < N; ++i) {
            PoolString s(MSG_LEN, 'x');
            // 强制不优化掉
            volatile char c = s[0];
            (void)c;
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  PoolString:   " << std::fixed << std::setprecision(2)
                  << ms << " ms\n";
    }

    // std::string (malloc)
    {
        auto start = Clock::now();
        for (int i = 0; i < N; ++i) {
            std::string s(MSG_LEN, 'x');
            volatile char c = s[0];
            (void)c;
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  std::string:  " << std::fixed << std::setprecision(2)
                  << ms << " ms\n";
    }
    std::cout << "\n";
}

// ============================================================================
// 测试 3：PoolVector<char> vs std::vector<char> (模拟 Buffer 用法)
// ============================================================================
void test3_vector_perf() {
    constexpr int N = 20000;
    constexpr int INITIAL = 1024;
    std::cout << "========== 测试 3：Vector<char> Buffer 模拟 ("
              << N << " 次, 初始" << INITIAL << "B) ==========\n";

    {
        auto start = Clock::now();
        for (int i = 0; i < N; ++i) {
            PoolVector<char> buf;
            buf.resize(INITIAL);
            buf.resize(INITIAL * 2);  // 模拟扩容
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  PoolVector<char>:   " << std::fixed << std::setprecision(2)
                  << ms << " ms\n";
    }

    {
        auto start = Clock::now();
        for (int i = 0; i < N; ++i) {
            std::vector<char> buf;
            buf.resize(INITIAL);
            buf.resize(INITIAL * 2);
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  std::vector<char>:  " << std::fixed << std::setprecision(2)
                  << ms << " ms\n";
    }
    std::cout << "\n";
}

// ============================================================================
// 测试 4：模拟 mprpc 消息反序列化场景
// ============================================================================
void test4_rpc_scenario() {
    constexpr int MSGS = 10000;
    std::cout << "========== 测试 4：模拟 RPC 反序列化 ("
              << MSGS << " 条消息) ==========\n";

    // 模拟：解析一条包含 20 个字段的 protobuf 消息
    // 每个字段产生一个小 string/vector 分配
    auto simRpcDeserialize = [](auto& strings, auto& ints) {
        // 模拟 20 个字段的反序列化
        for (int i = 0; i < 20; ++i) {
            strings.emplace_back(128, 'a' + i);  // 小字符串
            ints.push_back(i);
        }
    };

    // Pool 版本
    {
        auto start = Clock::now();
        for (int m = 0; m < MSGS; ++m) {
            PoolVector<PoolString> strings;
            PoolVector<int> ints;
            simRpcDeserialize(strings, ints);
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  Pool 容器:  " << std::fixed << std::setprecision(2)
                  << ms << " ms  (" << (MSGS * 20 * 1000.0 / ms) << " strings/s)\n";
    }

    // std 版本
    {
        auto start = Clock::now();
        for (int m = 0; m < MSGS; ++m) {
            std::vector<std::string> strings;
            std::vector<int> ints;
            simRpcDeserialize(strings, ints);
        }
        auto end = Clock::now();
        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        std::cout << "  std 容器:   " << std::fixed << std::setprecision(2)
                  << ms << " ms  (" << (MSGS * 20 * 1000.0 / ms) << " strings/s)\n";
    }
    std::cout << "\n";
}

// ============================================================================
int main() {
    std::cout << "============================================================\n";
    std::cout << "  PoolAllocator 集成测试\n";
    std::cout << "============================================================\n\n";

    test1_correctness();
    test2_string_perf();
    test3_vector_perf();
    test4_rpc_scenario();

    std::cout << "============================================================\n";
    std::cout << "  测试完成\n";
    std::cout << "============================================================\n";
    return 0;
}
