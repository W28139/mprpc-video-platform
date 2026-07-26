// ============================================================================
// Buffer 单元测试
// 测试 wevix_muduo::Buffer 的核心功能：读写、扩容、碎片整理、
// readFd 散射读、prepend 前置写入、findCRLF 行解析、边界条件等
// ============================================================================

#include "wevix_muduo/Buffer.h"
#include <iostream>
#include <cstring>
#include <cassert>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <iomanip>

using namespace wevix_muduo;

static int g_passed = 0;
static int g_failed = 0;

#define TEST(name)                                              \
    do {                                                        \
        std::cout << "  [" << std::setw(35) << std::left << name << "] "; \
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
// 测试 1：构造与初始状态
// ============================================================================
static void test_construction()
{
    TEST("构造与初始状态");

    Buffer buf;
    CHECK(buf.readableBytes() == 0);
    CHECK(buf.writableBytes() == 1024);
    CHECK(buf.prependableBytes() == 8);
    CHECK(buf.peek() != nullptr);

    // 自定义初始大小
    Buffer buf2(4096);
    CHECK(buf2.readableBytes() == 0);
    CHECK(buf2.writableBytes() == 4096);
    CHECK(buf2.prependableBytes() == 8);

    PASS();
}

// ============================================================================
// 测试 2：基本写入与读取
// ============================================================================
static void test_basic_write_read()
{
    TEST("基本写入与读取");

    Buffer buf;
    const std::string msg = "Hello, Buffer!";

    // 写入
    buf.append(msg);
    CHECK(buf.readableBytes() == msg.size());
    CHECK(buf.writableBytes() == 1024 - msg.size());

    // peek 验证内容
    CHECK(std::memcmp(buf.peek(), msg.data(), msg.size()) == 0);

    // 消费部分
    buf.retrieve(5);
    CHECK(buf.readableBytes() == msg.size() - 5);
    CHECK(std::memcmp(buf.peek(), ", Buffer!", 9) == 0);

    // 全部消费
    buf.retrieveAll();
    CHECK(buf.readableBytes() == 0);
    CHECK(buf.prependableBytes() == 8);

    PASS();
}

// ============================================================================
// 测试 3：追加 string
// ============================================================================
static void test_append_string()
{
    TEST("append(string) 正确性");

    Buffer buf;
    std::string s1 = "part1";
    std::string s2 = "part2";

    buf.append(s1);
    buf.append(s2);

    CHECK(buf.readableBytes() == 10);
    std::string result = buf.retrieveAllAsString();
    CHECK(result == "part1part2");

    PASS();
}

// ============================================================================
// 测试 4：扩容 - 写入超过初始容量
// ============================================================================
static void test_expansion()
{
    TEST("扩容：写入超过初始容量");

    Buffer buf;
    std::string big(2048, 'X');  // 2KB，超过初始 1KB

    buf.append(big);
    CHECK(buf.readableBytes() == 2048);
    CHECK(buf.writableBytes() >= 0);

    // 验证数据完整性
    CHECK(std::memcmp(buf.peek(), big.data(), 2048) == 0);

    buf.retrieveAll();
    CHECK(buf.readableBytes() == 0);

    PASS();
}

// ============================================================================
// 测试 5：碎片整理 - 不触发扩容
// ============================================================================
static void test_defragmentation()
{
    TEST("碎片整理：write-consume-write 不扩容");

    Buffer buf(256);

    // 第一步：写入 200 字节
    std::string data1(200, 'A');
    buf.append(data1);

    // 消费掉 150 字节，留下 50 可读、前面有 150+8 空洞
    buf.retrieve(150);
    CHECK(buf.readableBytes() == 50);

    // 第二步：写入 200 字节，此时可写区不够，但碎片整理后总空间够
    std::string data2(200, 'B');
    buf.append(data2);

    // 验证：应该有 250 字节可读 (50 的旧 A + 200 的 B)
    CHECK(buf.readableBytes() == 250);
    std::string result = buf.retrieveAllAsString();

    // 前 50 应该是 A，后 200 应该是 B
    CHECK(result.substr(0, 50) == std::string(50, 'A'));
    CHECK(result.substr(50, 200) == std::string(200, 'B'));

    PASS();
}

// ============================================================================
// 测试 6：retrieveAsString
// ============================================================================
static void test_retrieve_as_string()
{
    TEST("retrieveAsString 正确性");

    Buffer buf;
    buf.append("ABCDEFGHIJ");

    std::string part = buf.retrieveAsString(4);
    CHECK(part == "ABCD");
    CHECK(buf.readableBytes() == 6);

    std::string rest = buf.retrieveAllAsString();
    CHECK(rest == "EFGHIJ");
    CHECK(buf.readableBytes() == 0);

    PASS();
}

// ============================================================================
// 测试 7：prepend 前置写入
// ============================================================================
static void test_prepend()
{
    TEST("prepend 前置写入");

    Buffer buf;
    const std::string body = "World";

    // 先写入 body
    buf.append(body);
    CHECK(buf.readableBytes() == 5);

    // 再在前面插入 "Hello "（6 字节）
    // prependable 初始为 8，6 字节足够
    buf.prepend("Hello ", 6);
    CHECK(buf.readableBytes() == 11);

    // 验证顺序："Hello World"
    std::string result = buf.retrieveAllAsString();
    CHECK(result == "Hello World");

    PASS();
}

// ============================================================================
// 测试 8：findCRLF 行解析
// ============================================================================
static void test_find_crlf()
{
    TEST("findCRLF 行解析");

    Buffer buf;

    // 没有 \r\n 时返回 nullptr
    buf.append("GET / HTTP");
    CHECK(buf.findCRLF() == nullptr);

    // 追加 \r\n 后能找到
    buf.append("\r\n");
    const char* crlf = buf.findCRLF();
    CHECK(crlf != nullptr);
    CHECK(crlf == buf.peek() + 10);  // "GET / HTTP" = 10 字节，\r\n 从第 10 个开始
    CHECK(crlf[0] == '\r');
    CHECK(crlf[1] == '\n');

    PASS();
}

// ============================================================================
// 测试 9：readFd 管道读写（模拟真实 IO）
// ============================================================================
static void test_read_fd()
{
    TEST("readFd 管道读写");

    int pipefd[2];
    CHECK(::pipe(pipefd) == 0);

    // 读端设为非阻塞（readFd 要求），写端保持阻塞以确保大数据能完整写入
    ::fcntl(pipefd[0], F_SETFL, O_NONBLOCK);

    Buffer buf;

    // 情况 A：数据量 < Buffer 可写区 (1024)，只用第 1 段 iovec，数据直接落入 Buffer
    std::string small_msg(500, 'S');
    CHECK(::write(pipefd[1], small_msg.data(), small_msg.size()) == 500);

    int savedErrno = 0;
    ssize_t n = buf.readFd(pipefd[0], &savedErrno);
    CHECK(n == 500);
    CHECK(buf.readableBytes() == 500);
    CHECK(std::memcmp(buf.peek(), small_msg.data(), 500) == 0);

    buf.retrieveAll();

    // 情况 B：写入 50000 字节 >> Buffer 可写区 (1024)，触发 readv 两段路径 + extrabuf
    // 写端阻塞模式保证完整写入（Linux 默认 pipe 缓冲 ≈ 64KB）
    std::string big_msg(50000, 'B');
    CHECK(::write(pipefd[1], big_msg.data(), big_msg.size()) == 50000);

    n = buf.readFd(pipefd[0], &savedErrno);
    CHECK(n == 50000);
    CHECK(buf.readableBytes() == 50000);
    CHECK(std::memcmp(buf.peek(), big_msg.data(), 50000) == 0);

    ::close(pipefd[0]);
    ::close(pipefd[1]);

    PASS();
}

// ============================================================================
// 测试 10：空 Buffer 操作
// ============================================================================
static void test_empty_buffer_ops()
{
    TEST("空 Buffer 操作");

    Buffer buf;

    // retrieve(0) 不应崩溃
    buf.retrieve(0);
    CHECK(buf.readableBytes() == 0);

    // retrieveAsString 空
    std::string empty = buf.retrieveAsString(0);
    CHECK(empty.empty());

    // retrieveAllAsString 空
    std::string all = buf.retrieveAllAsString();
    CHECK(all.empty());

    // peek 在空 Buffer 上
    CHECK(buf.peek() != nullptr);

    // findCRLF 空 Buffer
    CHECK(buf.findCRLF() == nullptr);

    PASS();
}

// ============================================================================
// 测试 11：连续写入-消费循环（模拟消息处理）
// ============================================================================
static void test_write_consume_cycle()
{
    TEST("连续写入-消费循环");

    Buffer buf(512);

    for (int round = 0; round < 100; ++round) {
        // 写入 100 字节
        std::string msg(100, 'a' + (round % 26));
        buf.append(msg);

        // 消费 50 字节
        std::string consumed = buf.retrieveAsString(50);
        CHECK(consumed.size() == 50);

        // 剩余 50，再写入 80
        std::string msg2(80, '0' + (round % 10));
        buf.append(msg2);
        CHECK(buf.readableBytes() == 130);  // 50 + 80

        // 全部消费
        buf.retrieveAll();
        CHECK(buf.readableBytes() == 0);
    }

    PASS();
}

// ============================================================================
// 测试 12：大数据吞吐（5MB）
// ============================================================================
static void test_large_data()
{
    TEST("大数据吞吐 5MB");

    Buffer buf;
    const size_t SIZE = 5 * 1024 * 1024;  // 5MB
    std::string big_data(SIZE, 'Z');

    buf.append(big_data);
    CHECK(buf.readableBytes() == SIZE);

    std::string result = buf.retrieveAllAsString();
    CHECK(result.size() == SIZE);
    CHECK(result == big_data);

    PASS();
}

// ============================================================================
// 测试 13：prepend 后 readFd（组合操作校验）
// ============================================================================
static void test_prepend_then_read()
{
    TEST("prepend 后 readFd 组合");

    int pipefd[2];
    CHECK(::pipe(pipefd) == 0);

    Buffer buf;
    buf.append("World");   // 5 可读
    buf.prepend("Hello ", 6);  // 11 可读

    // readFd 追加更多数据
    std::string extra = "! How are you?";
    CHECK(::write(pipefd[1], extra.data(), extra.size()) == (ssize_t)extra.size());

    int savedErrno = 0;
    buf.readFd(pipefd[0], &savedErrno);

    // 验证整体："Hello World! How are you?"
    std::string result = buf.retrieveAllAsString();
    CHECK(result == "Hello World! How are you?");

    ::close(pipefd[0]);
    ::close(pipefd[1]);

    PASS();
}

// ============================================================================
// main
// ============================================================================
int main()
{
    std::cout << "\n\033[1;36m========================================\033[0m\n";
    std::cout << "\033[1;36m  Buffer 单元测试\033[0m\n";
    std::cout << "\033[1;36m========================================\033[0m\n\n";

    test_construction();
    test_basic_write_read();
    test_append_string();
    test_expansion();
    test_defragmentation();
    test_retrieve_as_string();
    test_prepend();
    test_find_crlf();
    test_read_fd();
    test_empty_buffer_ops();
    test_write_consume_cycle();
    test_large_data();
    test_prepend_then_read();

    std::cout << "\n\033[1;36m========================================\033[0m\n";
    std::cout << "  \033[1;32m通过: " << g_passed << "\033[0m  /  ";
    if (g_failed > 0)
        std::cout << "\033[1;31m失败: " << g_failed << "\033[0m\n";
    else
        std::cout << "\033[1;32m失败: 0\033[0m\n";
    std::cout << "\033[1;36m========================================\033[0m\n\n";

    return g_failed > 0 ? 1 : 0;
}
