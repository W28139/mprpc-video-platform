// ============================================================================
// Echo 服务端到端压测工具 (增强版)
//
// 功能：
//   - 命令行参数控制：--host, --port, --connections, --msgs, --msg-size, --duration
//   - 多消息大小压测（16B / 256B / 1KB / 4KB）
//   - 详细统计：QPS、P50/P90/P99/P99.9 延迟、失败率、连接成功率
//   - 长连接 / 短连接模式切换
//   - 阶梯式加压（逐步增加并发，观察性能退化）
//   - CSV 文件导出
//
// 用法示例：
//   ./bench_echo_stress --connections 500 --msgs 20 --msg-size 256
//   ./bench_echo_stress --connections 1000 --duration 30
//   ./bench_echo_stress --step-stress
//   ./bench_echo_stress --connections 200 --msgs 50 --csv result.csv
// ============================================================================

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <mutex>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include <cmath>
#include <fstream>
#include <csignal>

using Clock = std::chrono::high_resolution_clock;

// ============================================================================
// 配置参数
// ============================================================================
struct StressConfig {
    std::string host        = "127.0.0.1";
    int         port        = 8888;
    int         connections = 100;      // 并发连接数
    int         msgsPerConn = 10;       // 每连接消息数
    int         msgSize     = 256;      // 测试消息大小（字节）
    int         duration    = 0;        // 时长模式（秒），0=禁用
    bool        shortConn   = false;    // true=短连接模式（每消息新建连接）
    bool        stepStress  = false;    // true=阶梯加压模式
    std::string csvFile;                // CSV 输出路径
};

// ============================================================================
// 统计结果
// ============================================================================
struct StressResult {
    double totalTimeMs  = 0;
    long   successCount = 0;
    long   failCount    = 0;
    long   connFailCount = 0;
    double qps          = 0;
    double avgLatencyUs = 0;
    double p50Us        = 0;
    double p90Us        = 0;
    double p99Us        = 0;
    double p999Us       = 0;
    double minLatencyUs = 0;
    double maxLatencyUs = 0;
};

// ============================================================================
// 简易命令行参数解析
// ============================================================================
static void parseArgs(int argc, char* argv[], StressConfig& cfg) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto nextVal = [&]() -> std::string {
            if (i + 1 < argc && argv[i + 1][0] != '-') return argv[++i];
            return "";
        };

        if (arg == "--host" || arg == "-h")          cfg.host        = nextVal();
        else if (arg == "--port" || arg == "-p")      cfg.port        = std::stoi(nextVal());
        else if (arg == "--connections" || arg == "-c") cfg.connections = std::stoi(nextVal());
        else if (arg == "--msgs" || arg == "-m")      cfg.msgsPerConn = std::stoi(nextVal());
        else if (arg == "--msg-size" || arg == "-s")  cfg.msgSize     = std::stoi(nextVal());
        else if (arg == "--duration" || arg == "-d")  cfg.duration    = std::stoi(nextVal());
        else if (arg == "--short-conn")              cfg.shortConn   = true;
        else if (arg == "--step-stress")              cfg.stepStress  = true;
        else if (arg == "--csv")                      cfg.csvFile     = nextVal();
        else if (arg == "--help") {
            std::cout << R"(用法: bench_echo_stress [选项]

选项:
  -h, --host  HOST      目标主机 (默认: 127.0.0.1)
  -p, --port  PORT      目标端口 (默认: 8888)
  -c, --connections N   并发连接数 (默认: 100)
  -m, --msgs N          每连接消息数 (默认: 10)
  -s, --msg-size  N     消息大小/字节 (默认: 256)
  -d, --duration  N     压测时长/秒 (0=不限, 与 -m 配合使用)
  --short-conn          短连接模式：每次请求新建连接
  --step-stress          阶梯加压模式：逐步增加并发
  --csv  FILE           导出 CSV 结果文件
  --help                显示此帮助信息

示例:
  bench_echo_stress -c 500 -m 20 -s 1024
  bench_echo_stress -c 1000 -d 30 --csv result.csv
  bench_echo_stress --step-stress --msg-size 256
)";  exit(0);
        }
    }
}

// ============================================================================
// 单轮压测执行
// ============================================================================
static StressResult runStress(const StressConfig& cfg, int overrideConnections = 0) {
    int connCount = overrideConnections > 0 ? overrideConnections : cfg.connections;

    std::atomic<long> totalLatencyNs{0};
    std::atomic<long> successCount{0};
    std::atomic<long> failCount{0};
    std::atomic<long> connFailCount{0};
    std::mutex latMutex;
    std::vector<double> allLatencies;

    std::string msg(cfg.msgSize, 'X');  // 测试消息

    auto start = Clock::now();

    if (cfg.shortConn) {
        // ---- 短连接模式：每消息新建连接 ----
        int totalMsgs = connCount * cfg.msgsPerConn;
        std::vector<std::thread> threads;

        auto worker = [&](int begin, int end) {
            std::vector<double> localLats;
            for (int i = begin; i < end; ++i) {
                int sock = ::socket(AF_INET, SOCK_STREAM, 0);
                if (sock < 0) { failCount++; continue; }

                struct sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = ::htons(cfg.port);
                ::inet_pton(AF_INET, cfg.host.c_str(), &addr.sin_addr);

                if (::connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                    connFailCount++; ::close(sock); continue;
                }

                char buf[65536];
                auto t1 = Clock::now();
                ::send(sock, msg.data(), msg.size(), 0);
                ssize_t n = ::recv(sock, buf, sizeof(buf) - 1, 0);
                auto t2 = Clock::now();

                if (n > 0) {
                    long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                    totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                    successCount.fetch_add(1, std::memory_order_relaxed);
                    localLats.push_back(static_cast<double>(latNs));
                } else {
                    failCount.fetch_add(1);
                }
                ::close(sock);
            }

            std::lock_guard<std::mutex> lk(latMutex);
            allLatencies.insert(allLatencies.end(), localLats.begin(), localLats.end());
        };

        int numWorkers = std::min(connCount, 16);
        int perWorker = totalMsgs / numWorkers;
        int extra = totalMsgs % numWorkers;

        int idx = 0;
        for (int w = 0; w < numWorkers; ++w) {
            int count = perWorker + (w < extra ? 1 : 0);
            threads.emplace_back(worker, idx, idx + count);
            idx += count;
        }
        for (auto& th : threads) th.join();

    } else {
        // ---- 长连接模式：每连接发送多条消息 ----
        std::vector<std::thread> threads;

        for (int t = 0; t < connCount; ++t) {
            threads.emplace_back([&, cfg, msg, t]() {
                int sock = ::socket(AF_INET, SOCK_STREAM, 0);
                if (sock < 0) { failCount++; return; }

                struct sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = ::htons(cfg.port);
                ::inet_pton(AF_INET, cfg.host.c_str(), &addr.sin_addr);

                if (::connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                    connFailCount++; ::close(sock); return;
                }

                std::vector<double> localLats;
                char buf[65536];

                if (cfg.duration > 0) {
                    // 时长模式：在 duration 秒内持续发送
                    auto connStart = Clock::now();
                    while (true) {
                        auto elapsed = std::chrono::duration<double>(Clock::now() - connStart).count();
                        if (elapsed >= cfg.duration) break;

                        auto t1 = Clock::now();
                        ssize_t sent = ::send(sock, msg.data(), msg.size(), 0);
                        ssize_t n = ::recv(sock, buf, sizeof(buf) - 1, 0);
                        auto t2 = Clock::now();

                        if (sent <= 0 || n <= 0) { failCount.fetch_add(1); break; }

                        long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                        totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                        successCount.fetch_add(1, std::memory_order_relaxed);
                        localLats.push_back(static_cast<double>(latNs));
                    }
                } else {
                    // 固定消息数模式
                    for (int m = 0; m < cfg.msgsPerConn; ++m) {
                        auto t1 = Clock::now();
                        ssize_t sent = ::send(sock, msg.data(), msg.size(), 0);
                        ssize_t n = ::recv(sock, buf, sizeof(buf) - 1, 0);
                        auto t2 = Clock::now();

                        if (sent <= 0 || n <= 0) { failCount.fetch_add(1); break; }

                        long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                        totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                        successCount.fetch_add(1, std::memory_order_relaxed);
                        localLats.push_back(static_cast<double>(latNs));
                    }
                }

                ::close(sock);

                std::lock_guard<std::mutex> lk(latMutex);
                allLatencies.insert(allLatencies.end(), localLats.begin(), localLats.end());
            });
        }

        for (auto& th : threads) th.join();
    }

    auto end = Clock::now();

    // ---- 计算结果 ----
    StressResult res;
    res.totalTimeMs    = std::chrono::duration<double, std::milli>(end - start).count();
    res.successCount   = successCount.load();
    res.failCount      = failCount.load();
    res.connFailCount  = connFailCount.load();
    res.qps            = res.totalTimeMs > 0 ? (res.successCount / (res.totalTimeMs / 1000.0)) : 0;

    if (!allLatencies.empty()) {
        std::sort(allLatencies.begin(), allLatencies.end());
        double sum = std::accumulate(allLatencies.begin(), allLatencies.end(), 0.0);
        size_t n   = allLatencies.size();

        res.avgLatencyUs = (sum / n) / 1000.0;
        res.minLatencyUs = allLatencies.front() / 1000.0;
        res.maxLatencyUs = allLatencies.back() / 1000.0;
        res.p50Us        = allLatencies[n / 2] / 1000.0;
        res.p90Us        = allLatencies[n * 90 / 100] / 1000.0;
        res.p99Us        = allLatencies[n * 99 / 100] / 1000.0;
        res.p999Us       = allLatencies[n * 999 / 1000] / 1000.0;
    }

    return res;
}

// ============================================================================
// 打印结果表格
// ============================================================================
static void printResult(const StressResult& r, const std::string& label, int connCount) {
    std::cout << "\n  ┌─────────────────────────────────────────────────────────┐\n";
    std::cout << "  │ " << std::setw(55) << std::left << label << "│\n";
    std::cout << "  ├─────────────────────────────────────────────────────────┤\n";
    std::cout << "  │ 并发连接:  " << std::setw(44) << std::left << connCount      << "│\n";
    std::cout << "  │ 总耗时:    " << std::setw(40) << std::left
              << (std::to_string((int)r.totalTimeMs) + " ms")               << "│\n";
    std::cout << "  │ 成功请求:  " << std::setw(40) << std::left << r.successCount   << "│\n";
    std::cout << "  │ 失败请求:  " << std::setw(40) << std::left << r.failCount      << "│\n";
    std::cout << "  │ 连接失败:  " << std::setw(40) << std::left << r.connFailCount  << "│\n";
    std::cout << "  │ QPS:       " << std::setw(36) << std::left
              << std::fixed << std::setprecision(0) << r.qps << " msg/s"    << "│\n";
    std::cout << "  ├─────────────────────────────────────────────────────────┤\n";
    std::cout << "  │ 平均延迟:  " << std::setw(36) << std::left
              << std::fixed << std::setprecision(1) << r.avgLatencyUs << " us" << "│\n";
    std::cout << "  │ 最小延迟:  " << std::setw(36) << std::left
              << std::fixed << std::setprecision(1) << r.minLatencyUs << " us" << "│\n";
    std::cout << "  │ 最大延迟:  " << std::setw(36) << std::left
              << std::fixed << std::setprecision(1) << r.maxLatencyUs << " us" << "│\n";
    std::cout << "  │ P50:       " << std::setw(36) << std::left
              << std::fixed << std::setprecision(1) << r.p50Us  << " us"    << "│\n";
    std::cout << "  │ P90:       " << std::setw(36) << std::left
              << std::fixed << std::setprecision(1) << r.p90Us  << " us"    << "│\n";
    std::cout << "  │ P99:       " << std::setw(36) << std::left
              << std::fixed << std::setprecision(1) << r.p99Us  << " us"    << "│\n";
    std::cout << "  │ P99.9:     " << std::setw(36) << std::left
              << std::fixed << std::setprecision(1) << r.p999Us << " us"    << "│\n";
    std::cout << "  └─────────────────────────────────────────────────────────┘\n";
}

// ============================================================================
// CSV 导出
// ============================================================================
static void writeCSV(const std::string& path,
                     const std::vector<std::pair<int, StressResult>>& results) {
    std::ofstream f(path);
    f << "Connections,DurationMs,Success,Fail,ConnFail,QPS,"
      << "AvgUs,MinUs,MaxUs,P50Us,P90Us,P99Us,P999Us\n";
    for (const auto& [conn, r] : results) {
        f << conn << ","
          << std::fixed << std::setprecision(2) << r.totalTimeMs << ","
          << r.successCount << "," << r.failCount << "," << r.connFailCount << ","
          << std::fixed << std::setprecision(0) << r.qps << ","
          << r.avgLatencyUs << "," << r.minLatencyUs << "," << r.maxLatencyUs << ","
          << r.p50Us << "," << r.p90Us << "," << r.p99Us << "," << r.p999Us << "\n";
    }
    f.close();
    std::cout << "\n  CSV 已导出: " << path << "\n";
}

// ============================================================================
// 多消息大小对比压测
// ============================================================================
static void runMultiSize(const StressConfig& cfg) {
    std::vector<int> sizes = {16, 64, 256, 1024, 4096};

    std::cout << "\n\033[1;33m  ╔══════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1;33m  ║  多消息大小对比压测                                   ║\033[0m\n";
    std::cout << "\033[1;33m  ╚══════════════════════════════════════════════════════╝\033[0m\n";

    std::cout << "\n  " << std::setw(10) << std::left << "消息大小"
              << std::setw(14) << "QPS"
              << std::setw(14) << "Avg(us)"
              << std::setw(14) << "P50(us)"
              << std::setw(14) << "P99(us)"
              << std::setw(14) << "P99.9(us)\n";
    std::cout << "  " << std::string(80, '-') << "\n";

    for (int size : sizes) {
        StressConfig sc = cfg;
        sc.msgSize = size;

        auto r = runStress(sc);

        std::cout << "  " << std::setw(10) << std::left
                  << (std::to_string(size) + "B")
                  << std::setw(12) << std::fixed << std::setprecision(0) << r.qps
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.avgLatencyUs
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.p50Us
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.p99Us
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.p999Us
                  << "\n";
    }

    std::cout << "\n  \033[1;32m多尺寸压测完成\033[0m\n";
}

// ============================================================================
// 阶梯加压压测
// ============================================================================
static void runStepStress(const StressConfig& cfg) {
    std::vector<int> steps;

    // 动态选择阶梯：从小并发到大并发
    if (cfg.connections <= 100) {
        steps = {10, 50, 100};
    } else if (cfg.connections <= 500) {
        steps = {50, 100, 200, 500};
    } else if (cfg.connections <= 1000) {
        steps = {100, 200, 500, 1000};
    } else {
        steps = {100, 500, 1000, cfg.connections};
    }

    std::cout << "\n\033[1;33m  ╔══════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1;33m  ║  阶梯加压压测 (Step Stress Test)                      ║\033[0m\n";
    std::cout << "\033[1;33m  ╚══════════════════════════════════════════════════════╝\033[0m\n";

    std::cout << "\n  " << std::setw(10) << std::left << "并发数"
              << std::setw(14) << "QPS"
              << std::setw(14) << "Avg(us)"
              << std::setw(14) << "P50(us)"
              << std::setw(14) << "P99(us)"
              << std::setw(14) << "P99.9(us)"
              << std::setw(12) << "失败\n";
    std::cout << "  " << std::string(92, '-') << "\n";

    std::vector<std::pair<int, StressResult>> allResults;

    double prevQps = 0;
    for (int step : steps) {
        auto r = runStress(cfg, step);
        allResults.push_back({step, r});

        double qpsChange = prevQps > 0 ? ((r.qps - prevQps) / prevQps * 100.0) : 0;
        std::string qpsTrend;
        if (prevQps > 0) {
            qpsTrend = (qpsChange >= 0 ? "\033[32m+" : "\033[31m") + std::to_string((int)qpsChange) + "%\033[0m";
        }

        std::cout << "  " << std::setw(10) << std::left << step
                  << std::setw(12) << std::fixed << std::setprecision(0) << r.qps
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.avgLatencyUs
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.p50Us
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.p99Us
                  << std::setw(12) << std::fixed << std::setprecision(1) << r.p999Us
                  << std::setw(10) << std::left << r.failCount;
        if (!qpsTrend.empty()) std::cout << " " << qpsTrend;
        std::cout << "\n";

        prevQps = r.qps;

        // 如果失败率 > 10%，提前结束
        if (r.successCount + r.failCount > 0) {
            double failRate = (double)r.failCount / (r.successCount + r.failCount);
            if (failRate > 0.10) {
                std::cout << "\n  \033[1;33m⚠ 失败率超过 10%，停止加压\033[0m\n";
                break;
            }
        }
    }

    // CSV 导出
    if (!cfg.csvFile.empty()) {
        writeCSV(cfg.csvFile, allResults);
    }

    std::cout << "\n  \033[1;32m阶梯加压完成\033[0m\n";
}

// ============================================================================
// main
// ============================================================================
int main(int argc, char* argv[]) {
    // 忽略 SIGPIPE
    ::signal(SIGPIPE, SIG_IGN);

    StressConfig cfg;
    parseArgs(argc, argv, cfg);

    std::cout << "\n\033[1;36m╔══════════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1;36m║  Echo 端到端压测工具                                     ║\033[0m\n";
    std::cout << "\033[1;36m╚══════════════════════════════════════════════════════════╝\033[0m\n";
    std::cout << "\n  目标: " << cfg.host << ":" << cfg.port
              << "  并发: " << cfg.connections
              << "  消息/连接: " << cfg.msgsPerConn
              << "  消息大小: " << cfg.msgSize << "B"
              << (cfg.shortConn ? "  短连接" : "  长连接")
              << (cfg.duration > 0 ? "  时长: " + std::to_string(cfg.duration) + "s" : "")
              << "\n";

    // ---- 阶梯加压模式 ----
    if (cfg.stepStress) {
        runStepStress(cfg);
        return 0;
    }

    // ---- 多消息大小对比（默认长连接 + 无时长模式）----
    if (!cfg.shortConn && cfg.duration == 0 && cfg.msgSize == 256
        && cfg.connections <= 200 && argc <= 2) {
        // 默认参数时启用多尺寸对比
        runMultiSize(cfg);
    }

    // ---- 单轮压测 ----
    std::cout << "\n\033[1;33m  ── 主压测 ──\033[0m\n";
    auto result = runStress(cfg);
    printResult(result, "单轮压测结果: " + cfg.host + ":" + std::to_string(cfg.port), cfg.connections);

    // 端口被占用时的提示
    if (result.connFailCount > 0) {
        std::cout << "\n  \033[1;33m⚠ 提示: " << result.connFailCount
                  << " 个连接失败，请确保 echo_server 正在运行\033[0m\n";
        std::cout << "    启动命令: ./bin/echo_server\n\n";
    }

    // CSV 导出
    if (!cfg.csvFile.empty()) {
        std::vector<std::pair<int, StressResult>> data = {{cfg.connections, result}};
        writeCSV(cfg.csvFile, data);
    }

    std::cout << "\n  \033[1;32m压测完成\033[0m\n\n";
    return (result.failCount > 0 || result.connFailCount > 0) ? 1 : 0;
}
