// 简易并发客户端 —— 与 echo_server 配合做端到端压测
// 创建 N 个并发连接，每个连接发送 M 条消息，测量 QPS 和延迟

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

using Clock = std::chrono::high_resolution_clock;

int main(int argc, char* argv[]) {
    const char* host = "127.0.0.1";
    int port         = 9999;
    int connections  = 500;   // 并发连接数
    int msgsPerConn  = 10;    // 每连接消息数
    std::string msg  = "HelloWorld";  // 测试消息

    if (argc > 1) connections = std::stoi(argv[1]);
    if (argc > 2) msgsPerConn = std::stoi(argv[2]);

    std::cout << "=== EchoClient 压测 ===\n";
    std::cout << "Target: " << host << ":" << port << "\n";
    std::cout << "Connections: " << connections << ", msgs/conn: "
              << msgsPerConn << ", total: " << (connections * msgsPerConn) << " msgs\n\n";

    std::atomic<long> totalLatencyNs{0};
    std::atomic<long> successCount{0};
    std::atomic<long> failCount{0};
    std::vector<double> allLatencies;
    std::mutex latMutex;

    auto start = Clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < connections; ++t) {
        threads.emplace_back([&, t, host, port, msgsPerConn, msg]() {
            // 创建 socket
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) { failCount++; return; }

            struct sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = htons(port);
            inet_pton(AF_INET, host, &addr.sin_addr);

            if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                failCount++; close(sock); return;
            }

            std::vector<double> localLats;
            char buf[1024];

            for (int m = 0; m < msgsPerConn; ++m) {
                auto t1 = Clock::now();

                // 发送
                ssize_t sent = send(sock, msg.data(), msg.size(), 0);
                if (sent <= 0) { failCount++; break; }

                // 接收 echo
                ssize_t recvd = recv(sock, buf, sizeof(buf) - 1, 0);
                auto t2 = Clock::now();

                if (recvd <= 0) { failCount++; break; }

                buf[recvd] = '\0';
                long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                successCount.fetch_add(1, std::memory_order_relaxed);
                localLats.push_back(static_cast<double>(latNs));
            }

            close(sock);

            std::lock_guard<std::mutex> lk(latMutex);
            allLatencies.insert(allLatencies.end(), localLats.begin(), localLats.end());
        });
    }

    for (auto& th : threads) th.join();

    auto end = Clock::now();
    double totalMs = std::chrono::duration<double, std::milli>(end - start).count();

    long success = successCount.load();
    long failed  = failCount.load();

    std::cout << "总耗时: " << std::fixed << std::setprecision(2) << totalMs << " ms\n";
    std::cout << "成功: " << success << ", 失败: " << failed << "\n";
    std::cout << "QPS: " << std::fixed << std::setprecision(0)
              << (success / (totalMs / 1000.0)) << " msg/s\n";

    if (!allLatencies.empty()) {
        std::sort(allLatencies.begin(), allLatencies.end());
        double avg = std::accumulate(allLatencies.begin(), allLatencies.end(), 0.0) / allLatencies.size();
        std::cout << "平均延迟: " << std::fixed << std::setprecision(0) << avg / 1000.0 << " us\n";
        std::cout << "P50:  " << std::fixed << std::setprecision(0) << allLatencies[allLatencies.size()/2] / 1000.0 << " us\n";
        std::cout << "P99:  " << std::fixed << std::setprecision(0) << allLatencies[allLatencies.size()*99/100] / 1000.0 << " us\n";
    }

    return 0;
}
