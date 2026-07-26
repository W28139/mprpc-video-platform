// ============================================================================
// mprpc 框架端到端压测工具 (全功能版)
//
// 功能：
//   - 两种 RPC 通道模式：
//       Direct 模式 (--direct)：跳过 ZooKeeper，直连 host:port，测试纯 RPC 性能
//       ZK 模式 (默认)：使用标准 MprpcChannel，经 ZooKeeper 发现服务
//   - 长连接模式 (--keepalive)：复用 TCP 连接，减少握手开销，测 RPC 框架极限
//   - 命令行参数控制
//   - 详细统计：QPS、P50/P90/P99/P99.9 延迟、失败率
//   - 多消息大小对比
//   - 阶梯式加压
//   - CSV 文件导出
//
// Direct 模式用法：
//   ./bench_rpc_stress --direct --host 127.0.0.1 --port 8000 -c 100 -m 100
//   ./bench_rpc_stress --direct --keepalive -c 500 -d 30
//   ./bench_rpc_stress --direct --step-stress
//
// ZK 模式用法：
//   ./bench_rpc_stress -c 100 -m 100 -i rpc_test.conf
// ============================================================================

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <sys/time.h>
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
#include <cstdlib>

#include "rpcheader.pb.h"
#include "rpc_echo.pb.h"
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "mprpccodec.h"
#include "wevix_muduo/AsyncLogger.h"

using Clock = std::chrono::high_resolution_clock;

// ============================================================================
// 配置参数
// ============================================================================
struct StressConfig {
    std::string host        = "127.0.0.1";
    int         port        = 8000;
    int         connections = 100;      // 并发连接/线程数
    int         msgsPerConn = 10;       // 每连接/线程消息数
    int         msgSize     = 256;      // 测试消息大小（字节）
    int         duration    = 0;        // 时长模式（秒），0=禁用
    bool        directMode  = false;    // true=Direct 直连模式，false=ZK 模式
    bool        keepalive   = false;    // true=长连接复用（仅 Direct 模式有效）
    bool        stepStress  = false;    // true=阶梯加压模式
    std::string confFile;               // 配置文件路径（ZK 模式用）
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

        if (arg == "--host" || arg == "-h")            cfg.host        = nextVal();
        else if (arg == "--port" || arg == "-p")        cfg.port        = std::stoi(nextVal());
        else if (arg == "--connections" || arg == "-c")  cfg.connections = std::stoi(nextVal());
        else if (arg == "--msgs" || arg == "-m")        cfg.msgsPerConn = std::stoi(nextVal());
        else if (arg == "--msg-size" || arg == "-s")    cfg.msgSize     = std::stoi(nextVal());
        else if (arg == "--duration" || arg == "-d")    cfg.duration    = std::stoi(nextVal());
        else if (arg == "--direct")                    cfg.directMode  = true;
        else if (arg == "--keepalive")                 cfg.keepalive    = true;
        else if (arg == "--step-stress")                cfg.stepStress   = true;
        else if (arg == "--csv")                        cfg.csvFile      = nextVal();
        else if (arg == "-i")                           cfg.confFile     = nextVal();
        else if (arg == "--conf")                       cfg.confFile     = nextVal();
        else if (arg == "--help") {
            std::cout << R"(用法: bench_rpc_stress [选项]

=== 通用选项 ===
  -h, --host  HOST      目标主机 (默认: 127.0.0.1, Direct 模式用)
  -p, --port  PORT      目标端口 (默认: 8000, Direct 模式用)
  -c, --connections N   并发线程数 (默认: 100)
  -m, --msgs N          每线程消息数 (默认: 10)
  -s, --msg-size  N     消息 payload 大小/字节 (默认: 256)
  -d, --duration  N     压测时长/秒 (0=不限)
  --csv  FILE           导出 CSV 结果文件

=== RPC 通道模式 ===
  --direct              Direct 直连模式：跳过 ZooKeeper，直连 host:port
                        (默认使用 ZK 模式，需要 ZooKeeper 运行)
  --keepalive           长连接复用模式：同一线程的所有请求复用一条 TCP 连接
                        (仅 --direct 模式有效，大幅减少 connect 开销)

=== ZK 模式选项 ===
  -i, --conf  FILE      mprpc 配置文件 (默认: rpc_test.conf)

=== 压测模式 ===
  --step-stress          阶梯加压模式：逐步增加并发

=== 消息大小对比 ===
  默认参数 (--direct -c 100 -m 100) 时会自动运行多尺寸对比

示例:
  # Direct 直连 + 长连接，极限性能
  bench_rpc_stress --direct --keepalive -c 500 -m 1000 -s 256

  # Direct 直连 + 短连接（模拟真实短连接场景）
  bench_rpc_stress --direct -c 100 -m 100

  # Direct 直连 + 30秒持续压测
  bench_rpc_stress --direct --keepalive -c 200 -d 30

  # ZK 模式（需要 ZooKeeper 运行）
  bench_rpc_stress -c 10 -m 100 -i rpc_test.conf

  # 阶梯加压
  bench_rpc_stress --direct --keepalive --step-stress

  # 导出 CSV
  bench_rpc_stress --direct --keepalive -c 500 -m 1000 --csv result.csv
)";  exit(0);
        }
    }
}

// ============================================================================
// DirectRpcChannel: 跳过 ZooKeeper，直连 RPC 服务端
//
// 实现与 MprpcChannel::CallMethod 相同的 wire format：
//   请求帧：[total_len] + [header_size + RpcHeader + args]
//   响应帧：[total_len] + [response_header_size + RpcResponseHeader + response_data]
// ============================================================================
class DirectRpcChannel {
public:
    DirectRpcChannel(const std::string& ip, uint16_t port)
        : ip_(ip), port_(port), fd_(-1) {}

    ~DirectRpcChannel() { closeConnection(); }

    // 建立 TCP 连接（长连接模式调用一次）
    bool connect() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return false;
        // 压测端也设置超时，避免协议不匹配或服务端异常时线程一直卡在 recv。
        if (!setSocketTimeout(fd_)) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }

        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = ::htons(port_);
        ::inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr);

        if (::connect(fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            ::close(fd_);
            fd_ = -1;
            return false;
        }
        return true;
    }

    void closeConnection() {
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

    // 进行一次 RPC 调用
    // 短连接模式：内部完成 connect → send → recv → close
    // 长连接模式：要求先 connect()，多次 call() → 最后 closeConnection()
    bool call(const std::string& service_name,
              const std::string& method_name,
              const std::string& request_data,
              std::string& response_data,
              std::string& error_msg)
    {
        bool ownsFd = false;  // 短连接模式：本次调用负责关闭 fd
        int sock = fd_;

        if (sock < 0) {
            // 短连接模式：创建新连接
            sock = ::socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) { error_msg = "socket() failed"; return false; }
            // 短连接每次都会新建 fd，也必须重新设置超时。
            if (!setSocketTimeout(sock)) {
                error_msg = "setsockopt timeout failed";
                ::close(sock);
                return false;
            }

            struct sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port = ::htons(port_);
            ::inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr);

            if (::connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                error_msg = "connect() failed";
                ::close(sock);
                return false;
            }
            ownsFd = true;
        }

        // --- 1. 构建请求帧 ---
        // RpcHeader
        mprpc::RpcHeader rpcHeader;
        rpcHeader.set_service_name(service_name);
        rpcHeader.set_method_name(method_name);
        rpcHeader.set_args_size(static_cast<uint32_t>(request_data.size()));
        // Direct 模式绕过 MprpcChannel，但仍然要生成 request_id，保证协议覆盖一致。
        uint64_t requestId = nextRequestId_.fetch_add(1, std::memory_order_relaxed);
        rpcHeader.set_request_id(requestId);

        std::string header_str;
        if (!rpcHeader.SerializeToString(&header_str)) {
            error_msg = "serialize header failed";
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        uint32_t header_size = static_cast<uint32_t>(header_str.size());

        // 帧：BuildRpcFrame 统一写入 total_len + magic + version，避免直连工具绕过协议校验。
        std::string payload;
        payload.reserve(sizeof(uint32_t) + header_str.size() + request_data.size());
        mprpc::AppendNetworkUint32(&payload, header_size);
        payload += header_str;
        payload += request_data;

        if (payload.size() > mprpc::kRpcMaxFrameSize) {
            error_msg = "request frame too large";
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }
        std::string send_str = mprpc::BuildRpcFrame(payload);

        // --- 2. 发送请求 ---
        if (!sendAll(sock, send_str.data(), send_str.size())) {
            error_msg = "send failed";
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        // --- 3. 接收响应 ---
        // 读取 4 字节 total_len
        char responseLenBuf[sizeof(uint32_t)] = {0};
        if (!recvAll(sock, responseLenBuf, sizeof(responseLenBuf))) {
            error_msg = "recv response header failed";
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        uint32_t responseFrameSize = 0;
        if (!mprpc::ReadNetworkUint32(responseLenBuf, sizeof(responseLenBuf), &responseFrameSize) ||
            responseFrameSize < mprpc::kRpcFrameHeaderSize ||
            responseFrameSize > mprpc::kRpcMaxFrameSize) {
            error_msg = "invalid response frame size:" + std::to_string(responseFrameSize);
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        std::string responseFrameBody(responseFrameSize, '\0');
        if (!recvAll(sock, &responseFrameBody[0], responseFrameSize)) {
            error_msg = "recv response body failed";
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        // 先校验 magic/version，再把内部 payload 交给 RPC 响应头解析。
        std::string responsePayload;
        std::string frameError;
        if (!mprpc::DecodeRpcFramePayload(responseFrameBody, &responsePayload, &frameError)) {
            error_msg = frameError;
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        // 先解析框架响应头；只有 RPC_SUCCESS 才把 response_data 交给业务 protobuf 解析。
        mprpc::RpcResponseHeader responseHeader;
        if (!decodeResponsePayload(responsePayload, responseHeader, response_data, error_msg)) {
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        if (responseHeader.request_id() != requestId) {
            // 长连接压测会连续发多次请求，request_id 能暴露串包或读错响应的问题。
            error_msg = "response request_id mismatch";
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        if (responseHeader.error_code() != mprpc::RPC_SUCCESS) {
            error_msg = responseHeader.error_msg().empty()
                      ? "remote rpc failed, code=" + std::to_string(responseHeader.error_code())
                      : responseHeader.error_msg();
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        if (ownsFd) ::close(sock);
        return true;
    }

private:
    // Direct 压测不是正式客户端通道，但仍要有超时保护，避免坏连接拖住整轮压测。
    static bool setSocketTimeout(int fd) {
        struct timeval tv{};
        tv.tv_sec = 5;
        tv.tv_usec = 0;
        return ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0 &&
               ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0;
    }

    static bool sendAll(int fd, const char* data, size_t len) {
        size_t sent = 0;
        while (sent < len) {
            ssize_t n = ::send(fd, data + sent, len - sent, 0);
            if (n <= 0) return false;
            sent += static_cast<size_t>(n);
        }
        return true;
    }

    static bool recvAll(int fd, char* data, size_t len) {
        size_t recvd = 0;
        while (recvd < len) {
            ssize_t n = ::recv(fd, data + recvd, len - recvd, 0);
            if (n <= 0) return false;
            recvd += static_cast<size_t>(n);
        }
        return true;
    }

    static bool decodeResponsePayload(const std::string& payload,
                                      mprpc::RpcResponseHeader& responseHeader,
                                      std::string& responseData,
                                      std::string& errorMsg)
    {
        // 响应 payload 内部格式必须和 MprpcChannel 保持一致。
        if (payload.size() < sizeof(uint32_t)) {
            errorMsg = "response payload too small";
            return false;
        }

        uint32_t headerSize = 0;
        if (!mprpc::ReadNetworkUint32(payload.data(), payload.size(), &headerSize)) {
            errorMsg = "read response header size failed";
            return false;
        }

        if (headerSize == 0 || payload.size() - sizeof(uint32_t) < headerSize) {
            errorMsg = "invalid response header size:" + std::to_string(headerSize);
            return false;
        }

        std::string responseHeaderStr = payload.substr(sizeof(uint32_t), headerSize);
        if (!responseHeader.ParseFromString(responseHeaderStr)) {
            errorMsg = "parse response header failed";
            return false;
        }

        size_t bodyOffset = sizeof(uint32_t) + headerSize;
        size_t bodySize = payload.size() - bodyOffset;
        if (responseHeader.response_size() != bodySize) {
            // 这里失败说明服务端回包被截断或协议版本不一致。
            errorMsg = "response body size mismatch";
            return false;
        }

        responseData = payload.substr(bodyOffset, bodySize);
        return true;
    }

    std::string ip_;
    uint16_t port_;
    int fd_;
    inline static std::atomic<uint64_t> nextRequestId_{1};
};

// ============================================================================
// 单轮压测执行
// ============================================================================
static StressResult runStress(const StressConfig& cfg, int overrideConnections = 0) {
    int threadCount = overrideConnections > 0 ? overrideConnections : cfg.connections;

    std::atomic<long> totalLatencyNs{0};
    std::atomic<long> successCount{0};
    std::atomic<long> failCount{0};
    std::atomic<long> connFailCount{0};
    std::mutex latMutex;
    std::vector<double> allLatencies;

    std::string payload(cfg.msgSize, 'X');  // 测试 payload

    auto start = Clock::now();

    std::vector<std::thread> threads;

    if (cfg.directMode) {
        // ================================================================
        // Direct 模式：直连 RPC 服务端，跳过 ZooKeeper
        // ================================================================
        for (int t = 0; t < threadCount; ++t) {
            threads.emplace_back([&, cfg, t, payload]() {
                std::vector<double> localLats;
                DirectRpcChannel channel(cfg.host, cfg.port);

                // 长连接模式：提前建立连接
                if (cfg.keepalive) {
                    if (!channel.connect()) {
                        connFailCount.fetch_add(1, std::memory_order_relaxed);
                        return;
                    }
                }

                // 序列化 EchoRequest（只需做一次）
                rpc_test::EchoRequest req;
                req.set_payload(payload);
                std::string reqData;
                if (!req.SerializeToString(&reqData)) {
                    failCount.fetch_add(1, std::memory_order_relaxed);
                    return;
                }

                if (cfg.duration > 0) {
                    // ---- 时长模式 ----
                    auto connStart = Clock::now();
                    while (true) {
                        auto elapsed = std::chrono::duration<double>(
                            Clock::now() - connStart).count();
                        if (elapsed >= cfg.duration) break;

                        std::string respData;
                        std::string error;

                        auto t1 = Clock::now();
                        bool ok = channel.call("EchoService", "Echo", reqData, respData, error);
                        auto t2 = Clock::now();

                        if (ok) {
                            long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                            totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                            successCount.fetch_add(1, std::memory_order_relaxed);
                            localLats.push_back(static_cast<double>(latNs));
                        } else {
                            failCount.fetch_add(1, std::memory_order_relaxed);
                            // 长连接失败：连接已断开，重建连接后继续
                            if (cfg.keepalive) {
                                channel.closeConnection();
                                if (!channel.connect()) {
                                    connFailCount.fetch_add(1, std::memory_order_relaxed);
                                    break; // 重连失败则退出
                                }
                            }
                            // 短连接失败：继续下一个循环（每次新建连接）
                        }
                    }
                } else {
                    // ---- 固定消息数模式 ----
                    for (int m = 0; m < cfg.msgsPerConn; ++m) {
                        std::string respData;
                        std::string error;

                        auto t1 = Clock::now();
                        bool ok = channel.call("EchoService", "Echo", reqData, respData, error);
                        auto t2 = Clock::now();

                        if (ok) {
                            long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                            totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                            successCount.fetch_add(1, std::memory_order_relaxed);
                            localLats.push_back(static_cast<double>(latNs));
                        } else {
                            failCount.fetch_add(1, std::memory_order_relaxed);
                            // 长连接失败：尝试重建连接
                            if (cfg.keepalive) {
                                channel.closeConnection();
                                if (!channel.connect()) {
                                    connFailCount.fetch_add(1, std::memory_order_relaxed);
                                    break; // 重连失败则退出
                                }
                            }
                            // 短连接失败：继续下一个循环（每次新建连接）
                        }
                    }
                }

                if (cfg.keepalive) channel.closeConnection();

                std::lock_guard<std::mutex> lk(latMutex);
                allLatencies.insert(allLatencies.end(), localLats.begin(), localLats.end());
            });
        }

    } else {
        // ================================================================
        // ZK 模式：使用标准 MprpcChannel，经 ZooKeeper 发现服务
        //
        // 注意：MprpcChannel::CallMethod 每次调用都会：
        //       1. 连接 ZK 查询服务地址
        //       2. 创建新 TCP 连接
        //       3. 发送请求并等待响应
        //       4. 关闭连接
        //       因此 ZK 模式 QPS 受 ZooKeeper 性能限制
        // ================================================================
        for (int t = 0; t < threadCount; ++t) {
            threads.emplace_back([&, cfg, t, payload]() {
                std::vector<double> localLats;

                // 每个线程创建独立的 stub（MprpcChannel 每次 call 新建连接，线程安全）
                rpc_test::EchoService_Stub stub(new MprpcChannel());

                rpc_test::EchoRequest req;
                req.set_payload(payload);

                if (cfg.duration > 0) {
                    // ---- 时长模式 ----
                    auto connStart = Clock::now();
                    while (true) {
                        auto elapsed = std::chrono::duration<double>(
                            Clock::now() - connStart).count();
                        if (elapsed >= cfg.duration) break;

                        rpc_test::EchoResponse rsp;
                        MprpcController controller;

                        auto t1 = Clock::now();
                        stub.Echo(&controller, &req, &rsp, nullptr);
                        auto t2 = Clock::now();

                        if (!controller.Failed()) {
                            long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                            totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                            successCount.fetch_add(1, std::memory_order_relaxed);
                            localLats.push_back(static_cast<double>(latNs));
                        } else {
                            failCount.fetch_add(1, std::memory_order_relaxed);
                            break;  // ZK 查询失败，停止该线程
                        }
                    }
                } else {
                    // ---- 固定消息数模式 ----
                    for (int m = 0; m < cfg.msgsPerConn; ++m) {
                        rpc_test::EchoResponse rsp;
                        MprpcController controller;

                        auto t1 = Clock::now();
                        stub.Echo(&controller, &req, &rsp, nullptr);
                        auto t2 = Clock::now();

                        if (!controller.Failed()) {
                            long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                            totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                            successCount.fetch_add(1, std::memory_order_relaxed);
                            localLats.push_back(static_cast<double>(latNs));
                        } else {
                            failCount.fetch_add(1, std::memory_order_relaxed);
                            break;
                        }
                    }
                }

                std::lock_guard<std::mutex> lk(latMutex);
                allLatencies.insert(allLatencies.end(), localLats.begin(), localLats.end());
            });
        }
    }

    for (auto& th : threads) th.join();

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
static void printResult(const StressResult& r, const std::string& label, int threadCount) {
    std::cout << "\n  ┌─────────────────────────────────────────────────────────┐\n";
    std::cout << "  │ " << std::setw(55) << std::left << label << "│\n";
    std::cout << "  ├─────────────────────────────────────────────────────────┤\n";
    std::cout << "  │ 并发线程:  " << std::setw(44) << std::left << threadCount      << "│\n";
    std::cout << "  │ 总耗时:    " << std::setw(40) << std::left
              << (std::to_string((int)r.totalTimeMs) + " ms")               << "│\n";
    std::cout << "  │ 成功请求:  " << std::setw(40) << std::left << r.successCount   << "│\n";
    std::cout << "  │ 失败请求:  " << std::setw(40) << std::left << r.failCount      << "│\n";
    std::cout << "  │ 连接失败:  " << std::setw(40) << std::left << r.connFailCount  << "│\n";
    std::cout << "  │ QPS:       " << std::setw(36) << std::left
              << std::fixed << std::setprecision(0) << r.qps << " calls/s"  << "│\n";
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
    f << "Threads,DurationMs,Success,Fail,ConnFail,QPS,"
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
    std::cout << "\033[1;33m  ║  多消息大小对比压测 (RPC Echo)                        ║\033[0m\n";
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
    std::cout << "\033[1;33m  ║  阶梯加压压测 (Step Stress Test) — RPC Echo          ║\033[0m\n";
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
            qpsTrend = (qpsChange >= 0 ? "\033[32m+" : "\033[31m")
                     + std::to_string((int)qpsChange) + "%\033[0m";
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

        if (r.successCount + r.failCount > 0) {
            double failRate = (double)r.failCount / (r.successCount + r.failCount);
            if (failRate > 0.10) {
                std::cout << "\n  \033[1;33m⚠ 失败率超过 10%，停止加压\033[0m\n";
                break;
            }
        }
    }

    if (!cfg.csvFile.empty()) {
        writeCSV(cfg.csvFile, allResults);
    }

    std::cout << "\n  \033[1;32m阶梯加压完成\033[0m\n";
}

// ============================================================================
// 初始化 mprpc 框架（仅在 ZK 模式需要）
// ============================================================================
static bool initMprpcFramework(const StressConfig& cfg) {
    // MprpcApplication::Init 使用 getopt 解析 "-i configfile"
    // 构造 minimal argv 给它
    std::string conf = cfg.confFile.empty() ? "rpc_test.conf" : cfg.confFile;

    char arg0[] = "bench_rpc_stress";
    char arg1[] = "-i";
    char* argv[] = {arg0, arg1, const_cast<char*>(conf.c_str()), nullptr};
    int argc = 3;

    return MprpcApplication::Init(argc, argv);
}

// ============================================================================
// main
// ============================================================================
int main(int argc, char* argv[]) {
    ::signal(SIGPIPE, SIG_IGN);

    StressConfig cfg;
    parseArgs(argc, argv, cfg);

    std::string modeStr;
    if (cfg.directMode && cfg.keepalive)
        modeStr = "Direct + 长连接";
    else if (cfg.directMode)
        modeStr = "Direct + 短连接";
    else
        modeStr = "ZK 模式（经 ZooKeeper）";

    std::cout << "\n\033[1;36m╔══════════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1;36m║  mprpc RPC Echo 端到端压测工具                           ║\033[0m\n";
    std::cout << "\033[1;36m╚══════════════════════════════════════════════════════════╝\033[0m\n";
    std::cout << "\n  模式: " << modeStr
              << "  目标: " << cfg.host << ":" << cfg.port
              << "  线程: " << cfg.connections
              << "  消息/线程: " << cfg.msgsPerConn
              << "  消息大小: " << cfg.msgSize << "B"
              << (cfg.duration > 0 ? "  时长: " + std::to_string(cfg.duration) + "s" : "")
              << "\n";

    // ZK 模式需要初始化 mprpc 框架
    if (!cfg.directMode) {
        if (!initMprpcFramework(cfg)) {
            std::cerr << "mprpc init failed\n";
            return EXIT_FAILURE;
        }
    }

    // ---- 阶梯加压模式 ----
    if (cfg.stepStress) {
        runStepStress(cfg);
        return 0;
    }

    // ---- 多消息大小对比 ----
    // 默认参数（--direct，少参数）时启用多尺寸对比
    if (cfg.directMode && cfg.duration == 0 && argc <= 4) {
        runMultiSize(cfg);
    }

    // ---- 单轮压测 ----
    std::cout << "\n\033[1;33m  ── 主压测 ──\033[0m\n";
    auto result = runStress(cfg);
    printResult(result, modeStr + ": " + cfg.host + ":" + std::to_string(cfg.port), cfg.connections);

    // CSV 导出
    if (!cfg.csvFile.empty()) {
        std::vector<std::pair<int, StressResult>> data = {{cfg.connections, result}};
        writeCSV(cfg.csvFile, data);
    }

    std::cout << "\n  \033[1;32m压测完成\033[0m\n\n";
    return (result.failCount > 0 || result.connFailCount > 0) ? 1 : 0;
}
