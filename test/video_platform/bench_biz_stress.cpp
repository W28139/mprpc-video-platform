// ============================================================================
// video_platform 业务 RPC 压测工具
//
// 基于 test/mprpc/bench_rpc_stress.cpp 的 DirectRpcChannel + 压测框架裁剪：
//   - 只保留 Direct 直连模式（--direct --keepalive），跳过 ZooKeeper
//   - 删除：ZK 模式 / 阶梯加压 / 多消息大小对比 / msgSize
//   - 新增：--method 选择业务 RPC，支持
//       query_job     JobService.QueryJob        (默认端口 9001)
//       heartbeat     WorkerManagerService.Heartbeat (默认端口 9003)
//       list_workers  WorkerManagerService.ListWorkers (默认端口 9003)
//
// 统计口径（与 bench_rpc_stress 的区别）：
//   success = RPC 层成功（帧收发 + request_id 校验 + error_code==RPC_SUCCESS）
//   biz_fail = RPC 层成功但业务 error_code != 0 —— 单独计数，不混入 RPC 失败。
//   query_job 默认查询不存在的 job（error_code=1）属预期行为，业务失败率
//   接近 100% 是正常的，压测看的是 QPS / 延迟（服务端吞吐），不是业务失败率。
//
// 副作用控制：heartbeat 用 bench_w_<tid> 前缀 worker_id（不存在 → UPDATE 0 行），
// 不会刷新真实 Worker 的心跳；query_job / list_workers 为纯读。
//
// 用法：
//   ./bench_biz_stress --direct --keepalive --method query_job -c 50 -m 200
//   ./bench_biz_stress --direct --keepalive --method heartbeat -p 9003 -c 100 -m 500
//   ./bench_biz_stress --direct --keepalive --method list_workers -d 30 --csv out.csv
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
#include <fstream>
#include <csignal>
#include <cstdlib>

#include "rpcheader.pb.h"
#include "mprpccodec.h"
#include "job.pb.h"
#include "worker.pb.h"

using Clock = std::chrono::high_resolution_clock;

// ============================================================================
// 配置参数
// ============================================================================
struct StressConfig {
    std::string host        = "127.0.0.1";
    int         port        = 0;          // 0 = 按 --method 选默认端口
    int         connections = 100;        // 并发连接/线程数
    int         msgsPerConn = 10;         // 每连接消息数
    int         duration    = 0;          // 时长模式（秒），0=禁用
    bool        keepalive   = false;      // true=长连接复用
    std::string method      = "query_job"; // 业务 RPC：query_job / heartbeat / list_workers
    std::string csvFile;                  // CSV 输出路径
};

// ============================================================================
// 统计结果
// ============================================================================
struct StressResult {
    double totalTimeMs  = 0;
    long   successCount = 0;     // RPC 层成功
    long   bizFailCount = 0;     // RPC 层成功但业务 error_code != 0
    long   failCount    = 0;     // RPC 层失败
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

// 每个 method 的默认目标端口
static int defaultPort(const std::string& method)
{
    if (method == "query_job") return 9001;   // JobService
    return 9003;                              // WorkerManagerService
}

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

        if (arg == "--host" || arg == "-h")           cfg.host        = nextVal();
        else if (arg == "--port" || arg == "-p")      cfg.port        = std::stoi(nextVal());
        else if (arg == "--connections" || arg == "-c") cfg.connections = std::stoi(nextVal());
        else if (arg == "--msgs" || arg == "-m")      cfg.msgsPerConn = std::stoi(nextVal());
        else if (arg == "--duration" || arg == "-d")  cfg.duration    = std::stoi(nextVal());
        else if (arg == "--keepalive")                cfg.keepalive    = true;
        else if (arg == "--method")                   cfg.method      = nextVal();
        else if (arg == "--csv")                      cfg.csvFile     = nextVal();
        else if (arg == "--help") {
            std::cout << R"(用法: bench_biz_stress [选项]

=== 通用选项 ===
  -h, --host  HOST      目标主机 (默认: 127.0.0.1)
  -p, --port  PORT      目标端口 (默认按 --method 选: query_job→9001, 其余→9003)
  -c, --connections N   并发线程数 (默认: 100)
  -m, --msgs N          每线程消息数 (默认: 10)
  -d, --duration  N     压测时长/秒 (0=不限，与 -m 二选一)
  --keepalive           长连接复用模式（推荐：同线程所有请求复用一条 TCP 连接）
  --csv  FILE           导出 CSV 结果文件

=== 业务 RPC 选择 ===
  --method NAME         业务 RPC 方法 (默认: query_job)
                        query_job     JobService.QueryJob（纯读，查不存在的 job）
                        heartbeat     WorkerManagerService.Heartbeat（无副作用）
                        list_workers  WorkerManagerService.ListWorkers（纯读）

示例:
  # JobService 查询吞吐
  bench_biz_stress --direct --keepalive --method query_job -c 50 -m 200

  # WorkerManager 心跳吞吐（设计目标 QPS>10000）
  bench_biz_stress --direct --keepalive --method heartbeat -c 100 -m 500

  # 30 秒持续压测 + CSV 导出
  bench_biz_stress --direct --keepalive --method list_workers -d 30 --csv result.csv
)";  exit(0);
        }
    }
}

// ============================================================================
// DirectRpcChannel: 跳过 ZooKeeper，直连 RPC 服务端
// 与 MprpcChannel::CallMethod 相同的 wire format（从 bench_rpc_stress 原样复用）：
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

    // 进行一次 RPC 调用（短连接模式内部完成 connect→send→recv→close）
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
        mprpc::RpcHeader rpcHeader;
        rpcHeader.set_service_name(service_name);
        rpcHeader.set_method_name(method_name);
        rpcHeader.set_args_size(static_cast<uint32_t>(request_data.size()));
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

        // 帧：BuildRpcFrame 统一写入 total_len + magic + version
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

        std::string responsePayload;
        std::string frameError;
        if (!mprpc::DecodeRpcFramePayload(responseFrameBody, &responsePayload, &frameError)) {
            error_msg = frameError;
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        mprpc::RpcResponseHeader responseHeader;
        if (!decodeResponsePayload(responsePayload, responseHeader, response_data, error_msg)) {
            if (ownsFd) ::close(sock);
            else { ::close(fd_); fd_ = -1; }
            return false;
        }

        if (responseHeader.request_id() != requestId) {
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
// 业务请求构造 / 响应解析（按 method 分发）
// ============================================================================

// 构造一次业务请求，返回序列化后的 args（线程内复用）
static bool buildRequest(const StressConfig& cfg, int tid, std::string& reqData)
{
    if (cfg.method == "query_job")
    {
        video_platform::QueryJobRequest req;
        // 查询不存在的 job：纯读 + error_code=1 快速返回，测服务端吞吐
        req.set_job_id("bench_nonexistent_" + std::to_string(tid));
        return req.SerializeToString(&reqData);
    }
    if (cfg.method == "heartbeat")
    {
        video_platform::HeartbeatRequest req;
        auto* load = req.mutable_load();
        // bench_w_ 前缀：worker 不存在 → UPDATE 0 行，不会刷新真实 Worker
        load->set_worker_id("bench_w_" + std::to_string(tid));
        load->set_cpu_usage(10);
        load->set_memory_usage(20);
        load->set_running_shards(0);
        return req.SerializeToString(&reqData);
    }
    if (cfg.method == "list_workers")
    {
        video_platform::ListWorkersRequest req;
        req.set_filter_status(video_platform::WORKER_STATUS_UNKNOWN);   // 0=全部
        return req.SerializeToString(&reqData);
    }
    return false;
}

// 解析业务响应。@return 0=业务成功；>0=业务失败（error_code 值）；-1=RPC 层错误
static int parseResponse(const StressConfig& cfg, const std::string& respData)
{
    if (cfg.method == "query_job")
    {
        video_platform::QueryJobResponse resp;
        if (!resp.ParseFromString(respData)) return -1;
        return resp.error_code() == 0 ? 0 : resp.error_code();
    }
    if (cfg.method == "heartbeat")
    {
        video_platform::HeartbeatResponse resp;
        if (!resp.ParseFromString(respData)) return -1;
        return resp.error_code() == 0 ? 0 : resp.error_code();
    }
    if (cfg.method == "list_workers")
    {
        video_platform::ListWorkersResponse resp;
        if (!resp.ParseFromString(respData)) return -1;
        return resp.error_code() == 0 ? 0 : resp.error_code();
    }
    return -1;
}

// 每个 method 的 service/method 短名（ZK 注册与分发用非全限定名）
static void methodNames(const std::string& method, std::string& service, std::string& rpc)
{
    if (method == "query_job")        { service = "JobService";           rpc = "QueryJob"; }
    else if (method == "heartbeat")   { service = "WorkerManagerService"; rpc = "Heartbeat"; }
    else                              { service = "WorkerManagerService"; rpc = "ListWorkers"; }
}

// ============================================================================
// 单轮压测执行（仅 Direct 模式）
// ============================================================================
static StressResult runStress(const StressConfig& cfg) {
    int threadCount = cfg.connections;

    std::atomic<long> totalLatencyNs{0};
    std::atomic<long> successCount{0};
    std::atomic<long> bizFailCount{0};
    std::atomic<long> failCount{0};
    std::atomic<long> connFailCount{0};
    std::mutex latMutex;
    std::vector<double> allLatencies;

    std::string service, rpc;
    methodNames(cfg.method, service, rpc);

    auto start = Clock::now();

    std::vector<std::thread> threads;
    for (int t = 0; t < threadCount; ++t) {
        threads.emplace_back([&, t]() {
            std::vector<double> localLats;
            DirectRpcChannel channel(cfg.host, cfg.port);

            if (cfg.keepalive) {
                if (!channel.connect()) {
                    connFailCount.fetch_add(1, std::memory_order_relaxed);
                    return;
                }
            }

            // 业务请求序列化一次，线程内复用
            std::string reqData;
            if (!buildRequest(cfg, t, reqData)) {
                failCount.fetch_add(1, std::memory_order_relaxed);
                return;
            }

            auto doOneCall = [&](int msgIndex) {
                std::string respData;
                std::string error;

                auto t1 = Clock::now();
                bool ok = channel.call(service, rpc, reqData, respData, error);
                auto t2 = Clock::now();

                if (ok) {
                    long latNs = std::chrono::duration<long, std::nano>(t2 - t1).count();
                    totalLatencyNs.fetch_add(latNs, std::memory_order_relaxed);
                    successCount.fetch_add(1, std::memory_order_relaxed);
                    localLats.push_back(static_cast<double>(latNs));

                    // 业务层 error_code 单独计数（query_job 对不存在 job 返回 1 属预期）
                    int bizErr = parseResponse(cfg, respData);
                    if (bizErr != 0) bizFailCount.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }

                failCount.fetch_add(1, std::memory_order_relaxed);
                if (cfg.keepalive) {
                    // 长连接失败：连接已断开，重建后继续
                    channel.closeConnection();
                    if (!channel.connect()) {
                        connFailCount.fetch_add(1, std::memory_order_relaxed);
                        return false;
                    }
                }
                return true;   // 短连接失败：下次调用新建连接，继续
            };

            if (cfg.duration > 0) {
                auto connStart = Clock::now();
                while (true) {
                    auto elapsed = std::chrono::duration<double>(
                        Clock::now() - connStart).count();
                    if (elapsed >= cfg.duration) break;
                    if (!doOneCall(0)) break;
                }
            } else {
                for (int m = 0; m < cfg.msgsPerConn; ++m) {
                    if (!doOneCall(m)) break;
                }
            }

            if (cfg.keepalive) channel.closeConnection();

            std::lock_guard<std::mutex> lk(latMutex);
            allLatencies.insert(allLatencies.end(), localLats.begin(), localLats.end());
        });
    }

    for (auto& th : threads) th.join();

    auto end = Clock::now();

    StressResult res;
    res.totalTimeMs    = std::chrono::duration<double, std::milli>(end - start).count();
    res.successCount   = successCount.load();
    res.bizFailCount   = bizFailCount.load();
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
    std::cout << "  │ 业务失败:  " << std::setw(40) << std::left << r.bizFailCount   << "│\n";
    std::cout << "  │ RPC失败:   " << std::setw(40) << std::left << r.failCount      << "│\n";
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
static void writeCSV(const std::string& path, const StressResult& r) {
    std::ofstream f(path);
    f << "Threads,DurationMs,Success,BizFail,Fail,ConnFail,QPS,"
      << "AvgUs,MinUs,MaxUs,P50Us,P90Us,P99Us,P999Us\n";
    f << r.successCount + r.bizFailCount + r.failCount << ","
      << std::fixed << std::setprecision(2) << r.totalTimeMs << ","
      << r.successCount << "," << r.bizFailCount << "," << r.failCount << ","
      << r.connFailCount << ","
      << std::fixed << std::setprecision(0) << r.qps << ","
      << r.avgLatencyUs << "," << r.minLatencyUs << "," << r.maxLatencyUs << ","
      << r.p50Us << "," << r.p90Us << "," << r.p99Us << "," << r.p999Us << "\n";
    f.close();
    std::cout << "\n  CSV 已导出: " << path << "\n";
}

// ============================================================================
// main
// ============================================================================
int main(int argc, char* argv[]) {
    ::signal(SIGPIPE, SIG_IGN);

    StressConfig cfg;
    parseArgs(argc, argv, cfg);

    // 未显式指定端口时按 method 选默认端口
    if (cfg.port == 0) cfg.port = defaultPort(cfg.method);

    std::cout << "\n\033[1;36m╔══════════════════════════════════════════════════════════╗\033[0m\n";
    std::cout << "\033[1;36m║  video_platform 业务 RPC 压测工具                        ║\033[0m\n";
    std::cout << "\033[1;36m╚══════════════════════════════════════════════════════════╝\033[0m\n";
    std::cout << "\n  method: " << cfg.method
              << "  目标: " << cfg.host << ":" << cfg.port
              << "  线程: " << cfg.connections
              << "  消息/线程: " << cfg.msgsPerConn
              << "  连接模式: " << (cfg.keepalive ? "长连接" : "短连接")
              << (cfg.duration > 0 ? "  时长: " + std::to_string(cfg.duration) + "s" : "")
              << "\n";
    std::cout << "  说明: query_job 查询不存在的 job，业务失败率 ~100% 属预期，"
              << "指标看 QPS/延迟\n";

    auto result = runStress(cfg);
    printResult(result, cfg.method + ": " + cfg.host + ":" + std::to_string(cfg.port),
                cfg.connections);

    if (!cfg.csvFile.empty()) {
        writeCSV(cfg.csvFile, result);
    }

    std::cout << "\n  \033[1;32m压测完成\033[0m\n\n";
    return (result.failCount > 0 || result.connFailCount > 0) ? 1 : 0;
}
