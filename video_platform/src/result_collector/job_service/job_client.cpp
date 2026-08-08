#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "job.pb.h"
#include "wevix_muduo/AsyncLogger.h"

// 安全读取整数，输入非法时提示重试
static bool safeReadInt(const std::string& prompt, int& out) {
    std::string line;
    while (true) {
        std::cout << prompt;
        std::getline(std::cin, line);
        if (line.empty()) return false;
        try {
            out = std::stoi(line);
            return true;
        } catch (const std::invalid_argument&) {
            std::cerr << "  Invalid number, please try again." << std::endl;
        } catch (const std::out_of_range&) {
            std::cerr << "  Number out of range, please try again." << std::endl;
        }
    }
}

// 状态码 → 字符串映射
static const char* jobStatusStr(int status) {
    switch (status) {
        case 0: return "UNKNOWN";
        case 1: return "PENDING";
        case 2: return "SPLITTING";
        case 3: return "SCHEDULING";
        case 4: return "RUNNING";
        case 5: return "MERGING";
        case 6: return "SUCCESS";
        case 7: return "FAILED";
        case 8: return "CANCELED";
        default: return "?";
    }
}

static const char* shardStatusStr(int status) {
    switch (status) {
        case 0: return "UNKNOWN";
        case 1: return "CREATED";
        case 2: return "WAITING";
        case 3: return "ASSIGNED";
        case 4: return "RUNNING";
        case 5: return "SUCCESS";
        case 6: return "FAILED";
        case 7: return "RETRYING";
        case 8: return "CANCELED";
        default: return "?";
    }
}

// 判断 job 是否已到终态（SUCCESS / FAILED / CANCELED）
static bool isJobTerminal(int status) {
    return status == 6 || status == 7 || status == 8;
}

/// @brief 查询 job 状态和进度，打印格式化输出
/// @param stub  JobService RPC stub
/// @param job_id 要查询的 job ID
/// @return true 表示查询成功，false 表示 RPC 失败
static bool queryJob(video_platform::JobService_Stub& stub, const std::string& job_id) {
    video_platform::QueryJobRequest req;
    req.set_job_id(job_id);

    video_platform::QueryJobResponse resp;
    MprpcController ctrl;
    ctrl.SetTimeoutMs(5000);

    stub.QueryJob(&ctrl, &req, &resp, nullptr);

    if (ctrl.Failed()) {
        std::cerr << "RPC failed: " << ctrl.ErrorText() << std::endl;
        return false;
    }

    if (resp.error_code() != 0) {
        std::cerr << "Query failed: " << resp.error_msg() << std::endl;
        return false;
    }

    const auto& info = resp.job_info();
    std::cout << "\n┌─ Job: " << info.job_id() << std::endl;
    std::cout << "│  Status:      " << jobStatusStr(info.status())
              << " (" << info.status() << ")" << std::endl;
    std::cout << "│  Input:       " << info.input_path() << std::endl;
    std::cout << "│  Resolution:  " << info.target_resolution() << std::endl;
    std::cout << "│  Shard count: " << info.shard_count() << std::endl;
    std::cout << "│  Created:     " << info.created_at() << std::endl;
    std::cout << "│  Updated:     " << info.updated_at() << std::endl;

    if (resp.shards_size() > 0) {
        std::cout << "│" << std::endl;
        std::cout << "│  Shards (" << resp.shards_size() << "):" << std::endl;

        int done_count = 0;
        for (const auto& s : resp.shards()) {
            if (s.status() == 5) ++done_count;  // SHARD_SUCCESS
        }

        for (const auto& s : resp.shards()) {
            const char* marker = (s.status() == 5) ? "✓" :
                                 (s.status() == 6) ? "✗" : "○";
            std::cout << "│    " << marker << " [" << s.shard_index() << "] "
                      << s.shard_id() << std::endl;
            std::cout << "│       status=" << shardStatusStr(s.status());
            if (!s.assigned_worker_id().empty())
                std::cout << "  worker=" << s.assigned_worker_id();
            if (s.retry_count() > 0)
                std::cout << "  retry=" << s.retry_count() << "/" << s.max_retry();
            std::cout << std::endl;
        }

        // 进度条
        if (info.shard_count() > 0) {
            int pct = done_count * 100 / info.shard_count();
            std::cout << "│" << std::endl;
            std::cout << "│  Progress: [" << std::string(pct / 5, '=')
                      << std::string(20 - pct / 5, ' ') << "] "
                      << done_count << "/" << info.shard_count()
                      << " (" << pct << "%)" << std::endl;
        }
    }

    std::cout << "└──────────────────────────────────" << std::endl;
    return true;
}

// ────────────────────────────────────────────────────────────────────────────
// 用法说明
// ────────────────────────────────────────────────────────────────────────────

static void printUsage(const char* prog) {
    std::cout << "Usage:" << std::endl;
    std::cout << "  Submit a job (interactive):" << std::endl;
    std::cout << "    " << prog << " -i <config_file>" << std::endl;
    std::cout << std::endl;
    std::cout << "  Query a job:" << std::endl;
    std::cout << "    " << prog << " -i <config_file> --query <job_id>" << std::endl;
    std::cout << std::endl;
    std::cout << "  Poll a job until done (every 2s):" << std::endl;
    std::cout << "    " << prog << " -i <config_file> --query <job_id> --watch" << std::endl;
    std::cout << std::endl;
    std::cout << "  Cancel a job:" << std::endl;
    std::cout << "    " << prog << " -i <config_file> --cancel <job_id>" << std::endl;
}

// ────────────────────────────────────────────────────────────────────────────
// main
// ────────────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    signal(SIGPIPE, SIG_IGN);

    // 解析 --query / --watch / --cancel / -i 参数
    std::string query_job_id;
    std::string cancel_job_id;
    std::string config_path;
    bool watch_mode = false;

    int real_argc = 0;
    const char* real_argv[16] = {};
    for (int i = 0; i < argc && real_argc < 15; ++i) {
        if (std::strcmp(argv[i], "-i") == 0 && i + 1 < argc) {
            config_path = argv[++i];          // 记住配置文件路径
            real_argv[real_argc++] = argv[i - 1];
            real_argv[real_argc++] = argv[i];
        } else if (std::strcmp(argv[i], "--query") == 0 && i + 1 < argc) {
            query_job_id = argv[++i];
        } else if (std::strcmp(argv[i], "--cancel") == 0 && i + 1 < argc) {
            cancel_job_id = argv[++i];
        } else if (std::strcmp(argv[i], "--watch") == 0) {
            watch_mode = true;
        } else if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            printUsage(argv[0]);
            wevix_muduo::AsyncLogger::GetInstance().stop();
            return 0;
        } else {
            real_argv[real_argc++] = argv[i];
        }
    }

    if (!MprpcApplication::Init(real_argc, const_cast<char**>(real_argv))) {
        std::cerr << "Failed to init MprpcApplication. Usage: "
                  << argv[0] << " -i <config_file>" << std::endl;
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // ── Query 模式 ──────────────────────────────────────────────────────
    if (!query_job_id.empty()) {
        video_platform::JobService_Stub stub(new MprpcChannel());

        if (watch_mode) {
            std::cout << "Polling job " << query_job_id << " every 2s (Ctrl+C to stop)..."
                      << std::endl;
            while (true) {
                bool ok = queryJob(stub, query_job_id);
                if (!ok) {
                    std::cerr << "Query failed, retrying..." << std::endl;
                } else {
                    // 检查是否到终态
                    video_platform::QueryJobRequest req;
                    req.set_job_id(query_job_id);
                    video_platform::QueryJobResponse resp;
                    MprpcController ctrl;
                    stub.QueryJob(&ctrl, &req, &resp, nullptr);

                    if (!ctrl.Failed() && resp.error_code() == 0) {
                        int st = resp.job_info().status();
                        if (isJobTerminal(st)) {
                            std::cout << "\nJob reached terminal state: "
                                      << jobStatusStr(st) << std::endl;
                            break;
                        }
                    }
                }
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        } else {
            queryJob(stub, query_job_id);
        }

        wevix_muduo::AsyncLogger::GetInstance().stop();
        return 0;
    }

    // ── Cancel 模式 ─────────────────────────────────────────────────────
    if (!cancel_job_id.empty()) {
        video_platform::JobService_Stub stub(new MprpcChannel());

        video_platform::CancelJobRequest req;
        req.set_job_id(cancel_job_id);
        req.set_reason("cli cancel");

        video_platform::CancelJobResponse resp;
        MprpcController ctrl;
        ctrl.SetTimeoutMs(5000);

        stub.CancelJob(&ctrl, &req, &resp, nullptr);

        if (ctrl.Failed()) {
            std::cerr << "RPC failed: " << ctrl.ErrorText() << std::endl;
        } else if (resp.error_code() != 0) {
            std::cerr << "✗ Cancel failed: " << resp.error_msg() << std::endl;
        } else {
            std::cout << "✓ Job canceled: " << cancel_job_id << std::endl;
        }

        wevix_muduo::AsyncLogger::GetInstance().stop();
        return 0;
    }

    // ── Submit 模式（原有交互逻辑） ─────────────────────────────────────
    video_platform::JobService_Stub stub(new MprpcChannel());

    video_platform::SubmitJobRequest request;
    std::string line;

    std::cout << "=== Submit Video Transcode Job ===" << std::endl;

    std::cout << "User ID: ";
    std::getline(std::cin, line);
    request.set_user_id(line);

    std::cout << "Input video path (required): ";
    std::getline(std::cin, line);
    request.set_input_path(line);

    std::cout << "Output directory path: ";
    std::getline(std::cin, line);
    if (!line.empty()) request.set_output_path(line);

    std::cout << "Target format (mp4/mkv/flv, empty=keep original): ";
    std::getline(std::cin, line);
    if (!line.empty()) request.set_target_format(line);

    std::cout << "Target resolution (720p/1080p/4k, empty=keep original): ";
    std::getline(std::cin, line);
    if (!line.empty()) request.set_target_resolution(line);

    int int_val;
    if (safeReadInt("Target bitrate (kbps, 0=keep original): ", int_val))
        request.set_target_bitrate(int_val);

    if (safeReadInt("Priority (higher = more urgent, default 0): ", int_val))
        request.set_priority(int_val);

    if (safeReadInt("Shard duration (seconds, 0=default 20s): ", int_val))
        request.set_shard_duration_sec(int_val);

    std::cout << "\n--- Submitting Job ---" << std::endl;
    LOG_INFO("SubmitJob: user_id=%s input=%s format=%s resolution=%s bitrate=%d priority=%d",
             request.user_id().c_str(),
             request.input_path().c_str(),
             request.target_format().c_str(),
             request.target_resolution().c_str(),
             request.target_bitrate(),
             request.priority());

    video_platform::SubmitJobResponse response;
    MprpcController controller;

    stub.SubmitJob(&controller, &request, &response, nullptr);

    if (!controller.Failed()) {
        if (response.error_code() == 0) {
            std::cout << "\n✓ Job submitted successfully!" << std::endl;
            std::cout << "  Job ID: " << response.job_id() << std::endl;
            if (response.has_job_info()) {
                const auto& info = response.job_info();
                std::cout << "  Status: " << jobStatusStr(info.status()) << std::endl;
                std::cout << "  Created at: " << info.created_at() << std::endl;
            }
            std::string cfg = config_path.empty() ? "<config>" : config_path;
            std::cout << "\n  Query progress:" << std::endl;
            std::cout << "    " << argv[0] << " -i " << cfg << " --query "
                      << response.job_id() << std::endl;
            std::cout << "  Watch until done:" << std::endl;
            std::cout << "    " << argv[0] << " -i " << cfg << " --query "
                      << response.job_id() << " --watch" << std::endl;
            LOG_INFO("SubmitJob success: job_id=%s", response.job_id().c_str());
        } else {
            std::cerr << "\n✗ Job submission failed!" << std::endl;
            std::cerr << "  Error code: " << response.error_code() << std::endl;
            std::cerr << "  Error msg:  " << response.error_msg() << std::endl;
            LOG_ERROR("SubmitJob failed: code=%d msg=%s",
                      response.error_code(), response.error_msg().c_str());
        }
    } else {
        std::cerr << "\n✗ RPC call failed: " << controller.ErrorText() << std::endl;
        LOG_ERROR("SubmitJob RPC failed: %s", controller.ErrorText().c_str());
    }

    wevix_muduo::AsyncLogger::GetInstance().stop();
    return 0;
}
