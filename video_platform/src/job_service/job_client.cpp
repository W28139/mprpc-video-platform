#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

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
        if (line.empty()) return false;  // 空输入 → 不设置
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

// 交互式提交 Job 的客户端工具
int main(int argc, char** argv) {
    // 初始化日志
#ifndef NDEBUG
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::DEBUG, true);
#else
    wevix_muduo::AsyncLogger::GetInstance().init(
        "./program_log", wevix_muduo::LogLevel::INFO, false);
#endif

    signal(SIGPIPE, SIG_IGN);

    // 初始化 mprpc 框架（只需一次）
    if (!MprpcApplication::Init(argc, argv)) {
        std::cerr << "Failed to init MprpcApplication. Usage: "
                  << argv[0] << " -i <config_file>" << std::endl;
        wevix_muduo::AsyncLogger::GetInstance().stop();
        return EXIT_FAILURE;
    }

    // 创建 JobService 的 RPC stub（通过 ZooKeeper 发现服务地址）
    video_platform::JobService_Stub stub(new MprpcChannel());

    // ── 交互式输入 ──────────────────────────────────────────────────────
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

    if (safeReadInt("Shard duration (seconds, 0=default 30s): ", int_val))
        request.set_shard_duration_sec(int_val);

    std::cout << "\n--- Submitting Job ---" << std::endl;
    LOG_INFO("SubmitJob: user_id=%s input=%s format=%s resolution=%s bitrate=%d priority=%d",
             request.user_id().c_str(),
             request.input_path().c_str(),
             request.target_format().c_str(),
             request.target_resolution().c_str(),
             request.target_bitrate(),
             request.priority());

    // ── 发起 RPC 调用 ───────────────────────────────────────────────────
    video_platform::SubmitJobResponse response;
    MprpcController controller;

    stub.SubmitJob(&controller, &request, &response, nullptr);

    // ── 处理响应 ────────────────────────────────────────────────────────
    if (!controller.Failed()) {
        if (response.error_code() == 0) {
            std::cout << "\n✓ Job submitted successfully!" << std::endl;
            std::cout << "  Job ID: " << response.job_id() << std::endl;
            if (response.has_job_info()) {
                const auto& info = response.job_info();
                std::cout << "  Status: " << info.status() << std::endl;
                std::cout << "  Created at: " << info.created_at() << std::endl;
            }
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
