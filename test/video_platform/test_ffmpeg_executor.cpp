// ============================================================================
// FfmpegExecutor 功能测试
// 覆盖 video_platform::FfmpegExecutor 的核心能力：
//   CheckAvailable / Probe（正常 + 文件不存在）/ Transcode（全量、参数、
//   切片、进度回调、取消）/ Merge（两段合并 + 空列表）
//
// fixture 用 ffmpeg -f lavfi testsrc 生成 2 秒 320x240 小视频，无需外部资源。
// ffmpeg/ffprobe 不在 PATH 时打印 SKIP 并退出 0（CI 未装 ffmpeg 时安全跳过）。
// ============================================================================

#include "video_platform/ffmpeg_executor.h"

#include <iostream>
#include <iomanip>
#include <string>
#include <vector>
#include <cstdlib>
#include <cerrno>
#include <cstdio>
#include <sys/stat.h>
#include <unistd.h>

using namespace video_platform;

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

// 测试工作目录（/tmp/video_platform_ffmpeg_test/<pid>，getpid 防并发冲突）
static std::string g_dir;

// 执行 shell 命令（fixture 生成用，不走 Executor）
static bool runCmd(const std::string& cmd)
{
    return std::system(cmd.c_str()) == 0;
}

// 生成 2 秒 320x240 testsrc 源视频（与 integration_test.sh 同模式）
static bool genFixture()
{
    std::string src = g_dir + "/src.mp4";
    std::string cmd = "ffmpeg -y -loglevel error -f lavfi "
                      "-i testsrc=duration=2:size=320x240:rate=10 "
                      "-pix_fmt yuv420p -c:v libx264 -preset ultrafast " + src;
    return runCmd(cmd);
}

// ============================================================================
// 用例
// ============================================================================

static void test_probe_ok()
{
    TEST("Probe 正常");

    auto info = FfmpegExecutor::Probe(g_dir + "/src.mp4");
    CHECK(info.valid);
    CHECK(info.duration_ms >= 1500 && info.duration_ms <= 2600);  // 2s ± 容差
    CHECK(info.width == 320);
    CHECK(info.height == 240);
    CHECK(info.codec_name == "h264");
    CHECK(!info.format_name.empty());
    PASS();
}

static void test_probe_missing()
{
    TEST("Probe 文件不存在");

    auto info = FfmpegExecutor::Probe("/nonexistent_video_xyz.mp4");
    CHECK(!info.valid);
    PASS();
}

static void test_transcode_full()
{
    TEST("Transcode 全量转码");

    std::string out = g_dir + "/full.mp4";
    auto result = FfmpegExecutor::Transcode(g_dir + "/src.mp4", out, "", 0, 0, 0,
                                            nullptr, nullptr);
    CHECK(result.success);
    CHECK(access(out.c_str(), F_OK) == 0);  // 产物存在
    auto info = FfmpegExecutor::Probe(out);
    CHECK(info.valid);
    CHECK(info.duration_ms >= 1500 && info.duration_ms <= 2600);  // 时长保持
    CHECK(info.width == 320 && info.height == 240);
    PASS();
}

static void test_transcode_params()
{
    TEST("Transcode 参数生效");

    // 分辨率 160x120 + 码率 100kbps + threads=2
    std::string out = g_dir + "/params.mp4";
    auto result = FfmpegExecutor::Transcode(g_dir + "/src.mp4", out, "160x120", 100,
                                            0, 0, nullptr, nullptr, 2);
    CHECK(result.success);
    CHECK(result.elapsed_ms > 0);
    auto info = FfmpegExecutor::Probe(out);
    CHECK(info.valid);
    CHECK(info.width == 160 && info.height == 120);
    PASS();
}

static void test_transcode_slice()
{
    TEST("Transcode 切片");

    // 从 500ms 起切 1000ms
    std::string out = g_dir + "/slice.mp4";
    auto result = FfmpegExecutor::Transcode(g_dir + "/src.mp4", out, "", 0,
                                            500, 1000, nullptr, nullptr);
    CHECK(result.success);
    auto info = FfmpegExecutor::Probe(out);
    CHECK(info.valid);
    CHECK(info.duration_ms >= 800 && info.duration_ms <= 1500);  // 1s ± h264 首帧容差
    PASS();
}

static void test_transcode_progress()
{
    TEST("Transcode 进度回调");

    std::string out = g_dir + "/progress.mp4";
    std::vector<int> values;
    auto cb = [&values](int p) { values.push_back(p); };
    auto result = FfmpegExecutor::Transcode(g_dir + "/src.mp4", out, "", 0, 0, 0,
                                            cb, nullptr);
    CHECK(result.success);
    CHECK(!values.empty());                       // 至少回调一次
    // 注意：ffmpeg -progress 的 out_time 在 B 帧乱序输出时会回退，
    // 进度百分比非严格单调是固有行为，只断言峰值（转完接近 100%）
    int max_pct = *std::max_element(values.begin(), values.end());
    CHECK(max_pct >= 50);
    PASS();
}

static void test_transcode_cancel()
{
    TEST("Transcode 取消路径");

    std::string out = g_dir + "/cancel.mp4";
    auto should_cancel = []() -> bool { return true; };  // 第一次检查即取消
    auto result = FfmpegExecutor::Transcode(g_dir + "/src.mp4", out, "", 0, 0, 0,
                                            nullptr, should_cancel);
    CHECK(!result.success);
    CHECK(result.error_msg.find("cancelled") != std::string::npos);
    PASS();
}

static void test_merge_two()
{
    TEST("Merge 两段");

    // 先切两段：0-1000ms 与 1000-2000ms
    std::string part0 = g_dir + "/part0.mp4";
    std::string part1 = g_dir + "/part1.mp4";
    std::string merged = g_dir + "/merged.mp4";
    auto r0 = FfmpegExecutor::Transcode(g_dir + "/src.mp4", part0, "", 0, 0, 1000,
                                        nullptr, nullptr);
    auto r1 = FfmpegExecutor::Transcode(g_dir + "/src.mp4", part1, "", 0, 1000, 1000,
                                        nullptr, nullptr);
    CHECK(r0.success && r1.success);

    auto result = FfmpegExecutor::Merge({part0, part1}, merged);
    CHECK(result.success);
    auto info = FfmpegExecutor::Probe(merged);
    CHECK(info.valid);
    CHECK(info.duration_ms >= 1600 && info.duration_ms <= 2400);  // ≈2s ± 容差
    PASS();
}

static void test_merge_empty()
{
    TEST("Merge 空列表");

    auto result = FfmpegExecutor::Merge({}, g_dir + "/empty.mp4");
    CHECK(!result.success);
    PASS();
}

// ============================================================================
// main
// ============================================================================

int main()
{
    std::cout << "=== test_ffmpeg_executor ===" << std::endl;

    // ffmpeg/ffprobe 不可用 → SKIP（退出 0，CI 未装 ffmpeg 时安全跳过）
    if (!FfmpegExecutor::CheckAvailable())
    {
        std::cout << "SKIP (ffmpeg/ffprobe not found in PATH)" << std::endl;
        return 0;
    }

    // 工作目录：/tmp/video_platform_ffmpeg_test/<pid>/
    if (mkdir("/tmp/video_platform_ffmpeg_test", 0755) != 0 && errno != EEXIST)
    {
        std::cerr << "FATAL: cannot create /tmp/video_platform_ffmpeg_test" << std::endl;
        return 1;
    }
    char pidbuf[64];
    snprintf(pidbuf, sizeof(pidbuf), "/tmp/video_platform_ffmpeg_test/%d", (int)getpid());
    g_dir = pidbuf;
    if (mkdir(g_dir.c_str(), 0755) != 0)
    {
        std::cerr << "FATAL: cannot create " << g_dir << std::endl;
        return 1;
    }

    if (!genFixture())
    {
        std::cerr << "FATAL: failed to generate test video with ffmpeg" << std::endl;
        runCmd("rm -rf " + g_dir);
        return 1;
    }

    test_probe_ok();
    test_probe_missing();
    test_transcode_full();
    test_transcode_params();
    test_transcode_slice();
    test_transcode_progress();
    test_transcode_cancel();
    test_merge_two();
    test_merge_empty();

    std::cout << "\n=== Result: " << g_passed << " passed, "
              << g_failed << " failed ===" << std::endl;

    // best-effort 清理临时目录
    runCmd("rm -rf " + g_dir);

    return g_failed > 0 ? 1 : 0;
}
