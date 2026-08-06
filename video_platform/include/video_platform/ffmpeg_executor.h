#pragma once

#include <string>
#include <vector>
#include <functional>
#include <cstdint>

namespace video_platform {

// ============================================================================
// FfmpegExecutor — FFmpeg/FFprobe 命令行封装
// ============================================================================
//
// 封装 ffmpeg / ffprobe 命令行工具的调用，提供视频探测、切片、转码、
// 截图、合并等能力。所有方法均为静态方法，无状态，天然线程安全。
//
// 子进程管理：使用 fork() + execvp() + pipe() 启动 ffmpeg，记录子进程 PID。
// 进度回调中检测 cancel → kill(pid, SIGTERM) 终止子进程，实现即时取消。
// 父进程在 waitpid() 后获取退出码。
//
// 错误处理：捕获 ffmpeg 退出码和 stderr 内容。退出码非 0 时 success=false，
// error_msg 包含 stderr 尾部（最多 512 字节）便于定位问题。
//
// ⚠️ 前置条件：系统需安装 ffmpeg 和 ffprobe。启动时调用 CheckAvailable()
// 验证可用性，若不可用应拒绝以 ffmpeg 模式启动。

/// @brief FFmpeg 命令执行结果
struct FfmpegResult {
    bool        success     = false;   ///< 命令是否执行成功（退出码 == 0）
    int         exit_code   = 0;       ///< 进程退出码
    std::string error_msg;             ///< 失败时的 stderr 摘要（最多 512 字节）
    std::string output_path;           ///< 输出文件路径（Transcode / Merge 调用后回填）
    int64_t     elapsed_ms  = 0;       ///< 实际耗时（毫秒）
};

/// @brief 视频元信息
struct VideoInfo {
    bool        valid       = false;   ///< 探测是否成功
    int64_t     duration_ms = 0;       ///< 视频总时长（毫秒）
    int         width       = 0;       ///< 视频宽度（像素）
    int         height      = 0;       ///< 视频高度（像素）
    std::string codec_name;            ///< 视频编码名称（如 "h264"）
    std::string format_name;           ///< 容器格式名称（如 "mov,mp4,m4a,3gp,3g2,mj2"）
};

/// @brief FFmpeg 执行器（全静态方法）
///
/// 线程安全：所有方法为静态，每次调用创建独立子进程，不共享状态。
///
/// 使用示例：
/// @code
///   // 探测视频
///   auto info = FfmpegExecutor::Probe("/path/to/video.mp4");
///   if (!info.valid) { ... }
///
///   // 转码（带进度回调）
///   auto result = FfmpegExecutor::Transcode(
///       "/tmp/part_0.mp4", "/tmp/output_0.mp4",
///       "1280x720", 2000,
///       [](int progress) { printf("progress: %d%%\n", progress); });
///
///   // 合并
///   auto merged = FfmpegExecutor::Merge(
///       {"/tmp/output_0.mp4", "/tmp/output_1.mp4"}, "/tmp/final.mp4");
/// @endcode
class FfmpegExecutor {
public:
    // ── 可用性检查 ─────────────────────────────────────────────────────────

    /// @brief 检查 ffmpeg 和 ffprobe 是否在 PATH 中
    /// @return true 如果两者都可用
    static bool CheckAvailable();

    // ── 视频信息探测 ───────────────────────────────────────────────────────

    /// @brief 用 ffprobe 探测视频元信息
    /// @param input_path  输入视频文件路径
    /// @return VideoInfo，探测失败时 valid=false
    ///
    /// 执行命令：
    ///   ffprobe -v quiet -print_format json -show_format -show_streams <input_path>
    ///
    /// 从 JSON 输出中解析 duration / width / height / codec_name / format_name。
    /// 解析失败时返回 valid=false。
    static VideoInfo Probe(const std::string& input_path);

    // ── 视频转码 ───────────────────────────────────────────────────────────

    /// @brief 转码视频（可指定分辨率、码率、时间范围，带进度回调和取消检查）
    /// @param input_path         输入文件路径
    /// @param output_path        输出文件路径
    /// @param target_resolution  目标分辨率，如 "1280x720"、"1920x1080"
    ///                           空字符串表示保持原分辨率
    /// @param target_bitrate     目标视频码率（kbps），0 表示保持原码率
    /// @param start_ms           片段起始偏移（毫秒），0 表示从头开始
    /// @param duration_ms        片段时长（毫秒），0 表示到文件末尾
    /// @param progress_callback  进度回调（0-100），每解析到一行 ffmpeg 进度时调用
    /// @param should_cancel      取消检查回调，返回 true 时立即 kill 子进程并返回
    /// @return FfmpegResult，success=true 表示转码成功
    ///
    /// 执行命令：
    ///   ffmpeg -y [-ss {start}ms] [-t {duration}ms] -i {input_path}
    ///          [-s {W}x{H}] [-b:v {bitrate}k]
    ///          -c:v libx264 -preset fast -c:a aac
    ///          -progress pipe:1 -nostats {output_path}
    ///
    /// 当 start_ms 或 duration_ms 不为 0 时，-ss 放在 -i 之前实现快速 input seeking，
    /// 配合重编码做到帧精确定位。不再需要单独的 Slice 步骤。
    static FfmpegResult Transcode(const std::string& input_path,
                                  const std::string& output_path,
                                  const std::string& target_resolution,
                                  int target_bitrate,
                                  int64_t start_ms,
                                  int64_t duration_ms,
                                  std::function<void(int)> progress_callback,
                                  std::function<bool()> should_cancel = nullptr);

    // ── 视频合并 ───────────────────────────────────────────────────────────

    /// @brief 合并多个视频文件
    /// @param input_paths  输入文件路径列表（按合并顺序）
    /// @param output_path  输出合并文件路径
    /// @return FfmpegResult，success=true 表示合并成功
    ///
    /// 通过 concat demuxer 合并，先创建临时 filelist.txt，再执行：
    ///   ffmpeg -y -f concat -safe 0 -i filelist.txt -c copy {output_path}
    ///
    /// 使用 -c copy 流复制，要求所有输入文件具有相同的编码参数。
    /// 如果编码参数不一致，需要使用 concat filter 重新编码（后续迭代）。
    static FfmpegResult Merge(const std::vector<std::string>& input_paths,
                              const std::string& output_path);

private:
    // ── 内部方法 ───────────────────────────────────────────────────────────

    /// @brief 执行命令并捕获 stdout+stderr（fork+execvp，不走 shell）
    /// @param args        命令参数数组，args[0] 为可执行文件名
    /// @param progress_cb  可选进度回调，在每行输出解析后调用
    /// @param should_cancel 可选取消检查回调，返回 true 时立即 SIGTERM 子进程
    /// @return FfmpegResult
    ///
    /// 实现细节：
    /// - 使用 fork() + execvp() 启动子进程（不走 /bin/sh，消除命令注入）
    /// - 通过 pipe() 捕获 stdout+stderr
    /// - poll() 带超时轮询子进程输出 + 检查 cancel（修复卡死不输出的挂死）
    /// - 总执行时长上限 1 小时超时保护
    /// - waitpid() 获取退出码
    static FfmpegResult ExecuteCommand(const std::vector<std::string>& args,
                                       std::function<void(int)> progress_cb = nullptr,
                                       std::function<bool()> should_cancel = nullptr);

    /// @brief 从 ffmpeg -progress 输出行中解析进度百分比
    /// @param line              ffmpeg -progress 输出的一行
    /// @param total_duration_ms 总时长（毫秒），用于计算百分比
    /// @return 0-100 的进度百分比，-1 表示未识别到进度信息
    ///
    /// -progress 模式下的输出格式：
    ///   out_time_ms=5120000
    ///   out_time=00:00:05.120000
    /// 或普通 stderr 模式：
    ///   time=00:00:05.00
    ///
    /// 优先解析 out_time_ms（毫秒数），回退到解析 time=HH:MM:SS.ms 格式。
    static int ParseProgress(const std::string& line, int64_t total_duration_ms);

    /// @brief 检查可执行文件是否在 PATH 中
    static bool IsCommandAvailable(const std::string& name);
};

} // namespace video_platform
