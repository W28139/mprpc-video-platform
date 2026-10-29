#pragma once

#include <string>
#include <vector>
#include <functional>
#include <cstdint>

namespace video_platform {

// FfmpegExecutor — FFmpeg/FFprobe 命令行封装。
// 全静态方法，无状态，线程安全。子进程用 fork+execvp+pipe 管理（支持 PID kill 取消）。

struct FfmpegResult {
    bool        success     = false;
    int         exit_code   = 0;
    std::string error_msg;           // 失败时 stderr 尾部（最多 512 字节）
    std::string output_path;
    int64_t     elapsed_ms  = 0;
};

struct VideoInfo {
    bool        valid       = false;
    int64_t     duration_ms = 0;
    int         width       = 0;
    int         height      = 0;
    std::string codec_name;
    std::string format_name;
};

class FfmpegExecutor {
public:
    /// 检查 ffmpeg/ffprobe 是否在 PATH 中
    static bool CheckAvailable();

    /// ffprobe 探测视频元信息（JSON 解析 duration/width/height/codec）
    static VideoInfo Probe(const std::string& input_path);

    /// 转码视频（可指定分辨率、码率、时间范围，带进度回调和取消检查）。
    /// -ss 在 -i 之前实现快速 input seeking，切片+转码一 pass。
    /// @param threads 编码线程数，>0 生效（0=ffmpeg 默认）
    static FfmpegResult Transcode(const std::string& input_path,
                                  const std::string& output_path,
                                  const std::string& target_resolution,
                                  int target_bitrate,
                                  int64_t start_ms,
                                  int64_t duration_ms,
                                  std::function<void(int)> progress_callback,
                                  std::function<bool()> should_cancel = nullptr,
                                  int threads = 0);

    /// concat demuxer 合并多个视频（-c copy 流复制）
    static FfmpegResult Merge(const std::vector<std::string>& input_paths,
                              const std::string& output_path);

private:
    /// fork+execvp 执行命令，pipe 捕获 stdout+stderr，poll 轮询 + cancel 检查
    static FfmpegResult ExecuteCommand(const std::vector<std::string>& args,
                                       std::function<void(int)> progress_cb = nullptr,
                                       std::function<bool()> should_cancel = nullptr);

    /// 从 ffmpeg -progress 输出行解析百分比（优先 out_time_ms，回退 time=）
    static int ParseProgress(const std::string& line, int64_t total_duration_ms);

    static bool IsCommandAvailable(const std::string& name);
};

} // namespace video_platform