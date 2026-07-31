#include "video_platform/ffmpeg_executor.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <sstream>
#include <thread>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include "wevix_muduo/AsyncLogger.h"

namespace video_platform {

// ============================================================================
// 内部工具函数
// ============================================================================

/// @brief 检查命令是否在 PATH 中可执行
bool FfmpegExecutor::IsCommandAvailable(const std::string& name)
{
    std::string cmd = "which " + name + " > /dev/null 2>&1";
    return (std::system(cmd.c_str()) == 0);
}

/// @brief 递归创建目录（替代 system("mkdir -p")，避免 shell 注入和警告）
/// @return true 表示目录已存在或创建成功
static bool MakeDirs(const std::string& path)
{
    if (path.empty()) return true;
    // 已存在 → 直接返回
    struct stat st;
    if (stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) return true;
    // 递归创建父目录
    size_t pos = path.find_last_of('/');
    if (pos != std::string::npos && pos > 0)
    {
        if (!MakeDirs(path.substr(0, pos))) return false;
    }
    // 创建当前目录 (0755)
    return (mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) == 0
            || (errno == EEXIST));
}

/// @brief 将常用分辨率标签映射为 ffmpeg -s 参数所需的 WxH 格式
///        如 "720p" → "1280x720"，已含 'x' 的字符串原样返回
static std::string ResolveResolution(const std::string& res)
{
    if (res.empty()) return res;
    // 已含 'x' → 已经是 WxH 格式，直接返回
    if (res.find('x') != std::string::npos || res.find('X') != std::string::npos)
        return res;

    // 常见标签映射
    if (res == "720p"  || res == "720")  return "1280x720";
    if (res == "1080p" || res == "1080") return "1920x1080";
    if (res == "480p"  || res == "480")  return "854x480";
    if (res == "360p"  || res == "360")  return "640x360";
    if (res == "4k"    || res == "2160p" || res == "2160") return "3840x2160";
    if (res == "2k"    || res == "1440p" || res == "1440") return "2560x1440";

    // 未知格式：原样返回，让 ffmpeg 报错
    return res;
}

/// @brief 检查 ffmpeg 和 ffprobe 是否都可用
bool FfmpegExecutor::CheckAvailable()
{
    return IsCommandAvailable("ffmpeg") && IsCommandAvailable("ffprobe");
}

// ============================================================================
// ExecuteCommand — 子进程执行核心（fork+exec，支持 cancel kill）
// ============================================================================

FfmpegResult FfmpegExecutor::ExecuteCommand(const std::string& cmd,
                                            std::function<void(int)> progress_cb,
                                            std::function<bool()> should_cancel)
{
    FfmpegResult result;
    auto start = std::chrono::steady_clock::now();

    // 创建管道用于捕获子进程 stdout+stderr
    int pipefd[2];
    if (pipe(pipefd) != 0)
    {
        result.success = false;
        result.exit_code = -1;
        result.error_msg = "pipe() failed: " + std::string(std::strerror(errno));
        return result;
    }

    pid_t pid = fork();
    if (pid < 0)
    {
        result.success = false;
        result.exit_code = -1;
        result.error_msg = "fork() failed: " + std::string(std::strerror(errno));
        close(pipefd[0]);
        close(pipefd[1]);
        return result;
    }

    if (pid == 0)
    {
        // ── 子进程：重定向 stdout+stderr 到管道写端，执行命令 ──
        close(pipefd[0]);  // 关闭读端

        // 重定向 stdout 和 stderr 到管道写端
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);

        // 执行 shell 命令
        execl("/bin/sh", "sh", "-c", cmd.c_str(), nullptr);

        // execl 失败时才会走到这里
        _exit(127);
    }

    // ── 父进程：读取子进程输出 ──────────────────────────────────
    close(pipefd[1]);  // 关闭写端

    FILE* pipe = fdopen(pipefd[0], "r");
    if (!pipe)
    {
        result.success = false;
        result.exit_code = -1;
        result.error_msg = "fdopen() failed: " + std::string(std::strerror(errno));
        close(pipefd[0]);
        // 确保子进程被回收
        int status;
        waitpid(pid, &status, 0);
        return result;
    }

    // 逐行读取子进程输出，同时解析进度
    std::string stderr_tail;  // 保留尾部用于错误报告
    char buf[4096];
    bool killed = false;

    while (fgets(buf, sizeof(buf), pipe))
    {
        std::string line(buf);

        // 保留尾部 512 字节用于失败时的错误摘要
        stderr_tail += line;
        if (stderr_tail.size() > 512)
        {
            stderr_tail.erase(0, stderr_tail.size() - 512);
        }

        // 进度回调（如果提供）
        if (progress_cb)
        {
            int progress = ParseProgress(line, 0);
            progress_cb(progress);
        }

        // 取消检查（如果提供）
        if (should_cancel && should_cancel())
        {
            LOG_INFO("ExecuteCommand: cancel requested, sending SIGTERM to pid=%d", pid);
            kill(pid, SIGTERM);
            killed = true;

            // 等待子进程优雅退出（最多 5 秒），超时则 SIGKILL
            int status;
            pid_t wait_result = waitpid(pid, &status, WNOHANG);
            if (wait_result == 0)
            {
                // 子进程还没退出，等待最多 5 秒
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                for (int i = 0; i < 9; ++i)
                {
                    wait_result = waitpid(pid, &status, WNOHANG);
                    if (wait_result > 0) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
                if (wait_result == 0)
                {
                    LOG_WARN("ExecuteCommand: pid=%d did not exit after SIGTERM, sending SIGKILL", pid);
                    kill(pid, SIGKILL);
                    waitpid(pid, &status, 0);
                }
            }
            break;
        }
    }

    fclose(pipe);  // 也会关闭 pipefd[0]

    // 获取退出状态（如果之前未被 waitpid 回收）
    int status = 0;
    if (!killed)
    {
        waitpid(pid, &status, 0);
    }
    else
    {
        // 子进程已被回收，重新获取 status（若上面 waitpid 成功）
        waitpid(pid, &status, WNOHANG);
    }

    if (WIFEXITED(status))
        result.exit_code = WEXITSTATUS(status);
    else if (WIFSIGNALED(status))
        result.exit_code = -WTERMSIG(status);  // 负值表示被信号终止
    else
        result.exit_code = -1;

    auto end = std::chrono::steady_clock::now();
    result.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    if (killed)
    {
        result.success = false;
        result.error_msg = "cancelled by user request";
    }
    else if (result.exit_code == 0)
    {
        result.success = true;
    }
    else
    {
        result.success = false;
        result.error_msg = stderr_tail;
        // 截断过长的错误信息
        if (result.error_msg.size() > 512)
        {
            result.error_msg = "...(truncated)\n" + result.error_msg.substr(result.error_msg.size() - 400);
        }
    }

    return result;
}

// ============================================================================
// ParseProgress — 从 ffmpeg 输出行解析进度
// ============================================================================

int FfmpegExecutor::ParseProgress(const std::string& line, int64_t total_duration_ms)
{
    // ffmpeg -progress pipe:1 模式输出格式:
    //   out_time_ms=5120000    ← 微秒级时间戳（首选）
    //   out_time=00:00:05.120000
    //
    // 普通 stderr 模式输出格式:
    //   frame=  100 fps=30 q=28.0 size=    1024kB time=00:00:05.00 bitrate=...
    //
    // 由于 ExecuteCommand 不传入 total_duration_ms，这里只做解析不做百分比计算。
    // 调用方负责从 out_time_ms 或 time= 做百分比换算。

    // 优先匹配 out_time_ms=<microseconds>
    const char* key = "out_time_ms=";
    const char* pos = std::strstr(line.c_str(), key);
    if (pos)
    {
        int64_t us = std::atoll(pos + std::strlen(key));
        // 只有传入有效 total_duration_ms 才计算百分比
        if (total_duration_ms > 0)
        {
            int pct = static_cast<int>(us / (total_duration_ms * 10));  // us → 0.1%
            if (pct < 0)   pct = 0;
            if (pct > 100) pct = 100;
            return pct;
        }
        // 返回微秒数（调用方可以自己换算）
        return static_cast<int>(us / 10000);  // 返回 0.01% 精度（调用方可 clamp）
    }

    // 回退匹配 time=HH:MM:SS.ms（普通 stderr 模式）
    key = "time=";
    pos = std::strstr(line.c_str(), key);
    if (pos)
    {
        pos += std::strlen(key);
        // 解析 HH:MM:SS 或 HH:MM:SS.ms
        int hh = 0, mm = 0;
        double ss = 0.0;
        if (std::sscanf(pos, "%d:%d:%lf", &hh, &mm, &ss) >= 3)
        {
            int64_t ms = static_cast<int64_t>(hh * 3600000 + mm * 60000 + ss * 1000);
            if (total_duration_ms > 0)
            {
                int pct = static_cast<int>(ms * 100 / total_duration_ms);
                if (pct < 0)   pct = 0;
                if (pct > 100) pct = 100;
                return pct;
            }
            return static_cast<int>(ms / (total_duration_ms > 0 ? total_duration_ms / 100 : 1));
        }
    }

    return -1;  // 未识别到进度信息
}

// ============================================================================
// Probe — 视频信息探测
// ============================================================================

VideoInfo FfmpegExecutor::Probe(const std::string& input_path)
{
    VideoInfo info;

    // 使用 ffprobe 获取 JSON 格式的视频信息
    // 只查询 format 和第一个视频流，减少输出
    std::string cmd =
        "ffprobe -v quiet -print_format json -show_format -show_streams -select_streams v:0 \""
        + input_path + "\"";

    FILE* pipe = popen((cmd + " 2>&1").c_str(), "r");
    if (!pipe) return info;

    // 读取全部输出（ffprobe JSON 通常不超过 10KB）
    std::string json;
    char buf[2048];
    while (fgets(buf, sizeof(buf), pipe))
    {
        json += buf;
    }
    pclose(pipe);

    if (json.empty()) return info;

    // ── 手动解析 JSON（不依赖第三方库） ──────────────────────────────
    // 只提取我们需要的字段：duration, width, height, codec_name, format_name
    //
    // 简化解析策略：在字符串中搜索特定 key，提取紧随其后的值。

    auto extractString = [&json](const std::string& key, std::string& out) {
        // 搜索 "key": "value"
        std::string pattern = "\"" + key + "\": \"";
        size_t start = json.find(pattern);
        if (start == std::string::npos)
        {
            // 也尝试 "key":
            pattern = "\"" + key + "\": ";
            start = json.find(pattern);
            if (start == std::string::npos) return false;
            start += pattern.size();
            // 跳过引号（如果有）
            if (start < json.size() && json[start] == '\"') ++start;
            size_t end = json.find_first_of("\",\n\r}", start);
            if (end != std::string::npos)
                out = json.substr(start, end - start);
            return !out.empty();
        }
        start += pattern.size();
        size_t end = json.find('\"', start);
        if (end != std::string::npos)
            out = json.substr(start, end - start);
        return !out.empty();
    };

    auto extractInt = [&json](const std::string& key, int& out) {
        std::string pattern = "\"" + key + "\": ";
        size_t start = json.find(pattern);
        if (start == std::string::npos) return false;
        start += pattern.size();
        // 跳过引号（数字通常不带引号，但有些 JSON 带）
        std::string numStr;
        size_t end = json.find_first_of(",\n\r}", start);
        if (end != std::string::npos)
            numStr = json.substr(start, end - start);
        // 移除前后空白和引号
        while (!numStr.empty() && (numStr[0] == ' ' || numStr[0] == '\"')) numStr.erase(0, 1);
        while (!numStr.empty() && (numStr.back() == ' ' || numStr.back() == '\"')) numStr.pop_back();
        if (numStr.empty()) return false;
        out = std::atoi(numStr.c_str());
        return true;
    };

    auto extractDouble = [&json](const std::string& key, double& out) {
        std::string pattern = "\"" + key + "\": \"";
        size_t start = json.find(pattern);
        if (start == std::string::npos) return false;
        start += pattern.size();
        size_t end = json.find('\"', start);
        if (end == std::string::npos) return false;
        out = std::atof(json.substr(start, end - start).c_str());
        return true;
    };

    // 提取信息
    double duration_sec = 0.0;
    if (extractDouble("duration", duration_sec))
    {
        info.duration_ms = static_cast<int64_t>(duration_sec * 1000.0);
    }

    // width / height / codec_name 在 streams 数组中
    extractInt("width", info.width);
    extractInt("height", info.height);
    extractString("codec_name", info.codec_name);
    extractString("format_name", info.format_name);

    info.valid = (info.duration_ms > 0 || info.width > 0);
    return info;
}

// ============================================================================
// Slice — 视频切片
// ============================================================================

FfmpegResult FfmpegExecutor::Slice(const std::string& input_path,
                                    int64_t start_ms,
                                    int64_t duration_ms,
                                    const std::string& output_path)
{
    // 确保输出目录存在
    MakeDirs(output_path.substr(0, output_path.find_last_of('/')));

    // 用重新编码实现精密切片（-c copy 在非关键帧处不精确）
    // -ss 放 -i 前 = fast input seeking，配合重编码可做到帧精确
    // -preset ultrafast 追求切片速度（后续转码会再次编码）
    std::ostringstream cmd;
    cmd << "ffmpeg -y"
        << " -ss " << (start_ms / 1000.0)
        << " -t "  << (duration_ms / 1000.0)
        << " -i \"" << input_path << "\""
        << " -c:v libx264 -preset ultrafast -c:a aac"
        << " \"" << output_path << "\"";

    FfmpegResult result = ExecuteCommand(cmd.str());
    if (result.success)
    {
        result.output_path = output_path;
        // 验证输出文件存在且大小 > 0
        struct stat st;
        if (stat(output_path.c_str(), &st) != 0 || st.st_size == 0)
        {
            result.success = false;
            result.exit_code = -1;
            result.error_msg = "slice output file is empty or missing: " + output_path;
        }
    }
    return result;
}

// ============================================================================
// Transcode — 视频转码
// ============================================================================

FfmpegResult FfmpegExecutor::Transcode(const std::string& input_path,
                                        const std::string& output_path,
                                        const std::string& target_resolution,
                                        int target_bitrate,
                                        int64_t start_ms,
                                        int64_t duration_ms,
                                        std::function<void(int)> progress_callback,
                                        std::function<bool()> should_cancel)
{
    // 确保输出目录存在
    MakeDirs(output_path.substr(0, output_path.find_last_of('/')));

    // 探测输入文件时长，用于进度百分比计算
    int64_t total_ms = 0;
    {
        auto info = Probe(input_path);
        if (info.valid) total_ms = info.duration_ms;
    }

    // 若指定了时间范围，用它作为进度基准；否则用源文件时长
    if (duration_ms > 0)
        total_ms = duration_ms;

    // 构建转码命令
    // -ss 在 -i 前 = fast input seeking，配合重编码实现帧精确定位
    // -progress pipe:1  输出结构化进度到 stdout
    // -nostats          隐藏默认的统计输出
    // -c:v libx264      使用 H.264 编码
    // -preset fast      快速预设（平衡速度与质量）
    // -c:a aac          使用 AAC 音频编码
    std::ostringstream cmd;
    cmd << "ffmpeg -y";

    // 时间范围（精确切片，替代单独的 Slice 步骤）
    if (start_ms > 0)
        cmd << " -ss " << (start_ms / 1000.0);
    if (duration_ms > 0)
        cmd << " -t " << (duration_ms / 1000.0);

    cmd << " -i \"" << input_path << "\"";

    // 分辨率（如 "1280x720" 或 "720p" → "1280x720"）
    std::string resolved = ResolveResolution(target_resolution);
    if (!resolved.empty())
    {
        cmd << " -s " << resolved;
    }

    // 视频码率（kbps）
    if (target_bitrate > 0)
    {
        cmd << " -b:v " << target_bitrate << "k";
    }

    cmd << " -c:v libx264 -preset fast -c:a aac"
        << " -progress pipe:1 -nostats"
        << " \"" << output_path << "\"";

    // 进度回调包装：将 ParseProgress 的微秒值转为 0-100 百分比
    std::function<void(int)> wrapped_cb = nullptr;
    if (progress_callback)
    {
        wrapped_cb = [progress_callback, total_ms](int raw_progress) {
            if (raw_progress < 0) return;  // 无法解析的行，跳过

            if (total_ms > 0)
            {
                // ParseProgress 返回微秒/10000（即 0.01% 精度）
                // 重新计算百分比
                int pct = raw_progress;
                if (pct < 0)   pct = 0;
                if (pct > 100) pct = 100;

                // 如果 raw_progress 很大（> 100），说明是原始微秒值
                if (pct > 100)
                {
                    int64_t raw_ms = static_cast<int64_t>(raw_progress) * 10;  // 0.01% → ms 近似
                    pct = static_cast<int>(raw_ms * 100 / total_ms);
                    if (pct < 0) pct = 0;
                    if (pct > 100) pct = 100;
                }
                progress_callback(pct);
            }
        };
    }

    FfmpegResult result = ExecuteCommand(cmd.str(), wrapped_cb, should_cancel);
    if (result.success)
    {
        result.output_path = output_path;
    }
    return result;
}

// ============================================================================
// Screenshot — 截图
// ============================================================================

FfmpegResult FfmpegExecutor::Screenshot(const std::string& input_path,
                                         int64_t timestamp_ms,
                                         const std::string& output_path)
{
    // 确保输出目录存在
    MakeDirs(output_path.substr(0, output_path.find_last_of('/')));

    std::ostringstream cmd;
    cmd << "ffmpeg -y"
        << " -ss " << (timestamp_ms / 1000.0)
        << " -i \"" << input_path << "\""
        << " -vframes 1"
        << " \"" << output_path << "\"";

    FfmpegResult result = ExecuteCommand(cmd.str());
    if (result.success)
    {
        result.output_path = output_path;
    }
    return result;
}

// ============================================================================
// Merge — 视频合并
// ============================================================================

FfmpegResult FfmpegExecutor::Merge(const std::vector<std::string>& input_paths,
                                    const std::string& output_path)
{
    if (input_paths.empty())
    {
        FfmpegResult result;
        result.success = false;
        result.exit_code = -1;
        result.error_msg = "Merge: input_paths is empty";
        return result;
    }

    // 确保输出目录存在
    std::string output_dir = output_path.substr(0, output_path.find_last_of('/'));
    MakeDirs(output_dir);

    // 创建 filelist.txt（concat demuxer 用）
    std::string filelist_path = output_dir + "/filelist.txt";
    FILE* fl = fopen(filelist_path.c_str(), "w");
    if (!fl)
    {
        FfmpegResult result;
        result.success = false;
        result.exit_code = -1;
        result.error_msg = "Merge: cannot create filelist: " + filelist_path;
        return result;
    }

    for (const auto& path : input_paths)
    {
        // concat demuxer 格式: file '/path/to/file'
        fprintf(fl, "file '%s'\n", path.c_str());
    }
    fclose(fl);

    // 执行合并
    std::ostringstream cmd;
    cmd << "ffmpeg -y"
        << " -f concat -safe 0"
        << " -i \"" << filelist_path << "\""
        << " -c copy"
        << " \"" << output_path << "\"";

    FfmpegResult result = ExecuteCommand(cmd.str());

    // 清理临时文件
    std::remove(filelist_path.c_str());

    if (result.success)
    {
        result.output_path = output_path;
    }
    return result;
}

} // namespace video_platform
