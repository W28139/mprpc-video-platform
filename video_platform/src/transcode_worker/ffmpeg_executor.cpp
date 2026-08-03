// ============================================================================
// FfmpegExecutor — FFmpeg/FFprobe 命令行封装
// ============================================================================
//
// 阶段 8 重构（修复 #4, #9, #16, #20, #25, #27）：
//   - 全部方法改为 fork+execvp 直接传 argv，不走 /bin/sh -c → 消除命令注入
//   - ExecuteCommand 用 poll()+read() 替代阻塞 fgets → cancel 即时生效
//   - 进度百分比修复：正确按 raw_progress*10ms*100/total_ms 换算
//   - cancel 退出码不再被局部变量遮蔽
//   - fdopen 失败分支先 kill 再 waitpid
//   - Transcode 仅在 duration_ms==0 时才 Probe

#include "video_platform/ffmpeg_executor.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <thread>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>
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
// ExecuteCommand — 子进程执行核心（fork+execvp+pipe，不走 shell，支持 cancel）
// ============================================================================
//
// 阶段 8 重构（修复 #4, #9, #20, #25）：
//   - 不再通过 /bin/sh -c 拼接字符串，改为 fork+execvp 直接传 argv → 消除命令注入
//   - poll() 带 1s 超时轮询可读性，替代阻塞 fgets → cancel 即时生效（修复卡死不输出）
//   - 总执行时长上限 1 小时，超时 SIGKILL → 永远不永久阻塞
//   - cancel/超时时 exit_code 从 waitpid 正确获取（修复被局部变量遮蔽）
//   - fdopen 失败时先 kill 子进程再 waitpid（修复无限阻塞）
//   - raw read() + 手动分行，替代 fdopen/fgets（避免 stdio 与非阻塞 fd 的兼容问题）
// ============================================================================

FfmpegResult FfmpegExecutor::ExecuteCommand(const std::vector<std::string>& args,
                                            std::function<void(int)> progress_cb,
                                            std::function<bool()> should_cancel)
{
    FfmpegResult result;
    auto start = std::chrono::steady_clock::now();

    if (args.empty())
    {
        result.success   = false;
        result.exit_code = -1;
        result.error_msg = "ExecuteCommand: args is empty";
        return result;
    }

    // ── 1. 创建管道 ──────────────────────────────────────────────────
    int pipefd[2];
    if (pipe(pipefd) != 0)
    {
        result.success   = false;
        result.exit_code = -1;
        result.error_msg = "pipe() failed: " + std::string(std::strerror(errno));
        return result;
    }

    pid_t pid = fork();
    if (pid < 0)
    {
        result.success   = false;
        result.exit_code = -1;
        result.error_msg = "fork() failed: " + std::string(std::strerror(errno));
        close(pipefd[0]);
        close(pipefd[1]);
        return result;
    }

    // ── 2. 子进程：execvp 直接执行，不走 shell ──────────────────────
    if (pid == 0)
    {
        close(pipefd[0]);

        int devnull = open("/dev/null", O_RDONLY);
        if (devnull >= 0)
        {
            dup2(devnull, STDIN_FILENO);
            close(devnull);
        }

        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);

        // 构建 argv 数组（execvp 要求）
        std::vector<const char*> argv;
        argv.reserve(args.size() + 1);
        for (const auto& a : args) argv.push_back(a.c_str());
        argv.push_back(nullptr);

        execvp(args[0].c_str(), const_cast<char* const*>(argv.data()));
        _exit(127);
    }

    // ── 3. 父进程：poll + read 读子进程输出 ────────────────────────
    close(pipefd[1]);

    // poll + raw read（不使用 fdopen/fgets，避免 stdio 缓冲与非阻塞 fd 冲突）
    constexpr int    kPollTimeoutMs = 1000;        // poll 超时 1 秒
    constexpr int64_t kMaxExecMs    = 3600000;     // 最大执行时长 1 小时
    int     child_status      = 0;
    bool    child_reaped      = false;
    bool    killed            = false;

    std::string line_buf;
    std::string stderr_tail;
    char rbuf[4096];

    while (!killed)
    {
        // #16 修复：看门狗改用 steady_clock 实际耗时，每轮循环都检查。
        // 此前 total_elapsed_ms 只在 poll 超时（ret==0）分支累加——ffmpeg
        // `-progress pipe:1` 每 ~0.5s 输出一行，poll 几乎总是立即返回，
        // elapsed 几乎不增长 → 持续吐输出的病态进程永远不会被 kMaxExecMs
        // SIGKILL（对静默挂死仍有效，但那只是特殊情形）。
        int64_t total_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (total_elapsed_ms >= kMaxExecMs)
        {
            LOG_WARN("ExecuteCommand: max exec time (%lldms) exceeded, killing pid=%d",
                     (long long)kMaxExecMs, pid);
            kill(pid, SIGKILL);
            killed = true;
            break;
        }

        struct pollfd pfd;
        pfd.fd     = pipefd[0];
        pfd.events = POLLIN;
        int ret = poll(&pfd, 1, kPollTimeoutMs);

        if (ret < 0)
        {
            if (errno == EINTR) continue;
            break;  // poll 错误
        }

        if (ret == 0)
        {
            // 超时：检查 cancel
            if (should_cancel && should_cancel())
            {
                LOG_INFO("ExecuteCommand: cancel requested, sending SIGTERM to pid=%d", pid);
                kill(pid, SIGTERM);
                killed = true;
            }
            if (killed) break;
            continue;
        }

        // 有数据可读
        ssize_t n = read(pipefd[0], rbuf, sizeof(rbuf) - 1);
        if (n < 0)
        {
            if (errno == EINTR || errno == EAGAIN) continue;
            break;
        }
        if (n == 0) break;  // EOF

        rbuf[n] = '\0';
        line_buf += rbuf;

        // 从缓冲区中逐行提取
        size_t pos;
        while ((pos = line_buf.find('\n')) != std::string::npos)
        {
            std::string line = line_buf.substr(0, pos + 1);
            line_buf.erase(0, pos + 1);

            stderr_tail += line;
            if (stderr_tail.size() > 512)
                stderr_tail.erase(0, stderr_tail.size() - 512);

            if (progress_cb)
            {
                int progress = ParseProgress(line, 0);
                progress_cb(progress);
            }

            // 每行读取后检查 cancel（快速响应）
            if (should_cancel && should_cancel())
            {
                LOG_INFO("ExecuteCommand: cancel requested (inline check), "
                         "sending SIGTERM to pid=%d", pid);
                kill(pid, SIGTERM);
                killed = true;
                break;
            }
        }
    }

    // ── 4. cancel 路径：优雅退出 → SIGKILL 兜底 ────────────────────
    if (killed)
    {
        pid_t wr = waitpid(pid, &child_status, WNOHANG);
        for (int i = 0; i < 10 && wr == 0; ++i)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            wr = waitpid(pid, &child_status, WNOHANG);
        }
        if (wr == 0)
        {
            LOG_WARN("ExecuteCommand: pid=%d did not exit after SIGTERM, sending SIGKILL", pid);
            kill(pid, SIGKILL);
            waitpid(pid, &child_status, 0);
        }
        child_reaped = true;
    }

    // ── 5. 收尾 ─────────────────────────────────────────────────────
    close(pipefd[0]);
    if (!child_reaped) waitpid(pid, &child_status, 0);

    // 正确提取退出码（修复 #20：不再被局部变量遮蔽）
    if (WIFEXITED(child_status))
        result.exit_code = WEXITSTATUS(child_status);
    else if (WIFSIGNALED(child_status))
        result.exit_code = -WTERMSIG(child_status);
    else
        result.exit_code = -1;

    auto end = std::chrono::steady_clock::now();
    result.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    if (killed)
    {
        result.success   = false;
        result.error_msg = "cancelled or timed out";
    }
    else if (result.exit_code == 0)
    {
        result.success = true;
    }
    else
    {
        result.success = false;
        result.error_msg = stderr_tail;
        if (result.error_msg.size() > 512)
            result.error_msg = "...(truncated)\n"
                             + result.error_msg.substr(result.error_msg.size() - 400);
    }

    return result;
}

// ============================================================================
// ParseProgress — 从 ffmpeg 输出行解析进度
// ============================================================================
//
// 返回值语义（务必区分两种模式）：
//   * total_duration_ms > 0：直接返回 0-100 百分比；
//   * total_duration_ms == 0（当前唯一实际路径，ExecuteCommand 恒传 0）：
//     返回「微秒/10000」——单位是 0.01% 精度，即 out_time 每过 10ms 值 +1，
//     值 100 相当于 1 秒。换算成百分比需要调用方再乘 100 / total_ms。
//
// ⚠️ 已知问题（阶段8审查）：
//  ① time= 回退分支的百分比换算行（return ms / (total_duration_ms/100)）在
//     total_duration_ms ∈ (0, 100) 时除零（#16 关联项）；当前恒传 0 走不到，
//     一旦调用方按 API 约定传入真实时长即触发 SIGFPE。
//  ② out_time_ms 解析无合法性校验，atoll 失败/负值会得到 0 或垃圾值。
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
// Probe — 视频信息探测（fork+execvp，不走 shell）
// ============================================================================
//
// 阶段 8 重构：改用 fork+execvp 直接传 argv（修复 #4 命令注入），
// 替代原来的 popen + shell 字符串拼接。

VideoInfo FfmpegExecutor::Probe(const std::string& input_path)
{
    VideoInfo info;

    // 构建 argv
    std::vector<std::string> args = {
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", "-select_streams", "v:0", input_path
    };

    // fork+exec+pipe 执行，捕获 stdout
    int pipefd[2];
    if (pipe(pipefd) != 0) return info;

    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return info; }

    if (pid == 0)
    {
        close(pipefd[0]);
        int devnull = open("/dev/null", O_RDONLY);
        if (devnull >= 0) { dup2(devnull, STDIN_FILENO); close(devnull); }
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);

        std::vector<const char*> argv;
        for (const auto& a : args) argv.push_back(a.c_str());
        argv.push_back(nullptr);
        execvp("ffprobe", const_cast<char* const*>(argv.data()));
        _exit(127);
    }

    close(pipefd[1]);

    // #13 修复：read 循环带 poll 超时（此前裸阻塞 read——ffprobe 挂死
    // （损坏文件/慢挂载点）会永久阻塞调用方；ScheduleJob 在 work 线程内
    // 同步调 Probe，2 个并发即占满线程池瘫痪平台）。超时后 SIGKILL 兜底，
    // 返回无效 info 由调用方回退 fallback 时长。
    constexpr int    kProbePollMs    = 1000;   // poll 单次超时
    constexpr int64_t kProbeTimeoutMs = 15000;  // ffprobe 探测总上限 15s
    std::string json;
    char buf[2048];
    bool probe_timed_out = false;
    int64_t elapsed_ms = 0;
    for (;;)
    {
        struct pollfd pfd;
        pfd.fd     = pipefd[0];
        pfd.events = POLLIN;
        int ret = poll(&pfd, 1, kProbePollMs);
        if (ret < 0)
        {
            if (errno == EINTR) continue;
            break;  // poll 错误
        }
        if (ret == 0)
        {
            elapsed_ms += kProbePollMs;
            if (elapsed_ms >= kProbeTimeoutMs)
            {
                probe_timed_out = true;
                break;
            }
            continue;
        }
        ssize_t n = read(pipefd[0], buf, sizeof(buf) - 1);
        if (n <= 0) break;  // 0=EOF，<0=错误
        buf[n] = '\0';
        json += buf;
    }
    close(pipefd[0]);

    int status = 0;
    if (probe_timed_out)
    {
        LOG_WARN("Probe: ffprobe timed out after %lldms, killing pid=%d",
                 (long long)kProbeTimeoutMs, pid);
        kill(pid, SIGKILL);
        waitpid(pid, &status, 0);
        return info;
    }
    waitpid(pid, &status, 0);

    if (json.empty()) return info;

    // ── 手动解析 JSON（不依赖第三方库） ──────────────────────────────

    auto extractString = [&json](const std::string& key, std::string& out) {
        std::string pattern = "\"" + key + "\": \"";
        size_t start = json.find(pattern);
        if (start == std::string::npos)
        {
            pattern = "\"" + key + "\": ";
            start = json.find(pattern);
            if (start == std::string::npos) return false;
            start += pattern.size();
            if (start < json.size() && json[start] == '\"') ++start;
            size_t end = json.find_first_of("\",\n\r}", start);
            if (end != std::string::npos) out = json.substr(start, end - start);
            return !out.empty();
        }
        start += pattern.size();
        size_t end = json.find('\"', start);
        if (end != std::string::npos) out = json.substr(start, end - start);
        return !out.empty();
    };

    auto extractInt = [&json](const std::string& key, int& out) {
        std::string pattern = "\"" + key + "\": ";
        size_t start = json.find(pattern);
        if (start == std::string::npos) return false;
        start += pattern.size();
        std::string numStr;
        size_t end = json.find_first_of(",\n\r}", start);
        if (end != std::string::npos) numStr = json.substr(start, end - start);
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

    double duration_sec = 0.0;
    if (extractDouble("duration", duration_sec))
        info.duration_ms = static_cast<int64_t>(duration_sec * 1000.0);

    extractInt("width", info.width);
    extractInt("height", info.height);
    extractString("codec_name", info.codec_name);
    extractString("format_name", info.format_name);

    info.valid = (info.duration_ms > 0 || info.width > 0);
    return info;
}

// ============================================================================
// Slice — 视频切片（fork+execvp，不走 shell）
// ============================================================================

FfmpegResult FfmpegExecutor::Slice(const std::string& input_path,
                                    int64_t start_ms,
                                    int64_t duration_ms,
                                    const std::string& output_path)
{
    MakeDirs(output_path.substr(0, output_path.find_last_of('/')));

    std::vector<std::string> args = {
        "ffmpeg", "-y",
        "-ss", std::to_string(start_ms / 1000.0),
        "-t",  std::to_string(duration_ms / 1000.0),
        "-i", input_path,
        "-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac",
        output_path
    };

    FfmpegResult result = ExecuteCommand(args);
    if (result.success)
    {
        result.output_path = output_path;
        struct stat st;
        if (stat(output_path.c_str(), &st) != 0 || st.st_size == 0)
        {
            result.success   = false;
            result.exit_code = -1;
            result.error_msg = "slice output file is empty or missing: " + output_path;
        }
    }
    return result;
}

// ============================================================================
// Transcode — 视频转码（fork+execvp，不走 shell，修复进度百分比）
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
    MakeDirs(output_path.substr(0, output_path.find_last_of('/')));

    // 进度基准：优先用切窗时长；仅当 duration_ms==0 时才 Probe（修复 #27）
    int64_t total_ms = duration_ms;
    if (total_ms <= 0)
    {
        auto info = Probe(input_path);
        if (info.valid) total_ms = info.duration_ms;
    }

    // 构建 argv（不走 shell，修复 #4）
    std::vector<std::string> args;
    args.push_back("ffmpeg");
    args.push_back("-y");

    if (start_ms > 0)
    {
        args.push_back("-ss");
        args.push_back(std::to_string(start_ms / 1000.0));
    }
    if (duration_ms > 0)
    {
        args.push_back("-t");
        args.push_back(std::to_string(duration_ms / 1000.0));
    }

    args.push_back("-i");
    args.push_back(input_path);

    std::string resolved = ResolveResolution(target_resolution);
    if (!resolved.empty())
    {
        args.push_back("-s");
        args.push_back(resolved);
    }
    if (target_bitrate > 0)
    {
        args.push_back("-b:v");
        args.push_back(std::to_string(target_bitrate) + "k");
    }

    args.push_back("-c:v");  args.push_back("libx264");
    args.push_back("-preset"); args.push_back("fast");
    args.push_back("-c:a");  args.push_back("aac");
    args.push_back("-progress"); args.push_back("pipe:1");
    args.push_back("-nostats");
    args.push_back(output_path);

    // 进度回调包装：将 ParseProgress 的微秒原始值转为 0-100 百分比（修复 #16）
    std::function<void(int)> wrapped_cb = nullptr;
    if (progress_callback && total_ms > 0)
    {
        wrapped_cb = [progress_callback, total_ms](int raw_progress) {
            if (raw_progress < 0) return;
            // ParseProgress(total_duration_ms=0) 返回 us/10000（0.01% 单位）
            // out_time_ms = raw_progress * 10 ms，百分比 = out_time_ms * 100 / total_ms
            int64_t out_ms = static_cast<int64_t>(raw_progress) * 10;
            int pct = static_cast<int>(out_ms * 100 / total_ms);
            if (pct < 0)   pct = 0;
            if (pct > 100) pct = 100;
            progress_callback(pct);
        };
    }

    FfmpegResult result = ExecuteCommand(args, wrapped_cb, should_cancel);
    if (result.success)
    {
        result.output_path = output_path;
    }
    return result;
}

// ============================================================================
// Screenshot — 截图
// ============================================================================
//
// 阶段6遗留项5：转码成功后由 FfmpegExecute 调用，截取
// start_ms + duration_ms/2 时间点单帧，随 ReportShardResult 上报
// （screenshot_path 字段，写入 ShardRecord）。目前下游无消费方。
//
// -ss 在 -i 前 = fast input seeking（先跳到最近关键帧再精确解码），
// 只解码一个 GOP 而非从头解码。
//
// ⚠️ 已知问题：命令拼接仅双引号包裹路径 → 命令注入（#4）。
// ============================================================================

FfmpegResult FfmpegExecutor::Screenshot(const std::string& input_path,
                                         int64_t timestamp_ms,
                                         const std::string& output_path)
{
    MakeDirs(output_path.substr(0, output_path.find_last_of('/')));

    std::vector<std::string> args = {
        "ffmpeg", "-y",
        "-ss", std::to_string(timestamp_ms / 1000.0),
        "-i", input_path,
        "-vframes", "1",
        output_path
    };

    FfmpegResult result = ExecuteCommand(args);
    if (result.success)
    {
        result.output_path = output_path;
    }
    return result;
}

// ============================================================================
// Merge — 视频合并
// ============================================================================
//
// 阶段6遗留项1：ResultCollector.MarkJobTerminal 在 JOB_SUCCESS 时收集全部
// shard 的 output_path（按 shard_index 排序）调用本方法，产出 {job_id}_merged.mp4。
// 用 concat demuxer + -c copy（不重编码，秒级完成）。
//
// ⚠️ 已知问题（阶段8审查）：
//  ① filelist.txt 用固定名写在输出目录下，跨 job/并发共享——ResultCollector
//     侧并发 merge 会交错写同一文件（#7）；本方法写完即删，无残留。
//  ② 命令拼接仅双引号包裹路径 → 命令注入（#4）。
// ============================================================================

FfmpegResult FfmpegExecutor::Merge(const std::vector<std::string>& input_paths,
                                    const std::string& output_path)
{
    if (input_paths.empty())
    {
        FfmpegResult result;
        result.success   = false;
        result.exit_code = -1;
        result.error_msg = "Merge: input_paths is empty";
        return result;
    }

    std::string output_dir = output_path.substr(0, output_path.find_last_of('/'));
    MakeDirs(output_dir);

    // #11 修复：filelist 用含输出名的唯一文件（output 形如 {job_id}_merged.mp4，
    // 派生名必然唯一），不再跨 job 共享固定名 filelist.txt——并发 merge 时
    // concat 不再读到对方 job 的 file 条目导致产物串片。
    std::string output_basename = output_path.substr(output_path.find_last_of('/') + 1);
    std::string filelist_path = output_dir + "/." + output_basename + ".filelist.txt";
    FILE* fl = fopen(filelist_path.c_str(), "w");
    if (!fl)
    {
        FfmpegResult result;
        result.success   = false;
        result.exit_code = -1;
        result.error_msg = "Merge: cannot create filelist: " + filelist_path;
        return result;
    }

    for (const auto& path : input_paths)
        fprintf(fl, "file '%s'\n", path.c_str());
    fclose(fl);

    std::vector<std::string> args = {
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", filelist_path,
        "-c", "copy",
        output_path
    };

    FfmpegResult result = ExecuteCommand(args);

    std::remove(filelist_path.c_str());

    if (result.success)
    {
        result.output_path = output_path;
    }
    return result;
}

} // namespace video_platform
