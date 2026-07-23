#include "wevix_muduo/Timestamp.h"

#include <ctime>
#include <cstdio>

namespace wevix_muduo
{

Timestamp::Timestamp() 
    : microSecondsSinceEpoch_(0)
{
}

Timestamp::Timestamp(int64_t microSecondsSinceEpoch)
    : microSecondsSinceEpoch_(microSecondsSinceEpoch)
{
}

Timestamp Timestamp::now()
{
    // 使用 time(nullptr) 获取当前秒数，转换为微秒
    return Timestamp(time(nullptr) * kMicroSecondsPerSecond);
}

time_t Timestamp::toSeconds() const
{
    return static_cast<time_t>(microSecondsSinceEpoch_ / kMicroSecondsPerSecond);
}

std::string Timestamp::toString() const
{
    char buf[64] = {0};
    time_t seconds = toSeconds();
    struct tm tm_time;
    
    // 线程安全转换
    ::localtime_r(&seconds, &tm_time);

    ::snprintf(buf, sizeof(buf), "%4d-%02d-%02d %02d:%02d:%02d",
             tm_time.tm_year + 1900, 
             tm_time.tm_mon + 1,    
             tm_time.tm_mday,
             tm_time.tm_hour,
             tm_time.tm_min,
             tm_time.tm_sec);

    return buf;
}

} // namespace wevix_muduo