#pragma once

#include <iostream>
#include <string>
#include <cstdint>

namespace wevix_muduo
{

/**
 * @brief 时间戳类，提供精确到微秒的时间表示
 */
class Timestamp
{
public:
    // 默认构造函数
    Timestamp();

    // 显式构造函数，避免隐式转换
    explicit Timestamp(int64_t microSecondsSinceEpoch);

    // 获取当前时间戳
    static Timestamp now();

    // 转换为整数（秒）
    time_t toSeconds() const;

    // 转换为字符串格式 yyyy-mm-dd hh:mm:ss
    std::string toString() const;

    // 获取内部微秒数值
    int64_t microSecondsSinceEpoch() const 
    { 
        return microSecondsSinceEpoch_; 
    }

    // 一秒等于多少微秒
    static const int kMicroSecondsPerSecond = 1000 * 1000;

private:
    int64_t microSecondsSinceEpoch_;
};

} // namespace wevix_muduo