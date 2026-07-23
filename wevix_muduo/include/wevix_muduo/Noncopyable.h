// 用于禁止其派生类（子类）的对象被复制或移动。
#pragma once

namespace wevix_muduo {

class Noncopyable {
public:
    Noncopyable(const Noncopyable&) = delete;
    Noncopyable& operator=(const Noncopyable&) = delete;
    Noncopyable(Noncopyable&&) = delete;
    Noncopyable& operator=(Noncopyable&&) = delete;

protected:
    Noncopyable() = default;
    ~Noncopyable() = default;
};

}  // namespace wevix_muduo
