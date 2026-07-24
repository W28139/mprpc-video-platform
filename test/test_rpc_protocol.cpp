#include "mprpccodec.h"
#include "mprpccontroller.h"
#include "wevix_muduo/Buffer.h"

#include <cstdint>
#include <iostream>
#include <string>

namespace
{

int g_passed = 0;
int g_failed = 0;

void Check(bool condition, const std::string& name)
{
    if (condition)
    {
        ++g_passed;
        std::cout << "  [PASS] " << name << "\n";
    }
    else
    {
        ++g_failed;
        std::cout << "  [FAIL] " << name << "\n";
    }
}

void TestNetworkUint32()
{
    // 固定值 0x01020304 可以直接检查字节顺序，避免本机字节序影响 RPC 协议。
    std::string data;
    mprpc::AppendNetworkUint32(&data, 0x01020304);

    Check(data.size() == sizeof(uint32_t), "network uint32 size");
    Check(static_cast<unsigned char>(data[0]) == 0x01 &&
          static_cast<unsigned char>(data[1]) == 0x02 &&
          static_cast<unsigned char>(data[2]) == 0x03 &&
          static_cast<unsigned char>(data[3]) == 0x04,
          "network uint32 byte order");

    uint32_t value = 0;
    Check(mprpc::ReadNetworkUint32(data.data(), data.size(), &value) &&
          value == 0x01020304,
          "read network uint32");
}

void TestRpcMessageCodec()
{
    wevix_muduo::Buffer buf;
    std::string payload = "rpc-payload";
    std::string frame = mprpc::BuildRpcFrame(payload);
    std::string message;

    // 先只写入部分帧，验证 codec 会保留半包等待后续数据。
    buf.append(frame.data(), 2);
    Check(!RpcMessageCodec(&buf, message), "partial frame is incomplete");

    // 补齐剩余数据后，codec 应该一次提取完整 payload。
    buf.append(frame.data() + 2, frame.size() - 2);
    Check(RpcMessageCodec(&buf, message) && message == payload,
          "complete frame decode");
    Check(buf.readableBytes() == 0, "codec consumes complete frame");

    std::string badFrame;
    mprpc::AppendNetworkUint32(&badFrame, mprpc::kRpcMaxFrameSize + 1);
    buf.append(badFrame);
    // 超大帧直接拒绝并清空 Buffer，防止坏连接一直占用内存。
    Check(!RpcMessageCodec(&buf, message), "oversize frame rejected");
    Check(buf.readableBytes() == 0, "oversize frame clears buffer");
}

void TestRpcFrameHeader()
{
    std::string payload = "hello";
    std::string frame = mprpc::BuildRpcFrame(payload);

    uint32_t totalLen = 0;
    Check(mprpc::ReadNetworkUint32(frame.data(), frame.size(), &totalLen) &&
          totalLen == mprpc::kRpcFrameHeaderSize + payload.size(),
          "frame total length includes magic/version");

    std::string frameBody = frame.substr(sizeof(uint32_t));
    std::string decodedPayload;
    std::string errorMsg;
    Check(mprpc::DecodeRpcFramePayload(frameBody, &decodedPayload, &errorMsg) &&
          decodedPayload == payload,
          "frame payload decode");

    std::string badMagic = frameBody;
    badMagic[1] ^= 0x01;
    Check(!mprpc::DecodeRpcFramePayload(badMagic, &decodedPayload, &errorMsg),
          "invalid magic rejected");

    std::string badVersion = frameBody;
    badVersion[3] ^= 0x01;
    Check(!mprpc::DecodeRpcFramePayload(badVersion, &decodedPayload, &errorMsg),
          "invalid version rejected");
}

void TestController()
{
    // Controller 要同时保存错误码、错误文本和调用超时，供客户端上层做分类处理。
    MprpcController controller;
    controller.SetTimeoutMs(1234);
    controller.SetFailed(10, "parse failed");

    Check(controller.Failed(), "controller failed flag");
    Check(controller.ErrorCode() == 10, "controller error code");
    Check(controller.ErrorText() == "parse failed", "controller error text");
    Check(controller.HasTimeout() && controller.TimeoutMs() == 1234,
          "controller timeout");

    controller.Reset();
    Check(!controller.Failed(), "controller reset failed flag");
    Check(controller.ErrorCode() == 0, "controller reset error code");
    Check(controller.ErrorText().empty(), "controller reset error text");
    Check(!controller.HasTimeout(), "controller reset timeout");
}

} // namespace

int main()
{
    std::cout << "\nRPC protocol unit tests\n";

    TestNetworkUint32();
    TestRpcMessageCodec();
    TestRpcFrameHeader();
    TestController();

    std::cout << "\npassed=" << g_passed << " failed=" << g_failed << "\n";
    return g_failed == 0 ? 0 : 1;
}
