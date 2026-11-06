#pragma once
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace file_transfer {
constexpr size_t max_chunk_bytes = 256 * 1024;
enum class Operation { prepare, cancel };
enum class Feedback { ready, complete, error };

// 节点在线信息独立于文件协议；不创建任务，也不表示任何文件进度。
struct Presence {
    std::string receiver_id;
    std::string receiver_epoch;
};

// 一次准备握手同时协商文件、起点和数据 Reader。补发也使用这个接口。
// generation 在同一 transfer 内递增，只用于拒绝迟到控制，不是协议版本。
struct Request {
    Operation operation = Operation::prepare;
    std::string transfer_id, session_id, receiver_id, receiver_epoch;
    uint32_t generation = 0;
    std::string file_name, sha256;
    uint64_t total_bytes = 0, offset = 0;
    std::string prefix_sha256; // 源文件在 offset 处的前缀证明，由接收端验证本地文件。
    bool repair = false;
};

// 一个冻结名单的发送轮次共享一个通道；补发 Topic 不包含 receiver_id。
struct DataChannel {
    std::string transfer_id, session_id;
    bool repair = false;
};

// End 与 Data 经同一个 Writer 保序。End 的 offset 为文件长度，payload 必须为空。
struct DataBlock {
    std::string transfer_id, session_id;
    uint64_t offset = 0;
    std::vector<uint8_t> payload;
    bool end = false;
};

// Ready 返回候选断点；只有 reader_id 非空，才代表这个轮次的 Reader 已准备好。
// 同一 Prepare 重复到达时，Ready 的 next_offset 是当前已追加长度，用于识别卡死节点。
// Error.retryable 只决定是否进入补发队列，不改变正常发送游标。
struct Reply {
    Feedback state = Feedback::error;
    std::string transfer_id;
    uint64_t next_offset = 0;
    std::string error;
    std::string receiver_id{}, receiver_epoch{}, session_id{}, reader_id{};
    bool retryable = false;
};

// 核心逻辑只依赖此接口；DDS 实体、发现和样本转换由适配器负责。
class Transport {
public:
    virtual ~Transport() = default;
    virtual bool matched() = 0;
    virtual bool send(const Presence&) = 0;
    virtual bool send(const Request&) = 0;
    virtual bool send(const Reply&) = 0;
    virtual bool send(const DataBlock&) = 0;
    virtual bool take(Presence&) = 0;
    virtual bool take(Request&) = 0;
    virtual bool take(Reply&) = 0;
    virtual bool take(DataBlock&) = 0;
    virtual void open_writer(const DataChannel&) = 0;
    virtual bool writer_ready(const std::vector<std::string>& readers) = 0;
    // 让卡死节点不再拖住可靠 Writer：本发送进程此后忽略该 Reader 所属节点。
    virtual void isolate(const std::string& reader) = 0;
    virtual void close_writer() = 0;
    virtual std::string open_reader(const DataChannel&) = 0;
    virtual void close_reader(const std::string& transfer_id) = 0;
    virtual void wait(std::chrono::milliseconds timeout) = 0;
};
} // namespace file_transfer
