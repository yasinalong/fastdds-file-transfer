#pragma once
#include "file_transfer/file.hpp"
#include "file_transfer/protocol.hpp"
#include <functional>

namespace file_transfer {
enum class ReceivePhase { waiting, receiving, completed, failed, cancelled };

// 单文件状态机；常驻接收服务管理多个独立任务，任务结束不影响监听。
class Receiver {
public:
    Receiver(std::filesystem::path directory, uint64_t max_bytes, std::function<void()> tick = {},
            std::shared_ptr<const FileSink::Checkpoint> resume = {});
    Reply handle(const Request& request);
    Reply handle(const DataBlock& block);
    Reply fail(const std::string& error, bool retryable = false);
    // 部分文件当前（或关闭时）的哈希快照，供下一轮 Prepare 免重扫。
    std::shared_ptr<const FileSink::Checkpoint> checkpoint() const { return sink_ ? sink_->checkpoint() : checkpoint_; }
    uint64_t received() const { return received_; }
    ReceivePhase phase() const { return phase_; }
    const std::string& transfer_id() const { return id_; }
    const std::string& file_name() const { return name_; }
private:
    Reply current() const;
    Reply reject(const std::string& id, const std::string& error) const;
    void close(ReceivePhase next);
    std::filesystem::path directory_;
    uint64_t max_bytes_, total_ = 0, received_ = 0;
    std::string id_, name_, digest_, error_;
    ReceivePhase phase_ = ReceivePhase::waiting;
    std::unique_ptr<FileSink> sink_;
    std::shared_ptr<const FileSink::Checkpoint> checkpoint_;
    std::function<void()> tick_; // 前缀扫描让出控制权，文件写入仍保持单线程。
};

// 单次发送的配置；没有应用层滑动窗口，缓存背压由 Transport 提供。
struct SendOptions {
    size_t chunk_bytes = max_chunk_bytes;           //分块最大字节数
    std::chrono::milliseconds timeout{30000};       //超时时间
    std::chrono::milliseconds discovery{2000};     // 首个接收端出现后的发现窗口
    size_t receivers = 0;                          // 0=发现窗口内全部接收端；非0=至少等待指定数量
};
enum class Outcome { completed, failed, cancelled, unknown };
// 每个接收端的最终报告，不承担数据发送调度。
struct ReceiverResult {
    std::string id;
    Outcome outcome;
    uint64_t resumed_bytes, confirmed_bytes;
    std::string detail;
};
struct Result {
    Outcome outcome;
    std::string detail;
    std::vector<ReceiverResult> receivers{};
};

Result send_file(Transport& transport, const FileSource& source, const std::string& id,
        const SendOptions& options, const std::function<bool()>& cancelled);
} // namespace file_transfer
