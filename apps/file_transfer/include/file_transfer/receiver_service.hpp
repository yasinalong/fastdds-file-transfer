#pragma once
#include "file_transfer/transfer.hpp"
#include <deque>
#include <map>
#include <optional>
#include <set>

namespace file_transfer {
using ReceiveClock = std::chrono::steady_clock;
struct ReceiveOptions {
    uint64_t max_bytes = 1ULL << 40;
    size_t max_transfers = 8;
    std::chrono::milliseconds timeout{30000};
};
struct ReceiveEvent {
    Reply reply;
    std::string file_name;
    bool finished = false; // 首次结束会话时输出事件，重复 Prepare 只返回缓存结果。
};

// 只管理本地文件会话、通道生命周期及有界结果缓存，不调度补发。
class ReceiverService {
public:
    ReceiverService(std::filesystem::path directory, ReceiveOptions options, Transport* transport = nullptr);
    ReceiveEvent handle(const Request&, ReceiveClock::time_point now = ReceiveClock::now());
    ReceiveEvent handle(const DataBlock&, ReceiveClock::time_point now = ReceiveClock::now());
    std::vector<ReceiveEvent> expire(ReceiveClock::time_point now = ReceiveClock::now());
    size_t active() const;
    const std::string& id() const { return directory_.id(); }
    Presence presence() const { return {directory_.id(), epoch_}; }
    void set_pump(std::function<void()> pump) { pump_ = std::move(pump); }
private:
    // 有 receiver 表示持有文件资源；只有快照/结果的条目不保留文件锁或 Reader。
    // checkpoint 只是进程内的哈希快照，用于下一轮 Prepare 免重扫，不代表任何进度承诺。
    struct Entry {
        Request prepare;
        Reply reply;
        std::unique_ptr<Receiver> receiver;
        std::shared_ptr<const FileSink::Checkpoint> checkpoint;
        ReceiveClock::time_point progress;
    };
    ReceiveEvent event(Reply reply, std::string name = {}, bool finished = false) const;
    void release(const std::string& id, Entry& entry);
    void forget(const std::string& id);
    Transport* transport_;
    ReceiveDirectory directory_;
    ReceiveOptions options_;
    std::string epoch_ = new_transfer_id();
    std::map<std::string, Entry> tasks_;
    std::deque<std::string> cached_;
    std::function<void()> pump_;
    std::set<std::string> hashing_, cancelled_hashes_;
};
class IdleTimeout {
public:
    explicit IdleTimeout(std::chrono::milliseconds timeout) : timeout_(timeout) {}
    bool expired(bool sender_present, ReceiveClock::time_point now = ReceiveClock::now());
private:
    std::chrono::milliseconds timeout_;
    std::optional<ReceiveClock::time_point> absent_since_;
};
} // namespace file_transfer
