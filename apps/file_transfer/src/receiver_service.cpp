#include "file_transfer/receiver_service.hpp"
#include <algorithm>
#include <stdexcept>

namespace file_transfer {
ReceiverService::ReceiverService(std::filesystem::path directory, ReceiveOptions options, Transport* transport)
    : transport_(transport), directory_(std::move(directory)), options_(options)
{
    if (options_.max_transfers == 0 || options_.timeout.count() <= 0) throw std::invalid_argument("Invalid receiver options");
}
ReceiveEvent ReceiverService::event(Reply reply, std::string name, bool finished) const
{
    if (reply.transfer_id.empty()) return {};
    reply.receiver_id = directory_.id(); reply.receiver_epoch = epoch_;
    return {std::move(reply), std::move(name), finished};
}
size_t ReceiverService::active() const
{
    return std::count_if(tasks_.begin(), tasks_.end(), [](const auto& item) { return bool(item.second.receiver); });
}
void ReceiverService::forget(const std::string& id)
{
    cached_.erase(std::remove(cached_.begin(), cached_.end(), id), cached_.end());
    if (transport_) transport_->close_reader(id);
    tasks_.erase(id);
}
void ReceiverService::release(const std::string& id, Entry& entry)
{
    if (transport_) transport_->close_reader(id);
    entry.checkpoint = entry.receiver->checkpoint();
    entry.receiver.reset();
    cached_.push_back(id);
    if (cached_.size() > 64) { tasks_.erase(cached_.front()); cached_.pop_front(); }
}
ReceiveEvent ReceiverService::handle(const Request& r, ReceiveClock::time_point now)
{
    if (r.receiver_id != id()) return {};
    auto answer = [&](Reply reply, bool finished = false) {
        reply.session_id = r.session_id;
        return event(std::move(reply), r.file_name, finished);
    };
    if (r.receiver_epoch != epoch_) {
        Reply reply{Feedback::error, r.transfer_id, 0, "Receiver restarted"};
        reply.retryable = true;
        return answer(std::move(reply));
    }
    auto found = tasks_.find(r.transfer_id);
    if (hashing_.count(r.transfer_id)) {
        if (r.operation == Operation::cancel && found != tasks_.end() && r.session_id == found->second.prepare.session_id)
            cancelled_hashes_.insert(r.transfer_id);
        return {}; // 同一文件不可重入；Prepare 可重试，其他文件仍能通过事件泵工作。
    }
    std::shared_ptr<const FileSink::Checkpoint> resume;
    if (found != tasks_.end()) {
        auto& entry = found->second;
        if (r.generation < entry.prepare.generation) return {};
        if (r.operation == Operation::cancel) {
            if (r.session_id != entry.prepare.session_id || r.generation != entry.prepare.generation) return {};
            if (!entry.receiver) return answer(entry.reply);
            entry.reply = entry.receiver->handle(r);
            release(r.transfer_id, entry);
            return answer(entry.reply, true);
        }
        if (r.generation == entry.prepare.generation) {
            const auto& p = entry.prepare;
            if (r.session_id != p.session_id || r.file_name != p.file_name || r.sha256 != p.sha256 ||
                    r.total_bytes != p.total_bytes || r.offset != p.offset || r.prefix_sha256 != p.prefix_sha256 || r.repair != p.repair)
                return answer({Feedback::error, r.transfer_id, 0, "Conflicting prepare"});
            // 同一 Prepare 可用于补取丢失的最终回执，不重新打开或重置文件。
            // 传输中则报告当前追加长度：发送端据此区分卡死节点与被它拖住的健康节点。
            auto reply = entry.reply;
            if (entry.receiver) reply.next_offset = entry.receiver->received();
            return answer(std::move(reply));
        }
        resume = entry.receiver ? entry.receiver->checkpoint() : entry.checkpoint;
        forget(r.transfer_id); // 新轮次替换旧会话；generation 阻止迟到请求把它换回去。
    } else if (r.operation == Operation::cancel) return {};
    if (active() >= options_.max_transfers) return answer({Feedback::error, r.transfer_id, 0, "Receiver active transfer limit reached"});
    for (const auto& item : tasks_)
        if (item.second.receiver && item.second.prepare.file_name == r.file_name)
            return answer({Feedback::error, r.transfer_id, 0, "File name is already being received"});
    auto tick = [this, id = r.transfer_id] {
        if (pump_) pump_();
        if (cancelled_hashes_.count(id)) throw std::runtime_error("Cancelled during prefix verification");
    };
    auto& entry = tasks_[r.transfer_id];
    entry.prepare = r; entry.progress = now;
    entry.receiver = std::make_unique<Receiver>(directory_.path(), options_.max_bytes, std::move(tick), std::move(resume));
    hashing_.insert(r.transfer_id);
    entry.reply = entry.receiver->handle(r);
    hashing_.erase(r.transfer_id); cancelled_hashes_.erase(r.transfer_id);
    if (entry.reply.state == Feedback::ready && entry.reply.next_offset == r.offset) {
        try {
            entry.reply.reader_id = transport_ ? transport_->open_reader({r.transfer_id, r.session_id, r.repair}) : epoch_; // 无 DDS 时以进程身份代表 Reader。
            entry.progress = ReceiveClock::now();
            // 测试传入的时钟与业务状态时钟一致；长扫描不消耗开始发送后的进度期限。
            if (!transport_) entry.progress = now;
        } catch (const std::exception& e) { entry.reply = entry.receiver->fail(e.what()); }
    }
    const bool finished = entry.reply.state != Feedback::ready;
    if (entry.reply.reader_id.empty()) release(r.transfer_id, entry);
    return answer(entry.reply, finished);
}
ReceiveEvent ReceiverService::handle(const DataBlock& block, ReceiveClock::time_point now)
{
    auto found = tasks_.find(block.transfer_id);
    if (found == tasks_.end() || hashing_.count(block.transfer_id)) return {};
    auto& entry = found->second;
    if (entry.prepare.session_id != block.session_id) return {};
    if (!entry.receiver) return block.end ? event(entry.reply) : ReceiveEvent{};
    const auto before = entry.receiver->received();
    auto reply = entry.receiver->handle(block);
    if (entry.receiver->received() > before) entry.progress = now;
    if (reply.transfer_id.empty()) return {}; // 数据块不产生状态回执，也不做应用窗口。
    reply.session_id = block.session_id;
    entry.reply = reply;
    const auto name = entry.prepare.file_name;
    release(block.transfer_id, entry);
    return event(std::move(reply), name, true);
}
std::vector<ReceiveEvent> ReceiverService::expire(ReceiveClock::time_point now)
{
    std::vector<ReceiveEvent> events;
    std::vector<std::string> expired;
    for (auto& item : tasks_) {
        auto& entry = item.second;
        if (entry.receiver && !hashing_.count(item.first) && now - entry.progress >= options_.timeout) {
            entry.reply = entry.receiver->fail("Receive progress timeout; partial file retained", true);
            entry.reply.session_id = entry.prepare.session_id;
            events.push_back(event(entry.reply, entry.prepare.file_name, true));
            expired.push_back(item.first);
        }
    }
    for (const auto& id : expired) release(id, tasks_.at(id));
    return events;
}
bool IdleTimeout::expired(bool sender_present, ReceiveClock::time_point now)
{
    if (sender_present) absent_since_.reset();
    else if (!absent_since_) absent_since_ = now;
    return timeout_.count() > 0 && absent_since_ && now - *absent_since_ >= timeout_;
}
} // namespace file_transfer
