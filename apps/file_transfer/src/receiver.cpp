#include "file_transfer/transfer.hpp"
#include <algorithm>
#include <stdexcept>

namespace file_transfer {
namespace {
bool is_hex(const std::string& value, size_t length)
{
    return value.size() == length && value.find_first_not_of("0123456789abcdef") == std::string::npos;
}
}
Receiver::Receiver(std::filesystem::path directory, uint64_t max_bytes, std::function<void()> tick,
        std::shared_ptr<const FileSink::Checkpoint> resume)
    : directory_(std::move(directory)), max_bytes_(max_bytes), checkpoint_(std::move(resume)), tick_(std::move(tick)) {}
Reply Receiver::current() const
{
    return {phase_ == ReceivePhase::completed ? Feedback::complete :
        phase_ == ReceivePhase::receiving ? Feedback::ready : Feedback::error, id_, received_, error_};
}
Reply Receiver::reject(const std::string& id, const std::string& error) const
{
    return {Feedback::error, id, received_, error.substr(0, 256)};
}
// 释放文件锁前记下哈希快照；之后重新 Prepare 时文件未变即可免扫描。
void Receiver::close(ReceivePhase next)
{
    if (sink_) {
        try { checkpoint_ = sink_->checkpoint(); } catch (...) { checkpoint_.reset(); }
        sink_.reset();
    }
    phase_ = next;
}
Reply Receiver::fail(const std::string& error, bool retryable)
{
    if (phase_ == ReceivePhase::receiving) {
        error_ = error.substr(0, 256);
        close(ReceivePhase::failed);
    }
    auto reply = current();
    reply.retryable = retryable;
    return reply;
}
Reply Receiver::handle(const Request& r)
{
    if (!is_hex(r.transfer_id, 32)) return reject(r.transfer_id, "Invalid transfer ID");
    if (r.operation == Operation::cancel) {
        if (r.transfer_id != id_) return reject(r.transfer_id, "Prepare required");
        if (phase_ == ReceivePhase::receiving) {
            error_ = "Cancelled; partial file retained";
            close(ReceivePhase::cancelled);
        }
        return current();
    }
    if (phase_ != ReceivePhase::waiting) {
        if (r.transfer_id != id_ || r.total_bytes != total_ || r.sha256 != digest_ || r.file_name != name_)
            return reject(r.transfer_id, "Conflicting prepare");
        return current();
    }
    if (!is_hex(r.sha256, 64)) return reject(r.transfer_id, "Invalid SHA-256");
    if (r.file_name.empty() || r.file_name.size() > 255 || r.file_name == "." || r.file_name == ".." ||
            r.file_name.find_first_of("/\\") != std::string::npos ||
            std::any_of(r.file_name.begin(), r.file_name.end(), [](unsigned char c) { return c < 32 || c == 127; }))
        return reject(r.transfer_id, "Invalid file name: basename required");
    id_ = r.transfer_id; name_ = r.file_name; total_ = r.total_bytes; digest_ = r.sha256;
    try {
        if (total_ > max_bytes_) throw std::runtime_error("File exceeds receiver limit");
        if (r.offset > total_) throw std::runtime_error("Invalid offset");
        sink_ = std::make_unique<FileSink>(directory_ / name_, total_, digest_, tick_, std::move(checkpoint_));
        received_ = sink_->size();
        phase_ = sink_->complete() ? ReceivePhase::completed : ReceivePhase::receiving;
        if (phase_ == ReceivePhase::completed) sink_.reset();
        else if (r.repair && received_ == r.offset && sink_->prefix_digest() != r.prefix_sha256) {
            // 补发起点相同但前缀损坏：本地清零，Ready(0) 让调度器归入从零补发组。
            // 不再增加 RESET 指令及其确认状态。
            sink_->reset(); received_ = 0;
        }
    } catch (const std::exception& e) {
        sink_.reset(); error_ = e.what(); phase_ = ReceivePhase::failed;
    }
    return current();
}
Reply Receiver::handle(const DataBlock& block)
{
    if (block.transfer_id != id_) return reject(block.transfer_id, "Wrong transfer");
    if (phase_ != ReceivePhase::receiving) return current();
    try {
        if (block.end) {
            if (!block.payload.empty() || block.offset != total_) throw std::runtime_error("Invalid End marker");
            if (received_ != total_) return fail("Incomplete stream; replay required", true);
            // End 排在同一 Writer 的所有 Data 之后；收到它即可校验、同步和发布。
            sink_->commit(digest_); sink_.reset(); checkpoint_.reset(); phase_ = ReceivePhase::completed;
            return current();
        }
        if (block.payload.empty() || block.payload.size() > max_chunk_bytes ||
                block.offset > total_ || block.payload.size() > total_ - block.offset)
            throw std::runtime_error("Invalid data range");
        if (block.offset > received_) return fail("Data gap; replay required", true);
        if (block.offset + block.payload.size() > received_) {
            if (block.offset < received_)
                sink_->append(std::vector<uint8_t>(block.payload.begin() + (received_ - block.offset), block.payload.end()));
            else sink_->append(block.payload);
            received_ = sink_->size();
        }
        return {}; // 正常 Data 不产生应用层进度回执。
    } catch (const std::exception& e) { return fail(e.what()); }
}
} // namespace file_transfer
