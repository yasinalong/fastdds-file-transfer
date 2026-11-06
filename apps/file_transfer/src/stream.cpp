#include "file_transfer/stream.hpp"

namespace file_transfer {
DataStream::DataStream(Transport& transport, DataChannel channel)
    : transport_(transport), channel_(std::move(channel)) { transport_.open_writer(channel_); }
DataStream::~DataStream() { transport_.close_writer(); }
bool DataStream::send(const FileSource& source, uint64_t offset, size_t chunk_bytes, const Pump& pump)
{
    using Clock = std::chrono::steady_clock;
    auto write = [&](const DataBlock& block) {
        const auto started = Clock::now();
        while (pump(offset, Clock::now() - started)) {
            if (transport_.send(block)) return true;
            transport_.wait(std::chrono::milliseconds(5));
        }
        return false;
    };
    while (offset < source.size()) {
        DataBlock block{channel_.transfer_id, channel_.session_id, offset, source.read(offset, chunk_bytes)};
        if (!write(block)) return false;
        offset += block.payload.size(); // 只在 write 成功后前进，已提交的数据绝不应用层重发。
    }
    source.read(source.size(), 0); // End 授权接收端发布文件，发送前最后检查源文件。
    return write({channel_.transfer_id, channel_.session_id, source.size(), {}, true});
}
} // namespace file_transfer
