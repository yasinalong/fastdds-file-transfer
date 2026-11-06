#pragma once
#include "file_transfer/transfer.hpp"

namespace file_transfer {
// 一条有序数据流的资源与顺序写入器；不理解节点加入、断点分组或文件结果。
// pump 在写入之间处理发现、取消和错误，返回是否继续；参数是 DDS 已接受的位置和当前块被背压的时长。
// 是否放弃由调用方根据节点进度决定，DataStream 本身没有超时策略。
class DataStream {
public:
    using Pump = std::function<bool(uint64_t written, std::chrono::steady_clock::duration blocked)>;
    DataStream(Transport& transport, DataChannel channel);
    ~DataStream();
    DataStream(const DataStream&) = delete;
    DataStream& operator=(const DataStream&) = delete;
    bool send(const FileSource& source, uint64_t offset, size_t chunk_bytes, const Pump& pump);
private:
    Transport& transport_;
    DataChannel channel_;
};
} // namespace file_transfer
