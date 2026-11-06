#pragma once
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace file_transfer
{
    std::string new_transfer_id(); // 生成新的传输 ID。

    // 只读取普通、可定位文件；发送结束前源文件内容必须保持不变。
    class FileSource
    {
    public:
        explicit FileSource(const std::filesystem::path &path, const std::function<void()>& tick = {});
        ~FileSource();
        FileSource(const FileSource &) = delete;
        FileSource &operator=(const FileSource &) = delete;
        uint64_t size() const;                                           // 打开时记录的文件长度。
        const std::string &digest() const;                               // 打开时计算的完整 SHA-256。
        const std::string &name() const;                                 // 打开时记录的 basename。
        // 长前缀按块计算，tick 允许调用方维持控制租约、响应取消；异常向调用方传播。
        std::string prefix_digest(uint64_t length, const std::function<void()>& tick = {}) const;
        std::vector<uint8_t> read(uint64_t offset, size_t length) const; // 从指定偏移读取指定长度的字节；若源文件被删除或截断，返回值可能小于请求的长度。
    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    // 接收目录的稳定身份及进程锁：跨重启识别同一接收端，并保证单进程使用该目录。
    class ReceiveDirectory
    {
    public:
        explicit ReceiveDirectory(std::filesystem::path path);
        ~ReceiveDirectory();
        const std::filesystem::path &path() const;
        const std::string &id() const;

    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };

    // 以内容哈希命名部分文件；关闭时保留进度，校验与同步成功后才发布正式文件名。
    class FileSink
    {
    public:
        // 进程内的前缀哈希快照：部分文件的 inode、长度和修改时间都未变化时，重开可跳过整段重算。
        // 不落盘，重启后仍必须按实际文件重新扫描。
        struct Checkpoint;
        FileSink(const std::filesystem::path &destination, uint64_t size, const std::string &digest,
                const std::function<void()>& tick = {}, std::shared_ptr<const Checkpoint> resume = {});
        std::shared_ptr<const Checkpoint> checkpoint() const; // 已关闭或已提交时返回空。
        ~FileSink();
        FileSink(const FileSink &) = delete;
        FileSink &operator=(const FileSink &) = delete;
        uint64_t size() const;                           // 本对象已经追加的字节数。
        bool complete() const;                           // 是否已存在校验通过的完整文件。
        std::string prefix_digest() const;               // 当前连续前缀的 SHA-256。
        void reset();                                    // 清空部分文件并重置进度；若接收目录被删除或截断，可能抛出异常。
        void append(const std::vector<uint8_t> &bytes);  // 追加写入部分文件；若接收目录被删除或截断，可能抛出异常。
        void commit(const std::string &expected_digest); // 校验部分文件哈希并同步落盘后，原子发布正式文件名；若接收目录被删除或截断，可能抛出异常。

    private:
        struct Impl;
        std::unique_ptr<Impl> impl_;
    };
} // namespace file_transfer
