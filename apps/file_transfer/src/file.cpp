#include "file_transfer/file.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>

namespace file_transfer
{
    namespace
    {
        std::runtime_error io_error(const char *operation)
        {
            return std::runtime_error(std::string(operation) + ": " + std::strerror(errno));
        }

        std::string hex(const unsigned char *bytes, size_t size)
        {
            constexpr char digits[] = "0123456789abcdef";
            std::string result;
            result.reserve(size * 2);
            for (size_t i = 0; i < size; ++i)
            {
                result += digits[bytes[i] >> 4];
                result += digits[bytes[i] & 15];
            }
            return result;
        }

        class Digest
        {
        public:
            Digest() : context_(EVP_MD_CTX_new(), EVP_MD_CTX_free)
            {
                if (!context_ || EVP_DigestInit_ex(context_.get(), EVP_sha256(), nullptr) != 1)
                    throw std::runtime_error("SHA-256 initialization failed");
            }
            void update(const std::vector<uint8_t> &bytes)
            {
                if (EVP_DigestUpdate(context_.get(), bytes.data(), bytes.size()) != 1)
                    throw std::runtime_error("SHA-256 update failed");
            }
            std::string finish() const
            {
                std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> copy(EVP_MD_CTX_new(), EVP_MD_CTX_free);
                if (!copy || EVP_MD_CTX_copy_ex(copy.get(), context_.get()) != 1)
                    throw std::runtime_error("SHA-256 snapshot failed");
                std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
                unsigned size = 0;
                if (EVP_DigestFinal_ex(copy.get(), bytes.data(), &size) != 1)
                    throw std::runtime_error("SHA-256 finalization failed");
                return hex(bytes.data(), size);
            }
            Digest(const Digest &other) : Digest()
            {
                if (EVP_MD_CTX_copy_ex(context_.get(), other.context_.get()) != 1)
                    throw std::runtime_error("SHA-256 copy failed");
            }
            Digest(Digest &&) = default;
            Digest &operator=(Digest &&) = default;

        private:
            std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context_;
        };
    } // namespace

    // 随机生成一个 32 字节的十六进制字符串，作为传输任务的唯一标识符。
    std::string new_transfer_id()
    {
        std::array<unsigned char, 16> bytes{};
        if (RAND_bytes(bytes.data(), bytes.size()) != 1)
            throw std::runtime_error("Random ID generation failed");
        return hex(bytes.data(), bytes.size());
    }

    struct FileSource::Impl
    {
        int fd = -1;
        uint64_t size = 0;      // 打开时的长度快照。
        std::string digest;     // 初始内容哈希，不随外部修改自动更新。
        std::string name;       // 初始文件名。
        struct stat original{}; // 用于检测源文件在传输期间是否被修改。
        ~Impl()
        {
            if (fd >= 0)
                close(fd);
        }
    };

    FileSource::FileSource(const std::filesystem::path &path, const std::function<void()>& tick) : impl_(std::make_unique<Impl>())
    {
        impl_->name = path.filename().string();
        impl_->fd = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
        if (impl_->fd < 0)
            throw io_error("Open source");
        struct stat info{};
        if (fstat(impl_->fd, &info) != 0)
            throw io_error("Stat source");
        if (!S_ISREG(info.st_mode) || info.st_size < 0)
            throw std::runtime_error("Source must be a regular file");
        impl_->size = static_cast<uint64_t>(info.st_size);
        impl_->original = info;
        // 先流式计算整文件哈希，用作内容身份及最终校验值；内存只保留当前块。
        Digest digest;
        for (uint64_t offset = 0; offset < size();)
        {
            if (tick) tick();
            auto bytes = read(offset, 256 * 1024);
            digest.update(bytes);
            offset += bytes.size();
        }
        impl_->digest = digest.finish();
    }

    FileSource::~FileSource() = default;
    uint64_t FileSource::size() const { return impl_->size; }
    const std::string &FileSource::digest() const { return impl_->digest; }
    const std::string &FileSource::name() const { return impl_->name; }

    std::string FileSource::prefix_digest(uint64_t length, const std::function<void()>& tick) const
    {
        if (length > size())
            throw std::runtime_error("Prefix exceeds source size");
        if (length == size())
            return digest();
        Digest result;
        for (uint64_t offset = 0; offset < length;)
        {
            if (tick) tick();
            const auto bytes = read(offset, static_cast<size_t>(std::min<uint64_t>(256 * 1024, length - offset)));
            result.update(bytes);
            offset += bytes.size();
        }
        return result.finish();
    }

    std::vector<uint8_t> FileSource::read(uint64_t offset, size_t length) const
    {
        if (offset > size())
            throw std::runtime_error("Read offset exceeds file size");
        struct stat current{};
        if (fstat(impl_->fd, &current) != 0)
            throw io_error("Stat source");
#ifdef __APPLE__
        const bool changed_time = current.st_mtimespec.tv_sec != impl_->original.st_mtimespec.tv_sec ||
                                  current.st_mtimespec.tv_nsec != impl_->original.st_mtimespec.tv_nsec;
#else
        const bool changed_time = current.st_mtim.tv_sec != impl_->original.st_mtim.tv_sec ||
                                  current.st_mtim.tv_nsec != impl_->original.st_mtim.tv_nsec;
#endif
        // 避免一次传输混入源文件修改前后的内容；接收端仍会核对整文件 SHA-256。
        if (current.st_size != impl_->original.st_size || changed_time)
            throw std::runtime_error("Source changed during transfer");
        std::vector<uint8_t> bytes(static_cast<size_t>(std::min<uint64_t>(length, size() - offset)));
        size_t done = 0;
        while (done < bytes.size())
        {
            auto n = pread(impl_->fd, bytes.data() + done, bytes.size() - done, static_cast<off_t>(offset + done));
            if (n > 0)
                done += static_cast<size_t>(n);
            else if (n < 0 && errno == EINTR)
                continue;
            else if (n == 0)
                throw std::runtime_error("Source shortened during transfer");
            else
                throw io_error("Read source");
        }
        return bytes;
    }

    namespace
    {
        constexpr uint64_t checkpoint_bytes = 4 * 1024 * 1024;
        constexpr uint64_t disk_headroom = 16 * 1024 * 1024;

        std::filesystem::path cache_directory(const std::filesystem::path &directory)
        {
            std::filesystem::create_directories(directory);
            const auto cache = directory / ".file_transfer";
            std::filesystem::create_directory(cache);
            struct stat info{};
            if (lstat(cache.c_str(), &info) != 0 || !S_ISDIR(info.st_mode))
                throw std::runtime_error("Receive cache must be a real directory");
            return cache;
        }

        void sync_directory(const std::filesystem::path &directory)
        {
            const int fd = open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            if (fd < 0)
                throw io_error("Open directory for sync");
            const int result = fsync(fd);
            const int error = errno;
            close(fd);
            if (result != 0)
            {
                errno = error;
                throw io_error("Sync directory");
            }
        }

        struct FileStamp
        {
            dev_t device = 0;
            ino_t inode = 0;
            off_t size = 0;
            timespec modified{};
            bool operator==(const FileStamp &other) const
            {
                return device == other.device && inode == other.inode && size == other.size &&
                       modified.tv_sec == other.modified.tv_sec && modified.tv_nsec == other.modified.tv_nsec;
            }
        };

        FileStamp stamp(const struct stat &info)
        {
#ifdef __APPLE__
            return {info.st_dev, info.st_ino, info.st_size, info.st_mtimespec};
#else
            return {info.st_dev, info.st_ino, info.st_size, info.st_mtim};
#endif
        }

        void hash_prefix(int fd, uint64_t size, Digest &digest, const std::function<void()>& tick)
        {
            for (uint64_t offset = 0; offset < size;)
            {
                if (tick) tick();
                std::vector<uint8_t> bytes(static_cast<size_t>(std::min<uint64_t>(256 * 1024, size - offset)));
                auto n = pread(fd, bytes.data(), bytes.size(), static_cast<off_t>(offset));
                if (n < 0 && errno == EINTR)
                    continue;
                if (n <= 0)
                    throw std::runtime_error("Cannot read saved partial file");
                bytes.resize(static_cast<size_t>(n));
                digest.update(bytes);
                offset += bytes.size();
            }
        }
    } // namespace

    struct ReceiveDirectory::Impl
    {
        int fd = -1;
        std::filesystem::path path;
        std::string id;
        ~Impl()
        {
            if (fd >= 0)
                close(fd);
        }
    };

    ReceiveDirectory::ReceiveDirectory(std::filesystem::path path) : impl_(std::make_unique<Impl>())
    {
        impl_->path = std::move(path);
        const auto cache = cache_directory(impl_->path);
        impl_->fd = open((cache / "receiver.id").c_str(), O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
        if (impl_->fd < 0)
            throw io_error("Open receiver identity");
        // 身份文件跨重启保留；文件锁随进程释放，阻止两个服务同时使用同一保存目录。
        if (flock(impl_->fd, LOCK_EX | LOCK_NB) != 0)
            throw std::runtime_error("Receive directory is already used by another receiver");
        struct stat info{};
        if (fstat(impl_->fd, &info) != 0 || !S_ISREG(info.st_mode) || info.st_nlink != 1)
            throw std::runtime_error("Invalid receiver identity file");
        if (info.st_size == 0)
        {
            impl_->id = new_transfer_id();
            if (::write(impl_->fd, impl_->id.data(), impl_->id.size()) != static_cast<ssize_t>(impl_->id.size()) || fsync(impl_->fd) != 0)
                throw io_error("Save receiver identity");
            sync_directory(cache);
            sync_directory(impl_->path);
        }
        else
        {
            char id[32];
            if (info.st_size != 32 || pread(impl_->fd, id, sizeof(id), 0) != sizeof(id))
                throw std::runtime_error("Invalid receiver identity file");
            impl_->id.assign(id, sizeof(id));
            if (impl_->id.find_first_not_of("0123456789abcdef") != std::string::npos)
                throw std::runtime_error("Invalid receiver identity file");
        }
    }
    ReceiveDirectory::~ReceiveDirectory() = default;
    const std::filesystem::path &ReceiveDirectory::path() const { return impl_->path; }
    const std::string &ReceiveDirectory::id() const { return impl_->id; }

    struct FileSink::Impl
    {
        int fd = -1;
        std::filesystem::path destination, temporary, cache;
        uint64_t expected = 0, written = 0, synced = 0;
        bool complete = false;
        Digest digest;
        std::chrono::steady_clock::time_point last_sync = std::chrono::steady_clock::now();
        ~Impl()
        {
            if (fd >= 0)
                close(fd);
        } // 关闭时保留部分文件，取消、超时或退出后都可续传。
    };

    struct FileSink::Checkpoint
    {
        std::filesystem::path partial;
        FileStamp file;
        Digest digest;
    };

    FileSink::FileSink(const std::filesystem::path &destination, uint64_t size, const std::string &digest,
            const std::function<void()>& tick, std::shared_ptr<const Checkpoint> resume)
        : impl_(std::make_unique<Impl>())
    {
        if (size > static_cast<uint64_t>(std::numeric_limits<off_t>::max()))
            throw std::runtime_error("File exceeds platform size limit");
        if (digest.size() != 64 || digest.find_first_not_of("0123456789abcdef") != std::string::npos)
            throw std::runtime_error("Invalid file digest");
        impl_->destination = destination;
        impl_->expected = size;
        const auto directory = destination.parent_path().empty() ? std::filesystem::path(".") : destination.parent_path();
        impl_->cache = cache_directory(directory);
        impl_->temporary = impl_->cache / (digest + ".part");
        struct stat info{};
        if (lstat(destination.c_str(), &info) == 0)
        {
            if (S_ISREG(info.st_mode) && static_cast<uint64_t>(info.st_size) == size)
            {
                FileSource existing(destination, tick);
                if (existing.digest() == digest)
                {
                    // 恢复“正式名称已发布、部分名称尚未删除”时的崩溃，只清理同一 inode 的残留链接。
                    const int partial = open(impl_->temporary.c_str(), O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
                    if (partial >= 0)
                    {
                        struct stat saved{};
                        if (flock(partial, LOCK_EX | LOCK_NB) == 0 && fstat(partial, &saved) == 0 &&
                            saved.st_dev == info.st_dev && saved.st_ino == info.st_ino)
                            unlink(impl_->temporary.c_str());
                        close(partial);
                    }
                    impl_->written = size;
                    impl_->complete = true;
                    return;
                }
            }
            throw std::runtime_error("Destination already exists with different content or type");
        }
        if (errno != ENOENT)
            throw io_error("Inspect destination");
        impl_->fd = open(impl_->temporary.c_str(), O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK, 0600);
        if (impl_->fd < 0)
            throw io_error("Open partial file");
        if (fstat(impl_->fd, &info) != 0 || !S_ISREG(info.st_mode) || info.st_nlink != 1)
            throw std::runtime_error("Partial file must be a private regular file");
        if (flock(impl_->fd, LOCK_EX | LOCK_NB) != 0)
            throw std::runtime_error("Same content is already being received");
        if (info.st_size < 0 || static_cast<uint64_t>(info.st_size) > size)
            reset();
        else if (resume && resume->partial == impl_->temporary && resume->file == stamp(info))
        {
            // 本进程刚关闭的同一部分文件且未被改动：沿用哈希状态，避免排队后再次整段扫描。
            impl_->written = static_cast<uint64_t>(info.st_size);
            impl_->digest = Digest(resume->digest);
            impl_->synced = impl_->written;
        }
        else
        {
            impl_->written = static_cast<uint64_t>(info.st_size);
            // 不持久化独立进度表：读取实际文件长度、重算前缀哈希，交给发送端验证断点。
            hash_prefix(impl_->fd, impl_->written, impl_->digest, tick);
            impl_->synced = impl_->written;
        }
        const auto remaining = size - impl_->written;
        if (remaining && std::filesystem::space(directory).available < remaining + disk_headroom)
            throw std::runtime_error("Insufficient disk space for remaining file and reserve");
        sync_directory(impl_->cache);
    }

    FileSink::~FileSink() = default;
    uint64_t FileSink::size() const { return impl_->written; }
    bool FileSink::complete() const { return impl_->complete; }
    std::string FileSink::prefix_digest() const { return impl_->digest.finish(); }
    std::shared_ptr<const FileSink::Checkpoint> FileSink::checkpoint() const
    {
        struct stat info{};
        if (impl_->complete || impl_->fd < 0 || fstat(impl_->fd, &info) != 0 ||
                static_cast<uint64_t>(info.st_size) != impl_->written)
            return {}; // 写入异常后长度与哈希状态可能不一致，下次按实际文件重扫。
        return std::make_shared<const Checkpoint>(Checkpoint{impl_->temporary, stamp(info), impl_->digest});
    }

    void FileSink::reset()
    {
        if (impl_->complete)
            throw std::runtime_error("Cannot reset a committed file");
        if (ftruncate(impl_->fd, 0) != 0 || fsync(impl_->fd) != 0)
            throw io_error("Reset partial file");
        impl_->written = impl_->synced = 0;
        impl_->digest = Digest{};
    }

    void FileSink::append(const std::vector<uint8_t> &bytes)
    {
        if (impl_->complete || bytes.size() > impl_->expected - impl_->written)
            throw std::runtime_error("Data exceeds declared file size");
        if (std::filesystem::space(impl_->cache).available < disk_headroom + bytes.size())
            throw std::runtime_error("Insufficient disk space; partial file retained");
        // 按顺序直接写入部分文件；系统调用可能短写或被信号打断，必须写完当前块。
        size_t done = 0;
        while (done < bytes.size())
        {
            auto n = pwrite(impl_->fd, bytes.data() + done, bytes.size() - done, static_cast<off_t>(impl_->written + done));
            if (n > 0)
                done += static_cast<size_t>(n);
            else if (n < 0 && errno == EINTR)
                continue;
            else
                throw io_error("Write partial file");
        }
        impl_->digest.update(bytes);
        impl_->written += bytes.size();
        const auto now = std::chrono::steady_clock::now();
        // 批量同步兼顾吞吐和恢复：进度回执表示已写入，不保证每块都已跨过持久化屏障。
        if (impl_->written - impl_->synced >= checkpoint_bytes || now - impl_->last_sync >= std::chrono::milliseconds(250))
        {
            if (fsync(impl_->fd) != 0)
                throw io_error("Sync partial file");
            impl_->synced = impl_->written;
            impl_->last_sync = now;
        }
    }

    void FileSink::commit(const std::string &expected_digest)
    {
        if (impl_->complete)
            return;
        if (size() != impl_->expected)
            throw std::runtime_error("File is incomplete");
        if (impl_->digest.finish() != expected_digest)
        {
            reset();
            throw std::runtime_error("SHA-256 mismatch; partial file reset");
        }
        if (fsync(impl_->fd) != 0)
            throw io_error("Sync completed file");
        // 哈希通过且文件已同步后，用硬链接原子发布原名；目标已存在则失败，不覆盖。
        // 先同步正式名称所在目录，再删除部分名称；整个过程保持文件锁。
        if (link(impl_->temporary.c_str(), impl_->destination.c_str()) != 0)
            throw io_error("Publish destination");
        sync_directory(impl_->destination.parent_path().empty() ? std::filesystem::path(".") : impl_->destination.parent_path());
        impl_->complete = true;
        if (unlink(impl_->temporary.c_str()) == 0)
            sync_directory(impl_->cache);
    }
} // namespace file_transfer
