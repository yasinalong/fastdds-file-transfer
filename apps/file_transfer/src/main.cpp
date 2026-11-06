#include "file_transfer/dds_transport.hpp"
#include "file_transfer/transfer.hpp"
#include "file_transfer/receiver_service.hpp"
#include <charconv>
#include <csignal>
#include <iostream>
#include <optional>
#include <stdexcept>

namespace
{
    using namespace file_transfer;
    using Clock = std::chrono::steady_clock;
    using namespace std::chrono_literals;
    volatile std::sig_atomic_t stopped = 0;

    constexpr const char *usage =
        "Usage: file_transfer send SOURCE [options] | receive [options]\n"
        "  --domain 42 --topic files --timeout 30 --transport tcp|udp|udp-multicast (default: tcp)\n"
        "Send:    --chunk-bytes 262144 --receivers 0 --discover-ms 2000\n"
        "Receive: --output out/files --idle-timeout 0 --max-transfers 8\n"
        "         --max-bytes 1099511627776\n"
        "         --multicast-group 239.255.0.2[:PORT] (udp-multicast only; default port 7401 + 250 * domain)\n"
        "File names come from senders. Idle timeout starts with no sender present; 0 waits forever.\n";

    // 命令行解析后的运行配置，供发送或接收流程使用。
    struct Options
    {
        bool sender = false;                      // 发送者还是接收者
        std::filesystem::path path = "out/files"; // 发送的文件，或接收端保存目录
        uint32_t domain = 42;                     // domain
        std::string channel = "files";            // 通道的名字
        DdsTransportMode transport = DdsTransportMode::tcp;
        std::optional<MulticastGroup> multicast;  // 接收端数据 Reader 宣告的组播地址，仅 udp-multicast
        ReceiveOptions receive;                  // 单文件限额、并发任务数与任务无进展期限
        std::chrono::seconds idle_timeout{0};     // 没有发送端在线时的等待期限；0 表示一直监听
        SendOptions send;                         // 发送配置
    };

    Options parse(int argc, char **argv)
    {
        if (argc < 2)
            throw std::invalid_argument(usage);
        const std::string role = argv[1];
        if (role != "send" && role != "receive")
            throw std::invalid_argument(usage);
        Options o;
        o.sender = role == "send";
        if (o.sender)
        {
            if (argc < 3)
                throw std::invalid_argument("Source file required");
            o.path = argv[2];
        }
        for (int i = o.sender ? 3 : 2; i < argc; i += 2)
        {
            if (i + 1 >= argc)
                throw std::invalid_argument("Missing option value");
            const std::string key = argv[i], value = argv[i + 1];
            auto number = [&](uint64_t low, uint64_t high)
            {
                uint64_t n = 0;
                const auto result = std::from_chars(value.data(), value.data() + value.size(), n);
                if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || n < low || n > high)
                    throw std::invalid_argument("Invalid value for " + key);
                return n;
            };
            if (key == "--topic" || key == "--channel")
                o.channel = value;
            else if (key == "--output" && !o.sender)
                o.path = value;
            else if (key == "--domain")
                o.domain = number(0, 232);
            else if (key == "--transport")
            {
                if (value == "tcp")
                    o.transport = DdsTransportMode::tcp;
                else if (value == "udp")
                    o.transport = DdsTransportMode::udp;
                else if (value == "udp-multicast")
                    o.transport = DdsTransportMode::udp_multicast;
                else
                    throw std::invalid_argument("--transport must be tcp, udp or udp-multicast");
            }
            else if (key == "--multicast-group" && !o.sender)
            {
                // 地址合法性（是否属于 224.0.0.0/4）由 DDS 适配器校验。
                MulticastGroup group;
                const auto colon = value.find(':');
                group.address = value.substr(0, colon);
                if (colon != std::string::npos)
                {
                    uint64_t port = 0;
                    const auto text = value.substr(colon + 1);
                    const auto result = std::from_chars(text.data(), text.data() + text.size(), port);
                    if (result.ec != std::errc{} || result.ptr != text.data() + text.size() || port < 1 || port > 65535)
                        throw std::invalid_argument("Invalid port for --multicast-group");
                    group.port = static_cast<uint16_t>(port);
                }
                o.multicast = group;
            }
            else if (key == "--timeout")
                o.receive.timeout = o.send.timeout = std::chrono::seconds(number(1, 3600));
            else if (key == "--chunk-bytes" && o.sender)
                o.send.chunk_bytes = number(1024, max_chunk_bytes);
            else if (key == "--window")
                throw std::invalid_argument("--window was removed; DDS history now provides backpressure");
            else if (key == "--receivers" && o.sender)
                o.send.receivers = number(0, 64);
            else if (key == "--discover-ms" && o.sender)
                o.send.discovery = std::chrono::milliseconds(number(0, 10000));
            else if (key == "--max-bytes" && !o.sender)
                o.receive.max_bytes = number(0, uint64_t(INT64_MAX));
            else if (key == "--max-transfers" && !o.sender)
                o.receive.max_transfers = number(1, 64);
            else if (key == "--idle-timeout" && !o.sender)
                o.idle_timeout = std::chrono::seconds(number(0, 86400));
            else
                throw std::invalid_argument("Unknown option for this role: " + key);
        }
        if (o.channel.empty() || o.channel.size() > 64 ||
            o.channel.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != std::string::npos)
            throw std::invalid_argument("Channel must contain 1..64 letters, digits, underscores or hyphens");
        if (o.multicast && o.transport != DdsTransportMode::udp_multicast)
            throw std::invalid_argument("--multicast-group requires --transport udp-multicast");
        if (o.path.empty() || (o.sender && o.path.filename().empty()))
            throw std::invalid_argument("Local file/directory path required");
        return o;
    }

    int send(const Options &o)
    {
        FileSource source(o.path);
        const auto id = new_transfer_id();
        // 发送角色写控制和文件块，读取接收端回执。
        auto transport = make_dds_transport(o.domain, o.channel, true, o.transport);
        std::cout << "SENDING id=" << id << " bytes=" << source.size() << std::endl;
        const auto result = send_file(*transport, source, id, o.send, []
                                      { return stopped != 0; });
        for (const auto &peer : result.receivers)
        {
            const char *state = peer.outcome == Outcome::completed ? "COMPLETED" : peer.outcome == Outcome::unknown ? "UNKNOWN"
                                                                               : peer.outcome == Outcome::cancelled ? "CANCELLED"
                                                                                                                    : "FAILED";
            std::cout << "RECEIVER id=" << peer.id << " state=" << state << " resumed=" << peer.resumed_bytes
                      << " confirmed=" << peer.confirmed_bytes;
            if (!peer.detail.empty())
                std::cout << " error=" << peer.detail;
            std::cout << std::endl;
        }
        switch (result.outcome)
        {
        case Outcome::completed:
            std::cout << "COMPLETED bytes=" << source.size() << " sha256=" << result.detail << std::endl;
            return 0;
        case Outcome::cancelled:
            std::cerr << "CANCELLED " << result.detail << std::endl;
            return 2;
        case Outcome::unknown:
            std::cerr << "UNKNOWN " << result.detail << std::endl;
            return 3;
        case Outcome::failed:
            std::cerr << "FAILED " << result.detail << std::endl;
            return 1;
        }
        return 1;
    }

    int receive(const Options &o)
    {
        std::filesystem::create_directories(o.path);
        auto transport = make_dds_transport(o.domain, o.channel, false, o.transport, o.multicast.value_or(MulticastGroup{}));
        ReceiverService service(o.path, o.receive, transport.get());
        IdleTimeout idle(o.idle_timeout);
        std::cout << "LISTENING directory=" << o.path << " domain=" << o.domain
                  << " topic=" << o.channel << " receiver=" << service.id() << std::endl;
        auto respond = [&](const ReceiveEvent &event)
        {
            if (event.reply.transfer_id.empty())
                return;                   // 本端未被选中，不回复该任务。
            transport->send(event.reply); // 暂时拥塞时依靠重复请求获取当前结果。
            if (!event.finished)
                return;
            const char *label = event.reply.state == Feedback::complete ? "SAVED" : event.reply.error.find("Cancelled") == 0 ? "CANCELLED"
                                                                                                                       : "FAILED";
            std::cout << label << " id=" << event.reply.transfer_id << " file=" << event.file_name
                      << " bytes=" << event.reply.next_offset;
            if (!event.reply.error.empty())
                std::cout << " error=" << event.reply.error;
            std::cout << std::endl;
        };
        // 同一事件泵用于主循环与长前缀扫描；服务禁止正在扫描的文件重入。
        auto announced = Clock::now() - 1s;
        auto pump = [&] {
            if (Clock::now() - announced >= 100ms && transport->send(service.presence())) announced = Clock::now();
            Request request;
            for (unsigned n = 0; n < 32 && transport->take(request); ++n)
                respond(service.handle(request));
            DataBlock block;
            for (unsigned n = 0; n < 32 && transport->take(block); ++n) {
                auto event = service.handle(block);
                if (!event.reply.transfer_id.empty()) respond(event);
            }
        };
        service.set_pump([&] {
            if (stopped) throw std::runtime_error("Stopped during prefix verification");
            pump();
        });
        // 文件任务结束不退出服务；任务进度期限与无人在线期限分别处理。
        while (!stopped)
        {
            pump();
            for (const auto &event : service.expire())
                respond(event);
            if (idle.expired(transport->matched()))
            {
                std::cout << "IDLE_TIMEOUT no sender present; listener stopped" << std::endl;
                return 0;
            }
            transport->wait(50ms);
        }
        std::cout << "STOPPED" << std::endl;
        return 0;
    }

} // namespace

int main(int argc, char **argv)
{
    if (argc == 2 && std::string(argv[1]) == "--help")
    {
        std::cout << usage;
        return 0;
    }
    std::signal(SIGINT, [](int)
                { stopped = 1; });
    std::signal(SIGTERM, [](int)
                { stopped = 1; });
    try
    {
        const auto options = parse(argc, argv);
        return options.sender ? send(options) : receive(options);
    }
    catch (const std::exception &e)
    {
        std::cerr << "ERROR " << e.what() << std::endl;
        return 1;
    }
}
