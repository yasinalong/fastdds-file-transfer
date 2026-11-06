#include "file_transfer/dds_transport.hpp"
#include "FileTransferPubSubTypes.hpp"
#include <fastdds/dds/core/condition/WaitSet.hpp>
#include <fastdds/dds/core/condition/StatusCondition.hpp>
#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/rtps/transport/shared_mem/SharedMemTransportDescriptor.hpp>
#include <fastdds/utils/IPLocator.hpp>
#include <algorithm>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>

namespace file_transfer {
namespace dds = eprosima::fastdds::dds;
namespace rtps = eprosima::fastdds::rtps;
namespace {
// 校验 create_* 返回的实体指针：Fast DDS 用 nullptr 表示创建失败，这里统一转成异常。
template<class T> T* require(T* value)
{
    if (!value) throw std::runtime_error("DDS entity creation failed");
    return value;
}
// 不允许失败的 DDS 调用统一走这里，非 RETCODE_OK 时抛出带返回码的异常。
void check(dds::ReturnCode_t result)
{
    if (result != dds::RETCODE_OK) throw std::runtime_error("DDS operation failed: " + std::to_string(result));
}
// 把 16 字节实例句柄（即实体 GUID）编码成 32 位小写十六进制，作为跨进程传递的 Reader 身份。
std::string endpoint_id(const dds::InstanceHandle_t& handle)
{
    const char* hex = "0123456789abcdef";
    std::string result;
    for (unsigned i = 0; i < 16; ++i) {
        result += hex[handle.value[i] >> 4];
        result += hex[handle.value[i] & 15];
    }
    return result;
}
// 校验字符串是否为 32 位小写十六进制身份；拼进 Topic 名或解析成 GUID 前必须先过这一关。
bool identity(const std::string& value)
{
    return value.size() == 32 && value.find_first_not_of("0123456789abcdef") == std::string::npos;
}
// 按模式配置参与者传输。udp / udp_multicast：纯 UDPv4，限制 RTPS 报文大小以避免 IP 分片；
// tcp：LARGE_DATA（UDP 发现 + TCP 单播数据），并移除共享内存传输及 SHM 定位器，同机进程也走 TCP。
void configure_transport(dds::DomainParticipantQos& qos, DdsTransportMode mode)
{
    if (mode != DdsTransportMode::tcp) {
        rtps::BuiltinTransportsOptions options;
        // 1400 字节 UDP 负载加 IPv4/UDP 首部仍低于常见的 1500 字节 MTU。
        options.maxMessageSize = 1400;
        qos.setup_transports(rtps::BuiltinTransports::UDPv4, options);
        return;
    }

    qos.setup_transports(rtps::BuiltinTransports::LARGE_DATA);
    auto& transports = qos.transport().user_transports;
    transports.erase(std::remove_if(transports.begin(), transports.end(), [](const auto& item) {
        return std::dynamic_pointer_cast<rtps::SharedMemTransportDescriptor>(item) != nullptr;
    }), transports.end());
    rtps::LocatorList_t locators;
    for (const auto& locator : qos.wire_protocol().default_unicast_locator_list)
        if (locator.kind != LOCATOR_KIND_SHM) locators.push_back(locator);
    qos.wire_protocol().default_unicast_locator_list = locators;
}
// 把组播配置转成 UDPv4 locator；地址必须在 224.0.0.0/4 内，端口 0 取 RTPS 默认用户组播端口。
rtps::Locator_t multicast_locator(uint32_t domain, const MulticastGroup& group)
{
    rtps::Locator_t locator; // 默认 kind 为 UDPv4。
    if (!rtps::IPLocator::setIPv4(locator, group.address) || !rtps::IPLocator::isMulticast(locator))
        throw std::invalid_argument("Multicast group must be an IPv4 address in 224.0.0.0/4: " + group.address);
    const uint32_t port = group.port ? group.port : 7400 + 250 * domain + 1;
    if (port > 65535) throw std::invalid_argument("Multicast port exceeds 65535 for this domain");
    locator.port = port;
    return locator;
}
// DDS 实体和缓存归适配器所有；文件逻辑只看到可重试的 write 和不透明 Reader 身份。
class DdsTransport final : public Transport {
public:
    // 创建参与者、注册四种线上类型，并按角色建立端点：
    // 发送端写 control、读 reply 和 presence；接收端写 reply 和 presence、读 control。
    // 数据 Topic 按传输任务动态创建，见 open_writer / open_reader。任一步失败都会回滚已创建的实体。
    DdsTransport(uint32_t domain, const std::string& channel, bool sender, DdsTransportMode mode,
            const MulticastGroup& group)
        : sender_(sender), prefix_("file_transfer/" + channel)
    {
        // 只有接收端的数据 Reader 宣告组播地址；发送端 Writer 由 Fast DDS 按匹配 Reader 的 locator 自动选择。
        if (mode == DdsTransportMode::udp_multicast && !sender)
            data_multicast_ = multicast_locator(domain, group);
        try {
            dds::DomainParticipantQos qos;
            configure_transport(qos, mode);
            participant_ = require(dds::DomainParticipantFactory::get_instance()->create_participant(domain, qos));
            control_type_ = dds::TypeSupport(new filewire::ControlPubSubType());
            reply_type_ = dds::TypeSupport(new filewire::ReplyPubSubType());
            data_type_ = dds::TypeSupport(new filewire::DataBlockPubSubType());
            presence_type_ = dds::TypeSupport(new filewire::PresencePubSubType());
            check(control_type_.register_type(participant_));
            check(reply_type_.register_type(participant_));
            check(data_type_.register_type(participant_));
            check(presence_type_.register_type(participant_));
            auto* control = require(participant_->create_topic(prefix_ + "/control", control_type_.get_type_name(), dds::TOPIC_QOS_DEFAULT));
            auto* reply = require(participant_->create_topic(prefix_ + "/reply", reply_type_.get_type_name(), dds::TOPIC_QOS_DEFAULT));
            publisher_ = require(participant_->create_publisher(dds::PUBLISHER_QOS_DEFAULT));
            subscriber_ = require(participant_->create_subscriber(dds::SUBSCRIBER_QOS_DEFAULT));
            writer_ = require(publisher_->create_datawriter(sender ? control : reply, writer_qos()));
            reader_ = require(subscriber_->create_datareader(sender ? reply : control, reader_qos()));
            attach(reader_);
            auto* presence = require(participant_->create_topic(prefix_ + "/presence", presence_type_.get_type_name(), dds::TOPIC_QOS_DEFAULT));
            if (sender_) {
                presence_reader_ = require(subscriber_->create_datareader(presence, presence_reader_qos()));
                attach(presence_reader_);
            } else presence_writer_ = require(publisher_->create_datawriter(presence, presence_writer_qos()));
        } catch (...) { cleanup(); throw; }
    }
    // 析构时释放全部 DDS 实体和参与者。
    ~DdsTransport() override { cleanup(); }
    // 控制通道是否已与对端匹配：总要求 Reader 有匹配；发送端还要求控制 Writer 有匹配。
    bool matched() override
    {
        dds::PublicationMatchedStatus writers;
        dds::SubscriptionMatchedStatus readers;
        check(writer_->get_publication_matched_status(writers));
        check(reader_->get_subscription_matched_status(readers));
        return readers.current_count > 0 && (!sender_ || writers.current_count > 0);
    }
    // 接收端发布在线心跳（receiver_id 与 receiver_epoch），best-effort，返回 false 表示本次未入队。
    bool send(const Presence& p) override
    {
        if (!presence_writer_) throw std::logic_error("Sender cannot announce receiver presence");
        filewire::Presence wire;
        wire.receiver_id(p.receiver_id); wire.receiver_epoch(p.receiver_epoch);
        return write(presence_writer_, wire);
    }
    // 发送端取一条接收端心跳；没有新样本时返回 false。
    bool take(Presence& p) override
    {
        if (!presence_reader_) throw std::logic_error("Receiver cannot take presence");
        filewire::Presence wire;
        if (!read(presence_reader_, wire)) return false;
        p = {wire.receiver_id().c_str(), wire.receiver_epoch().c_str()};
        return true;
    }
    // 发送端下发控制请求；先按 IDL 的字符串上界检查，避免越界数据进入序列化。
    bool send(const Request& r) override
    {
        if (!sender_) throw std::logic_error("Receiver cannot send control");
        if (r.transfer_id.size() > 64 || r.sha256.size() > 64 || r.file_name.size() > 255 ||
                r.receiver_id.size() > 64 || r.receiver_epoch.size() > 64 || r.session_id.size() > 64 || r.prefix_sha256.size() > 64)
            throw std::runtime_error("Control exceeds wire bounds");
        filewire::Control wire;
        wire.operation(static_cast<filewire::Operation>(r.operation));
        wire.transfer_id(r.transfer_id); wire.total_bytes(r.total_bytes); wire.offset(r.offset);
        wire.sha256(r.sha256); wire.file_name(r.file_name); wire.receiver_id(r.receiver_id);
        wire.receiver_epoch(r.receiver_epoch); wire.session_id(r.session_id); wire.repair(r.repair);
        wire.generation(r.generation); wire.prefix_sha256(r.prefix_sha256);
        return write(writer_, wire);
    }
    // 接收端回复控制请求；错误信息截断到线上上限 256 字节。
    bool send(const Reply& r) override
    {
        if (sender_) throw std::logic_error("Sender cannot send reply");
        filewire::Reply wire;
        wire.state(static_cast<filewire::Feedback>(r.state)); wire.transfer_id(r.transfer_id);
        wire.next_offset(r.next_offset); wire.error(r.error.substr(0, 256));
        wire.receiver_id(r.receiver_id); wire.receiver_epoch(r.receiver_epoch);
        wire.session_id(r.session_id); wire.reader_id(r.reader_id); wire.retryable(r.retryable);
        return write(writer_, wire);
    }
    // 发送端在当前数据通道写一个文件块；块必须属于 open_writer 打开的传输任务和会话。
    bool send(const DataBlock& block) override
    {
        if (!data_writer_ || block.transfer_id != writing_.transfer_id || block.session_id != writing_.session_id)
            throw std::logic_error("Data writer session mismatch");
        if (block.payload.size() > max_chunk_bytes) throw std::runtime_error("Data exceeds wire bounds");
        filewire::DataBlock wire;
        wire.transfer_id(block.transfer_id); wire.session_id(block.session_id);
        wire.offset(block.offset); wire.payload(block.payload); wire.end(block.end);
        return write(data_writer_, wire);
    }
    // 接收端取一条控制请求，并校验枚举取值，防止非法 operation 进入业务层。
    bool take(Request& r) override
    {
        if (sender_) throw std::logic_error("Sender cannot take control");
        filewire::Control wire;
        if (!read(reader_, wire)) return false;
        if (wire.operation() < filewire::Operation::PREPARE || wire.operation() > filewire::Operation::CANCEL)
            throw std::runtime_error("Invalid wire operation");
        r.operation = static_cast<Operation>(wire.operation());
        r.transfer_id = wire.transfer_id(); r.total_bytes = wire.total_bytes(); r.offset = wire.offset();
        r.sha256 = wire.sha256(); r.file_name = std::string(wire.file_name().c_str(), wire.file_name().size()); r.receiver_id = wire.receiver_id();
        r.receiver_epoch = wire.receiver_epoch(); r.session_id = wire.session_id(); r.repair = wire.repair();
        r.generation = wire.generation(); r.prefix_sha256 = wire.prefix_sha256();
        return true;
    }
    // 发送端取一条接收端回复，并校验枚举取值。
    bool take(Reply& r) override
    {
        if (!sender_) throw std::logic_error("Receiver cannot take reply");
        filewire::Reply wire;
        if (!read(reader_, wire)) return false;
        if (wire.state() < filewire::Feedback::READY || wire.state() > filewire::Feedback::ERROR)
            throw std::runtime_error("Invalid wire state");
        r = {static_cast<Feedback>(wire.state()), wire.transfer_id().c_str(), wire.next_offset(), wire.error().c_str(),
            wire.receiver_id().c_str(), wire.receiver_epoch().c_str(), wire.session_id().c_str(), wire.reader_id().c_str(), wire.retryable()};
        return true;
    }
    // 接收端从所有已打开的数据 Reader 中轮询取一个文件块；全部无数据时返回 false。
    bool take(DataBlock& block) override
    {
        if (sender_) throw std::logic_error("Sender cannot take data");
        if (streams_.empty()) return false;
        // 轮询起点前移，防止一个大文件持续占满服务循环，饿死其他发送端。
        auto it = streams_.upper_bound(last_read_);
        for (size_t n = 0; n < streams_.size(); ++n) {
            if (it == streams_.end()) it = streams_.begin();
            auto& stream = it->second;
            filewire::DataBlock wire;
            last_read_ = it->first;
            ++it;
            if (!read(stream.reader, wire)) continue;
            // 在业务过滤前计数：验收可以证明旁路样本没有进入非目标端 Reader。
            stream.bytes += wire.payload().size();
            ++stream.samples;
            block = {wire.transfer_id().c_str(), wire.session_id().c_str(), wire.offset(), wire.payload(), wire.end()};
            return true;
        }
        return false;
    }
    // 发送端为一次传输打开数据 Writer（普通通道或修复通道），会先关闭上一条通道。
    void open_writer(const DataChannel& channel) override
    {
        if (!sender_) throw std::logic_error("Receiver cannot open data writer");
        close_writer();
        data_topic_ = topic(channel);
        data_writer_ = publisher_->create_datawriter(data_topic_, writer_qos());
        if (!data_writer_) { participant_->delete_topic(data_topic_); data_topic_ = nullptr; require(data_writer_); }
        writing_ = channel;
    }
    // 判断数据 Writer 是否已与给定的全部 Reader 匹配；Reader 身份来自接收端 open_reader 的返回值。
    bool writer_ready(const std::vector<std::string>& readers) override
    {
        if (!data_writer_ || readers.empty()) return false;
        std::vector<dds::InstanceHandle_t> handles;
        check(data_writer_->get_matched_subscriptions(handles));
        std::set<std::string> matched;
        for (const auto& h : handles) matched.insert(endpoint_id(h));
        return std::all_of(readers.begin(), readers.end(), [&](const auto& id) { return matched.count(id); });
    }
    // 发送端隔离一个卡死或已失效的接收端 Reader，使其不再拖慢可靠传输。
    void isolate(const std::string& reader) override
    {
        if (!sender_) throw std::logic_error("Receiver cannot isolate readers");
        if (!identity(reader)) throw std::runtime_error("Invalid Reader identity");
        // Fast DDS 3.6 未实现 ignore_subscription，只能忽略 Reader 所属参与者：
        // GUID 前缀取前 12 字节，实体 ID 换成参与者的 00 00 01 c1。立即解除匹配，Writer 不再等它确认。
        dds::InstanceHandle_t handle;
        for (unsigned i = 0; i < 12; ++i) handle.value[i] = static_cast<uint8_t>(std::stoi(reader.substr(i * 2, 2), nullptr, 16));
        handle.value[12] = 0x00; handle.value[13] = 0x00; handle.value[14] = 0x01; handle.value[15] = 0xc1;
        const auto result = participant_->ignore_participant(handle);
        std::cout << "ISOLATED reader=" << reader << " result=" << result << std::endl;
    }
    // 发送端关闭当前数据 Writer 及其 Topic；未打开时为空操作。
    void close_writer() override
    {
        if (data_writer_) publisher_->delete_datawriter(data_writer_);
        data_writer_ = nullptr;
        if (data_topic_) participant_->delete_topic(data_topic_);
        data_topic_ = nullptr;
    }
    // 接收端为一次传输打开数据 Reader，并挂到 WaitSet；同一 transfer_id 的旧 Reader 会先被关闭。
    // 返回 Reader 的身份，接收端通过 Reply 告知发送端，用于 writer_ready 判断匹配。
    std::string open_reader(const DataChannel& channel) override
    {
        if (sender_) throw std::logic_error("Sender cannot open data reader");
        close_reader(channel.transfer_id);
        Stream stream;
        stream.channel = channel;
        stream.topic = topic(channel);
        stream.reader = subscriber_->create_datareader(stream.topic, data_reader_qos());
        if (!stream.reader) { participant_->delete_topic(stream.topic); require(stream.reader); }
        try { attach(stream.reader); }
        catch (...) {
            subscriber_->delete_datareader(stream.reader); participant_->delete_topic(stream.topic); throw;
        }
        streams_.emplace(channel.transfer_id, stream);
        std::cout << "DATA_OPEN transfer=" << channel.transfer_id << " route="
                  << (channel.repair ? "repair" : "normal") << " session=" << channel.session_id;
        if (data_multicast_)
            std::cout << " multicast=" << rtps::IPLocator::ip_to_string(*data_multicast_) << ':' << data_multicast_->port;
        std::cout << std::endl;
        return endpoint_id(stream.reader->get_instance_handle());
    }
    // 接收端关闭指定传输的数据 Reader 和 Topic，并打印累计收到的字节与样本数；不存在时为空操作。
    void close_reader(const std::string& id) override
    {
        auto found = streams_.find(id);
        if (found == streams_.end()) return;
        auto& s = found->second;
        waitset_.detach_condition(s.reader->get_statuscondition());
        subscriber_->delete_datareader(s.reader);
        participant_->delete_topic(s.topic);
        std::cout << "DATA_STATS transfer=" << id << " route=" << (s.channel.repair ? "repair" : "normal")
                  << " session=" << s.channel.session_id << " bytes=" << s.bytes << " samples=" << s.samples << std::endl;
        streams_.erase(found);
    }
    // 阻塞等待任一已挂载 Reader 有新数据，或超时返回；超时不视为错误。
    void wait(std::chrono::milliseconds timeout) override
    {
        dds::ConditionSeq active;
        const auto ms = timeout.count();
        const auto result = waitset_.wait(active, {static_cast<int32_t>(ms / 1000), static_cast<uint32_t>((ms % 1000) * 1000000)});
        if (result != dds::RETCODE_TIMEOUT) check(result);
    }
private:
    // 每个活跃接收任务一个有界数据缓存；控制 Reader 独立，不被文件块填满。
    struct Stream {
        DataChannel channel;
        dds::Topic* topic = nullptr;
        dds::DataReader* reader = nullptr;
        uint64_t bytes = 0, samples = 0;
    };
    // 控制与数据 Writer 共用的 QoS：可靠、易失、KEEP_ALL，最多缓存 64 个样本；
    // 写满后最多阻塞 20 ms，异步发布，关闭 Data-sharing。
    dds::DataWriterQos writer_qos() const
    {
        dds::DataWriterQos q;
        q.reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
        q.reliability().max_blocking_time = {0, 20000000};
        q.durability().kind = dds::VOLATILE_DURABILITY_QOS;
        q.history().kind = dds::KEEP_ALL_HISTORY_QOS;
        q.resource_limits().max_instances = 1;
        q.resource_limits().max_samples = q.resource_limits().max_samples_per_instance = 64;
        q.resource_limits().allocated_samples = 4;
        q.publish_mode().kind = dds::ASYNCHRONOUS_PUBLISH_MODE;
        q.endpoint().history_memory_policy = rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
        q.data_sharing().off();
        return q;
    }
    // Presence 是周期心跳：丢一个无妨，但绝不能因某个发送端不取样本而阻塞接收端主循环。
    dds::DataWriterQos presence_writer_qos() const
    {
        auto q = writer_qos();
        q.reliability().kind = dds::BEST_EFFORT_RELIABILITY_QOS;
        q.history().kind = dds::KEEP_LAST_HISTORY_QOS;
        q.history().depth = 1;
        q.resource_limits().max_samples = q.resource_limits().max_samples_per_instance = 1;
        q.resource_limits().allocated_samples = 1;
        return q;
    }
    // 发送端接收心跳用的 Reader QoS，与 presence_writer_qos 配套使用 best-effort。
    dds::DataReaderQos presence_reader_qos() const
    {
        // Presence 无 key，所有接收端共用一个实例；保留最近 64 条，满时覆盖最旧的心跳。
        auto q = reader_qos();
        q.reliability().kind = dds::BEST_EFFORT_RELIABILITY_QOS;
        q.history().kind = dds::KEEP_LAST_HISTORY_QOS;
        q.history().depth = 64;
        return q;
    }
    // 控制与数据 Reader 共用的 QoS，与 writer_qos 对应：可靠、易失、KEEP_ALL，最多缓存 64 个样本。
    dds::DataReaderQos reader_qos() const
    {
        dds::DataReaderQos q;
        q.reliability().kind = dds::RELIABLE_RELIABILITY_QOS;
        q.durability().kind = dds::VOLATILE_DURABILITY_QOS;
        q.history().kind = dds::KEEP_ALL_HISTORY_QOS;
        q.resource_limits().max_instances = 1;
        q.resource_limits().max_samples = q.resource_limits().max_samples_per_instance = 64;
        q.resource_limits().allocated_samples = 4;
        q.endpoint().history_memory_policy = rtps::PREALLOCATED_WITH_REALLOC_MEMORY_MODE;
        q.data_sharing().off();
        return q;
    }
    // 数据 Reader 的 QoS。组播模式下额外宣告组播 locator，单播 locator 仍保留：
    // 同一 Writer 有 ≥2 个 Reader 宣告同一组时 Fast DDS 只发一份组播，只有 1 个时退回单播。
    dds::DataReaderQos data_reader_qos() const
    {
        auto q = reader_qos();
        if (data_multicast_) q.endpoint().multicast_locator_list.push_back(*data_multicast_);
        return q;
    }
    // 按数据通道创建 Topic：普通传输为 <prefix>/data/<transfer>，修复传输为 <prefix>/repair/<transfer>/<session>。
    dds::Topic* topic(const DataChannel& c)
    {
        if (!identity(c.transfer_id) || !identity(c.session_id))
            throw std::runtime_error("Invalid data channel identity");
        const auto suffix = c.repair ? "/repair/" + c.transfer_id + "/" + c.session_id : "/data/" + c.transfer_id;
        return require(participant_->create_topic(prefix_ + suffix, data_type_.get_type_name(), dds::TOPIC_QOS_DEFAULT));
    }
    // 让 Reader 只在 data_available 时触发条件，并挂到 WaitSet，供 wait() 统一等待。
    void attach(dds::DataReader* reader)
    {
        check(reader->get_statuscondition().set_enabled_statuses(dds::StatusMask::data_available()));
        check(waitset_.attach_condition(reader->get_statuscondition()));
    }
    // 写一个样本。返回 true 表示已入队，false 表示历史缓存已满或阻塞超时；其他错误抛异常。
    template<class T> bool write(dds::DataWriter* writer, T& value)
    {
        const auto result = writer->write(&value);
        // false 表示本次未入队；调用方只重试当前块。入队成功后的网络重传归 DDS。
        if (result == dds::RETCODE_TIMEOUT || result == dds::RETCODE_OUT_OF_RESOURCES) return false;
        check(result);
        return true;
    }
    // 取下一条有效样本，跳过实例状态变化等无效样本；单次最多检查 64 条，避免长时间占用调用方。
    template<class T> bool read(dds::DataReader* reader, T& value)
    {
        dds::SampleInfo info;
        for (unsigned n = 0; n < 64; ++n) {
            const auto result = reader->take_next_sample(&value, &info);
            if (result == dds::RETCODE_NO_DATA) return false;
            check(result);
            if (info.valid_data) return true;
        }
        return false;
    }
    // 按依赖顺序释放实体：先关数据通道、从 WaitSet 摘除条件，再删除参与者下的剩余实体和参与者本身。
    void cleanup()
    {
        if (!participant_) return;
        while (!streams_.empty()) close_reader(streams_.begin()->first);
        close_writer();
        if (reader_) waitset_.detach_condition(reader_->get_statuscondition());
        if (presence_reader_) waitset_.detach_condition(presence_reader_->get_statuscondition());
        participant_->delete_contained_entities();
        dds::DomainParticipantFactory::get_instance()->delete_participant(participant_);
        participant_ = nullptr;
    }
    bool sender_;
    std::optional<rtps::Locator_t> data_multicast_;
    std::string prefix_, last_read_;
    dds::TypeSupport control_type_, reply_type_, data_type_, presence_type_;
    dds::DomainParticipant* participant_ = nullptr;
    dds::Publisher* publisher_ = nullptr;
    dds::Subscriber* subscriber_ = nullptr;
    dds::DataWriter* writer_ = nullptr;
    dds::DataReader* reader_ = nullptr;
    dds::DataWriter* presence_writer_ = nullptr;
    dds::DataReader* presence_reader_ = nullptr;
    dds::DataWriter* data_writer_ = nullptr;
    dds::Topic* data_topic_ = nullptr;
    DataChannel writing_;
    std::map<std::string, Stream> streams_;
    dds::WaitSet waitset_;
};
} // namespace
// 工厂函数：对外只暴露 Transport 接口，Fast DDS 类型留在本文件内。
std::unique_ptr<Transport> make_dds_transport(uint32_t domain, const std::string& channel, bool sender,
        DdsTransportMode mode, const MulticastGroup& group)
{
    return std::make_unique<DdsTransport>(domain, channel, sender, mode, group);
}
} // namespace file_transfer
