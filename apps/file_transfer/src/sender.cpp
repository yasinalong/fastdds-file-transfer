#include "file_transfer/stream.hpp"
#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>

namespace file_transfer {
namespace {
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
// 检查身份字符串是否由 32 个小写十六进制字符组成，用于校验传输、接收端和 Reader 标识。
bool identity(const std::string& value)
{
    return value.size() == 32 && value.find_first_not_of("0123456789abcdef") == std::string::npos;
}
struct Stopped {};

// 每端只记录身份、补发候选起点和结果，没有独立发送状态机或数据游标。
struct Peer {
    std::string epoch, error, reader; // reader: 本轮已就绪的数据 Reader，仅用于隔离卡死节点。
    std::set<std::string> old_epochs;
    std::optional<Outcome> outcome;
    std::optional<Reply> ready;
    Request request;
    uint64_t offset = 0, resumed = 0, confirmed = 0;
    bool replay = true;
    bool end_sent = false; // End 可能已触发远端发布；收不到结果时必须保留 UNKNOWN。
    unsigned attempts = 0;       // 实际拿到 Reader 的补发轮次。
    unsigned renegotiations = 0; // 连续起点不符次数：合法情况最多两次（未知→实际断点，前缀损坏→0）。
    Clock::time_point seen = Clock::now(), control = seen - 1s;
};

// 协调器只负责发现、冻结每轮名单和按断点分组。DataStream 独立负责顺序 Data/End。
class Sender {
public:
    // 保存本次发送的依赖和配置；传输适配器、源文件及回调由调用方持有，发送在 run() 中开始。
    Sender(Transport& transport, const FileSource& source, std::string id, const SendOptions& options,
            const std::function<bool()>& cancelled)
        : transport_(transport), source_(source), id_(std::move(id)), options_(options), cancelled_(cancelled) {}
    // 执行一次完整发送：发现初始节点、发送正常轮次，再按断点分组补发。
    // 统一处理取消和异常，最后汇总各接收端的结果。
    Result run()
    {
        try {
            discover();
            std::vector<std::string> initial;
            for (const auto& item : peers_) initial.push_back(item.first);
            deliver(initial, 0, false);
            // 每轮只收集当前待补发且起点相同的节点，冻结后才开始写入。
            // 这一轮中途出现的节点保留在队列里，下一轮才加入。
            for (;;) {
                poll();
                auto groups = replay_groups();
                if (groups.empty()) break;
                deliver(groups.begin()->second, groups.begin()->first, true);
            }
            source_.read(source_.size(), 0);
        } catch (const Stopped&) {
            for (auto& item : peers_) if (!item.second.outcome) {
                cancel(item.second);
                finish(item.second, item.second.end_sent ? Outcome::unknown : Outcome::cancelled, "Stopped; partial file retained");
            }
            fallback_ = Outcome::cancelled;
        } catch (const std::exception& e) {
            detail_ = e.what();
            for (auto& item : peers_) if (!item.second.outcome) fail(item.second, e.what());
        }
        return result();
    }
private:
    // 记录单个接收端的最终状态与原因，后续调度跳过已结束的节点。
    void finish(Peer& p, Outcome outcome, const std::string& error = {})
    {
        p.outcome = outcome; p.error = error;
    }
    // 尽力取消已准备但尚未发送 End 的会话，通知接收端释放资源并保留部分文件。
    // End 发出后文件可能已经提交，不再发送取消；清理过程中的异常不向外传播。
    void cancel(Peer& p) noexcept
    {
        if (p.request.transfer_id.empty() || p.end_sent) return;
        try { auto request = p.request; request.operation = Operation::cancel; transport_.send(request); } catch (...) {}
    }
    // 尝试隔离本轮 Reader，避免离线或卡死节点继续拖住共享 Writer，并记录解除匹配的等待起点。
    void isolate(Peer& p) noexcept
    {
        if (p.reader.empty()) return;
        try { transport_.isolate(p.reader); } catch (...) {}
        p.reader.clear();
        isolated_at_ = Clock::now(); // 离线、卡死等任一路径隔离后，都给 DDS 解除匹配留出时间。
    }
    // 结束失败节点：尝试取消；没有确定失败结论时，还需隔离它的 Reader。
    // 若 End 已发出但无法确认远端结果，则记为 unknown，不能断言文件提交失败。
    void fail(Peer& p, const std::string& error, bool definitive = false)
    {
        cancel(p);
        if (!definitive) isolate(p);
        finish(p, p.end_sent && !definitive ? Outcome::unknown : Outcome::failed, error);
    }
    // 清除本轮就绪信息，将节点按候选偏移放回补发队列；实际起点在后续 Prepare 中确认。
    void queue(Peer& p, uint64_t offset)
    {
        p.replay = true; p.ready.reset(); p.reader.clear(); p.offset = offset;
    }
    // 对仍在本轮的节点按 100 ms 间隔重发同一 Prepare，查询状态或已追加长度，不重置会话。
    void ask(const std::vector<std::string>& ids)
    {
        for (const auto& id : ids) {
            auto& p = peers_.at(id);
            if (active(p) && Clock::now() - p.control >= 100ms && transport_.send(p.request)) p.control = Clock::now();
        }
    }
    // 判断节点是否仍参与当前轮次：既未结束，也未转入补发队列；不表示 Reader 已匹配。
    bool active(const Peer& p) const { return !p.outcome && !p.replay; }
    // 判断固定名单中是否还有本轮节点，用于决定是否继续发送或等待结果。
    bool any_active(const std::vector<std::string>& ids) const
    {
        return std::any_of(ids.begin(), ids.end(), [&](const auto& id) { return active(peers_.at(id)); });
    }
    // 处理在线身份并刷新存活时间；新节点默认排队，不能插入已开始的轮次。
    // 同一接收端 epoch 改变表示进程重启，需要隔离旧 Reader，并重新协商断点。
    void presence(const Presence& value)
    {
        if (!identity(value.receiver_id) || !identity(value.receiver_epoch)) return;
        if (!peers_.count(value.receiver_id) && peers_.size() >= 64) return;
        auto& p = peers_[value.receiver_id];
        if (p.outcome || p.old_epochs.count(value.receiver_epoch)) return;
        if (!p.epoch.empty() && p.epoch != value.receiver_epoch) {
            if (p.old_epochs.size() >= 8) { fail(p, "Too many receiver restarts"); return; }
            p.old_epochs.insert(p.epoch);
            isolate(p); // 旧进程可能还挂着本轮 Reader，直到发现租约过期才会解除匹配。
            p.renegotiations = 0;
            queue(p, 0); // 新进程的文件长度未知；从 0 协商的证明无需扫描，Ready 会带回实际断点。
        }
        p.epoch = value.receiver_epoch; p.seen = Clock::now();
    }
    // 处理一批在线公告和回复，检查取消、失联及回执是否合法，更新进度与最终结果。
    // Ready 确认本轮起点和 Reader；起点变化或可重试错误交给补发队列，旧会话回复忽略。
    void poll()
    {
        if (cancelled_()) throw Stopped{};
        Presence online;
        for (unsigned n = 0; n < 128 && transport_.take(online); ++n) presence(online);
        Reply reply;
        for (unsigned n = 0; n < 128 && transport_.take(reply); ++n) {
            auto found = peers_.find(reply.receiver_id);
            if (reply.transfer_id != id_ || found == peers_.end()) continue;
            auto& p = found->second;
            if (p.outcome || reply.session_id != p.request.session_id || p.old_epochs.count(reply.receiver_epoch)) continue;
            presence({reply.receiver_id, reply.receiver_epoch});
            if (p.outcome || p.replay) continue;
            if (reply.next_offset > source_.size()) { fail(p, "Invalid receiver offset", true); continue; }
            p.confirmed = reply.next_offset;
            if (reply.state == Feedback::complete) {
                if (reply.next_offset != source_.size()) { fail(p, "Invalid completed size", true); continue; }
                if (!p.end_sent) p.resumed = source_.size();
                finish(p, Outcome::completed);
            } else if (reply.state == Feedback::error) {
                if (reply.retryable) queue(p, reply.next_offset);
                else fail(p, reply.error, true);
            } else if (p.ready) {
                // 本轮已就绪：重复 Prepare 的 Ready 只更新实时进度。
            } else if (reply.next_offset != p.request.offset || reply.reader_id.empty()) {
                // 起点不符只是协商出实际断点，不计入补发次数；接收端快照使下一轮免重扫。
                if (++p.renegotiations > 2) fail(p, "Receiver offset keeps changing", true);
                else queue(p, reply.next_offset);
            } else if (!identity(reply.reader_id)) fail(p, "Invalid Reader identity", true);
            else if (p.request.repair && ++p.attempts > 8) fail(p, "Too many replay attempts", true);
            else { p.ready = reply; p.reader = reply.reader_id; p.resumed = reply.next_offset; p.renegotiations = 0; }
        }
        for (auto& item : peers_)
            if (!item.second.outcome && Clock::now() - item.second.seen >= options_.timeout)
                fail(item.second, "Receiver offline timeout");
    }
    // 收集初始接收端；达到所需数量且首个节点出现后的发现窗口已结束时返回。
    // 此处只发现节点，不准备数据 Reader；超过发现期限仍不满足条件则抛出异常。
    void discover()
    {
        const auto started = Clock::now();
        std::optional<Clock::time_point> first;
        for (;;) {
            poll();
            if (!peers_.empty() && !first) first = Clock::now();
            if (first && peers_.size() >= std::max(size_t(1), options_.receivers) && Clock::now() - *first >= options_.discovery) return;
            if (Clock::now() - started >= options_.timeout) throw std::runtime_error("Discovery timeout: required receivers not found");
            transport_.wait(10ms);
        }
    }
    // 向本轮节点重试发送 Prepare，先等待 Ready，再核实回复中的具体 Reader 已与 Writer 匹配。
    // 两步共用一个截止时间；失败或转入补发队列的节点不再阻挡本轮开始。
    void prepare(const std::vector<std::string>& ids)
    {
        const auto deadline = Clock::now() + options_.timeout;
        for (;;) {
            poll();
            bool waiting = false;
            for (const auto& id : ids) {
                auto& p = peers_.at(id);
                if (!active(p) || p.ready) continue;
                if (Clock::now() >= deadline) { fail(p, "Receiver preparation timeout"); continue; }
                waiting = true;
                if (Clock::now() - p.control >= 100ms && transport_.send(p.request)) p.control = Clock::now();
            }
            if (!waiting) break;
            transport_.wait(5ms);
        }
        // Ready 标识具体 Reader，防止只看到其他节点的匹配就提前发送。
        while (any_active(ids)) {
            poll();
            bool matched = true;
            for (const auto& id : ids) {
                auto& p = peers_.at(id);
                if (active(p) && !transport_.writer_ready({p.ready->reader_id})) {
                    if (Clock::now() >= deadline) fail(p, "Data reader match timeout");
                    else matched = false;
                }
            }
            if (matched) break;
            transport_.wait(5ms);
        }
    }
    // End 发出后等待本轮最终回执；用相同 Prepare 查询状态，不重新发送文件块或 End。
    // 超时节点按结果不确定的规则结束，仍需补发的节点留给下一轮处理。
    void await_results(const std::vector<std::string>& ids)
    {
        const auto deadline = Clock::now() + options_.timeout;
        while (any_active(ids)) {
            poll();
            for (const auto& id : ids) {
                auto& p = peers_.at(id);
                if (!active(p)) continue;
                if (Clock::now() >= deadline) fail(p, "Completion reply timeout");
            }
            ask(ids); // 只重取结果，不重发 Data 或 End。
            if (any_active(ids)) transport_.wait(5ms);
        }
    }
    // 对固定名单执行一轮发送：生成会话和前缀证明，准备 Reader，再从 offset 顺序发送到 End。
    // repair 决定使用正常或补发通道；发送期间持续处理公告、回执和背压，结束后等待结果。
    void deliver(const std::vector<std::string>& ids, uint64_t offset, bool repair)
    {
        const DataChannel channel{id_, new_transfer_id(), repair};
        const auto proof = source_.prefix_digest(offset, [&] { poll(); });
        ++generation_;
        for (const auto& id : ids) {
            auto& p = peers_.at(id);
            if (p.outcome) continue;
            p.request = {Operation::prepare, id_, channel.session_id, id, p.epoch, generation_,
                source_.name(), source_.digest(), source_.size(), offset, proof, repair};
            p.replay = false; p.ready.reset(); p.reader.clear(); p.control = Clock::now() - 1s;
        }
        // 每轮是同样的三个步骤，普通发送和补发都不另建状态分支。
        DataStream stream(transport_, channel);
        prepare(ids);
        const bool ended = stream.send(source_, offset, options_.chunk_bytes, [&](uint64_t written, Clock::duration blocked) {
            poll();
            return any_active(ids) && unblock(ids, written, blocked);
        });
        // 放弃本轮时 confirmed 已由实时进度刷新，节点按真实断点重新分组。
        for (const auto& id : ids) if (active(peers_.at(id))) {
            if (ended) peers_.at(id).end_sent = true;
            else queue(peers_.at(id), peers_.at(id).confirmed);
        }
        if (ended) await_results(ids);
    }
    // 当前块持续背压时查询各端进度，超过期限后隔离仍落后于 written 的节点，其余节点继续当前流。
    // written 是 DDS 已接受的文件位置，blocked 是当前块连续写入受阻的时长。
    // 返回是否继续本轮；无法解除背压或隔离后没有本轮节点时，返回 false 交给补发调度。
    bool unblock(const std::vector<std::string>& ids, uint64_t written, Clock::duration blocked)
    {
        const auto half = options_.timeout / 2;
        if (blocked < half) return true;
        ask(ids); // 先用半个期限收集实时进度，再做判断。
        if (blocked < options_.timeout || Clock::now() - isolated_at_ < half) return true;
        bool isolated = false;
        for (const auto& id : ids) {
            auto& p = peers_.at(id);
            if (active(p) && p.confirmed < written) { fail(p, "Receiver stalled; isolated from stream"); isolated = true; }
        }
        return isolated && any_active(ids);
    }
    // 将未结束且待补发的节点按候选偏移分组；map 按偏移排序，同一起点共享一轮数据流。
    std::map<uint64_t, std::vector<std::string>> replay_groups() const
    {
        std::map<uint64_t, std::vector<std::string>> groups;
        for (const auto& item : peers_)
            if (!item.second.outcome && item.second.replay) groups[item.second.offset].push_back(item.first);
        return groups;
    }
    // 汇总逐端结果、续传起点和确认字节数；unknown 优先于其他失败，全部成功时返回源文件哈希。
    Result result() const
    {
        Result result{peers_.empty() || !detail_.empty() ? fallback_ : Outcome::completed, detail_};
        for (const auto& item : peers_) {
            const auto& p = item.second;
            const auto outcome = p.outcome.value_or(Outcome::failed);
            result.receivers.push_back({item.first, outcome, p.resumed, p.confirmed, p.error});
            if (outcome == Outcome::unknown) result.outcome = outcome;
            else if (outcome != Outcome::completed && result.outcome != Outcome::unknown) result.outcome = outcome;
        }
        if (result.outcome == Outcome::completed) result.detail = source_.digest();
        else if (result.detail.empty()) result.detail = "Some receivers did not complete; see per-receiver results";
        return result;
    }
    Transport& transport_;
    const FileSource& source_;
    std::string id_;
    const SendOptions& options_;
    const std::function<bool()>& cancelled_;
    std::map<std::string, Peer> peers_;
    uint32_t generation_ = 0;
    Clock::time_point isolated_at_{};
    Outcome fallback_ = Outcome::failed;
    std::string detail_;
};
} // namespace
// 对外发送入口：校验选项和传输标识，再创建仅用于本次调用的 Sender，返回完整发送结果。
Result send_file(Transport& transport, const FileSource& source, const std::string& id,
        const SendOptions& options, const std::function<bool()>& cancelled)
{
    if (options.chunk_bytes == 0 || options.chunk_bytes > max_chunk_bytes || options.timeout <= 0ms ||
            options.discovery < 0ms || options.receivers > 64 || !identity(id))
        return {Outcome::failed, "Invalid send options"};
    return Sender(transport, source, id, options, cancelled).run();
}
} // namespace file_transfer
