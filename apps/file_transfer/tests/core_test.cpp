#include "file_transfer/receiver_service.hpp"
#include <algorithm>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <thread>

using namespace file_transfer;
namespace fs = std::filesystem;
using namespace std::chrono_literals;
namespace {
void check(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
void put(const fs::path& path, const std::string& value)
{
    std::ofstream stream(path, std::ios::binary); stream.write(value.data(), value.size());
    if (!stream) throw std::runtime_error("Fixture write failed");
}
Request prepare(const FileSource& file)
{
    Request r;
    r.transfer_id = new_transfer_id(); r.session_id = new_transfer_id(); r.generation = 1;
    r.total_bytes = file.size(); r.sha256 = file.digest(); r.file_name = file.name();
    r.prefix_sha256 = file.prefix_digest(0);
    return r;
}
DataBlock data(const Request& r, const FileSource& file, uint64_t offset, size_t size)
{
    return {r.transfer_id, r.session_id, offset, file.read(offset, size)};
}
DataBlock end(const Request& r) { return {r.transfer_id, r.session_id, r.total_bytes, {}, true}; }
fs::path partial(const fs::path& directory, const FileSource& file)
{
    return directory / ".file_transfer" / (file.digest() + ".part");
}
void seed(const fs::path& directory, const FileSource& file, size_t size, bool corrupt = false)
{
    Receiver receiver(directory, 1ULL << 40);
    const auto r = prepare(file); receiver.handle(r);
    for (size_t offset = 0; offset < size;) {
        auto block = data(r, file, offset, std::min(size - offset, max_chunk_bytes));
        if (corrupt && offset == 0) block.payload[0] ^= 1;
        receiver.handle(block); offset += block.payload.size();
    }
}

// 真实核心 + 内存路由。只向已创建相应 Reader 的端交付；可注入背压、缺块、重启、迟到和卡死节点。
// 无 DDS 时 Reader 身份就是接收进程 epoch，隔离语义与 DDS 中忽略整个参与者一致。
class Loopback : public Transport {
public:
    std::vector<std::unique_ptr<ReceiverService>> receivers;
    std::deque<Presence> online;
    std::deque<Reply> replies;
    std::map<std::string, DataChannel> subscriptions;
    std::map<std::string, std::set<std::string>> sessions;
    std::set<std::string> hung, isolated; // hung: 主循环卡住但仍占着 Reader 的接收端 ID；isolated: 被忽略的 epoch。
    DataChannel channel;
    fs::path first_path;
    bool backpressure = false, drop_data = false, drop_completion = false, restart = false;
    bool hung_presence = false; // 卡住的节点仍发心跳：只能靠进度落后识别。
    std::function<void()> during_normal, during_repair;
    uint64_t normal_bytes = 0, repair_bytes = 0;
    unsigned opened = 0, closed = 0, ends = 0, normal_completed = 0;
    std::vector<uint64_t> normal_offsets;
    Loopback(const fs::path& path) : first_path(path) { add(path); }
    void add(const fs::path& path) {
        receivers.push_back(std::make_unique<ReceiverService>(path, ReceiveOptions{}));
        online.push_back(receivers.back()->presence());
    }
    bool ignored(const ReceiverService& r) const { return isolated.count(r.presence().receiver_epoch) > 0; }
    bool responsive(const ReceiverService& r) const { return !ignored(r) && !hung.count(r.id()); }
    bool matched() override { return true; }
    bool send(const Presence&) override { throw std::logic_error("Wrong direction"); }
    bool take(Presence& p) override {
        if (online.empty()) return false;
        p = online.front(); online.pop_front(); return true;
    }
    void reply(Reply r) {
        if (r.transfer_id.empty() || (drop_completion && r.state == Feedback::complete) || isolated.count(r.receiver_epoch)) return;
        replies.push_back(std::move(r));
    }
    bool send(const Request& r) override {
        for (auto& receiver : receivers) {
            if (!responsive(*receiver)) continue;
            auto result = receiver->handle(r).reply;
            if (result.transfer_id.empty()) continue;
            const auto id = receiver->id();
            if (result.state == Feedback::ready && !result.reader_id.empty()) {
                subscriptions[id] = {r.transfer_id, r.session_id, r.repair};
                sessions[id].insert(r.session_id);
            } else subscriptions.erase(id);
            reply(std::move(result));
        }
        return true;
    }
    bool send(const DataBlock& block) override {
        if (backpressure) { backpressure = false; return false; }
        // 卡住的节点不取样本但仍匹配：可靠 Writer 被它拖住，直到它被隔离。
        for (const auto& receiver : receivers) {
            auto found = subscriptions.find(receiver->id());
            if (hung.count(receiver->id()) && !ignored(*receiver) && found != subscriptions.end() &&
                    found->second.session_id == channel.session_id) return false;
        }
        if (block.end) ++ends;
        else if (channel.repair) repair_bytes += block.payload.size();
        else { normal_bytes += block.payload.size(); normal_offsets.push_back(block.offset); }
        if (drop_data && !block.end) { drop_data = false; return true; }
        for (size_t i = 0; i < receivers.size(); ++i) {
            auto id = receivers[i]->id();
            auto found = subscriptions.find(id);
            if (found == subscriptions.end() || found->second.session_id != channel.session_id || !responsive(*receivers[i])) continue;
            auto result = receivers[i]->handle(block).reply;
            if (!result.transfer_id.empty()) {
                if (result.state == Feedback::complete && !channel.repair) ++normal_completed;
                subscriptions.erase(id);
                reply(std::move(result));
            }
            if (restart && !block.end && i == 0) {
                restart = false; subscriptions.erase(id); receivers[0].reset();
                receivers[0] = std::make_unique<ReceiverService>(first_path, ReceiveOptions{});
                online.push_back(receivers[0]->presence());
            }
        }
        auto& callback = channel.repair ? during_repair : during_normal;
        if (callback && !block.end) { auto once = std::move(callback); callback = {}; once(); }
        return true;
    }
    bool send(const Reply&) override { throw std::logic_error("Wrong direction"); }
    bool take(Request&) override { throw std::logic_error("Wrong direction"); }
    bool take(DataBlock&) override { throw std::logic_error("Wrong direction"); }
    bool take(Reply& r) override {
        if (replies.empty()) return false;
        r = replies.front(); replies.pop_front(); return true;
    }
    void open_writer(const DataChannel& c) override { channel = c; ++opened; }
    bool writer_ready(const std::vector<std::string>& readers) override {
        return std::all_of(readers.begin(), readers.end(), [&](const auto& reader) {
            return std::any_of(receivers.begin(), receivers.end(), [&](const auto& r) {
                return r->presence().receiver_epoch == reader && !ignored(*r) && subscriptions.count(r->id()) &&
                    subscriptions[r->id()].session_id == channel.session_id;
            });
        });
    }
    void isolate(const std::string& reader) override { isolated.insert(reader); }
    void close_writer() override { ++closed; channel = {}; }
    std::string open_reader(const DataChannel&) override { throw std::logic_error("Wrong direction"); }
    void close_reader(const std::string&) override { throw std::logic_error("Wrong direction"); }
    void wait(std::chrono::milliseconds duration) override {
        std::this_thread::sleep_for(duration);
        for (const auto& receiver : receivers)
            if (responsive(*receiver) || (hung_presence && !ignored(*receiver))) online.push_back(receiver->presence());
    }
};

void storage(const fs::path& dir, const FileSource& source)
{
    auto r = prepare(source);
    Receiver receiver(dir / "copy", 3);
    check(receiver.handle(r).state == Feedback::ready, "Prepare ready");
    auto conflicting = r; conflicting.total_bytes = 2;
    check(receiver.handle(conflicting).state == Feedback::error, "Conflicting prepare rejected");
    check(receiver.handle(data(r, source, 0, 1)).transfer_id.empty(), "Data has no progress reply");
    receiver.handle(data(r, source, 0, 1));
    check(receiver.received() == 1, "Duplicate data does not append");
    receiver.handle(data(r, source, 1, 2));
    check(!fs::exists(dir / "copy" / source.name()), "All bytes alone do not publish before End");
    check(receiver.handle(end(r)).state == Feedback::complete, "Ordered End commits file");
    check(receiver.handle(end(r)).state == Feedback::complete, "Duplicate End is idempotent");
    check(FileSource(dir / "copy" / source.name()).digest() == source.digest(), "Exact bytes preserved");
    Receiver gap(dir / "gap", 3); gap.handle(r);
    check(gap.handle(data(r, source, 1, 2)).retryable, "Gap becomes a replay error");
    Receiver incomplete(dir / "incomplete", 3); incomplete.handle(r);
    check(incomplete.handle(end(r)).retryable, "Incomplete End cannot commit");
    Receiver corrupt(dir / "corrupt", 3); corrupt.handle(r);
    auto bad = data(r, source, 0, 3); bad.payload[0] ^= 1; corrupt.handle(bad);
    check(corrupt.handle(end(r)).state == Feedback::error && fs::file_size(partial(dir / "corrupt", source)) == 0,
            "Full hash corruption resets bytes without publishing");
    Receiver limited(dir / "limited", 2);
    check(limited.handle(r).error == "File exceeds receiver limit", "Size limit before allocation");
    Receiver existing(dir / "copy", 3);
    check(existing.handle(r).state == Feedback::complete, "Complete copy requires no data");
    fs::create_directories(dir / "symlink"); fs::create_symlink(dir / "absent", dir / "symlink" / source.name());
    Receiver symlink(dir / "symlink", 3);
    check(symlink.handle(r).state == Feedback::error && fs::is_symlink(dir / "symlink" / source.name()), "Symlink preserved");
    Receiver race(dir / "race", 3); race.handle(r); race.handle(data(r, source, 0, 3));
    put(dir / "race" / source.name(), "sentinel");
    check(race.handle(end(r)).state == Feedback::error && fs::file_size(dir / "race" / source.name()) == 8, "Publish never overwrites");
    Receiver cancelled(dir / "cancelled", 3); cancelled.handle(r); cancelled.handle(data(r, source, 0, 1));
    auto cancel = r; cancel.operation = Operation::cancel; cancelled.handle(cancel);
    check(cancelled.handle(end(r)).state == Feedback::error && fs::file_size(partial(dir / "cancelled", source)) == 1,
            "Cancellation retains prefix and blocks late End");
    for (const std::string name : {"", ".", "..", "../escape", "/absolute", "a/b", "a\\b", "a\nb"}) {
        Receiver invalid(dir / "invalid", 3); auto request = r; request.file_name = name;
        check(invalid.handle(request).state == Feedback::error && invalid.phase() == ReceivePhase::waiting, "Unsafe name rejected");
    }
}
void sessions(const fs::path& dir, const FileSource& file)
{
    const auto now = ReceiveClock::now();
    ReceiveOptions options; options.timeout = 1s; options.max_transfers = 1;
    ReceiverService service(dir / "service", options);
    unsigned scans = 0; service.set_pump([&] { ++scans; });
    auto r = prepare(file); r.receiver_id = service.id(); r.receiver_epoch = service.presence().receiver_epoch;
    check(!service.handle(r, now).reply.reader_id.empty(), "Prepare also creates Reader");
    auto other = r; other.transfer_id = new_transfer_id(); other.file_name = "other";
    check(service.handle(other, now).reply.state == Feedback::error, "Concurrent admission bounded");
    service.handle(data(r, file, 0, 1), now);
    auto live = service.handle(r, now + 800ms).reply;
    check(live.state == Feedback::ready && live.next_offset == 1, "Duplicate Prepare reports live progress without reset");
    check(fs::file_size(partial(dir / "service", file)) == 1, "Repeated Prepare preserves bytes");
    check(service.expire(now + 1100ms).size() == 1, "Prepare retries cannot hide lack of data progress");
    auto fresh = r; ++fresh.generation; fresh.session_id = new_transfer_id();
    auto snapshot = service.handle(fresh, now + 1200ms).reply;
    check(snapshot.next_offset == 1 && snapshot.reader_id.empty() && service.active() == 0,
            "Different saved offset releases resources while queued");
    auto old_cancel = r; old_cancel.operation = Operation::cancel;
    check(service.handle(old_cancel).reply.transfer_id.empty(), "Old generation cannot cancel new snapshot");
    check(service.handle(r).reply.transfer_id.empty(), "Old Prepare cannot replace new session");
    ++fresh.generation; fresh.session_id = new_transfer_id(); fresh.offset = 1; fresh.prefix_sha256 = file.prefix_digest(1); fresh.repair = true;
    check(!service.handle(fresh).reply.reader_id.empty(), "Replay Prepare verifies prefix and opens shared stream");
    check(scans == 0, "Queued snapshot and replay reuse the in-process hash checkpoint");
    auto wrong = fresh; wrong.receiver_epoch = new_transfer_id(); wrong.operation = Operation::cancel;
    check(service.handle(wrong).reply.retryable && service.active() == 1, "Old epoch cannot modify live file");
    auto block = data(fresh, file, 1, 2); service.handle(block);
    check(service.handle(end(fresh)).reply.state == Feedback::complete, "End finalizes current session");
    check(service.handle(fresh).reply.state == Feedback::complete && !service.handle(fresh).finished,
            "Prepare retrieves cached completion without another End");
    bool locked = false;
    try { ReceiveDirectory duplicate(dir / "service"); } catch (const std::exception&) { locked = true; }
    check(locked, "Receive directory is exclusive");
    IdleTimeout idle(3s);
    check(!idle.expired(false, now) && !idle.expired(true, now + 100s) && !idle.expired(false, now + 101s) &&
            idle.expired(false, now + 104s), "Presence controls idle service independently of files");
}
void checkpoints(const fs::path& dir, const FileSource& file)
{
    const auto folder = dir / "checkpoint"; fs::create_directories(folder);
    const auto destination = folder / file.name();
    unsigned ticks = 0; auto count = [&] { ++ticks; };
    std::shared_ptr<const FileSink::Checkpoint> saved;
    { FileSink sink(destination, file.size(), file.digest()); sink.append(file.read(0, 2)); saved = sink.checkpoint(); }
    {
        FileSink sink(destination, file.size(), file.digest(), count, saved);
        check(ticks == 0 && sink.size() == 2 && sink.prefix_digest() == file.prefix_digest(2), "Unchanged partial file reuses checkpoint");
        saved = sink.checkpoint();
    }
    std::this_thread::sleep_for(20ms); // 越过粗粒度文件时间戳。
    put(partial(folder, file), "xy");
    FileSink changed(destination, file.size(), file.digest(), count, saved);
    check(ticks == 1 && changed.prefix_digest() != file.prefix_digest(2), "Modified partial file is rescanned");
}
void transfers(const fs::path& dir, const FileSource& file, const FileSource& small)
{
    SendOptions options; options.discovery = 0ms; options.timeout = 1500ms;
    Loopback normal(dir / "normal"); normal.add(dir / "normal-b"); normal.backpressure = true;
    check(send_file(normal, file, new_transfer_id(), options, [] { return false; }).outcome == Outcome::completed, "Normal broadcast with backpressure");
    check(normal.normal_bytes == file.size() && normal.repair_bytes == 0 && normal.ends == 1 && normal.opened == normal.closed,
            "One ordered stream and End for all normal receivers");
    check(std::adjacent_find(normal.normal_offsets.begin(), normal.normal_offsets.end(), std::greater_equal<uint64_t>()) == normal.normal_offsets.end(),
            "Normal offsets strictly increase");
    seed(dir / "prefix-a", file, 12345); seed(dir / "prefix-b", file, 12345); seed(dir / "prefix-c", file, 23456);
    Loopback group(dir / "fresh"); group.add(dir / "prefix-a"); group.add(dir / "prefix-b"); group.add(dir / "prefix-c");
    auto result = send_file(group, file, new_transfer_id(), options, [] { return false; });
    check(result.outcome == Outcome::completed, "Grouped resume completes all receivers");
    check(group.normal_bytes == file.size() && group.repair_bytes == 2 * file.size() - 12345 - 23456,
            "Equal offsets share one suffix; distinct offsets use separate streams");
    check(group.sessions[group.receivers[1]->id()] == group.sessions[group.receivers[2]->id()], "Equal offsets share Topic/session");
    check(group.sessions[group.receivers[1]->id()] != group.sessions[group.receivers[3]->id()], "Distinct offsets stay isolated");
    seed(dir / "corrupt-prefix", file, 12345, true);
    Loopback repaired(dir / "corrupt-prefix");
    auto repaired_result = send_file(repaired, file, new_transfer_id(), options, [] { return false; });
    check(repaired_result.outcome == Outcome::completed && repaired_result.receivers[0].resumed_bytes == 0 && repaired.normal_bytes == 0,
            "Corrupt prefix resets through Prepare then replays from zero");
    Loopback loss(dir / "loss"); loss.drop_data = true;
    check(send_file(loss, file, new_transfer_id(), options, [] { return false; }).outcome == Outcome::completed, "Gap repaired outside normal stream");
    Loopback restart(dir / "restart"); restart.restart = true;
    check(send_file(restart, file, new_transfer_id(), options, [] { return false; }).outcome == Outcome::completed, "Restart detected through presence");
    Loopback late(dir / "late-normal");
    late.during_normal = [&] { late.add(dir / "late-a"); late.add(dir / "late-b"); };
    late.during_repair = [&] { check(late.normal_completed == 1, "Normal round finishes before replay"); late.add(dir / "late-c"); };
    auto late_result = send_file(late, file, new_transfer_id(), options, [] { return false; });
    check(late_result.outcome == Outcome::completed && late_result.receivers.size() == 4 && late.repair_bytes == 2 * file.size(),
            "Late joiners share a round; arrivals during replay use next round");
    check(late.sessions[late.receivers[1]->id()] == late.sessions[late.receivers[2]->id()] &&
            late.sessions[late.receivers[2]->id()] != late.sessions[late.receivers[3]->id()], "Late cohorts are frozen per stream");
    // 两种卡死：心跳也停（离线超时识别）；心跳仍在（进度落后于已写入位置识别）。
    for (const bool presence : {false, true}) {
        Loopback stall(dir / (presence ? "stall-online-a" : "stall-a")); stall.add(dir / (presence ? "stall-online-b" : "stall-b"));
        stall.hung_presence = presence;
        const auto stuck = stall.receivers[1]->id();
        stall.during_normal = [&] { stall.hung.insert(stuck); };
        auto stalled = send_file(stall, file, new_transfer_id(), options, [] { return false; });
        bool healthy_done = false, stuck_failed = false;
        for (const auto& peer : stalled.receivers) {
            if (peer.id == stuck) stuck_failed = peer.outcome == Outcome::failed &&
                peer.detail == (presence ? "Receiver stalled; isolated from stream" : "Receiver offline timeout");
            else healthy_done = peer.outcome == Outcome::completed;
        }
        check(healthy_done && stuck_failed && stall.isolated.size() == 1, "Stuck receiver is isolated; healthy receiver finishes");
        check(stall.normal_bytes == file.size() && stall.repair_bytes == 0 && stall.ends == 1,
                "Healthy receiver keeps the normal stream instead of regrouping");
    }
    Loopback unknown(dir / "unknown"); unknown.drop_completion = true; options.timeout = 120ms;
    check(send_file(unknown, small, new_transfer_id(), options, [] { return false; }).outcome == Outcome::unknown &&
            fs::exists(dir / "unknown" / small.name()), "Lost final reply is unknown even when file exists");
    options.timeout = 1500ms;
    Loopback mixed(dir / "good"); fs::create_directories(dir / "conflict"); put(dir / "conflict" / file.name(), "sentinel"); mixed.add(dir / "conflict");
    check(send_file(mixed, file, new_transfer_id(), options, [] { return false; }).outcome == Outcome::failed &&
            FileSource(dir / "good" / file.name()).digest() == file.digest(), "One receiver failure preserves healthy outcome");
}
}
int main(int argc, char** argv)
{
    try {
        check(argc == 2, "Pass a fresh directory under out/"); const fs::path dir = argv[1];
        check(fs::create_directories(dir), "Test directory must be new");
        put(dir / "source", "abc"); FileSource small(dir / "source");
        check(small.digest() == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", "SHA-256 vector");
        storage(dir, small); sessions(dir, small); checkpoints(dir, small);
        put(dir / "binary", std::string(1000004, '\xff')); FileSource file(dir / "binary");
        transfers(dir, file, small);
        put(dir / "empty", ""); FileSource empty(dir / "empty"); Loopback empty_link(dir / "empty-copy");
        SendOptions options; options.discovery = 0ms;
        check(send_file(empty_link, empty, new_transfer_id(), options, [] { return false; }).outcome == Outcome::completed && empty_link.ends == 1,
                "Empty file commits via End");
        unsigned ticks = 0; file.prefix_digest(524288, [&] { ++ticks; }); check(ticks == 2, "Hash scan yields between blocks");
        put(dir / "changing", "before"); FileSource changing(dir / "changing"); put(dir / "changing", "after-change");
        bool changed = false; try { changing.read(0, 1); } catch (const std::exception&) { changed = true; }
        check(changed, "Source mutation detected");
        std::cout << "PASS Prepare/Data/End, storage, grouped replay, late cohorts, stall isolation, hash checkpoints, restart, backpressure, session isolation, completion uncertainty" << std::endl;
        return 0;
    } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << std::endl; return 1; }
}
