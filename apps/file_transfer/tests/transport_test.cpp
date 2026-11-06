#include "file_transfer/dds_transport.hpp"
#include "file_transfer/file.hpp"
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>

using namespace file_transfer;
using namespace std::chrono_literals;
namespace {
void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
template<class F> void until(F condition)
{
    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (!condition()) {
        if (std::chrono::steady_clock::now() >= deadline) throw std::runtime_error("Transport test timeout");
        std::this_thread::sleep_for(5ms);
    }
}
}
int main(int argc, char** argv)
{
    try {
        check(argc == 2, "Pass DDS domain");
        const auto topic = "transport_" + new_transfer_id();
        auto sender = make_dds_transport(std::stoul(argv[1]), topic, true);
        auto target = make_dds_transport(std::stoul(argv[1]), topic, false);
        auto other = make_dds_transport(std::stoul(argv[1]), topic, false);
        until([&] { return sender->matched() && target->matched() && other->matched(); });
        const auto id = new_transfer_id();
        DataChannel normal{id, id, false};
        const auto target_reader = target->open_reader(normal);
        const auto other_reader = other->open_reader(normal);
        sender->open_writer(normal);
        until([&] { return sender->writer_ready({target_reader, other_reader}); });
        check(!sender->writer_ready({new_transfer_id()}), "An unrelated Reader cannot satisfy readiness");

        // 不 take 文件数据，直到 Reader + Writer 的有界缓存施加背压。
        DataBlock block{id, id, 0, std::vector<uint8_t>(max_chunk_bytes, 17)};
        unsigned accepted = 0;
        while (accepted < 512 && sender->send(block)) { ++accepted; block.offset += block.payload.size(); }
        check(accepted > 0 && accepted < 512, "Slow readers must bound outstanding samples");
        // 数据缓存已满时控制通道仍能工作。
        Request probe; probe.operation = Operation::prepare; probe.transfer_id = id;
        until([&] { return sender->send(probe); });
        Request control;
        until([&] { return target->take(control); });
        check(control.transfer_id == id, "Control must bypass full data history");

        uint64_t target_offset = 0, other_offset = 0;
        auto drain = [&](Transport& transport, uint64_t& offset) {
            DataBlock received;
            while (transport.take(received)) {
                check(received.offset == offset && received.payload == block.payload, "Backpressure must preserve ordered exact data");
                offset += received.payload.size();
            }
        };
        until([&] {
            drain(*target, target_offset); drain(*other, other_offset);
            return target_offset == block.offset && other_offset == block.offset;
        });
        until([&] { return sender->send(block); });
        until([&] {
            drain(*target, target_offset); drain(*other, other_offset);
            return target_offset == block.offset + block.payload.size() && other_offset == target_offset;
        });
        DataBlock end{id, id, target_offset, {}, true};
        until([&] { return sender->send(end); });
        DataBlock ending;
        until([&] { return target->take(ending); });
        check(ending.end && ending.offset == target_offset, "End must follow all Data on the same Writer");
        until([&] { return other->take(ending); });
        check(ending.end && ending.offset == other_offset, "Every Reader observes ordered End");
        sender->close_writer();
        target->close_reader(id);
        // 非目标端仍订阅正常 Topic；旁路写入不能进入这个 Reader。
        DataChannel repair{id, new_transfer_id(), true};
        auto private_reader = target->open_reader(repair);
        sender->open_writer(repair);
        until([&] { return sender->writer_ready({private_reader}); });
        check(!sender->writer_ready({other_reader}), "Normal Reader must not match repair Writer");
        DataBlock patch{id, repair.session_id, 0, {1, 2, 3, 4}};
        until([&] { return sender->send(patch); });
        DataBlock received;
        until([&] { return target->take(received); });
        check(received.payload == patch.payload, "Target must receive repair data");
        for (unsigned n = 0; n < 20; ++n) {
            check(!other->take(received), "Non-target Reader must receive zero repair payload");
            std::this_thread::sleep_for(5ms);
        }
        sender->close_writer(); target->close_reader(id); other->close_reader(id);
        // 同一个 transfer 反复创建/关闭端点，检查旧匹配不会满足新会话。
        for (unsigned n = 0; n < 8; ++n) {
            repair.session_id = new_transfer_id();
            const auto fresh = target->open_reader(repair);
            sender->open_writer(repair);
            check(!sender->writer_ready({private_reader}), "Retired endpoint cannot match new session");
            until([&] { return sender->writer_ready({fresh}); });
            sender->close_writer(); target->close_reader(id);
            private_reader = fresh;
        }
        // 卡死节点：Reader 已匹配但不取数据，拖住可靠 Writer；隔离其参与者后 Writer 恢复，健康 Reader 继续接收。
        auto stuck = make_dds_transport(std::stoul(argv[1]), topic, false);
        until([&] { return stuck->matched(); });
        const auto stall_id = new_transfer_id();
        const DataChannel stall{stall_id, stall_id, false};
        const auto stuck_reader = stuck->open_reader(stall);
        const auto live_reader = other->open_reader(stall);
        sender->open_writer(stall);
        until([&] { return sender->writer_ready({stuck_reader, live_reader}); });
        DataBlock filler{stall_id, stall_id, 0, std::vector<uint8_t>(max_chunk_bytes, 9)};
        uint64_t live_offset = 0;
        auto drain_live = [&] {
            DataBlock got;
            while (other->take(got)) {
                check(got.offset == live_offset, "Healthy Reader keeps ordered data while another Reader stalls");
                live_offset += got.payload.size();
            }
        };
        auto progressed = std::chrono::steady_clock::now();
        while (std::chrono::steady_clock::now() - progressed < 500ms) {
            if (sender->send(filler)) { filler.offset += filler.payload.size(); progressed = std::chrono::steady_clock::now(); }
            drain_live();
            std::this_thread::sleep_for(2ms);
        }
        check(filler.offset > 0 && live_offset == filler.offset, "Stalled Reader blocks Writer although healthy Reader is caught up");
        sender->isolate(stuck_reader);
        until([&] { drain_live(); return sender->send(filler); });
        check(!sender->writer_ready({stuck_reader}) && sender->writer_ready({live_reader}), "Isolation unmatches only the stalled participant");
        filler.offset += filler.payload.size();
        until([&] { drain_live(); return live_offset == filler.offset; });
        sender->close_writer(); other->close_reader(stall_id); stuck->close_reader(stall_id);
        std::cout << "PASS DDS target matching, bounded backpressure, control independence, zero non-target repair payload, endpoint recycling, stalled participant isolation; buffered_samples=" << accepted << std::endl;
        return 0;
    } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << std::endl; return 1; }
}
