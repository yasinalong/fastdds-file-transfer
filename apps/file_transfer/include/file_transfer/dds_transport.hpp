#pragma once
#include "file_transfer/protocol.hpp"
#include <cstdint>
#include <memory>
#include <string>

namespace file_transfer {
// udp_multicast 与 udp 使用相同的 UDPv4 配置，区别只在接收端数据 Reader 额外宣告组播地址。
enum class DdsTransportMode { tcp, udp, udp_multicast };

// 接收端数据 Reader 宣告的 IPv4 组播地址。同一轮次的接收端必须使用相同的组，发送端才会只发一份。
// port 为 0 时按 RTPS 默认用户组播端口 7400 + 250 * domain + 1 取值。发送端忽略此配置。
struct MulticastGroup {
    std::string address = "239.255.0.2";
    uint16_t port = 0;
};

// All Fast DDS types, QoS and wire conversions stay inside the implementation.
std::unique_ptr<Transport> make_dds_transport(uint32_t domain, const std::string& channel, bool sender,
        DdsTransportMode mode = DdsTransportMode::tcp, const MulticastGroup& group = {});
} // namespace file_transfer
