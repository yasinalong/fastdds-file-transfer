// A sender that stays discovered without offering files, for idle-timeout tests.
#include "file_transfer/dds_transport.hpp"
#include <chrono>
#include <iostream>
#include <stdexcept>

int main(int argc, char** argv)
{
    try {
        if (argc != 3) throw std::runtime_error("presence_peer DOMAIN TOPIC");
        auto transport = file_transfer::make_dds_transport(std::stoul(argv[1]), argv[2], true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!transport->matched()) {
            if (std::chrono::steady_clock::now() > deadline) throw std::runtime_error("Presence discovery timeout");
            transport->wait(std::chrono::milliseconds(20));
        }
        std::cout << "ONLINE" << std::endl;
        std::string line;
        std::getline(std::cin, line); // DDS discovery threads stay alive while stdin waits.
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
