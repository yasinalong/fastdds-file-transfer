cmake_minimum_required(VERSION 3.22)
project(file_transfer LANGUAGES CXX)

find_package(fastdds 3.6.2 REQUIRED CONFIG)
find_package(fastcdr 2 REQUIRED CONFIG)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)
find_package(Threads REQUIRED)

# The file and transfer logic have no DDS dependency.
add_library(file_transfer_core STATIC src/file.cpp src/receiver.cpp src/receiver_service.cpp src/sender.cpp src/stream.cpp)
target_include_directories(file_transfer_core PUBLIC include)
target_compile_features(file_transfer_core PUBLIC cxx_std_17)
target_link_libraries(file_transfer_core PRIVATE OpenSSL::Crypto)

add_library(file_transfer_types STATIC
    generated/FileTransferPubSubTypes.cxx
    generated/FileTransferTypeObjectSupport.cxx)
target_include_directories(file_transfer_types PUBLIC generated)
target_compile_features(file_transfer_types PUBLIC cxx_std_17)
target_link_libraries(file_transfer_types PUBLIC fastdds fastcdr)

add_executable(file_transfer src/main.cpp src/dds_transport.cpp)
target_link_libraries(file_transfer PRIVATE file_transfer_core file_transfer_types)

add_executable(file_transfer_core_test tests/core_test.cpp)
target_link_libraries(file_transfer_core_test PRIVATE file_transfer_core Threads::Threads)
add_executable(file_transfer_presence_test tests/presence_peer.cpp src/dds_transport.cpp)
target_link_libraries(file_transfer_presence_test PRIVATE file_transfer_core file_transfer_types)
add_executable(file_transfer_transport_test tests/transport_test.cpp src/dds_transport.cpp)
target_link_libraries(file_transfer_transport_test PRIVATE file_transfer_core file_transfer_types Threads::Threads)
foreach(target file_transfer_transport_test file_transfer_core file_transfer file_transfer_core_test file_transfer_presence_test)
    target_compile_options(${target} PRIVATE "$<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Wall;-Wextra;-Wpedantic>")
endforeach()
