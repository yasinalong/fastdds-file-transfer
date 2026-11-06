# Fast DDS File Transfer

基于 **C++17 / eProsima Fast DDS 3.6.2** 的局域网命令行文件传输工具。
支持任意类型文件、一对多分发、常驻接收、SHA-256 校验及进程重启后的断点续传。
按块读写文件，无需把整个文件载入内存。

## 构建

支持 macOS / Linux，需要 Git、Python 3.10+、CMake 3.22+、Make、C++17 编译器和 OpenSSL 开发包。
macOS 需要 Xcode Command Line Tools，可通过 Homebrew 安装 CMake、Python 和 OpenSSL 3；
Debian / Ubuntu 可安装 `build-essential cmake git python3 python3-venv libssl-dev`。

在仓库根目录运行：

```bash
./scripts/bootstrap_sdk.sh
./scripts/build_sdk.sh
./scripts/build_apps.sh release --target file_transfer
```

首次准备需要联网。Python 工具安装到 `.venv/`，SDK 源码下载到 `sdk/src/`，
编译和下载缓存位于 `out/`。普通构建使用仓库中已有的 IDL 生成代码，不需要 Java。

| 依赖 | 固定版本 |
| --- | --- |
| Fast DDS | 3.6.2 |
| Fast CDR | 2.3.6 |
| foonathan_memory_vendor | 1.4.1 |

macOS 构建脚本自动查找 Homebrew `openssl@3`。如果应用配置时找不到 OpenSSL，可执行：

```bash
cmake --preset release -DOPENSSL_ROOT_DIR="$(brew --prefix openssl@3)"
./scripts/build_apps.sh release --target file_transfer
```

可以通过 `DDS_BUILD_JOBS=8` 调整构建并行数。SDK 和应用使用 CMake 安装目标链接。
程序依赖 SDK 与 OpenSSL 动态库；复制到其他机器时，需要一并部署对应平台的运行库并配置运行时路径。

## 快速开始

接收设备运行：

```bash
./out/apps/release/bin/file_transfer receive --output out/received --domain 42 --topic files
```

发送设备运行：

```bash
./out/apps/release/bin/file_transfer send /path/to/example.bin --domain 42 --topic files --receivers 1
```

两端需要能通过局域网发现彼此，并使用相同的 domain、topic 和 transport。
默认数据传输走 TCP，发现使用 UDP；请允许本机防火墙中的相关流量。

接收端沿用源文件名，完成长度和 SHA-256 校验后才发布最终文件。
已存在的同名不同内容文件不会被覆盖。传输中断后重新执行相同发送命令即可协商续传。
接收进程默认持续监听，可用 `--idle-timeout 60` 在没有发送端在线 60 秒后退出。

一对多时，在各设备启动接收端，并使用 `--receivers N` 指定至少等待的接收端数量。
传输方式可选 `tcp`、可靠 `udp` 以及实验性 `udp-multicast`。

完整参数、退出码、断点文件处理和限制见[使用说明](apps/file_transfer/README.md)。
本工具面向受信任局域网，未提供身份认证和传输加密。文件系统须支持 POSIX 文件锁、硬链接及同步操作。
当前源码面向 macOS / Linux，未适配 Windows。

## 功能验证

```bash
./scripts/build_apps.sh dev
python3 scripts/file_transfer_test.py
```

测试使用自动生成的数据，检查多接收端、重启续传、并发发送、完整性校验和错误处理。
结果和临时文件保存在 `out/tests/`，需要允许本机 TCP / UDP 通信。

## 源码

- `apps/file_transfer/src/`、`include/`：文件传输与 DDS 通信实现。
- `apps/file_transfer/idl/`、`generated/`：协议定义和生成代码。
- `apps/file_transfer/tests/`：功能测试。
- `scripts/`、`sdk/`：依赖下载与构建入口。

生成代码的工具版本、来源与许可证见[生成来源](apps/file_transfer/generated/README.md)。
如需重新生成协议代码，见[构建说明](docs/build.md)。
第三方依赖及许可证见[第三方说明](THIRD_PARTY_NOTICES.md)。本仓库暂未为手写代码指定开源许可证。
