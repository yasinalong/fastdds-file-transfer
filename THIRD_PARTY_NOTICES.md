# 第三方组件

## 协议生成代码

`apps/file_transfer/generated/` 由官方 Fast DDS-Gen 4.3.0 根据仓库中的 IDL 生成。
保留 eProsima 原始版权头及 [Apache License 2.0](apps/file_transfer/generated/LICENSE) 副本。
生成器及 IDL-Parser 的确切来源提交见[生成来源](apps/file_transfer/generated/README.md)。

## 构建及运行依赖

以下项目由构建脚本下载或从系统安装中查找，其源码和二进制不随本仓库提交：

- [Fast DDS 3.6.2](https://github.com/eProsima/Fast-DDS/tree/v3.6.2)：Apache-2.0。
- [Fast CDR 2.3.6](https://github.com/eProsima/Fast-CDR/tree/v2.3.6)：Apache-2.0。
- [foonathan_memory_vendor 1.4.1](https://github.com/eProsima/foonathan_memory_vendor/tree/v1.4.1)：具体许可及其下载的内存分配器许可见上游仓库。
- [OpenSSL](https://www.openssl.org/)：许可取决于系统安装版本。
- SDK 自带的 Asio、TinyXML2 子模块及 Python 构建工具：以各自上游许可证为准。

再分发运行库时，请同时遵守对应版本的许可证要求并保留必要声明。
