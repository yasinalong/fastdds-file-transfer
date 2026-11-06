# 构建说明

## 使用已有 SDK

若已有兼容的 Fast DDS 3.6.2 / Fast CDR 安装，可直接配置应用：

```bash
cmake -S . -B out/apps/custom \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PREFIX_PATH=/path/to/sdk/install
cmake --build out/apps/custom --parallel 4 --target file_transfer
```

OpenSSL 未自动发现时，通过 `-DOPENSSL_ROOT_DIR=/path/to/openssl` 提供安装前缀。
请保留 SDK 本身的运行库查找配置，或在运行前加载其环境脚本。

## 重新生成 IDL

普通构建不需要此步骤。修改协议后，使用官方 Fast DDS-Gen **4.3.0** 重新生成；需要 JDK 和网络：

```bash
git clone --branch v4.3.0 --depth 1 --recurse-submodules \
  https://github.com/eProsima/Fast-DDS-Gen.git out/tools/Fast-DDS-Gen
(
  cd out/tools/Fast-DDS-Gen
  GRADLE_USER_HOME="$PWD/../../cache/gradle" ./gradlew --no-daemon assemble
)
./scripts/generate_file_types.sh
./scripts/build_apps.sh dev
python3 scripts/file_transfer_test.py
```

可通过 `DDS_FASTDDSGEN=/path/to/fastddsgen` 使用已安装的生成器。
请保留生成代码中的版权头和许可证，并同步更新生成来源说明。
