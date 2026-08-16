# z_ipc 构建说明（Linux）

> **一句话概括：** 在 Linux 下编译 z_ipc，只需要装好基础开发工具，然后跑一个脚本，剩下的全自动。  
> Windows 用户直接使用预编译好的 `z_ipc_64.dll` / `z_ipc_32.dll`，无需折腾编译。

---

## 📦 文件清单

| 文件                           | 说明                           |
| ------------------------------ | ------------------------------ |
| `boost_1_83_0.tar.gz`          | Boost 源码包（构建必需）       |
| `build_on_linux.sh`            | 全自动构建脚本（核心）         |
| `CMakeLists.txt`               | CMake 工程文件（脚本自动生成） |
| `z_ipc_api.h` / `.cpp`         | C API 接口层                   |
| `z_ipc_client_impl.h` / `.cpp` | 客户端实现                     |
| `z_ipc_server_impl.h` / `.cpp` | 服务端实现                     |
| `z_ipc_md5.h`                  | MD5 工具（只调试模式日志用）   |
| `readme.md`                    | 项目总览                       |

---

## 🛠 依赖项（Linux）

构建前请确保系统已安装以下基础工具：

| 依赖      | 说明                                   | 安装命令（Ubuntu/Debian） | 安装命令（CentOS/RHEL）    |
| --------- | -------------------------------------- | ------------------------- | -------------------------- |
| **g++**   | C++ 编译器（支持 C++17）               | `sudo apt install g++`    | `sudo yum install gcc-c++` |
| **make**  | 构建工具                               | `sudo apt install make`   | `sudo yum install make`    |
| **cmake** | ≥ 3.15                                 | `sudo apt install cmake`  | `sudo yum install cmake`   |
| **b2**    | Boost 构建工具（脚本会自动下载并构建） | 无需额外安装              | 无需额外安装               |
| **tar**   | 解压工具                               | 通常已预装                | 通常已预装                 |

> 💡 **b2 会自动编译**，但需要系统有 Python（用于 Boost 的构建系统，一般系统自带）。  
> 如果你使用 CentOS 7，可能需要额外安装 `devtoolset` 来获得较新的 GCC。

---

## 🚀 一键构建（全自动）

### 1. 解压 Boost

- `boost_1_83_0.tar.gz` 放在 [https://github.com/PassByYou888/zIPC/releases/download/boost_1_83_0.tar.gz/boost_1_83_0.tar.gz](https://github.com/PassByYou888/zIPC/releases/download/boost_1_83_0.tar.gz/boost_1_83_0.tar.gz)
- 将 `boost_1_83_0.tar.gz` 放在项目source目录（与 `build_on_linux.sh` 同级），脚本会自动解压。

### 2. 赋予脚本执行权限

```bash
chmod +x build_on_linux.sh
```

### 3. 运行构建脚本

```bash
./build_on_linux.sh
```

**脚本会依次自动完成：**

1. 解压 `boost_1_83_0.tar.gz`（若尚未解压）
2. 进入 Boost 目录，执行 `./bootstrap.sh --with-libraries=date_time`
3. 执行 `./b2 -j$(nproc) stage` 编译静态库 `libboost_date_time.a`
4. 在项目根目录生成 `CMakeLists.txt`（硬编码了本地 Boost 路径）
5. 创建 `build/` 目录，执行 `cmake ..` + `make -j$(nproc)`
6. 输出最终产物：`build/libz_ipc.so`

### 4. 构建产物

| 文件           | 路径                                    |
| -------------- | --------------------------------------- |
| **共享库**     | `build/libz_ipc.so`                     |
| 静态库（若有） | `build/libz_ipc.a`（取决于 CMake 配置） |
| 头文件         | 项目根目录下的 `*.h`（直接引用即可）    |

---

## 🧩 多架构与多平台支持

`build_on_linux.sh` 设计为 **架构无关**，不硬编码任何 CPU 指令集，因此可以在以下平台顺利构建：

| 架构                   | 支持状态               | 说明                                    |
| ---------------------- | ---------------------- | --------------------------------------- |
| **x86_64**             | ✅ 完整支持             | 主流服务器                              |
| **ARM64 (aarch64)**    | ✅ 完整支持             | 鲲鹏、飞腾、AWS Graviton                |
| **ARM32 (armv7)**      | ✅ 支持（需适当工具链） | 嵌入式场景                              |
| **RISC-V 64 (rv64gc)** | ✅ 完整支持             | 已在 Debian RISC-V 上通过编译并稳定运行 |
| **龙芯 (loongarch64)** | ✅ 完整支持             | 已在 Loongnix 系统上通过编译并稳定运行  |
| **RISC-V 32**          | ⚠️ 待验证               | 理论上兼容，暂无测试环境                |
| **LoongArch (旧世界)** | ⚠️ 待验证               | 需自行确认编译器兼容性                  |

脚本自动使用 `-j$(nproc)` 并行编译，充分利用多核性能。

支持的 Linux 发行版（不限于）：

- Ubuntu 18.04 / 20.04 / 22.04 / 24.04
- Debian 10 / 11 / 12（含 RISC-V 移植版）
- Loongnix（龙芯平台官方系统）
- CentOS 7 / 8 / Stream
- RHEL 8 / 9
- Fedora 36+
- openSUSE Leap 15+
- Alpine Linux（需额外安装 `libstdc++` 等）

---

## ⚙️ 构建选项（可选）

若需要调整构建参数，可以修改 `build_on_linux.sh` 中的变量：

```bash
# 在脚本顶部可调整
ZIPC_BUILD_SHARED=ON        # 编译为共享库（默认）
ZIPC_ENABLE_LOGGING=OFF     # 是否启用调试日志
CMAKE_BUILD_TYPE=Release    # 或 Debug
```

---

## 🧪 验证构建结果

```bash
# 查看产物
ls -l build/libz_ipc.so

# 检查符号
nm -D build/libz_ipc.so | grep -i "ipc"

# 运行测试（若有）
cd build && make test
```

---

## 📝 注意事项

1. **Boost 版本固定为 1.83.0**，请勿替换其他版本，否则可能 ABI 不兼容。
2. **磁盘空间**：Boost 解压后约 800 MB，构建临时文件约 1.5 GB，请确保 `/tmp` 或构建目录有足够空间。
3. **网络**：脚本不需要联网（Boost 已包含），全部本地完成。
4. **权限**：若提示 `Permission denied`，请执行 `chmod +x build_on_linux.sh`。
5. **交叉编译**：如需为其他架构交叉编译，需手动设置 `CMAKE_TOOLCHAIN_FILE`，脚本暂未内置。

---

## 🔗 相关链接

- [项目 README](./readme.md)
- [zAPI项目](https://github.com/PassByYou888/zAPI)
- [Boost 官网](https://www.boost.org/)
- [CMake 文档](https://cmake.org/documentation/)

---

**构建愉快！** 🚀 如果遇到问题，先看控制台输出——脚本会将每一步的日志都打印出来，方便排查。
