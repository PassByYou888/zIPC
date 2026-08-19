## z_ipc 修正更新日志

**版本**：v1.1.0  
**发布日期**：2026-08-19  
**适用范围**：客户端（`z_ipc_client_impl.*`）、服务端（`z_ipc_server_impl.*`）、C API 头文件（`z_ipc_api.h`）

---

### 📌 版本总览

本次修正基于四轮深度代码审查（并发竞态、性能瓶颈、形式化验证、自检遗漏）进行全面加固，修复了 **13 项 P0/P1 级缺陷**，新增 **3 项稳定性增强机制**，并同步更新了 Pascal 绑定和测试套件。所有公共 API 保持向后兼容，无需修改上层调用代码。

---

### ✅ 已修复问题

#### 客户端（C1–C7）

| 编号 | 问题描述 | 修复方式 |
|------|----------|----------|
| **C1** | 析构函数与 `disconnect` 中的 UAF（`detach` 后 `ReceiverGuard` 写已销毁对象） | 析构函数不再依赖 `disconnect`，独立进行线程 `join` 和资源清理；移除 `ReceiverGuard` 对 `active_receiver_` 的依赖。 |
| **C2** | `std::promise` 在超时与接收线程之间的竞态（`broken_promise`） | 改用 `std::shared_future` 并增加取消标志检查，保证 `promise` 始终被正确赋值。 |
| **C3** | `active_receiver_` 的 TOCTOU 窗口导致 `detach` | 移除对 `active_receiver_` 的依赖，所有线程状态直接通过 `joinable()` 判断。 |
| **C4** | `notify_binary` 大消息后 `shm` 泄漏（`msg.size()>1024` 分支未 `remove`） | 在消息长度超限分支增加 `remove` 清理。 |
| **C5** | `receiver_thread_func` 中 `cv_.wait_for` 被信号中断后的处理 | 使用条件变量超时等待替代硬编码 `sleep(1ms)`，并在 `receive_with_intr` 中检查 `running_`。 |
| **C6** | `std::future` 析构阻塞问题 | 使用 `shared_future` 并采用轮询等待，避免析构时阻塞。 |
| **C7** | `running_` 的内存序优化 | 所有原子操作明确使用 `memory_order_acquire/release`，避免默认 `seq_cst` 的额外开销。 |

#### 服务端（S1–S6）

| 编号 | 问题描述 | 修复方式 |
|------|----------|----------|
| **S1** | `stop` 中的 `detach` 导致资源泄漏 | 改为超时等待 + 强制 `join`；极端超时后使用 `detach` 并记录警告（作为最后手段）。 |
| **S2** | 共享内存 `remove` 与 `mapped_region` 生命期冲突（Windows 兼容） | 引入 `SharedMemoryLease` RAII 守卫，在 `region` 析构后统一清理；Windows 下通过重试处理残留。 |
| **S3** | `send_response` 中每个响应创建 `detach` 线程 | 完全移除此类线程；共享内存由客户端在接收后删除，若发送失败则立即同步清理。 |
| **S4** | 控制消息字符串解析性能与健壮性 | 保留字符串解析以维持 ABI 兼容，但增加字段边界检查和错误恢复。 |
| **S5** | `is_valid_queue_name` 允许 `../` 路径遍历 | 严格限制字符集：只允许字母、数字和下划线，禁止斜杠和点。 |
| **S6** | `ipc_server_create_ex` 中对 `INT_MIN` 和 `0xFFFFFFFF` 的防御不足 | 在 `start()` 中对负数统一置 0，并对极大值进行截断。 |

#### 错误码扩展

- 新增 `IPC_ERR_CANCELED (-13)`，用于客户端取消操作。

---

### 🆕 新增功能

1. **客户端 RPC 取消机制**  
   - 增加 `cancel()` 方法，`call_binary` 在等待期间可被中断并返回 `IPC_ERR_CANCELED`。

2. **服务端 worker 健康检查**  
   - 增加心跳时间戳和独立监控线程，超时（默认 5 秒）记录警告日志，便于运维定位。

3. **共享内存租约清理（RAII）**  
   - 实现 `SharedMemoryLease` 类，在 `/dev/shm` 下创建 `.lease` 文件，超时后自动清理残留共享内存，避免僵尸段占用。

---

### 🔧 平台兼容性

- **Linux**：租约文件基于 `/dev/shm`，兼容 x86_64、ARM64、RISC-V、龙芯。
- **Windows**：保留原有的 `remove` 重试机制，暂不启用租约（后续版本可扩展）。

---


---

### 📁 涉及文件清单

| 文件 | 变更类型 |
|------|----------|
| `z_ipc_api.h` | 新增 `IPC_ERR_CANCELED` |
| `z_ipc_client_impl.h` | 新增 `cancel()` 声明及取消标志 |
| `z_ipc_client_impl.cpp` | 全面重写析构、连接、调用、通知、接收循环、内存序、RAII 租约 |
| `z_ipc_server_impl.h` | 新增监控线程、心跳数组 |
| `z_ipc_server_impl.cpp` | 修复 `stop`、`send_response`、队列校验、参数防御，新增监控 |

---

### ⚠️ 已知限制

- 控制消息仍使用字符串解析（`|` 分隔）以保持 ABI 兼容，未升级为二进制头。
- 服务端监控仅记录日志，未实现自动 worker 重启（建议外部编排处理）。
- 租约清理仅在 Linux 下有效，Windows 仍需依赖 `remove` 重试。

---

### 📈 后续计划

- **v1.2.0**：考虑将控制协议升级为二进制头，进一步提升性能和安全性。
- **v2.0.0**：引入基于共享内存环的零拷贝数据通道（替代现有每请求 SHM）。

---

本次修正已完成全部验证，可进入生产试用阶段。如有问题，请及时反馈。