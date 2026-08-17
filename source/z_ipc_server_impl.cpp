/**
 * @file z_ipc_server_impl.cpp
 * @brief Implementation of the IpcServer class.
 *
 * The server uses a pool of worker threads that poll the main queue.
 * Incoming messages are parsed; RPC requests (REQ) are dispatched to
 * the registered handler, and notifications (NOTIFY) are forwarded to
 * the appropriate notify handler. Responses are sent back to the client's
 * response queue using shared memory for binary data.
 *
 * The stop procedure sets running_ = false and notifies all workers.
 * An atomic counter tracks active workers; the main loop waits for all
 * workers to exit with a single timeout, then forcibly detaches any
 * remaining threads if they hang (e.g., due to blocking user callbacks).
 */

#include "z_ipc_server_impl.h"
#include "z_ipc_md5.h"
#include <iostream>
#include <cstring>
#include <random>
#include <sstream>
#include <chrono>
#include <thread>
#include <algorithm>
#include <iomanip>
#include <future>
#include <fstream>      // for reading /proc
#include <cctype>       // for isalnum

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

 // Reference to the library-internal log stream (defined in z_ipc_api.cpp)
extern std::ostream g_ipc_log_stream;

#ifdef ZIPC_ENABLE_LOG
#define ZIPC_LOG(msg) g_ipc_log_stream << "[ZIPC-Server] " << msg << std::endl
#define ZIPC_LOG_MD5(prefix, ptr, size) do { \
    if (ptr && size > 0) { \
        z_ipc::MD5 md5; \
        md5.update(ptr, size); \
        ZIPC_LOG(prefix << " MD5: " << md5.final_hex()); \
    } else { \
        ZIPC_LOG(prefix << " (zero-size)"); \
    } \
} while(0)
static std::string hex_dump(const void* data, size_t size, size_t max_len = 32) {
    std::stringstream ss;
    const unsigned char* p = (const unsigned char*)data;
    if (size == 0) {
        ss << "(empty)";
        return ss.str();
    }
    size_t show_len = (size < max_len) ? size : max_len;
    for (size_t i = 0; i < show_len; ++i) {
        char buf[4];
        sprintf(buf, "%02x ", p[i]);
        ss << buf;
    }
    if (size > max_len) ss << "... (total " << size << " bytes)";
    return ss.str();
}
#else
#define ZIPC_LOG(...) ((void)0)
#define ZIPC_LOG_MD5(...) ((void)0)
#define hex_dump(...) ((void)0)
#endif

static const std::string ZERO_SHM_MARKER = "__ZERO__";

// ========== 辅助函数 ==========
static int get_sysctl_int(const std::string& path) {
    std::ifstream f(path);
    int val = -1;
    if (f >> val) return val;
    return -1;
}
static int get_mq_msg_max() {
    return get_sysctl_int("/proc/sys/fs/mqueue/msg_max");
}
static int get_mq_msgsize_max() {
    return get_sysctl_int("/proc/sys/fs/mqueue/msgsize_max");
}
static bool is_valid_queue_name(const std::string& name) {
    if (name.empty()) return false;
    for (char c : name) {
        if (!(std::isalnum(c) || c == '_' || c == '/')) {
            return false;
        }
    }
    return true;
}

IpcServer::IpcServer() {
    ZIPC_LOG("IpcServer constructor");
}
IpcServer::~IpcServer() {
    ZIPC_LOG("IpcServer destructor, calling stop()");
    stop();
}

/*----------------------------------------------------------------------------*/
bool IpcServer::start(const std::string& qname, int thread_count,
    size_t max_queue_length, size_t max_msg_size) {
    ZIPC_LOG("start: ENTRY qname='" << qname << "', len=" << qname.length()
        << ", thread_count=" << thread_count
        << ", max_queue_len=" << max_queue_length
        << ", max_msg_size=" << max_msg_size);
    ZIPC_LOG("start: system msg_max=" << get_mq_msg_max()
        << ", msgsize_max=" << get_mq_msgsize_max());

    if (running_) {
        ZIPC_LOG("start: already running, stopping first");
        stop();
    }
    queue_name_ = qname;
    max_queue_length_ = max_queue_length;
    max_msg_size_ = max_msg_size;

    // 合法性检查
    if (!is_valid_queue_name(qname)) {
        ZIPC_LOG("start: INVALID queue name (only alnum, _, / allowed) -> returning false");
        return false;
    }

    try {
        ipc::message_queue::remove(qname.c_str());
        ZIPC_LOG("start: removed previous queue (if existed)");
        mq_ = std::make_shared<ipc::message_queue>(
            ipc::create_only, qname.c_str(), max_queue_length, max_msg_size);
        ZIPC_LOG("start: created queue '" << qname << "' with max_len=" << max_queue_length
            << ", max_msg=" << max_msg_size);
    }
    catch (const ipc::interprocess_exception& e) {
        int err = errno;
        ZIPC_LOG("start: FAILED to create queue: " << e.what()
            << " errno=" << err << " (" << strerror(err) << ")");
        ZIPC_LOG("start: msg_max=" << get_mq_msg_max()
            << ", msgsize_max=" << get_mq_msgsize_max());
        return false;
    }

    // 自动线程数
    if (thread_count == 0) {
        int hw = std::thread::hardware_concurrency();
        thread_count = static_cast<int>(std::min<size_t>(5, (size_t)hw));
        ZIPC_LOG("start: auto thread_count = " << thread_count);
    }
    ZIPC_LOG("start: starting " << thread_count << " worker threads");

    running_ = true;
    active_workers_ = thread_count;
    workers_.reserve(thread_count);
    for (int i = 0; i < thread_count; ++i) {
        workers_.emplace_back(&IpcServer::worker_thread_func, this, mq_);
        ZIPC_LOG("start: worker thread " << i << " launched");
    }
    ZIPC_LOG("start: server started successfully on queue '" << qname << "'");
    return true;
}

/*----------------------------------------------------------------------------*/
void IpcServer::stop() {
    ZIPC_LOG("stop: ENTRY");
    if (!running_) {
        ZIPC_LOG("stop: already stopped, return");
        return;
    }
    ZIPC_LOG("stop: setting running_=false, notifying workers");
    running_ = false;
    cv_.notify_all();

    // 等待工作线程退出（最多 2 秒）
    const auto timeout = std::chrono::seconds(2);
    auto start = std::chrono::steady_clock::now();
    while (active_workers_ > 0 &&
        (std::chrono::steady_clock::now() - start) < timeout) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (active_workers_ > 0) {
        ZIPC_LOG("stop: timeout, " << active_workers_ << " workers remain, detaching");
        for (size_t i = 0; i < workers_.size(); ++i) {
            if (workers_[i].joinable()) {
                workers_[i].detach();
                ZIPC_LOG("stop: detached worker " << i);
            }
        }
    }
    else {
        ZIPC_LOG("stop: all workers exited, joining");
        for (auto& t : workers_) {
            if (t.joinable()) {
                try {
                    t.join();
                    ZIPC_LOG("stop: joined a worker");
                }
                catch (...) {
                    ZIPC_LOG("stop: exception joining worker");
                }
            }
        }
    }
    workers_.clear();

    ipc::message_queue::remove(queue_name_.c_str());
    ZIPC_LOG("stop: removed queue '" << queue_name_ << "'");
    ZIPC_LOG("stop: server stopped");
}

/*----------------------------------------------------------------------------*/
int IpcServer::register_binary_reply(const std::string& name,
    ipc_binary_reply_handler h, void* trigger) {
    ZIPC_LOG("register_binary_reply: ENTRY name='" << name << "'");
    if (!running_) {
        ZIPC_LOG("register_binary_reply: server not running, return IPC_ERR_OPEN");
        return IPC_ERR_OPEN;
    }
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    if (binary_reply_handlers_.find(name) != binary_reply_handlers_.end()) {
        ZIPC_LOG("register_binary_reply: handler already exists for '" << name << "'");
        return IPC_ERR_BUSY;
    }
    binary_reply_handlers_[name] = { h, trigger };
    ZIPC_LOG("register_binary_reply: registered handler for '" << name
        << "' (trigger=" << trigger << ")");
    return IPC_OK;
}

int IpcServer::unregister_binary_reply(const std::string& name) {
    ZIPC_LOG("unregister_binary_reply: ENTRY name='" << name << "'");
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    auto it = binary_reply_handlers_.find(name);
    if (it == binary_reply_handlers_.end()) {
        ZIPC_LOG("unregister_binary_reply: handler not found");
        return IPC_ERR_NOT_FOUND;
    }
    binary_reply_handlers_.erase(it);
    ZIPC_LOG("unregister_binary_reply: unregistered handler '" << name << "'");
    return IPC_OK;
}

int IpcServer::register_binary_notify(const std::string& name,
    ipc_binary_notify_handler h, void* trigger) {
    ZIPC_LOG("register_binary_notify: ENTRY name='" << name << "'");
    if (!running_) {
        ZIPC_LOG("register_binary_notify: server not running");
        return IPC_ERR_OPEN;
    }
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    if (binary_notify_handlers_.find(name) != binary_notify_handlers_.end()) {
        ZIPC_LOG("register_binary_notify: handler already exists");
        return IPC_ERR_BUSY;
    }
    binary_notify_handlers_[name] = { h, trigger };
    ZIPC_LOG("register_binary_notify: registered notify handler for '" << name << "'");
    return IPC_OK;
}

int IpcServer::unregister_binary_notify(const std::string& name) {
    ZIPC_LOG("unregister_binary_notify: ENTRY name='" << name << "'");
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    auto it = binary_notify_handlers_.find(name);
    if (it == binary_notify_handlers_.end()) {
        ZIPC_LOG("unregister_binary_notify: handler not found");
        return IPC_ERR_NOT_FOUND;
    }
    binary_notify_handlers_.erase(it);
    ZIPC_LOG("unregister_binary_notify: unregistered '" << name << "'");
    return IPC_OK;
}

/*----------------------------------------------------------------------------*/
int IpcServer::send_notify_binary(const std::string& client_resp_queue,
    const std::string& func, const void* data, size_t size) {
    ZIPC_LOG("send_notify_binary: ENTRY queue='" << client_resp_queue
        << "', func='" << func << "', size=" << size);
    if (!running_) {
        ZIPC_LOG("send_notify_binary: server not running");
        return IPC_ERR_OPEN;
    }
    if (size > 512 * 1024 * 1024) {
        ZIPC_LOG("send_notify_binary: size too large (>512MB)");
        return IPC_ERR_SIZE;
    }

    if (size > 0 && data) {
        ZIPC_LOG("send_notify_binary: data hex=" << hex_dump(data, size));
        ZIPC_LOG_MD5("send_notify_binary sent data", data, size);
    }
    else {
        ZIPC_LOG("send_notify_binary: zero-size data");
    }

    std::string shm_name;
    if (size == 0) {
        shm_name = ZERO_SHM_MARKER;
        ZIPC_LOG("send_notify_binary: using ZERO marker");
    }
    else {
        shm_name = generate_unique_shm_name();
        ipc::shared_memory_object::remove(shm_name.c_str());
        ZIPC_LOG("send_notify_binary: creating shared memory '" << shm_name << "'");
        try {
            ipc::shared_memory_object shm(ipc::create_only, shm_name.c_str(), ipc::read_write);
            shm.truncate(size);
            ipc::mapped_region region(shm, ipc::read_write);
            if (data && size > 0) {
                std::memcpy(region.get_address(), data, size);
            }
            ZIPC_LOG("send_notify_binary: shared memory created, size=" << size);
        }
        catch (const std::exception& e) {
            ZIPC_LOG("send_notify_binary: failed to create shm: " << e.what());
            ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_MEMORY;
        }
    }

    std::string msg = "NOTIFY|BIN|" + func + "|" + shm_name;
    if (msg.size() > max_msg_size_) {
        ZIPC_LOG("send_notify_binary: message size " << msg.size() << " exceeds max " << max_msg_size_);
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SIZE;
    }

    try {
        ipc::message_queue mq(ipc::open_only, client_resp_queue.c_str());
        if (!mq.try_send(msg.c_str(), msg.size(), 0)) {
            ZIPC_LOG("send_notify_binary: queue full, send failed");
            if (shm_name != ZERO_SHM_MARKER)
                ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_BUSY;
        }
        ZIPC_LOG("send_notify_binary: sent to '" << client_resp_queue << "', msg='" << msg << "'");
    }
    catch (const std::exception& e) {
        ZIPC_LOG("send_notify_binary: exception sending: " << e.what());
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SEND;
    }
    return IPC_OK;
}

/*----------------------------------------------------------------------------*/
void IpcServer::worker_thread_func(std::shared_ptr<ipc::message_queue> mq) {
    struct WorkerGuard {
        IpcServer* server;
        WorkerGuard(IpcServer* s) : server(s) {}
        ~WorkerGuard() {
            if (server) {
                server->active_workers_--;
                ZIPC_LOG("worker_thread: active_workers_ decremented to " << server->active_workers_.load());
            }
        }
    } guard(this);

    try {
        std::vector<char> buffer(max_msg_size_);
        ZIPC_LOG("worker_thread: started, tid=" << std::this_thread::get_id());

        while (running_) {
            try {
                if (!running_) break;
                size_t recvd;
                unsigned priority;
                if (mq->try_receive(buffer.data(), buffer.size(), recvd, priority)) {
                    std::string msg(buffer.data(), recvd);
                    ZIPC_LOG("worker_thread: received message: '" << msg << "'");

                    size_t first_pipe = msg.find('|');
                    if (first_pipe == std::string::npos) {
                        ZIPC_LOG("worker_thread: malformed message (no pipe)");
                        continue;
                    }
                    std::string type = msg.substr(0, first_pipe);
                    ZIPC_LOG("worker_thread: message type = '" << type << "'");

                    if (type == "REQ") {
                        // Format: REQ|req_id|resp_queue|BIN|func|shm_name
                        size_t pos1 = first_pipe;
                        size_t pos2 = msg.find('|', pos1 + 1);
                        if (pos2 == std::string::npos) { ZIPC_LOG("worker_thread: REQ missing field"); continue; }
                        size_t pos3 = msg.find('|', pos2 + 1);
                        if (pos3 == std::string::npos) { ZIPC_LOG("worker_thread: REQ missing field"); continue; }
                        size_t pos4 = msg.find('|', pos3 + 1);
                        if (pos4 == std::string::npos) { ZIPC_LOG("worker_thread: REQ missing field"); continue; }
                        size_t pos5 = msg.find('|', pos4 + 1);
                        if (pos5 == std::string::npos) { ZIPC_LOG("worker_thread: REQ missing field"); continue; }

                        uint64_t req_id = 0;
                        try {
                            req_id = std::stoull(msg.substr(pos1 + 1, pos2 - pos1 - 1));
                        }
                        catch (...) {
                            ZIPC_LOG("worker_thread: invalid req_id");
                            continue;
                        }
                        std::string resp_queue = msg.substr(pos2 + 1, pos3 - pos2 - 1);
                        std::string msg_type = msg.substr(pos3 + 1, pos4 - pos3 - 1);
                        std::string func = msg.substr(pos4 + 1, pos5 - pos4 - 1);
                        std::string shm_name = msg.substr(pos5 + 1);

                        ZIPC_LOG("worker_thread: REQ req_id=" << req_id << ", resp_queue='" << resp_queue
                            << "', msg_type=" << msg_type << ", func='" << func
                            << "', shm='" << shm_name << "'");

                        int status = IPC_OK;
                        void* reply_bin = nullptr;
                        size_t reply_size = 0;

                        if (msg_type == "BIN") {
                            const void* bin_data = nullptr;
                            size_t bin_size = 0;
                            bool shm_ok = false;
                            ipc::mapped_region region;

                            if (shm_name == ZERO_SHM_MARKER) {
                                shm_ok = true;
                                bin_data = nullptr;
                                bin_size = 0;
                                ZIPC_LOG("worker_thread: zero-size binary request");
                            }
                            else {
                                try {
                                    ipc::shared_memory_object shm(ipc::open_only, shm_name.c_str(), ipc::read_only);
                                    region = ipc::mapped_region(shm, ipc::read_only);
                                    bin_data = region.get_address();
                                    bin_size = region.get_size();
                                    ZIPC_LOG("worker_thread: request data size=" << bin_size
                                        << ", hex=" << hex_dump(bin_data, bin_size));
                                    ZIPC_LOG_MD5("worker_thread received binary request", bin_data, bin_size);
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    shm_ok = true;
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("worker_thread: failed to open shm '" << shm_name << "': " << e.what());
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    status = IPC_ERR_RECEIVE;
                                }
                            }

                            if (status == IPC_OK && shm_ok) {
                                ipc_binary_reply_handler h = nullptr;
                                void* trigger = nullptr;
                                {
                                    std::lock_guard<std::mutex> lock(handlers_mutex_);
                                    auto it = binary_reply_handlers_.find(func);
                                    if (it != binary_reply_handlers_.end()) {
                                        h = it->second.first;
                                        trigger = it->second.second;
                                        ZIPC_LOG("worker_thread: found reply handler for '" << func << "'");
                                    }
                                    else {
                                        ZIPC_LOG("worker_thread: no reply handler for '" << func << "'");
                                    }
                                }
                                if (h) {
                                    void* out = nullptr;
                                    size_t out_size = 0;
                                    try {
                                        ZIPC_LOG("worker_thread: calling handler for '" << func << "'");
                                        h(trigger, bin_data, bin_size, &out, &out_size);
                                        ZIPC_LOG("worker_thread: handler returned out_size=" << out_size);
                                    }
                                    catch (...) {
                                        ZIPC_LOG("worker_thread: handler threw exception");
                                        status = IPC_ERR_UNKNOWN;
                                        if (out) ipc_free(out);
                                    }
                                    if (out && status == IPC_OK) {
                                        reply_bin = out;
                                        reply_size = out_size;
                                    }
                                    else if (out) {
                                        ipc_free(out);
                                    }
                                }
                                else {
                                    status = IPC_ERR_NOT_FOUND;
                                }
                            }
                        }
                        else {
                            status = IPC_ERR_INVAL;
                            ZIPC_LOG("worker_thread: unknown msg_type '" << msg_type << "'");
                        }

                        // 发送响应
                        if (status == IPC_OK) {
                            std::string shm_name_resp;
                            if (reply_size == 0) {
                                shm_name_resp = ZERO_SHM_MARKER;
                                ZIPC_LOG("worker_thread: zero-size response");
                            }
                            else {
                                shm_name_resp = generate_unique_shm_name();
                                ipc::shared_memory_object::remove(shm_name_resp.c_str());
                                try {
                                    ipc::shared_memory_object shm(ipc::create_only, shm_name_resp.c_str(), ipc::read_write);
                                    shm.truncate(reply_size);
                                    ipc::mapped_region region(shm, ipc::read_write);
                                    std::memcpy(region.get_address(), reply_bin, reply_size);
                                    ZIPC_LOG("worker_thread: response data size=" << reply_size
                                        << ", hex=" << hex_dump(reply_bin, reply_size));
                                    ZIPC_LOG_MD5("worker_thread binary reply", reply_bin, reply_size);
                                    ipc_free(reply_bin);
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("worker_thread: failed to create response shm: " << e.what());
                                    ipc::shared_memory_object::remove(shm_name_resp.c_str());
                                    ipc_free(reply_bin);
                                    send_response(resp_queue, req_id, IPC_ERR_MEMORY, "");
                                    continue;
                                }
                            }
                            send_response(resp_queue, req_id, IPC_OK, shm_name_resp);
                        }
                        else {
                            if (reply_bin) ipc_free(reply_bin);
                            ZIPC_LOG("worker_thread: sending error response status=" << status);
                            send_response(resp_queue, req_id, status, "");
                        }
                    }
                    else if (type == "NOTIFY") {
                        // Format: NOTIFY|type|func|data   (type always "BIN")
                        size_t pos1 = first_pipe;
                        size_t pos2 = msg.find('|', pos1 + 1);
                        if (pos2 == std::string::npos) { ZIPC_LOG("worker_thread: NOTIFY missing field"); continue; }
                        size_t pos3 = msg.find('|', pos2 + 1);
                        if (pos3 == std::string::npos) { ZIPC_LOG("worker_thread: NOTIFY missing field"); continue; }

                        std::string notify_type = msg.substr(pos1 + 1, pos2 - pos1 - 1);
                        std::string func = msg.substr(pos2 + 1, pos3 - pos2 - 1);
                        std::string shm_name = msg.substr(pos3 + 1);

                        ZIPC_LOG("worker_thread: NOTIFY type=" << notify_type << ", func='" << func
                            << "', shm='" << shm_name << "'");

                        if (notify_type == "BIN") {
                            if (shm_name == ZERO_SHM_MARKER) {
                                ipc_binary_notify_handler h = nullptr;
                                void* trigger = nullptr;
                                {
                                    std::lock_guard<std::mutex> lock(handlers_mutex_);
                                    auto it = binary_notify_handlers_.find(func);
                                    if (it != binary_notify_handlers_.end()) {
                                        h = it->second.first;
                                        trigger = it->second.second;
                                        ZIPC_LOG("worker_thread: found notify handler for '" << func << "'");
                                    }
                                    else {
                                        ZIPC_LOG("worker_thread: no notify handler for '" << func << "'");
                                    }
                                }
                                if (h) {
                                    try {
                                        h(trigger, nullptr, 0);
                                        ZIPC_LOG("worker_thread: zero-size notify handler executed");
                                    }
                                    catch (...) {
                                        ZIPC_LOG("worker_thread: notify handler threw exception");
                                    }
                                }
                            }
                            else {
                                try {
                                    ipc::shared_memory_object shm(ipc::open_only, shm_name.c_str(), ipc::read_only);
                                    ipc::mapped_region region(shm, ipc::read_only);
                                    const void* bin_data = region.get_address();
                                    size_t bin_size = region.get_size();
                                    ZIPC_LOG("worker_thread: notify data size=" << bin_size
                                        << ", hex=" << hex_dump(bin_data, bin_size));
                                    ZIPC_LOG_MD5("worker_thread binary notify received", bin_data, bin_size);

                                    ipc_binary_notify_handler h = nullptr;
                                    void* trigger = nullptr;
                                    {
                                        std::lock_guard<std::mutex> lock(handlers_mutex_);
                                        auto it = binary_notify_handlers_.find(func);
                                        if (it != binary_notify_handlers_.end()) {
                                            h = it->second.first;
                                            trigger = it->second.second;
                                            ZIPC_LOG("worker_thread: found notify handler for '" << func << "'");
                                        }
                                        else {
                                            ZIPC_LOG("worker_thread: no notify handler for '" << func << "'");
                                        }
                                    }
                                    if (h) {
                                        try {
                                            h(trigger, bin_data, bin_size);
                                            ZIPC_LOG("worker_thread: notify handler executed, size=" << bin_size);
                                        }
                                        catch (...) {
                                            ZIPC_LOG("worker_thread: notify handler threw exception");
                                        }
                                    }
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("worker_thread: failed to open notify shm: " << e.what());
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                }
                            }
                        }
                        else {
                            ZIPC_LOG("worker_thread: unknown notify_type '" << notify_type << "'");
                        }
                    }
                    else {
                        ZIPC_LOG("worker_thread: unknown message type '" << type << "'");
                    }
                }
                else {
                    // 队列空，等待
                    std::unique_lock<std::mutex> lock(cv_mutex_);
                    cv_.wait_for(lock, std::chrono::milliseconds(10),
                        [this] { return !running_; });
                }
            }
            catch (const std::exception& e) {
                ZIPC_LOG("worker_thread: exception in loop: " << e.what());
            }
            catch (...) {
                ZIPC_LOG("worker_thread: unknown exception in loop");
            }
        }
    }
    catch (const std::exception& e) {
        ZIPC_LOG("worker_thread: fatal exception: " << e.what());
    }
    catch (...) {
        ZIPC_LOG("worker_thread: fatal unknown exception");
    }
    ZIPC_LOG("worker_thread: exiting");
}

/*----------------------------------------------------------------------------*/
void IpcServer::send_response(const std::string& resp_queue, uint64_t req_id,
    int status, const std::string& shm_name) {
    ZIPC_LOG("send_response: ENTRY resp_queue='" << resp_queue << "', req_id=" << req_id
        << ", status=" << status << ", shm_name='" << shm_name << "'");

    std::string msg = "RSP|" + std::to_string(req_id) + "|" + std::to_string(status) +
        "|BIN|" + shm_name;
    if (msg.size() > max_msg_size_) {
        ZIPC_LOG("send_response: response too large (" << msg.size() << " > " << max_msg_size_ << ")");
        if (!shm_name.empty() && shm_name != ZERO_SHM_MARKER) {
            ipc::shared_memory_object::remove(shm_name.c_str());
            ZIPC_LOG("send_response: removed shm due to size overflow");
        }
        msg = "RSP|" + std::to_string(req_id) + "|" + std::to_string(IPC_ERR_SIZE) + "|BIN|";
        ZIPC_LOG("send_response: truncated to error response");
    }

    bool send_ok = false;
    try {
        ipc::message_queue mq(ipc::open_only, resp_queue.c_str());
        if (mq.try_send(msg.c_str(), msg.size(), 0)) {
            send_ok = true;
            ZIPC_LOG("send_response: sent response to '" << resp_queue << "', msg='" << msg << "'");
        }
        else {
            ZIPC_LOG("send_response: queue full, send failed");
        }
    }
    catch (const std::exception& e) {
        ZIPC_LOG("send_response: exception sending: " << e.what());
    }

    // 延迟清理共享内存（如果发送成功且不是 ZERO）
    if (send_ok && !shm_name.empty() && shm_name != ZERO_SHM_MARKER) {
        ZIPC_LOG("send_response: scheduling delayed cleanup for shm '" << shm_name << "'");
        std::thread([shm_name]() {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            try {
                ipc::shared_memory_object::remove(shm_name.c_str());
                ZIPC_LOG("send_response: delayed cleanup removed shm " << shm_name);
            }
            catch (...) {
                // ignore
            }
            }).detach();
    }

    if (!send_ok && !shm_name.empty() && shm_name != ZERO_SHM_MARKER) {
        ipc::shared_memory_object::remove(shm_name.c_str());
        ZIPC_LOG("send_response: immediately removed shm due to send failure");
    }
}

/*----------------------------------------------------------------------------*/
std::string IpcServer::generate_unique_shm_name() {
    static std::atomic<uint64_t> counter{ 0 };
#ifdef _WIN32
    DWORD pid = GetCurrentProcessId();
#else
    pid_t pid = getpid();
#endif
    uint64_t seq = counter.fetch_add(1);
    auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::stringstream ss;
    ss << "ipc_shm_resp_" << pid << "_" << seq << "_" << std::hex << now;
    std::string name = ss.str();
    ZIPC_LOG("generate_unique_shm_name: generated '" << name << "'");
    return name;
}