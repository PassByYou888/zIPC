/**
 * @file z_ipc_client_impl.cpp
 * @brief Implementation of the IpcClient class.
 *
 * This file contains all client?side logic: connecting, sending messages,
 * handling replies, and processing server?originated notifications.
 * Binary payloads are transferred via shared memory segments.
 * A special marker "__ZERO__" is used for zero size data to avoid creating
 * unnecessary shared memory objects.
 *
 * The disconnect procedure uses the running_ flag and condition variable;
 * an atomic counter (active_receiver_) tracks the receiver thread's lifetime,
 * allowing a short timeout (500 ms) instead of a long join wait.
 * This eliminates the 5?second delay observed in previous versions.
 */

#include "z_ipc_client_impl.h"
#include "z_ipc_api.h"
#include "z_ipc_md5.h"
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

 // Reference to the library-internal log stream (defined in z_ipc_api.cpp)
    extern std::ostream g_ipc_log_stream;

#ifdef ZIPC_ENABLE_LOG
#define ZIPC_LOG(msg) g_ipc_log_stream << "[ZIPC-Client] " << msg << std::endl
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

IpcClient::IpcClient() = default;
IpcClient::~IpcClient() { disconnect(); }

/*----------------------------------------------------------------------------*/
bool IpcClient::connect(const std::string& qname) {
    if (running_) {
        ZIPC_LOG("connect: already running, disconnecting first");
        disconnect();
    }
    server_queue_name_ = qname;

    try {
        mq_ = std::make_unique<ipc::message_queue>(ipc::open_only, qname.c_str());
        ZIPC_LOG("connect: opened main queue '" << qname << "'");
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("connect: failed to open main queue '" << qname << "': " << e.what());
        return false;
    }

    resp_queue_name_ = generate_unique_resp_queue();
    try {
        ipc::message_queue::remove(resp_queue_name_.c_str());
        resp_mq_ = std::make_shared<ipc::message_queue>(
            ipc::create_only, resp_queue_name_.c_str(), 100, 1024);
        ZIPC_LOG("connect: created response queue '" << resp_queue_name_ << "'");
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("connect: failed to create response queue: " << e.what());
        mq_.reset();
        return false;
    }

    running_ = true;
    active_receiver_ = 1;   // Mark receiver as active
    receiver_ = std::thread(&IpcClient::receiver_thread_func, this, resp_mq_);
    ZIPC_LOG("connect: connected to server queue '" << qname << "', resp queue '" << resp_queue_name_ << "'");
    return true;
}

/*----------------------------------------------------------------------------*/
void IpcClient::disconnect() {
    if (!running_) {
        ZIPC_LOG("disconnect: already disconnected");
        return;
    }
    ZIPC_LOG("disconnect: disconnecting...");

    running_ = false;
    cv_.notify_all();

    // Wait for the receiver thread to finish (max 500 ms)
    const auto timeout = std::chrono::milliseconds(500);
    auto start = std::chrono::steady_clock::now();
    while (active_receiver_ > 0 &&
        (std::chrono::steady_clock::now() - start) < timeout) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (active_receiver_ > 0) {
        ZIPC_LOG("disconnect: receiver thread did not finish within timeout, detaching");
        if (receiver_.joinable())
            receiver_.detach();
    }
    else {
        if (receiver_.joinable()) {
            try {
                receiver_.join();
            }
            catch (const std::exception& e) {
                ZIPC_LOG("disconnect: receiver join exception: " << e.what());
                if (receiver_.joinable())
                    receiver_.detach();
            }
        }
    }

    mq_.reset();
    resp_mq_.reset();
    ipc::message_queue::remove(resp_queue_name_.c_str());
    ZIPC_LOG("disconnect: removed response queue '" << resp_queue_name_ << "'");

    // Force all pending requests to fail.
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (auto& kv : pending_) {
            Response resp;
            resp.status = IPC_ERR_UNKNOWN;
            resp.bin_data = nullptr;
            resp.bin_size = 0;
            kv.second->set_value(resp);
            ZIPC_LOG("disconnect: forced response for req_id " << kv.first);
        }
        pending_.clear();
    }

    {
        std::lock_guard<std::mutex> lock(notify_mutex_);
        binary_notify_handlers_.clear();
        ZIPC_LOG("disconnect: cleared notification handlers");
    }

    ZIPC_LOG("disconnect: disconnected");
}

/*----------------------------------------------------------------------------*/
int IpcClient::register_binary_notify(const std::string& name,
    ipc_binary_notify_handler h, void* trigger) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    if (binary_notify_handlers_.find(name) != binary_notify_handlers_.end()) {
        ZIPC_LOG("register_binary_notify: handler '" << name << "' already exists");
        return IPC_ERR_BUSY;
    }
    binary_notify_handlers_[name] = { h, trigger };
    ZIPC_LOG("register_binary_notify: registered binary notify handler '" << name << "'");
    return IPC_OK;
}

int IpcClient::unregister_binary_notify(const std::string& name) {
    std::lock_guard<std::mutex> lock(notify_mutex_);
    auto it = binary_notify_handlers_.find(name);
    if (it == binary_notify_handlers_.end()) {
        ZIPC_LOG("unregister_binary_notify: handler '" << name << "' not found");
        return IPC_ERR_NOT_FOUND;
    }
    binary_notify_handlers_.erase(it);
    ZIPC_LOG("unregister_binary_notify: unregistered binary notify handler '" << name << "'");
    return IPC_OK;
}

/*----------------------------------------------------------------------------*/
int IpcClient::call_binary(const std::string& func, const void* data, size_t size,
    void** out_data, size_t* out_size) {
    if (!mq_ || !resp_mq_) {
        ZIPC_LOG("call_binary: not connected");
        return IPC_ERR_OPEN;
    }
    if (func.empty()) {
        ZIPC_LOG("call_binary: empty function name");
        return IPC_ERR_INVAL;
    }
    if (size > 512 * 1024 * 1024) {
        ZIPC_LOG("call_binary: size " << size << " exceeds 512MB");
        return IPC_ERR_SIZE;
    }

    if (size > 0 && data) {
        ZIPC_LOG("call_binary: func='" << func << "', send_size=" << size
            << ", hex=" << hex_dump(data, size));
        ZIPC_LOG_MD5("call_binary sent data", data, size);
    }
    else {
        ZIPC_LOG("call_binary: func='" << func << "', zero-size");
    }

    uint64_t req_id = next_req_id_.fetch_add(1);
    std::string shm_name;

    if (size == 0) {
        shm_name = ZERO_SHM_MARKER;
        ZIPC_LOG("call_binary: zero-size data, using marker");
    }
    else {
        shm_name = generate_unique_shm_name();
        ipc::shared_memory_object::remove(shm_name.c_str());
        try {
            ipc::shared_memory_object shm(ipc::create_only, shm_name.c_str(), ipc::read_write);
            shm.truncate(size);
            ipc::mapped_region region(shm, ipc::read_write);
            if (data && size > 0) {
                std::memcpy(region.get_address(), data, size);
            }
            ZIPC_LOG("call_binary: created shared memory '" << shm_name << "', size=" << size);
        }
        catch (const std::exception& e) {
            ZIPC_LOG("call_binary: failed to create shared memory: " << e.what());
            ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_MEMORY;
        }
    }

    std::string msg = "REQ|" + std::to_string(req_id) + "|" + resp_queue_name_ +
        "|BIN|" + func + "|" + shm_name;
    if (msg.size() > 1024) {
        ZIPC_LOG("call_binary: message size " << msg.size() << " exceeds 1024");
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SIZE;
    }

    auto promise = std::make_shared<std::promise<Response>>();
    auto future = promise->get_future();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_[req_id] = promise;
        ZIPC_LOG("call_binary: pending req_id=" << req_id << ", func='" << func << "', shm='" << shm_name << "'");
    }

    try {
        mq_->send(msg.c_str(), msg.size(), 0);
        ZIPC_LOG("call_binary: sent request req_id=" << req_id);
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("call_binary: send failed: " << e.what());
        pending_.erase(req_id);
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SEND;
    }

    auto status = future.wait_for(timeout_);
    if (status == std::future_status::timeout) {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_.erase(req_id);
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        ZIPC_LOG("call_binary: timeout for req_id=" << req_id);
        return IPC_ERR_TIMEOUT;
    }

    Response resp = future.get();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_.erase(req_id);
    }
    if (shm_name != ZERO_SHM_MARKER)
        ipc::shared_memory_object::remove(shm_name.c_str());

    if (resp.status != IPC_OK) {
        ZIPC_LOG("call_binary: response status error " << resp.status << " for req_id=" << req_id);
        return resp.status;
    }
    *out_data = resp.bin_data;
    *out_size = resp.bin_size;
    if (resp.bin_data && resp.bin_size > 0) {
        ZIPC_LOG("call_binary received reply size=" << resp.bin_size
            << ", hex=" << hex_dump(resp.bin_data, resp.bin_size));
        ZIPC_LOG_MD5("call_binary received reply", resp.bin_data, resp.bin_size);
    }
    else {
        ZIPC_LOG("call_binary received zero-size reply");
    }
    ZIPC_LOG("call_binary: success, reply_size=" << *out_size);
    return IPC_OK;
}

/*----------------------------------------------------------------------------*/
int IpcClient::notify_binary(const std::string& func, const void* data, size_t size) {
    if (!mq_) {
        ZIPC_LOG("notify_binary: not connected");
        return IPC_ERR_OPEN;
    }
    if (func.empty()) {
        ZIPC_LOG("notify_binary: empty function name");
        return IPC_ERR_INVAL;
    }
    if (size > 512 * 1024 * 1024) {
        ZIPC_LOG("notify_binary: size " << size << " exceeds 512MB");
        return IPC_ERR_SIZE;
    }

    if (size > 0 && data) {
        ZIPC_LOG("notify_binary: func='" << func << "', size=" << size
            << ", hex=" << hex_dump(data, size));
        ZIPC_LOG_MD5("notify_binary sent data", data, size);
    }
    else {
        ZIPC_LOG("notify_binary: func='" << func << "', zero-size");
    }

    std::string shm_name;
    if (size == 0) {
        shm_name = ZERO_SHM_MARKER;
        ZIPC_LOG("notify_binary: zero-size data, using marker");
    }
    else {
        shm_name = generate_unique_shm_name();
        ipc::shared_memory_object::remove(shm_name.c_str());
        try {
            ipc::shared_memory_object shm(ipc::create_only, shm_name.c_str(), ipc::read_write);
            shm.truncate(size);
            ipc::mapped_region region(shm, ipc::read_write);
            if (data && size > 0) {
                std::memcpy(region.get_address(), data, size);
            }
            ZIPC_LOG("notify_binary: created shared memory '" << shm_name << "', size=" << size);
        }
        catch (const std::exception& e) {
            ZIPC_LOG("notify_binary: failed to create shared memory: " << e.what());
            ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_MEMORY;
        }
    }

    std::string msg = "NOTIFY|BIN|" + func + "|" + shm_name;
    if (msg.size() > 1024) {
        ZIPC_LOG("notify_binary: message size " << msg.size() << " exceeds 1024");
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SIZE;
    }

    try {
        mq_->send(msg.c_str(), msg.size(), 0);
        ZIPC_LOG("notify_binary: sent, func='" << func << "', shm='" << shm_name << "', size=" << size);
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("notify_binary: send failed: " << e.what());
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SEND;
    }
    return IPC_OK;
}

/*----------------------------------------------------------------------------*/
void IpcClient::receiver_thread_func(std::shared_ptr<ipc::message_queue> resp_mq) {
    // RAII guard to decrement active_receiver_ on exit
    struct ReceiverGuard {
        IpcClient* client;
        ReceiverGuard(IpcClient* c) : client(c) {}
        ~ReceiverGuard() {
            if (client) {
                client->active_receiver_ = 0;
                ZIPC_LOG("receiver_thread: active_receiver_ set to 0");
            }
        }
    } guard(this);

    try {
        char buffer[1024];
        ZIPC_LOG("receiver_thread: started, tid=" << std::this_thread::get_id());

        while (running_) {
            try {
                size_t recvd;
                unsigned priority;
                if (resp_mq->try_receive(buffer, sizeof(buffer), recvd, priority)) {
                    std::string msg(buffer, recvd);

                    size_t first_pipe = msg.find('|');
                    if (first_pipe == std::string::npos) {
                        ZIPC_LOG("receiver_thread: malformed message (no pipe)");
                        continue;
                    }
                    std::string type = msg.substr(0, first_pipe);

                    if (type == "RSP") {
                        // Format: RSP|req_id|status|BIN|shm_name
                        size_t pos1 = first_pipe;
                        size_t pos2 = msg.find('|', pos1 + 1);
                        if (pos2 == std::string::npos) continue;
                        size_t pos3 = msg.find('|', pos2 + 1);
                        if (pos3 == std::string::npos) continue;
                        size_t pos4 = msg.find('|', pos3 + 1);
                        if (pos4 == std::string::npos) continue;

                        uint64_t req_id = 0;
                        int status = 0;
                        try {
                            req_id = std::stoull(msg.substr(pos1 + 1, pos2 - pos1 - 1));
                            status = std::stoi(msg.substr(pos2 + 1, pos3 - pos2 - 1));
                        }
                        catch (...) {
                            ZIPC_LOG("receiver_thread: invalid numeric in RSP");
                            continue;
                        }
                        std::string resp_type = msg.substr(pos3 + 1, pos4 - pos3 - 1);
                        std::string shm_name = msg.substr(pos4 + 1);

                        ZIPC_LOG("receiver_thread: RSP req_id=" << req_id << ", status=" << status
                            << ", type=" << resp_type);

                        Response resp;
                        resp.status = status;
                        resp.bin_data = nullptr;
                        resp.bin_size = 0;

                        if (resp_type == "BIN" && status == IPC_OK) {
                            if (shm_name == ZERO_SHM_MARKER) {
                                resp.bin_data = nullptr;
                                resp.bin_size = 0;
                                ZIPC_LOG("receiver_thread: zero-size binary response");
                            }
                            else {
                                void* buf = nullptr;
                                try {
                                    ipc::shared_memory_object shm(ipc::open_only, shm_name.c_str(), ipc::read_only);
                                    ipc::mapped_region region(shm, ipc::read_only);
                                    size_t sz = region.get_size();
                                    buf = ipc_alloc(sz);
                                    if (buf) {
                                        std::memcpy(buf, region.get_address(), sz);
                                        resp.bin_data = buf;
                                        resp.bin_size = sz;
                                        ZIPC_LOG("receiver_thread: binary response size=" << sz
                                            << ", hex=" << hex_dump(buf, sz));
                                        ZIPC_LOG_MD5("receiver_thread binary response", buf, sz);
                                    }
                                    else {
                                        resp.status = IPC_ERR_MEMORY;
                                        ZIPC_LOG("receiver_thread: ipc_alloc failed for binary response");
                                    }
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("receiver_thread: failed to open binary response shm: " << e.what());
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    resp.status = IPC_ERR_RECEIVE;
                                    if (buf) ipc_free(buf);
                                }
                                catch (...) {
                                    if (buf) ipc_free(buf);
                                    throw;
                                }
                            }
                        }
                        else if (resp_type != "BIN") {
                            resp.status = IPC_ERR_UNKNOWN;
                            ZIPC_LOG("receiver_thread: unknown response type '" << resp_type << "'");
                        }

                        std::shared_ptr<std::promise<Response>> promise;
                        {
                            std::lock_guard<std::mutex> lock(pending_mutex_);
                            auto it = pending_.find(req_id);
                            if (it != pending_.end()) {
                                promise = it->second;
                                pending_.erase(it);
                                ZIPC_LOG("receiver_thread: found promise for req_id=" << req_id);
                            }
                            else {
                                ZIPC_LOG("receiver_thread: no pending promise for req_id=" << req_id);
                            }
                        }
                        if (promise) {
                            promise->set_value(std::move(resp));
                        }
                        else {
                            if (resp.bin_data) ipc_free(resp.bin_data);
                        }
                    }
                    else if (type == "NOTIFY") {
                        // NOTIFY|BIN|func|shm_name
                        size_t pos1 = first_pipe;
                        size_t pos2 = msg.find('|', pos1 + 1);
                        if (pos2 == std::string::npos) continue;
                        size_t pos3 = msg.find('|', pos2 + 1);
                        if (pos3 == std::string::npos) continue;

                        std::string notify_type = msg.substr(pos1 + 1, pos2 - pos1 - 1);
                        std::string func = msg.substr(pos2 + 1, pos3 - pos2 - 1);
                        std::string shm_name = msg.substr(pos3 + 1);

                        ZIPC_LOG("receiver_thread: NOTIFY type=" << notify_type << ", func='" << func << "'");

                        if (notify_type == "BIN") {
                            if (shm_name == ZERO_SHM_MARKER) {
                                ipc_binary_notify_handler h = nullptr;
                                void* trigger = nullptr;
                                {
                                    std::lock_guard<std::mutex> lock(notify_mutex_);
                                    auto it = binary_notify_handlers_.find(func);
                                    if (it != binary_notify_handlers_.end()) {
                                        h = it->second.first;
                                        trigger = it->second.second;
                                        ZIPC_LOG("receiver_thread: found binary notify handler for '" << func << "'");
                                    }
                                    else {
                                        ZIPC_LOG("receiver_thread: binary notify handler not found for '" << func << "'");
                                    }
                                }
                                if (h) {
                                    try {
                                        h(trigger, nullptr, 0);
                                        ZIPC_LOG("receiver_thread: zero-size binary notify handler executed");
                                    }
                                    catch (...) {
                                        ZIPC_LOG("receiver_thread: binary notify handler threw exception");
                                    }
                                }
                            }
                            else {
                                try {
                                    ipc::shared_memory_object shm(ipc::open_only, shm_name.c_str(), ipc::read_only);
                                    ipc::mapped_region region(shm, ipc::read_only);
                                    const void* bin_data = region.get_address();
                                    size_t bin_size = region.get_size();
                                    ZIPC_LOG("receiver_thread: binary notify size=" << bin_size
                                        << ", hex=" << hex_dump(bin_data, bin_size));
                                    ZIPC_LOG_MD5("receiver_thread binary notify", bin_data, bin_size);

                                    ipc_binary_notify_handler h = nullptr;
                                    void* trigger = nullptr;
                                    {
                                        std::lock_guard<std::mutex> lock(notify_mutex_);
                                        auto it = binary_notify_handlers_.find(func);
                                        if (it != binary_notify_handlers_.end()) {
                                            h = it->second.first;
                                            trigger = it->second.second;
                                            ZIPC_LOG("receiver_thread: found binary notify handler for '" << func << "'");
                                        }
                                        else {
                                            ZIPC_LOG("receiver_thread: binary notify handler not found for '" << func << "'");
                                        }
                                    }
                                    if (h) {
                                        try {
                                            h(trigger, bin_data, bin_size);
                                            ZIPC_LOG("receiver_thread: binary notify handler executed, size=" << bin_size);
                                        }
                                        catch (...) {
                                            ZIPC_LOG("receiver_thread: binary notify handler threw exception");
                                        }
                                    }
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("receiver_thread: failed to open binary notify shm: " << e.what());
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                }
                            }
                        }
                        else {
                            ZIPC_LOG("receiver_thread: unknown notify_type '" << notify_type << "'");
                        }
                    }
                    else {
                        ZIPC_LOG("receiver_thread: unknown message type '" << type << "'");
                    }
                }
                else {
                    std::unique_lock<std::mutex> lock(cv_mutex_);
                    cv_.wait_for(lock, std::chrono::milliseconds(10),
                        [this] { return !running_; });
                }
            }
            catch (const std::exception& e) {
                ZIPC_LOG("receiver_thread: exception in loop: " << e.what());
            }
            catch (...) {
                ZIPC_LOG("receiver_thread: unknown exception in loop");
            }
        }
    }
    catch (const std::exception& e) {
        ZIPC_LOG("receiver_thread: fatal std::exception: " << e.what());
    }
    catch (...) {
        ZIPC_LOG("receiver_thread: fatal unknown exception");
    }
    ZIPC_LOG("receiver_thread: exiting");
}

/*----------------------------------------------------------------------------*/
std::string IpcClient::generate_unique_shm_name() {
    static std::atomic<uint64_t> counter{ 0 };
#ifdef _WIN32
    DWORD pid = GetCurrentProcessId();
#else
    pid_t pid = getpid();
#endif
    uint64_t seq = counter.fetch_add(1);
    auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::stringstream ss;
    ss << "ipc_shm_" << pid << "_" << seq << "_" << std::hex << now;
    return ss.str();
}

std::string IpcClient::generate_unique_resp_queue() {
    static std::atomic<uint64_t> counter{ 0 };
#ifdef _WIN32
    DWORD pid = GetCurrentProcessId();
#else
    pid_t pid = getpid();
#endif
    uint64_t seq = counter.fetch_add(1);
    auto now = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::stringstream ss;
    ss << "resp_" << pid << "_" << seq << "_" << std::hex << now;
    return ss.str();
}

bool IpcClient::is_connected() const {
    if (!mq_) return false;
    try {
        (void)mq_->get_num_msg();
        return true;
    }
    catch (...) {
        return false;
    }
}