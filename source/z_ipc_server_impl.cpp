/**
 * @file z_ipc_server_impl.cpp
 * @brief Server implementation ¨C hardened with exception safety, non-blocking sends, and data integrity checks.
 */

#include "z_ipc_server_impl.h"
#include "z_ipc_md5.h"
#include <iostream>
#include <cstring>
#include <sstream>
#include <chrono>
#include <thread>
#include <algorithm>
#include <fstream>
#include <cctype>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

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
    if (size == 0) { ss << "(empty)"; return ss.str(); }
    size_t show_len = (size < max_len) ? size : max_len;
    for (size_t i = 0; i < show_len; ++i) {
        char buf[4]; sprintf(buf, "%02x ", p[i]); ss << buf;
    }
    if (size > max_len) ss << "... (total " << size << " bytes)";
    return ss.str();
}
#else
#define ZIPC_LOG(msg) ((void)0)
#define ZIPC_LOG_MD5(...) ((void)0)
#define hex_dump(...) ((void)0)
#endif

static const std::string ZERO_SHM_MARKER = "__ZERO__";
static const int WORKER_TIMEOUT_SEC = 5;

// ---------- Data integrity helpers (CRC32 and shared memory header) ----------
static uint32_t crc32_table[256];
static bool crc32_table_initialized = false;
static void init_crc32_table() {
    if (crc32_table_initialized) return;
    for (int i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j)
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
        crc32_table[i] = crc;
    }
    crc32_table_initialized = true;
}
static uint32_t crc32(const void* data, size_t len) {
    init_crc32_table();
    uint32_t crc = 0xFFFFFFFF;
    const uint8_t* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i)
        crc = (crc >> 8) ^ crc32_table[(crc ^ p[i]) & 0xFF];
    return crc ^ 0xFFFFFFFF;
}

#define SHM_MAGIC 0xDEC0DEAD
struct ShmHeader {
    uint32_t magic;
    uint32_t checksum;
    uint32_t data_len;   // size of payload (excluding header)
};
static const size_t SHM_HEADER_SIZE = sizeof(ShmHeader);

static bool is_valid_queue_name(const std::string& name) {
    if (name.empty()) return false;
    for (char c : name) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') {
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

bool IpcServer::start(const std::string& qname, int thread_count,
    size_t max_queue_length, size_t max_msg_size) {
    ZIPC_LOG("start: ENTRY qname='" << qname << "', thread_count=" << thread_count
        << ", max_queue_len=" << max_queue_length << ", max_msg_size=" << max_msg_size);

    if (running_) {
        ZIPC_LOG("start: already running, stopping first");
        stop();
    }

    if (thread_count < 0) {
        if (thread_count == INT_MIN) thread_count = 0;
        else thread_count = 0;
    }
    if (thread_count > 100) thread_count = 0;
    if (max_queue_length == 0 || max_queue_length > 1000000) max_queue_length = 1000;
    if (max_msg_size < 64 || max_msg_size > 65536) max_msg_size = 1024;

    if (!is_valid_queue_name(qname)) {
        ZIPC_LOG("start: INVALID queue name (only alnum and underscore allowed)");
        return false;
    }

    queue_name_ = qname;
    max_queue_length_ = max_queue_length;
    max_msg_size_ = max_msg_size;

    try {
        ipc::message_queue::remove(qname.c_str());
        mq_ = std::make_shared<ipc::message_queue>(
            ipc::create_only, qname.c_str(), max_queue_length, max_msg_size);
        ZIPC_LOG("start: created queue '" << qname << "'");
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("start: FAILED to create queue: " << e.what());
        return false;
    }

    if (thread_count == 0) {
        int hw = std::thread::hardware_concurrency();
        thread_count = static_cast<int>(std::min<size_t>(5, (size_t)hw));
        ZIPC_LOG("start: auto thread_count = " << thread_count);
    }

    running_ = true;
    active_workers_ = thread_count;
    workers_.reserve(thread_count);
    worker_last_active_.resize(thread_count);

    for (int i = 0; i < thread_count; ++i) {
        workers_.emplace_back(&IpcServer::worker_thread_func, this, mq_, i);
        worker_last_active_[i] = std::chrono::steady_clock::now();
        ZIPC_LOG("start: worker thread " << i << " launched");
    }

    monitor_running_ = true;
    monitor_ = std::thread(&IpcServer::monitor_thread_func, this);

    ZIPC_LOG("start: server started successfully on queue '" << qname << "'");
    return true;
}

void IpcServer::stop() {
    ZIPC_LOG("stop: ENTRY");
    if (!running_) {
        ZIPC_LOG("stop: already stopped, return");
        return;
    }

    running_ = false;
    cv_.notify_all();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);

    while (active_workers_ > 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    for (size_t i = 0; i < workers_.size(); ++i) {
        auto& t = workers_[i];
        if (t.joinable()) {
            if (std::chrono::steady_clock::now() < deadline) {
                try {
                    t.join();
                    ZIPC_LOG("stop: joined worker " << i);
                }
                catch (...) {
                    ZIPC_LOG("stop: exception joining worker " << i);
                }
            }
            else {
                ZIPC_LOG("stop: worker " << i << " timeout, detaching (last resort)");
                t.detach();
            }
        }
    }
    workers_.clear();

    monitor_running_ = false;
    if (monitor_.joinable()) {
        monitor_.join();
        ZIPC_LOG("stop: monitor joined");
    }

    ipc::message_queue::remove(queue_name_.c_str());
    ZIPC_LOG("stop: removed queue '" << queue_name_ << "'");
    ZIPC_LOG("stop: server stopped");
}

int IpcServer::register_binary_reply(const std::string& name,
    ipc_binary_reply_handler h, void* trigger) {
    ZIPC_LOG("register_binary_reply: ENTRY name='" << name << "'");
    if (!running_) return IPC_ERR_OPEN;
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    if (binary_reply_handlers_.find(name) != binary_reply_handlers_.end())
        return IPC_ERR_BUSY;
    binary_reply_handlers_[name] = { h, trigger };
    ZIPC_LOG("register_binary_reply: registered handler for '" << name << "'");
    return IPC_OK;
}

int IpcServer::unregister_binary_reply(const std::string& name) {
    ZIPC_LOG("unregister_binary_reply: ENTRY name='" << name << "'");
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    auto it = binary_reply_handlers_.find(name);
    if (it == binary_reply_handlers_.end()) return IPC_ERR_NOT_FOUND;
    binary_reply_handlers_.erase(it);
    ZIPC_LOG("unregister_binary_reply: unregistered handler '" << name << "'");
    return IPC_OK;
}

int IpcServer::register_binary_notify(const std::string& name,
    ipc_binary_notify_handler h, void* trigger) {
    ZIPC_LOG("register_binary_notify: ENTRY name='" << name << "'");
    if (!running_) return IPC_ERR_OPEN;
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    if (binary_notify_handlers_.find(name) != binary_notify_handlers_.end())
        return IPC_ERR_BUSY;
    binary_notify_handlers_[name] = { h, trigger };
    ZIPC_LOG("register_binary_notify: registered notify handler for '" << name << "'");
    return IPC_OK;
}

int IpcServer::unregister_binary_notify(const std::string& name) {
    ZIPC_LOG("unregister_binary_notify: ENTRY name='" << name << "'");
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    auto it = binary_notify_handlers_.find(name);
    if (it == binary_notify_handlers_.end()) return IPC_ERR_NOT_FOUND;
    binary_notify_handlers_.erase(it);
    ZIPC_LOG("unregister_binary_notify: unregistered '" << name << "'");
    return IPC_OK;
}

int IpcServer::send_notify_binary(const std::string& client_resp_queue,
    const std::string& func, const void* data, size_t size) {
    ZIPC_LOG("send_notify_binary: ENTRY queue='" << client_resp_queue
        << "', func='" << func << "', size=" << size);
    if (!running_) return IPC_ERR_OPEN;
    if (size > 512 * 1024 * 1024) return IPC_ERR_SIZE;

    std::string shm_name;
    if (size == 0) {
        shm_name = ZERO_SHM_MARKER;
    }
    else {
        shm_name = generate_unique_shm_name();
        ipc::shared_memory_object::remove(shm_name.c_str());
        try {
            size_t total_size = SHM_HEADER_SIZE + size;
            ipc::shared_memory_object shm(ipc::create_only, shm_name.c_str(), ipc::read_write);
            shm.truncate(total_size);
            ipc::mapped_region region(shm, ipc::read_write);

            ShmHeader header;
            header.magic = SHM_MAGIC;
            header.data_len = static_cast<uint32_t>(size);
            if (size > 0 && data) {
                void* payload = static_cast<char*>(region.get_address()) + SHM_HEADER_SIZE;
                std::memcpy(payload, data, size);
                header.checksum = crc32(data, size);
            }
            else {
                header.checksum = 0;
            }
            std::memcpy(region.get_address(), &header, SHM_HEADER_SIZE);
            ZIPC_LOG("send_notify_binary: created shared memory '" << shm_name
                << "', total_size=" << total_size << ", payload=" << size);
        }
        catch (const std::exception& e) {
            ZIPC_LOG("send_notify_binary: failed to create shm: " << e.what());
            ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_MEMORY;
        }
    }

    std::string msg = "NOTIFY|BIN|" + func + "|" + shm_name;
    if (msg.size() > max_msg_size_) {
        if (shm_name != ZERO_SHM_MARKER) ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SIZE;
    }

    try {
        ipc::message_queue mq(ipc::open_only, client_resp_queue.c_str());
        if (!mq.try_send(msg.c_str(), msg.size(), 0)) {
            ZIPC_LOG("send_notify_binary: queue full, send failed");
            if (shm_name != ZERO_SHM_MARKER) ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_BUSY;
        }
        ZIPC_LOG("send_notify_binary: sent to '" << client_resp_queue << "', msg='" << msg << "'");
    }
    catch (const std::exception& e) {
        ZIPC_LOG("send_notify_binary: exception sending: " << e.what());
        if (shm_name != ZERO_SHM_MARKER) ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SEND;
    }
    return IPC_OK;
}

void IpcServer::worker_thread_func(std::shared_ptr<ipc::message_queue> mq, int worker_id) {
    struct WorkerGuard {
        IpcServer* server;
        int id;
        WorkerGuard(IpcServer* s, int i) : server(s), id(i) {}
        ~WorkerGuard() {
            if (server) {
                server->active_workers_--;
                ZIPC_LOG("worker_thread " << id << ": active_workers_ decremented to "
                    << server->active_workers_.load());
            }
        }
    } guard(this, worker_id);

    std::vector<char> buffer(max_msg_size_);
    ZIPC_LOG("worker_thread " << worker_id << ": started, tid=" << std::this_thread::get_id());

    while (running_) {
        {
            std::lock_guard<std::mutex> lock(worker_heartbeat_mutex_);
            worker_last_active_[worker_id] = std::chrono::steady_clock::now();
        }

        try {
            if (!running_) break;
            size_t recvd;
            unsigned priority;
            if (mq->try_receive(buffer.data(), buffer.size(), recvd, priority)) {
                std::string msg(buffer.data(), recvd);
                ZIPC_LOG("worker_thread " << worker_id << ": received message: '" << msg << "'");

                size_t first_pipe = msg.find('|');
                if (first_pipe == std::string::npos) {
                    ZIPC_LOG("worker_thread: malformed message (no pipe)");
                    continue;
                }
                std::string type = msg.substr(0, first_pipe);
                ZIPC_LOG("worker_thread: message type = '" << type << "'");

                if (type == "REQ") {
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
                                size_t region_size = region.get_size();
                                if (region_size < SHM_HEADER_SIZE) {
                                    ZIPC_LOG("worker_thread: request shm too small");
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    status = IPC_ERR_RECEIVE;
                                }
                                else {
                                    const ShmHeader* hdr = static_cast<const ShmHeader*>(region.get_address());
                                    if (hdr->magic != SHM_MAGIC) {
                                        ZIPC_LOG("worker_thread: bad magic in request");
                                        ipc::shared_memory_object::remove(shm_name.c_str());
                                        status = IPC_ERR_RECEIVE;
                                    }
                                    else {
                                        size_t payload_len = hdr->data_len;
                                        if (region_size != SHM_HEADER_SIZE + payload_len) {
                                            ZIPC_LOG("worker_thread: size mismatch in request");
                                            ipc::shared_memory_object::remove(shm_name.c_str());
                                            status = IPC_ERR_RECEIVE;
                                        }
                                        else {
                                            bin_data = static_cast<const char*>(region.get_address()) + SHM_HEADER_SIZE;
                                            bin_size = payload_len;
                                            uint32_t calc = crc32(bin_data, bin_size);
                                            if (calc != hdr->checksum) {
                                                ZIPC_LOG("worker_thread: checksum mismatch in request");
                                                ipc::shared_memory_object::remove(shm_name.c_str());
                                                status = IPC_ERR_RECEIVE;
                                            }
                                            else {
                                                shm_ok = true;
                                                ipc::shared_memory_object::remove(shm_name.c_str());
                                                // region remains valid until end of scope
                                            }
                                        }
                                    }
                                }
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
                                catch (const std::exception& e) {
                                    ZIPC_LOG("worker_thread: handler threw exception: " << e.what());
                                    status = IPC_ERR_UNKNOWN;
                                    if (out) {
                                        ipc_free(out);
                                        out = nullptr;
                                    }
                                }
                                catch (...) {
                                    ZIPC_LOG("worker_thread: handler threw unknown exception");
                                    status = IPC_ERR_UNKNOWN;
                                    if (out) {
                                        ipc_free(out);
                                        out = nullptr;
                                    }
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

                    // Send response
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
                                size_t total_size = SHM_HEADER_SIZE + reply_size;
                                ipc::shared_memory_object shm(ipc::create_only, shm_name_resp.c_str(), ipc::read_write);
                                shm.truncate(total_size);
                                ipc::mapped_region region(shm, ipc::read_write);

                                ShmHeader header;
                                header.magic = SHM_MAGIC;
                                header.data_len = static_cast<uint32_t>(reply_size);
                                if (reply_size > 0 && reply_bin) {
                                    void* payload = static_cast<char*>(region.get_address()) + SHM_HEADER_SIZE;
                                    std::memcpy(payload, reply_bin, reply_size);
                                    header.checksum = crc32(reply_bin, reply_size);
                                }
                                else {
                                    header.checksum = 0;
                                }
                                std::memcpy(region.get_address(), &header, SHM_HEADER_SIZE);
                                ZIPC_LOG("worker_thread: response shm created, total_size=" << total_size);
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
                                size_t region_size = region.get_size();
                                if (region_size < SHM_HEADER_SIZE) {
                                    ZIPC_LOG("worker_thread: notify shm too small");
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    continue;
                                }
                                const ShmHeader* hdr = static_cast<const ShmHeader*>(region.get_address());
                                if (hdr->magic != SHM_MAGIC) {
                                    ZIPC_LOG("worker_thread: bad magic in notify");
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    continue;
                                }
                                size_t payload_len = hdr->data_len;
                                if (region_size != SHM_HEADER_SIZE + payload_len) {
                                    ZIPC_LOG("worker_thread: size mismatch in notify");
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    continue;
                                }
                                const void* bin_data = static_cast<const char*>(region.get_address()) + SHM_HEADER_SIZE;
                                uint32_t calc = crc32(bin_data, payload_len);
                                if (calc != hdr->checksum) {
                                    ZIPC_LOG("worker_thread: checksum mismatch in notify");
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    continue;
                                }
                                ZIPC_LOG("worker_thread: notify data size=" << payload_len
                                    << ", hex=" << hex_dump(bin_data, payload_len));
                                ZIPC_LOG_MD5("worker_thread binary notify received", bin_data, payload_len);

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
                                        h(trigger, bin_data, payload_len);
                                        ZIPC_LOG("worker_thread: notify handler executed, size=" << payload_len);
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
                std::unique_lock<std::mutex> lock(cv_mutex_);
                cv_.wait_for(lock, std::chrono::milliseconds(10),
                    [this] { return !running_; });
            }
        }
        catch (const std::exception& e) {
            ZIPC_LOG("worker_thread " << worker_id << ": exception in loop: " << e.what());
        }
        catch (...) {
            ZIPC_LOG("worker_thread " << worker_id << ": unknown exception in loop");
        }
    }
    ZIPC_LOG("worker_thread " << worker_id << ": exiting");
}

void IpcServer::monitor_thread_func() {
    ZIPC_LOG("monitor_thread: started");
    while (monitor_running_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (!monitor_running_) break;
        auto now = std::chrono::steady_clock::now();
        for (size_t i = 0; i < worker_last_active_.size(); ++i) {
            std::chrono::steady_clock::time_point last;
            {
                std::lock_guard<std::mutex> lock(worker_heartbeat_mutex_);
                last = worker_last_active_[i];
            }
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last).count();
            if (elapsed > WORKER_TIMEOUT_SEC) {
                ZIPC_LOG("monitor_thread: worker " << i << " seems stuck (no heartbeat for " << elapsed << "s)");
            }
        }
    }
    ZIPC_LOG("monitor_thread: exiting");
}

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
        }
        msg = "RSP|" + std::to_string(req_id) + "|" + std::to_string(IPC_ERR_SIZE) + "|BIN|";
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

    if (!send_ok && !shm_name.empty() && shm_name != ZERO_SHM_MARKER) {
        ipc::shared_memory_object::remove(shm_name.c_str());
        ZIPC_LOG("send_response: immediately removed shm due to send failure");
    }
}

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