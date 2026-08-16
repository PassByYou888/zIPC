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
#include <future>   // for std::async (now replaced with std::thread in send_response)

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

IpcServer::IpcServer() = default;
IpcServer::~IpcServer() { stop(); }

/*----------------------------------------------------------------------------*/
/**
 * start ÿ Initialise the server and start worker threads.
 * @param qname            Main queue name.
 * @param thread_count     Number of worker threads (0 = auto).
 * @param max_queue_length Max pending messages in the queue.
 * @param max_msg_size     Max control message size.
 * @return true on success.
 *
 * Flow:
 * 1. If already running, stop first.
 * 2. Remove any existing queue with the same name, then create it.
 * 3. Set thread_count to min(5, hardware_concurrency) if 0.
 * 4. Set running_=true and active_workers_=thread_count.
 * 5. Launch worker threads, each running worker_thread_func.
 *
 * Called by ipc_server_create_ex (C API). The server is now ready to accept
 * clients and messages.
 */
bool IpcServer::start(const std::string& qname, int thread_count,
    size_t max_queue_length, size_t max_msg_size) {
    if (running_) stop();
    queue_name_ = qname;
    max_queue_length_ = max_queue_length;
    max_msg_size_ = max_msg_size;

    try {
        ipc::message_queue::remove(qname.c_str());
        mq_ = std::make_shared<ipc::message_queue>(
            ipc::create_only, qname.c_str(), max_queue_length, max_msg_size);
        ZIPC_LOG("start: created queue '" << qname << "', max_len=" << max_queue_length
            << ", max_msg=" << max_msg_size);
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("start: failed to create queue '" << qname << "': " << e.what());
        return false;
    }

    if (thread_count == 0) {
        thread_count = static_cast<int>(std::min<size_t>(5, std::thread::hardware_concurrency()));
    }
    ZIPC_LOG("start: starting with " << thread_count << " worker threads");

    running_ = true;
    active_workers_ = thread_count;   // Set active worker count.
    workers_.reserve(thread_count);
    for (int i = 0; i < thread_count; ++i) {
        workers_.emplace_back(&IpcServer::worker_thread_func, this, mq_);
    }
    ZIPC_LOG("start: server started on queue '" << qname << "'");
    return true;
}

/*----------------------------------------------------------------------------*/
/**
 * stop ÿ Shut down the server and clean up.
 *
 * Flow:
 * 1. If not running, return.
 * 2. Set running_=false and notify all workers via condition variable.
 * 3. Wait for active_workers_ to reach 0, with a timeout of 2 seconds.
 * 4. If timeout, detach any remaining joinable threads (last resort).
 * 5. Otherwise, join all threads.
 * 6. Clear the workers vector and remove the main queue.
 *
 * Called by ipc_server_destroy (C API) or destructor.
 * The use of active_workers_ allows all threads to exit concurrently without
 * per-thread join delays.
 */
void IpcServer::stop() {
    if (!running_) return;
    ZIPC_LOG("stop: stopping server...");

    // 1. Send exit signal.
    running_ = false;
    cv_.notify_all();

    // 2. Wait for all workers to exit (total timeout 2 seconds).
    const auto timeout = std::chrono::seconds(2);
    auto start = std::chrono::steady_clock::now();
    while (active_workers_ > 0 &&
        (std::chrono::steady_clock::now() - start) < timeout) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // 3. If still alive, force detach.
    if (active_workers_ > 0) {
        ZIPC_LOG("stop: timeout waiting for workers, forcing detach");
        for (size_t i = 0; i < workers_.size(); ++i) {
            if (workers_[i].joinable()) {
                workers_[i].detach();
            }
        }
    }
    else {
        // All threads exited, join them normally.
        for (auto& t : workers_) {
            if (t.joinable()) {
                try {
                    t.join();
                }
                catch (...) {
                    // Ignore join exceptions.
                }
            }
        }
    }

    workers_.clear();

    ipc::message_queue::remove(queue_name_.c_str());
    ZIPC_LOG("stop: server stopped, queue removed");
}

/*----------------------------------------------------------------------------*/
/**
 * register_binary_reply ÿ Register a handler for RPC requests.
 * @param name     Function name.
 * @param h        Callback.
 * @param trigger  User pointer.
 * @return IPC_OK on success, IPC_ERR_BUSY if already registered.
 *
 * The handler is stored in binary_reply_handlers_ map, protected by handlers_mutex_.
 * It will be invoked by worker threads when a REQ message with matching name arrives.
 */
int IpcServer::register_binary_reply(const std::string& name,
    ipc_binary_reply_handler h, void* trigger) {
    if (!running_) {
        ZIPC_LOG("register_binary_reply: server not running");
        return IPC_ERR_OPEN;
    }
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    if (binary_reply_handlers_.find(name) != binary_reply_handlers_.end()) {
        ZIPC_LOG("register_binary_reply: handler '" << name << "' already exists");
        return IPC_ERR_BUSY;
    }
    binary_reply_handlers_[name] = { h, trigger };
    ZIPC_LOG("register_binary_reply: registered binary reply handler '" << name << "'");
    return IPC_OK;
}

/**
 * unregister_binary_reply ÿ Remove an RPC handler.
 */
int IpcServer::unregister_binary_reply(const std::string& name) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    auto it = binary_reply_handlers_.find(name);
    if (it == binary_reply_handlers_.end()) {
        ZIPC_LOG("unregister_binary_reply: handler '" << name << "' not found");
        return IPC_ERR_NOT_FOUND;
    }
    binary_reply_handlers_.erase(it);
    ZIPC_LOG("unregister_binary_reply: unregistered binary reply handler '" << name << "'");
    return IPC_OK;
}

/**
 * register_binary_notify ÿ Register a handler for client�Lserver notifications.
 */
int IpcServer::register_binary_notify(const std::string& name,
    ipc_binary_notify_handler h, void* trigger) {
    if (!running_) {
        ZIPC_LOG("register_binary_notify: server not running");
        return IPC_ERR_OPEN;
    }
    std::lock_guard<std::mutex> lock(handlers_mutex_);
    if (binary_notify_handlers_.find(name) != binary_notify_handlers_.end()) {
        ZIPC_LOG("register_binary_notify: handler '" << name << "' already exists");
        return IPC_ERR_BUSY;
    }
    binary_notify_handlers_[name] = { h, trigger };
    ZIPC_LOG("register_binary_notify: registered binary notify handler '" << name << "'");
    return IPC_OK;
}

/**
 * unregister_binary_notify ÿ Remove a notification handler.
 */
int IpcServer::unregister_binary_notify(const std::string& name) {
    std::lock_guard<std::mutex> lock(handlers_mutex_);
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
/**
 * send_notify_binary ÿ Send a binary notification from the server to a client.
 * @param client_resp_queue  Client's response queue name.
 * @param func               Notification name.
 * @param data               Payload.
 * @param size               Payload size.
 * @return IPC_OK on success.
 *
 * Flow:
 * 1. Validate parameters.
 * 2. If size>0, create shared memory segment and copy data.
 * 3. Format a NOTIFY control message and send it to the client's response queue.
 *
 * Called by ipc_server_send_notify_binary (C API).
 */
int IpcServer::send_notify_binary(const std::string& client_resp_queue,
    const std::string& func, const void* data, size_t size) {
    if (!running_) {
        ZIPC_LOG("send_notify_binary: server not running");
        return IPC_ERR_OPEN;
    }
    if (size > 512 * 1024 * 1024) {
        ZIPC_LOG("send_notify_binary: size " << size << " exceeds 512MB");
        return IPC_ERR_SIZE;
    }

    if (size > 0 && data) {
        ZIPC_LOG("send_notify_binary: func='" << func << "', size=" << size
            << ", hex=" << hex_dump(data, size));
        ZIPC_LOG_MD5("send_notify_binary sent data", data, size);
    }
    else {
        ZIPC_LOG("send_notify_binary: func='" << func << "', zero-size");
    }

    std::string shm_name;
    if (size == 0) {
        shm_name = ZERO_SHM_MARKER;
        ZIPC_LOG("send_notify_binary: zero-size data, using marker");
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
            ZIPC_LOG("send_notify_binary: created shared memory '" << shm_name << "', size=" << size);
        }
        catch (const std::exception& e) {
            ZIPC_LOG("send_notify_binary: failed to create shared memory: " << e.what());
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
            ZIPC_LOG("send_notify_binary: queue '" << client_resp_queue << "' full");
            if (shm_name != ZERO_SHM_MARKER)
                ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_BUSY;
        }
        ZIPC_LOG("send_notify_binary: sent to '" << client_resp_queue << "', func='" << func
            << "', shm='" << shm_name << "', size=" << size);
    }
    catch (const std::exception& e) {
        ZIPC_LOG("send_notify_binary: failed to send to '" << client_resp_queue << "': " << e.what());
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SEND;
    }
    return IPC_OK;
}

/*----------------------------------------------------------------------------*/
/**
 * worker_thread_func ÿ Main loop for each worker thread.
 * @param mq  Shared pointer to the main message queue.
 *
 * This thread runs while running_ is true. It polls the main queue for messages
 * and dispatches them:
 * - REQ messages: extracts function name, request ID, response queue, and shared memory name.
 *   Finds the registered reply handler, invokes it to get a reply, then sends
 *   the reply via send_response().
 * - NOTIFY messages: extracts notification name and shared memory name, invokes
 *   the registered notify handler.
 *
 * The thread uses a condition variable to wait when the queue is empty.
 * It also decrements active_workers_ upon exit using the WorkerGuard RAII helper.
 */
void IpcServer::worker_thread_func(std::shared_ptr<ipc::message_queue> mq) {
    // WorkerGuard ensures active_workers_ is decremented when the thread exits.
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

                    // We no longer process __STOP__ messages; the loop exits
                    // when running_ becomes false.

                    size_t first_pipe = msg.find('|');
                    if (first_pipe == std::string::npos) {
                        ZIPC_LOG("worker_thread: malformed message (no pipe)");
                        continue;
                    }
                    std::string type = msg.substr(0, first_pipe);

                    if (type == "REQ") {
                        // Format: REQ|req_id|resp_queue|BIN|func|shm_name
                        size_t pos1 = first_pipe;
                        size_t pos2 = msg.find('|', pos1 + 1);
                        if (pos2 == std::string::npos) continue;
                        size_t pos3 = msg.find('|', pos2 + 1);
                        if (pos3 == std::string::npos) continue;
                        size_t pos4 = msg.find('|', pos3 + 1);
                        if (pos4 == std::string::npos) continue;
                        size_t pos5 = msg.find('|', pos4 + 1);
                        if (pos5 == std::string::npos) continue;

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
                            << "', type=" << msg_type << ", func='" << func << "'");

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
                                    ZIPC_LOG("worker_thread: REQ binary data size=" << bin_size
                                        << ", hex=" << hex_dump(bin_data, bin_size));
                                    ZIPC_LOG_MD5("worker_thread received binary request", bin_data, bin_size);
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    shm_ok = true;
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("worker_thread: failed to open shared memory '" << shm_name
                                        << "': " << e.what());
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
                                        ZIPC_LOG("worker_thread: found binary reply handler for '" << func << "'");
                                    }
                                    else {
                                        ZIPC_LOG("worker_thread: binary reply handler not found for '" << func << "'");
                                    }
                                }
                                if (h) {
                                    void* out = nullptr;
                                    size_t out_size = 0;
                                    try {
                                        h(trigger, bin_data, bin_size, &out, &out_size);
                                        ZIPC_LOG("worker_thread: binary reply handler executed, out_size=" << out_size);
                                    }
                                    catch (...) {
                                        ZIPC_LOG("worker_thread: binary reply handler threw exception");
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

                        // Send response (binary)
                        if (status == IPC_OK) {
                            std::string shm_name_resp;
                            if (reply_size == 0) {
                                shm_name_resp = ZERO_SHM_MARKER;
                                ZIPC_LOG("worker_thread: zero-size binary response");
                            }
                            else {
                                shm_name_resp = generate_unique_shm_name();
                                ipc::shared_memory_object::remove(shm_name_resp.c_str());
                                try {
                                    ipc::shared_memory_object shm(ipc::create_only, shm_name_resp.c_str(), ipc::read_write);
                                    shm.truncate(reply_size);
                                    ipc::mapped_region region(shm, ipc::read_write);
                                    std::memcpy(region.get_address(), reply_bin, reply_size);
                                    ZIPC_LOG("worker_thread: binary reply size=" << reply_size
                                        << ", hex=" << hex_dump(reply_bin, reply_size));
                                    ZIPC_LOG_MD5("worker_thread binary reply", reply_bin, reply_size);
                                    ipc_free(reply_bin);
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("worker_thread: failed to create response shared memory: " << e.what());
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
                            send_response(resp_queue, req_id, status, "");
                        }
                    }
                    else if (type == "NOTIFY") {
                        // Format: NOTIFY|type|func|data   (type always "BIN")
                        size_t pos1 = first_pipe;
                        size_t pos2 = msg.find('|', pos1 + 1);
                        if (pos2 == std::string::npos) continue;
                        size_t pos3 = msg.find('|', pos2 + 1);
                        if (pos3 == std::string::npos) continue;

                        std::string notify_type = msg.substr(pos1 + 1, pos2 - pos1 - 1);
                        std::string func = msg.substr(pos2 + 1, pos3 - pos2 - 1);
                        std::string shm_name = msg.substr(pos3 + 1);

                        ZIPC_LOG("worker_thread: NOTIFY type=" << notify_type << ", func='" << func << "'");

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
                                        ZIPC_LOG("worker_thread: found binary notify handler for '" << func << "'");
                                    }
                                    else {
                                        ZIPC_LOG("worker_thread: binary notify handler not found for '" << func << "'");
                                    }
                                }
                                if (h) {
                                    try {
                                        h(trigger, nullptr, 0);
                                        ZIPC_LOG("worker_thread: zero-size binary notify handler executed");
                                    }
                                    catch (...) {
                                        ZIPC_LOG("worker_thread: binary notify handler threw exception");
                                    }
                                }
                            }
                            else {
                                try {
                                    ipc::shared_memory_object shm(ipc::open_only, shm_name.c_str(), ipc::read_only);
                                    ipc::mapped_region region(shm, ipc::read_only);
                                    const void* bin_data = region.get_address();
                                    size_t bin_size = region.get_size();
                                    ZIPC_LOG("worker_thread: NOTIFY binary data size=" << bin_size
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
                                            ZIPC_LOG("worker_thread: found binary notify handler for '" << func << "'");
                                        }
                                        else {
                                            ZIPC_LOG("worker_thread: binary notify handler not found for '" << func << "'");
                                        }
                                    }
                                    if (h) {
                                        try {
                                            h(trigger, bin_data, bin_size);
                                            ZIPC_LOG("worker_thread: binary notify handler executed, size=" << bin_size);
                                        }
                                        catch (...) {
                                            ZIPC_LOG("worker_thread: binary notify handler threw exception");
                                        }
                                    }
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                }
                                catch (const std::exception& e) {
                                    ZIPC_LOG("worker_thread: failed to open binary notify shared memory: " << e.what());
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
                    // Queue empty: wait with condition variable to avoid busy loop.
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
        ZIPC_LOG("worker_thread: fatal std::exception: " << e.what());
    }
    catch (...) {
        ZIPC_LOG("worker_thread: fatal unknown exception");
    }
    ZIPC_LOG("worker_thread: exiting");
}

/*----------------------------------------------------------------------------*/
/**
 * send_response ÿ Send an RPC reply to a client.
 * @param resp_queue  Client's response queue name.
 * @param req_id      Request ID.
 * @param status      Return code.
 * @param shm_name    Name of shared memory containing the reply (or ZERO marker).
 *
 * Formats a RSP message and sends it to the client's response queue.
 * If the reply data is in shared memory, this function will also schedule a
 * delayed cleanup of that shared memory (using a detached thread) to avoid leaks
 * if the client never reads it.
 *
 * Called by worker_thread_func after handling a REQ.
 */
void IpcServer::send_response(const std::string& resp_queue, uint64_t req_id,
    int status, const std::string& shm_name) {
    std::string msg = "RSP|" + std::to_string(req_id) + "|" + std::to_string(status) +
        "|BIN|" + shm_name;
    if (msg.size() > max_msg_size_) {
        if (!shm_name.empty() && shm_name != ZERO_SHM_MARKER) {
            ipc::shared_memory_object::remove(shm_name.c_str());
            ZIPC_LOG("send_response: response too large, cleaned up shm '" << shm_name << "'");
        }
        msg = "RSP|" + std::to_string(req_id) + "|" + std::to_string(IPC_ERR_SIZE) + "|BIN|";
        ZIPC_LOG("send_response: response too large, truncated to error");
    }

    bool send_ok = false;
    try {
        ipc::message_queue mq(ipc::open_only, resp_queue.c_str());
        if (mq.try_send(msg.c_str(), msg.size(), 0)) {
            send_ok = true;
            ZIPC_LOG("send_response: sent response req_id=" << req_id << " to '" << resp_queue
                << "', status=" << status);
        }
        else {
            ZIPC_LOG("send_response: queue '" << resp_queue << "' full for req_id=" << req_id);
        }
    }
    catch (const std::exception& e) {
        ZIPC_LOG("send_response: failed to send response to '" << resp_queue << "': " << e.what());
    }

    // If the response was sent successfully and we used a real shared memory
    // (not the zero marker), schedule a delayed cleanup to remove the memory
    // even if the client never fetches it (e.g., due to timeout or disconnect).
    if (send_ok && !shm_name.empty() && shm_name != ZERO_SHM_MARKER) {
        // Launch a detached thread to perform cleanup after 10 seconds.
        std::thread([shm_name]() {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            try {
                ipc::shared_memory_object::remove(shm_name.c_str());
                ZIPC_LOG("send_response: delayed cleanup removed shared memory " << shm_name);
            }
            catch (...) {
                // ignore
            }
            }).detach();
    }

    // If send failed, remove immediately.
    if (!send_ok && !shm_name.empty() && shm_name != ZERO_SHM_MARKER) {
        ipc::shared_memory_object::remove(shm_name.c_str());
        ZIPC_LOG("send_response: removed shared memory due to send failure");
    }
}

/*----------------------------------------------------------------------------*/
/**
 * generate_unique_shm_name ÿ Create a unique name for a shared memory segment.
 * @return String like "ipc_shm_resp_<pid>_<seq>_<timestamp>".
 *
 * Used to avoid collisions when multiple servers or clients create shared memory.
 * Called whenever the server needs to allocate shared memory for a response.
 */
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
    return ss.str();
}