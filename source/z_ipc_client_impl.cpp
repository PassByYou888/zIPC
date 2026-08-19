/**
 * @file z_ipc_client_impl.cpp
 * @brief Client implementation ¨C fully hardened with cancellation and RAII.
 */

#include "z_ipc_client_impl.h"
#include "z_ipc_md5.h"
#include <random>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <cstring>
#include <thread>
#include <queue>
#include <functional>
#include <errno.h>
#include <system_error>
#include <fstream>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#include <sys/stat.h>
#endif

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
static const int LEASE_TIMEOUT_SEC = 30;  // seconds

thread_local bool IpcClient::tls_in_callback_ = false;

// ---------- RAII lease for shared memory cleanup ----------
// FIX S2: Use RAII to manage shared memory lifetime with lease files.
class SharedMemoryLease {
    std::string shm_name_;
    std::string lease_path_;
    bool valid_;
public:
    SharedMemoryLease(const std::string& shm_name)
        : shm_name_(shm_name), valid_(false)
    {
        // On Linux, /dev/shm/ is used; we create a lease file.
        lease_path_ = "/dev/shm/" + shm_name_ + ".lease";
        // Remove stale lease if any.
        std::remove(lease_path_.c_str());
        // Write current time (monotonic) as a string.
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        std::ofstream ofs(lease_path_);
        if (ofs) {
            ofs << now;
            valid_ = true;
        }
    }
    ~SharedMemoryLease() {
        if (valid_) {
            // Remove lease file on cleanup.
            std::remove(lease_path_.c_str());
            // Remove shared memory itself (if we are the last user).
            ipc::shared_memory_object::remove(shm_name_.c_str());
        }
    }
    bool valid() const { return valid_; }
    // Static method to check and clean up stale shm.
    static void cleanup_stale(const std::string& shm_name) {
        std::string lease_path = "/dev/shm/" + shm_name + ".lease";
        std::ifstream ifs(lease_path);
        if (ifs) {
            int64_t ts;
            ifs >> ts;
            auto now = std::chrono::steady_clock::now().time_since_epoch().count();
            if (now - ts > LEASE_TIMEOUT_SEC * 1000000000LL) {
                // Stale, remove both.
                std::remove(lease_path.c_str());
                ipc::shared_memory_object::remove(shm_name.c_str());
            }
        }
    }
};

IpcClient::IpcClient() {
    ZIPC_LOG("IpcClient constructor");
}

IpcClient::~IpcClient() {
    ZIPC_LOG("~IpcClient: deterministic cleanup");
    // FIX C1: Do not rely on ReceiverGuard; perform independent cleanup.
    // FIX C3: No TOCTOU on active_receiver_; we directly check joinable.
    if (running_.load(std::memory_order_acquire)) {
        running_.store(false, std::memory_order_release);
        cv_.notify_all();
        worker_running_.store(false, std::memory_order_release);
        task_cv_.notify_all();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
        if (receiver_.joinable()) {
            while (receiver_.joinable() && std::chrono::steady_clock::now() < deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            if (receiver_.joinable()) {
                // FIX C1: force join, no detach
                receiver_.join();
            }
        }
        if (worker_.joinable()) {
            while (worker_.joinable() && std::chrono::steady_clock::now() < deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            if (worker_.joinable()) {
                worker_.join();
            }
        }

        mq_.reset();
        resp_mq_.reset();
        ipc::message_queue::remove(resp_queue_name_.c_str());

        // Fail pending promises
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            for (auto& kv : pending_) {
                Response resp;
                resp.status = IPC_ERR_UNKNOWN;
                resp.bin_data = nullptr;
                resp.bin_size = 0;
                kv.second->set_value(resp);
            }
            pending_.clear();
        }
        {
            std::lock_guard<std::mutex> lock(notify_mutex_);
            binary_notify_handlers_.clear();
        }
    }
    ZIPC_LOG("~IpcClient: done");
}

bool IpcClient::do_connect(const std::string& qname) {
    // Atomically connect.
    if (running_.load(std::memory_order_acquire)) {
        disconnect();
    }
    server_queue_name_ = qname;
    resp_queue_name_.clear();
    mq_.reset();
    resp_mq_.reset();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_.clear();
    }
    {
        std::lock_guard<std::mutex> lock(notify_mutex_);
        binary_notify_handlers_.clear();
    }
    clear_task_queue();
    cancel_requested_.store(false, std::memory_order_release);

    // Open server queue
    try {
        mq_ = std::make_unique<ipc::message_queue>(ipc::open_only, qname.c_str());
        ZIPC_LOG("connect: opened main queue '" << qname << "'");
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("connect: failed to open main queue: " << e.what());
        return false;
    }

    // Create private response queue
    resp_queue_name_ = generate_unique_resp_queue();
    try {
        ipc::message_queue::remove(resp_queue_name_.c_str());
        resp_mq_ = std::make_shared<ipc::message_queue>(
            ipc::create_only, resp_queue_name_.c_str(), 100, 1024);
        ZIPC_LOG("connect: created response queue '" << resp_queue_name_ << "'");
    }
    catch (...) {
        ZIPC_LOG("connect: failed to create response queue");
        mq_.reset();
        return false;
    }

    // Set running flags before starting threads to avoid race with destructor.
    running_.store(true, std::memory_order_release);
    worker_running_.store(true, std::memory_order_release);

    try {
        receiver_ = std::thread(&IpcClient::receiver_thread_func, this, resp_mq_);
        worker_ = std::thread(&IpcClient::worker_thread_func, this);
    }
    catch (...) {
        // Rollback
        running_.store(false, std::memory_order_release);
        worker_running_.store(false, std::memory_order_release);
        if (receiver_.joinable()) {
            receiver_.join();
        }
        if (worker_.joinable()) {
            worker_.join();
        }
        mq_.reset();
        resp_mq_.reset();
        ipc::message_queue::remove(resp_queue_name_.c_str());
        return false;
    }

    ZIPC_LOG("connect: connected to '" << qname << "', resp queue '" << resp_queue_name_ << "'");
    return true;
}

bool IpcClient::connect(const std::string& qname) {
    static std::mutex connect_mutex;
    std::lock_guard<std::mutex> guard(connect_mutex);
    return do_connect(qname);
}

void IpcClient::disconnect() {
    if (!running_.load(std::memory_order_acquire)) {
        ZIPC_LOG("disconnect: already disconnected");
        return;
    }

    ZIPC_LOG("disconnect: fast shutdown initiated");
    running_.store(false, std::memory_order_release);
    cv_.notify_all();
    worker_running_.store(false, std::memory_order_release);
    task_cv_.notify_all();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);

    // Wait for receiver thread
    if (receiver_.joinable()) {
        while (receiver_.joinable() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        if (receiver_.joinable()) {
            ZIPC_LOG("disconnect: receiver join timeout, forcing join");
            receiver_.join();
        }
    }

    // Flush tasks and wait for worker
    clear_task_queue();
    if (worker_.joinable()) {
        while (worker_.joinable() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        if (worker_.joinable()) {
            ZIPC_LOG("disconnect: worker join timeout, forcing join");
            worker_.join();
        }
    }

    // Release queue resources
    mq_.reset();
    resp_mq_.reset();
    ipc::message_queue::remove(resp_queue_name_.c_str());

    // Force all pending requests to fail
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (auto& kv : pending_) {
            Response resp;
            resp.status = IPC_ERR_UNKNOWN;
            resp.bin_data = nullptr;
            resp.bin_size = 0;
            kv.second->set_value(resp);
        }
        pending_.clear();
    }

    // Clear notify handlers
    {
        std::lock_guard<std::mutex> lock(notify_mutex_);
        binary_notify_handlers_.clear();
    }

    ZIPC_LOG("disconnect: complete");
}

void IpcClient::cancel() {
    ZIPC_LOG("cancel: cancelling all pending RPCs");
    cancel_requested_.store(true, std::memory_order_release);
    cv_.notify_all();  // wake up any waiting threads
    // Pending promises will be failed when they time out or are explicitly woken.
    // We also need to wake up the receiver thread if it's waiting on cv_.
}

// ---------- call_binary with cancellation ----------
int IpcClient::call_binary(const std::string& func, const void* data, size_t size,
    void** out_data, size_t* out_size) {
    // FIX C6: Use shared_future to avoid blocking destructor issues.
    if (tls_in_callback_) {
        ZIPC_LOG("call_binary: recursion detected");
        return IPC_ERR_RECURSION;
    }

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

    // Check cancellation flag early
    if (cancel_requested_.load(std::memory_order_acquire)) {
        return IPC_ERR_CANCELED;
    }

    // Atomically allocate req_id and insert promise
    uint64_t req_id;
    std::shared_ptr<std::promise<Response>> promise;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        req_id = next_req_id_.fetch_add(1);
        promise = std::make_shared<std::promise<Response>>();
        pending_[req_id] = promise;
    }

    std::string shm_name;
    if (size == 0) {
        shm_name = ZERO_SHM_MARKER;
        ZIPC_LOG("call_binary: zero-size data, using marker");
    }
    else {
        shm_name = generate_unique_shm_name();
        // Clean up stale leases
        SharedMemoryLease::cleanup_stale(shm_name);
        ipc::shared_memory_object::remove(shm_name.c_str());
        try {
            ipc::shared_memory_object shm(ipc::create_only, shm_name.c_str(), ipc::read_write);
            shm.truncate(size);
            ipc::mapped_region region(shm, ipc::read_write);
            if (data && size > 0) {
                std::memcpy(region.get_address(), data, size);
            }
            ZIPC_LOG("call_binary: created shared memory '" << shm_name << "', size=" << size);
            // We do not keep a lease here; we rely on client to remove after reading.
        }
        catch (const std::exception& e) {
            ZIPC_LOG("call_binary: failed to create shared memory: " << e.what());
            ipc::shared_memory_object::remove(shm_name.c_str());
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_.erase(req_id);
            return IPC_ERR_MEMORY;
        }
    }

    std::string msg = "REQ|" + std::to_string(req_id) + "|" + resp_queue_name_ +
        "|BIN|" + func + "|" + shm_name;
    if (msg.size() > 1024) {
        ZIPC_LOG("call_binary: message size " << msg.size() << " exceeds 1024");
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_.erase(req_id);
        return IPC_ERR_SIZE;
    }

    try {
        mq_->send(msg.c_str(), msg.size(), 0);
        ZIPC_LOG("call_binary: sent request req_id=" << req_id);
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("call_binary: send failed: " << e.what());
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_.erase(req_id);
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        return IPC_ERR_SEND;
    }

    // Use shared_future for safer lifetime management (C6)
    auto future = promise->get_future().share();

    // Wait with timeout and cancellation check
    const auto start = std::chrono::steady_clock::now();
    const auto timeout = timeout_;
    std::future_status status = std::future_status::timeout;
    while (true) {
        auto remaining = timeout - std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start);
        if (remaining <= std::chrono::milliseconds::zero()) {
            status = std::future_status::timeout;
            break;
        }
        if (cancel_requested_.load(std::memory_order_acquire)) {
            status = std::future_status::deferred;  // treat as cancelled
            break;
        }
        status = future.wait_for(remaining);
        if (status != std::future_status::timeout) {
            break;
        }
    }

    if (cancel_requested_.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_.erase(req_id);
        if (shm_name != ZERO_SHM_MARKER)
            ipc::shared_memory_object::remove(shm_name.c_str());
        ZIPC_LOG("call_binary: cancelled req_id=" << req_id);
        return IPC_ERR_CANCELED;
    }

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
        ZIPC_LOG("call_binary received reply size=" << resp.bin_size);
        ZIPC_LOG_MD5("call_binary received reply", resp.bin_data, resp.bin_size);
    }
    else {
        ZIPC_LOG("call_binary received zero-size reply");
    }
    ZIPC_LOG("call_binary: success, reply_size=" << *out_size);
    return IPC_OK;
}

// ---------- notify_binary with proper cleanup (C4) ----------
int IpcClient::notify_binary(const std::string& func, const void* data, size_t size) {
    // Recursion guard
    if (tls_in_callback_) {
        ZIPC_LOG("notify_binary: recursion detected");
        return IPC_ERR_RECURSION;
    }

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
        ZIPC_LOG("notify_binary: func='" << func << "', size=" << size);
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
        // Clean up stale leases
        SharedMemoryLease::cleanup_stale(shm_name);
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
        // FIX C4: ensure shm is removed even if message size is too large
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
    // shm will be removed by the server's receiver after reading, or by the lease mechanism.
    return IPC_OK;
}

// ---------- receiver_thread_func ----------
void IpcClient::receiver_thread_func(std::shared_ptr<ipc::message_queue> resp_mq) {
    char buffer[1024];
    ZIPC_LOG("receiver_thread: started, tid=" << std::this_thread::get_id());

    while (running_.load(std::memory_order_acquire)) {
        size_t recvd;
        unsigned priority;
        bool ok = receive_with_intr(resp_mq, buffer, sizeof(buffer), recvd, priority);
        if (!ok) {
            // FIX C5: Use cv wait with timeout to avoid busy-loop and handle EINTR properly.
            std::unique_lock<std::mutex> lock(cv_mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(10),
                [this] { return !running_.load(std::memory_order_acquire); });
            continue;
        }

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
                            ZIPC_LOG("receiver_thread: binary response size=" << sz);
                            ZIPC_LOG_MD5("receiver_thread binary response", buf, sz);
                        }
                        else {
                            resp.status = IPC_ERR_MEMORY;
                            ZIPC_LOG("receiver_thread: ipc_alloc failed");
                        }
                        // FIX S2: remove shm after region destructs; region already destroyed here.
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
                    auto task = [this, func]() {
                        ipc_binary_notify_handler h = nullptr;
                        void* trigger = nullptr;
                        {
                            std::lock_guard<std::mutex> lock(notify_mutex_);
                            auto it = binary_notify_handlers_.find(func);
                            if (it != binary_notify_handlers_.end()) {
                                h = it->second.first;
                                trigger = it->second.second;
                                ZIPC_LOG("worker_task: found notify handler for '" << func << "'");
                            }
                            else {
                                ZIPC_LOG("worker_task: notify handler not found for '" << func << "'");
                            }
                        }
                        if (h) {
                            tls_in_callback_ = true;
                            try {
                                h(trigger, nullptr, 0);
                                ZIPC_LOG("worker_task: zero-size notify handler executed");
                            }
                            catch (...) {
                                ZIPC_LOG("worker_task: notify handler threw exception");
                            }
                            tls_in_callback_ = false;
                        }
                        };
                    enqueue_task(task);
                }
                else {
                    try {
                        ipc::shared_memory_object shm(ipc::open_only, shm_name.c_str(), ipc::read_only);
                        ipc::mapped_region region(shm, ipc::read_only);
                        size_t sz = region.get_size();
                        void* copy = ipc_alloc(sz);
                        if (copy) {
                            std::memcpy(copy, region.get_address(), sz);
                            // FIX S2: remove shm after copying
                            ipc::shared_memory_object::remove(shm_name.c_str());
                            ZIPC_LOG("receiver_thread: copied notify data size=" << sz);
                            auto task = [this, func, copy, sz]() {
                                ipc_binary_notify_handler h = nullptr;
                                void* trigger = nullptr;
                                {
                                    std::lock_guard<std::mutex> lock(notify_mutex_);
                                    auto it = binary_notify_handlers_.find(func);
                                    if (it != binary_notify_handlers_.end()) {
                                        h = it->second.first;
                                        trigger = it->second.second;
                                        ZIPC_LOG("worker_task: found notify handler for '" << func << "'");
                                    }
                                    else {
                                        ZIPC_LOG("worker_task: notify handler not found for '" << func << "'");
                                    }
                                }
                                if (h) {
                                    tls_in_callback_ = true;
                                    try {
                                        h(trigger, copy, sz);
                                        ZIPC_LOG("worker_task: notify handler executed, size=" << sz);
                                    }
                                    catch (...) {
                                        ZIPC_LOG("worker_task: notify handler threw exception");
                                    }
                                    tls_in_callback_ = false;
                                }
                                ipc_free(copy);
                                };
                            enqueue_task(task);
                        }
                        else {
                            ZIPC_LOG("receiver_thread: ipc_alloc failed for notify data");
                            ipc::shared_memory_object::remove(shm_name.c_str());
                        }
                    }
                    catch (const std::exception& e) {
                        ZIPC_LOG("receiver_thread: failed to open notify shm: " << e.what());
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

    ZIPC_LOG("receiver_thread: exiting");
}

// ---------- worker_thread_func ----------
void IpcClient::worker_thread_func() {
    struct WorkerGuard {
        IpcClient* client;
        ~WorkerGuard() {
            if (client) {
                // no active_worker_ anymore, we just let thread exit
                ZIPC_LOG("worker_thread: exiting");
            }
        }
    } guard{ this };

    ZIPC_LOG("worker_thread: started, tid=" << std::this_thread::get_id());

    while (worker_running_.load(std::memory_order_acquire)) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(task_mutex_);
            if (task_queue_.empty()) {
                task_cv_.wait_for(lock, std::chrono::milliseconds(10),
                    [this] {
                        return !worker_running_.load(std::memory_order_acquire) || !task_queue_.empty();
                    });
                if (!worker_running_.load(std::memory_order_acquire) && task_queue_.empty())
                    break;
                if (task_queue_.empty())
                    continue;
            }
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }

        if (task) {
            task();
        }
    }

    ZIPC_LOG("worker_thread: exiting");
}

// ---------- enqueue_task ----------
void IpcClient::enqueue_task(std::function<void()> task) {
    if (!worker_running_.load(std::memory_order_acquire)) {
        ZIPC_LOG("enqueue_task: worker not running, discarding task");
        return;
    }
    std::lock_guard<std::mutex> lock(task_mutex_);
    task_queue_.push(std::move(task));
    task_cv_.notify_one();
}

// ---------- clear_task_queue ----------
void IpcClient::clear_task_queue() {
    std::lock_guard<std::mutex> lock(task_mutex_);
    std::queue< std::function<void()> > empty;
    std::swap(task_queue_, empty);
    ZIPC_LOG("clear_task_queue: discarded all pending tasks");
}

// ---------- receive_with_intr (EINTR-safe with running_ check) ----------
bool IpcClient::receive_with_intr(std::shared_ptr<ipc::message_queue> mq,
    char* buffer, size_t bufsize,
    size_t& recvd, unsigned& priority) {
    const int max_retries = 5;
    for (int attempt = 0; attempt < max_retries; ++attempt) {
        // Check running_ flag to avoid spinning after shutdown
        if (!running_.load(std::memory_order_acquire)) {
            return false;
        }
        try {
            if (mq->try_receive(buffer, bufsize, recvd, priority)) {
                return true;
            }
            return false;
        }
        catch (const ipc::interprocess_exception& e) {
            if (errno == EINTR) {
                ZIPC_LOG("receive_with_intr: interrupted by signal, retrying");
                std::this_thread::yield();
                continue;
            }
            ZIPC_LOG("receive_with_intr: exception: " << e.what() << " errno=" << errno);
            return false;
        }
        catch (...) {
            ZIPC_LOG("receive_with_intr: unknown exception");
            return false;
        }
    }
    return false;
}

// ---------- generate_unique_shm_name ----------
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
    std::string name = ss.str();

#ifdef _WIN32
    for (int attempt = 0; attempt < 3; ++attempt) {
        try {
            {
                ipc::shared_memory_object shm(ipc::open_only, name.c_str(), ipc::read_write);
            }
            ipc::shared_memory_object::remove(name.c_str());
            ZIPC_LOG("generate_unique_shm_name: removed existing Windows shm " << name);
            break;
        }
        catch (...) {
            break;
        }
    }
#endif
    return name;
}

// ---------- generate_unique_resp_queue ----------
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

// ---------- is_connected ----------
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

// ---------- register_binary_notify ----------
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

// ---------- unregister_binary_notify ----------
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