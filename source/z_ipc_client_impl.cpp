/**
 * @file z_ipc_client_impl.cpp
 * @brief Client implementation ¨C hardened with non-blocking sends, exception safety, and data integrity checks.
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
#include <io.h>
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

// ---------- RAII lease for shared memory cleanup (Linux-only, with fallback) ----------
class SharedMemoryLease {
    std::string shm_name_;
    std::string lease_path_;
    bool valid_;
public:
    SharedMemoryLease(const std::string& shm_name)
        : shm_name_(shm_name), valid_(false)
    {
#ifdef _WIN32
        char temp_path[MAX_PATH];
        if (GetTempPathA(MAX_PATH, temp_path)) {
            lease_path_ = std::string(temp_path) + shm_name_ + ".lease";
            std::remove(lease_path_.c_str());
            auto now = std::chrono::steady_clock::now().time_since_epoch().count();
            std::ofstream ofs(lease_path_);
            if (ofs) {
                ofs << now;
                valid_ = true;
            }
        }
#else
        lease_path_ = "/dev/shm/" + shm_name_ + ".lease";
        std::remove(lease_path_.c_str());
        auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        std::ofstream ofs(lease_path_);
        if (ofs) {
            ofs << now;
            valid_ = true;
        }
#endif
    }
    ~SharedMemoryLease() {
        if (valid_) {
            std::remove(lease_path_.c_str());
            ipc::shared_memory_object::remove(shm_name_.c_str());
        }
    }
    bool valid() const { return valid_; }
    static void cleanup_stale(const std::string& shm_name) {
#ifdef _WIN32
        // Windows: no standard cleanup, rely on kernel cleanup when all handles closed.
#else
        std::string lease_path = "/dev/shm/" + shm_name + ".lease";
        std::ifstream ifs(lease_path);
        if (ifs) {
            int64_t ts;
            ifs >> ts;
            auto now = std::chrono::steady_clock::now().time_since_epoch().count();
            if (now - ts > LEASE_TIMEOUT_SEC * 1000000000LL) {
                std::remove(lease_path.c_str());
                ipc::shared_memory_object::remove(shm_name.c_str());
            }
        }
#endif
    }
};

IpcClient::IpcClient() {
    ZIPC_LOG("IpcClient constructor");
}

IpcClient::~IpcClient() {
    ZIPC_LOG("~IpcClient: deterministic cleanup");
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

    try {
        mq_ = std::make_unique<ipc::message_queue>(ipc::open_only, qname.c_str());
        ZIPC_LOG("connect: opened main queue '" << qname << "'");
    }
    catch (const ipc::interprocess_exception& e) {
        ZIPC_LOG("connect: failed to open main queue: " << e.what());
        return false;
    }

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

    running_.store(true, std::memory_order_release);
    worker_running_.store(true, std::memory_order_release);

    try {
        receiver_ = std::thread(&IpcClient::receiver_thread_func, this, resp_mq_);
        worker_ = std::thread(&IpcClient::worker_thread_func, this);
    }
    catch (...) {
        running_.store(false, std::memory_order_release);
        worker_running_.store(false, std::memory_order_release);
        if (receiver_.joinable()) receiver_.join();
        if (worker_.joinable()) worker_.join();
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

    if (receiver_.joinable()) {
        while (receiver_.joinable() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        if (receiver_.joinable()) {
            ZIPC_LOG("disconnect: receiver join timeout, forcing join");
            receiver_.join();
        }
    }

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

    mq_.reset();
    resp_mq_.reset();
    ipc::message_queue::remove(resp_queue_name_.c_str());

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

    ZIPC_LOG("disconnect: complete");
}

void IpcClient::cancel() {
    ZIPC_LOG("cancel: cancelling all pending RPCs");
    cancel_requested_.store(true, std::memory_order_release);
    cv_.notify_all();
}

// ---------- call_binary with non-blocking send and integrity header ----------
int IpcClient::call_binary(const std::string& func, const void* data, size_t size,
    void** out_data, size_t* out_size) {
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

    if (cancel_requested_.load(std::memory_order_acquire)) {
        return IPC_ERR_CANCELED;
    }

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
    }
    else {
        shm_name = generate_unique_shm_name();
        SharedMemoryLease::cleanup_stale(shm_name);
        ipc::shared_memory_object::remove(shm_name.c_str());
        try {
            size_t total_size = SHM_HEADER_SIZE + size;
            ipc::shared_memory_object shm(ipc::create_only, shm_name.c_str(), ipc::read_write);
            shm.truncate(total_size);
            ipc::mapped_region region(shm, ipc::read_write);

            // Write header and payload with checksum
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
            ZIPC_LOG("call_binary: created shared memory '" << shm_name
                << "', total_size=" << total_size << ", payload=" << size);
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
        if (!mq_->try_send(msg.c_str(), msg.size(), 0)) {
            ZIPC_LOG("call_binary: main queue full, cannot send");
            if (shm_name != ZERO_SHM_MARKER)
                ipc::shared_memory_object::remove(shm_name.c_str());
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_.erase(req_id);
            return IPC_ERR_BUSY;
        }
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

    auto future = promise->get_future().share();

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
            status = std::future_status::deferred;
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

// ---------- notify_binary with non-blocking send and integrity header ----------
int IpcClient::notify_binary(const std::string& func, const void* data, size_t size) {
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
    }
    else {
        shm_name = generate_unique_shm_name();
        SharedMemoryLease::cleanup_stale(shm_name);
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
            ZIPC_LOG("notify_binary: created shared memory '" << shm_name
                << "', total_size=" << total_size << ", payload=" << size);
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
        if (!mq_->try_send(msg.c_str(), msg.size(), 0)) {
            ZIPC_LOG("notify_binary: main queue full, cannot send");
            if (shm_name != ZERO_SHM_MARKER)
                ipc::shared_memory_object::remove(shm_name.c_str());
            return IPC_ERR_BUSY;
        }
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

// ---------- receiver_thread_func with integrity verification ----------
void IpcClient::receiver_thread_func(std::shared_ptr<ipc::message_queue> resp_mq) {
    char buffer[1024];
    ZIPC_LOG("receiver_thread: started, tid=" << std::this_thread::get_id());

    while (running_.load(std::memory_order_acquire)) {
        size_t recvd;
        unsigned priority;
        bool ok = receive_with_intr(resp_mq, buffer, sizeof(buffer), recvd, priority);
        if (!ok) {
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
                        size_t region_size = region.get_size();
                        if (region_size < SHM_HEADER_SIZE) {
                            ZIPC_LOG("receiver_thread: response shm too small");
                            ipc::shared_memory_object::remove(shm_name.c_str());
                            resp.status = IPC_ERR_RECEIVE;
                        }
                        else {
                            const ShmHeader* hdr = static_cast<const ShmHeader*>(region.get_address());
                            if (hdr->magic != SHM_MAGIC) {
                                ZIPC_LOG("receiver_thread: bad magic in response");
                                ipc::shared_memory_object::remove(shm_name.c_str());
                                resp.status = IPC_ERR_RECEIVE;
                            }
                            else {
                                size_t payload_len = hdr->data_len;
                                if (region_size != SHM_HEADER_SIZE + payload_len) {
                                    ZIPC_LOG("receiver_thread: size mismatch in response");
                                    ipc::shared_memory_object::remove(shm_name.c_str());
                                    resp.status = IPC_ERR_RECEIVE;
                                }
                                else {
                                    const void* payload = static_cast<const char*>(region.get_address()) + SHM_HEADER_SIZE;
                                    uint32_t calc = crc32(payload, payload_len);
                                    if (calc != hdr->checksum) {
                                        ZIPC_LOG("receiver_thread: checksum mismatch in response");
                                        ipc::shared_memory_object::remove(shm_name.c_str());
                                        resp.status = IPC_ERR_RECEIVE;
                                    }
                                    else {
                                        buf = ipc_alloc(payload_len);
                                        if (buf) {
                                            std::memcpy(buf, payload, payload_len);
                                            resp.bin_data = buf;
                                            resp.bin_size = payload_len;
                                            ZIPC_LOG("receiver_thread: binary response size=" << payload_len);
                                            ZIPC_LOG_MD5("receiver_thread binary response", buf, payload_len);
                                        }
                                        else {
                                            resp.status = IPC_ERR_MEMORY;
                                            ZIPC_LOG("receiver_thread: ipc_alloc failed");
                                        }
                                        ipc::shared_memory_object::remove(shm_name.c_str());
                                    }
                                }
                            }
                        }
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
                        size_t region_size = region.get_size();
                        if (region_size < SHM_HEADER_SIZE) {
                            ZIPC_LOG("receiver_thread: notify shm too small");
                            ipc::shared_memory_object::remove(shm_name.c_str());
                            continue;
                        }
                        const ShmHeader* hdr = static_cast<const ShmHeader*>(region.get_address());
                        if (hdr->magic != SHM_MAGIC) {
                            ZIPC_LOG("receiver_thread: bad magic in notify");
                            ipc::shared_memory_object::remove(shm_name.c_str());
                            continue;
                        }
                        size_t payload_len = hdr->data_len;
                        if (region_size != SHM_HEADER_SIZE + payload_len) {
                            ZIPC_LOG("receiver_thread: size mismatch in notify");
                            ipc::shared_memory_object::remove(shm_name.c_str());
                            continue;
                        }
                        const void* payload = static_cast<const char*>(region.get_address()) + SHM_HEADER_SIZE;
                        uint32_t calc = crc32(payload, payload_len);
                        if (calc != hdr->checksum) {
                            ZIPC_LOG("receiver_thread: checksum mismatch in notify");
                            ipc::shared_memory_object::remove(shm_name.c_str());
                            continue;
                        }
                        void* copy = ipc_alloc(payload_len);
                        if (copy) {
                            std::memcpy(copy, payload, payload_len);
                            ipc::shared_memory_object::remove(shm_name.c_str());
                            ZIPC_LOG("receiver_thread: copied notify data size=" << payload_len);
                            auto task = [this, func, copy, payload_len]() {
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
                                        h(trigger, copy, payload_len);
                                        ZIPC_LOG("worker_task: notify handler executed, size=" << payload_len);
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

void IpcClient::worker_thread_func() {
    struct WorkerGuard {
        IpcClient* client;
        ~WorkerGuard() {
            if (client) {
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

void IpcClient::enqueue_task(std::function<void()> task) {
    if (!worker_running_.load(std::memory_order_acquire)) {
        ZIPC_LOG("enqueue_task: worker not running, discarding task");
        return;
    }
    std::lock_guard<std::mutex> lock(task_mutex_);
    task_queue_.push(std::move(task));
    task_cv_.notify_one();
}

void IpcClient::clear_task_queue() {
    std::lock_guard<std::mutex> lock(task_mutex_);
    std::queue< std::function<void()> > empty;
    std::swap(task_queue_, empty);
    ZIPC_LOG("clear_task_queue: discarded all pending tasks");
}

bool IpcClient::receive_with_intr(std::shared_ptr<ipc::message_queue> mq,
    char* buffer, size_t bufsize,
    size_t& recvd, unsigned& priority) {
    const int max_retries = 5;
    for (int attempt = 0; attempt < max_retries; ++attempt) {
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