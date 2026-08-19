/**
 * @file z_ipc_client_impl.h
 * @brief Declaration of the IpcClient class – final hardened with cancellation and RAII.
 */

#pragma once

#include "z_ipc_api.h"
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <string>
#include <memory>
#include <unordered_map>
#include <future>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <utility>
#include <queue>
#include <functional>

namespace ipc = boost::interprocess;

// Extend error codes if not defined in api.h
#ifndef IPC_ERR_CANCELED
#define IPC_ERR_CANCELED -13
#endif

struct Response {
    int status;
    void* bin_data;
    size_t bin_size;
};

class IpcClient : public std::enable_shared_from_this<IpcClient> {
public:
    IpcClient();
    ~IpcClient();

    bool connect(const std::string& queue_name);
    void disconnect();
    void cancel();  // New: cancel all pending RPCs
    bool is_connected() const;
    void set_timeout(int milliseconds) { timeout_ = std::chrono::milliseconds(milliseconds); }

    int call_binary(const std::string& func, const void* data, size_t size,
        void** out_data, size_t* out_size);
    int notify_binary(const std::string& func, const void* data, size_t size);

    const char* get_resp_queue_name() const { return resp_queue_name_.c_str(); }

    int register_binary_notify(const std::string& name,
        ipc_binary_notify_handler h, void* trigger);
    int unregister_binary_notify(const std::string& name);

private:
    void receiver_thread_func(std::shared_ptr<ipc::message_queue> resp_mq);
    void worker_thread_func();
    void enqueue_task(std::function<void()> task);
    void clear_task_queue();
    bool receive_with_intr(std::shared_ptr<ipc::message_queue> mq,
        char* buffer, size_t bufsize,
        size_t& recvd, unsigned& priority);

    std::string generate_unique_shm_name();
    std::string generate_unique_resp_queue();

    // ---------- Core resources ----------
    std::string server_queue_name_;
    std::string resp_queue_name_;
    std::unique_ptr<ipc::message_queue> mq_;
    std::shared_ptr<ipc::message_queue> resp_mq_;

    // ---------- Threads & control ----------
    std::atomic<bool> running_{ false };
    std::thread receiver_;
    std::thread worker_;

    std::atomic<bool> worker_running_{ false };
    std::atomic<bool> cancel_requested_{ false };   // for cancellation

    // ---------- Task queue ----------
    std::mutex task_mutex_;
    std::condition_variable task_cv_;
    std::queue< std::function<void()> > task_queue_;

    // ---------- Pending RPC calls ----------
    std::mutex pending_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<std::promise<Response>>> pending_;

    // ---------- Notification handlers ----------
    std::mutex notify_mutex_;
    std::unordered_map<std::string, std::pair<ipc_binary_notify_handler, void*>> binary_notify_handlers_;

    // ---------- Synchronization ----------
    std::mutex cv_mutex_;
    std::condition_variable cv_;

    std::atomic<uint64_t> next_req_id_{ 1 };
    std::chrono::milliseconds timeout_{ 5000 };

    static thread_local bool tls_in_callback_;

    // Internal connect helper (atomic)
    bool do_connect(const std::string& qname);
};