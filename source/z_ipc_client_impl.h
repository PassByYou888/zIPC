/**
 * @file z_ipc_client_impl.h
 * @brief Declaration of the IpcClient class that implements client?side
 *        functionality for binary RPC and notifications.
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

namespace ipc = boost::interprocess;

/** Structure used to store a pending RPC response */
struct Response {
    int status;      /**< Return code */
    void* bin_data;  /**< Binary reply data (must be freed with ipc_free) */
    size_t bin_size; /**< Size of binary reply */
};

/**
 * IpcClient ¨C client for binary IPC.
 * Connects to a server queue, sends RPC requests and notifications,
 * and receives asynchronous notifications from the server.
 */
class IpcClient {
public:
    IpcClient();
    ~IpcClient();

    /** Connect to the server queue. Returns true on success. */
    bool connect(const std::string& queue_name);

    /** Disconnect from the server and clean up. */
    void disconnect();

    /** Check if the connection is alive. */
    bool is_connected() const;

    /** Set the timeout for RPC calls (in milliseconds). */
    void set_timeout(int milliseconds) { timeout_ = std::chrono::milliseconds(milliseconds); }

    /**
     * Perform a binary RPC call.
     * @param func     function name registered on the server
     * @param data     request payload (may be nullptr)
     * @param size     payload size
     * @param out_data output pointer (allocated with ipc_alloc; caller must free)
     * @param out_size output size
     * @return IPC_OK on success, or an error code
     */
    int call_binary(const std::string& func, const void* data, size_t size,
        void** out_data, size_t* out_size);

    /**
     * Send a binary notification (fire?and?forget).
     * @param func     function name registered on the server
     * @param data     notification payload (may be nullptr)
     * @param size     payload size
     * @return IPC_OK on success
     */
    int notify_binary(const std::string& func, const void* data, size_t size);

    /** Get the name of this client's private response queue. */
    const char* get_resp_queue_name() const { return resp_queue_name_.c_str(); }

    /** Register a handler for binary notifications from the server. */
    int register_binary_notify(const std::string& name,
        ipc_binary_notify_handler h, void* trigger);

    /** Unregister a handler for binary notifications. */
    int unregister_binary_notify(const std::string& name);

private:
    /** Receiver thread: listens for RPC replies and server?originated notifications. */
    void receiver_thread_func(std::shared_ptr<ipc::message_queue> resp_mq);

    /** Generate a unique name for a shared memory segment. */
    std::string generate_unique_shm_name();

    /** Generate a unique name for a response queue. */
    std::string generate_unique_resp_queue();

    std::string server_queue_name_;      /**< Name of the server's main queue */
    std::string resp_queue_name_;        /**< Name of this client's response queue */
    std::unique_ptr<ipc::message_queue> mq_;          /**< Main queue (server) */
    std::shared_ptr<ipc::message_queue> resp_mq_;     /**< Response queue (private) */

    std::atomic<bool> running_{ false }; /**< Whether the client is active */
    std::thread receiver_;               /**< Receiver thread */
    std::atomic<size_t> active_receiver_{ 0 }; /**< 1 when receiver is running, 0 after exit */

    /** Pending RPC calls: request ID -> promise for the response */
    std::mutex pending_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<std::promise<Response>>> pending_;

    /** Registered binary notify handlers (server¡úclient) */
    std::mutex notify_mutex_;
    std::unordered_map<std::string, std::pair<ipc_binary_notify_handler, void*>> binary_notify_handlers_;

    std::mutex cv_mutex_;
    std::condition_variable cv_;

    std::atomic<uint64_t> next_req_id_{ 1 };     /**< Request ID counter */
    std::chrono::milliseconds timeout_{ 5000 };  /**< Default timeout */
};