/**
 * @file z_ipc_server_impl.h
 * @brief Server declaration with health monitoring and RAII.
 *
 * This revision fixes:
 *   - S1: stop() uses timeout-based join, no detach (except extreme case).
 *   - S2: Shared memory remove after region destruction (RAII).
 *   - S3: No per-response detached thread; cleanup via lease or client.
 *   - S4: String parsing retained for ABI compatibility, but with added checks.
 *   - S5: Strict queue name validation (alnum and underscore only).
 *   - S6: Defensive parameter correction for extreme values.
 *   - New: Worker heartbeat monitoring.
 */

#pragma once

#include "z_ipc_api.h"
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <string>
#include <unordered_map>
#include <thread>
#include <vector>
#include <atomic>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <chrono>

namespace ipc = boost::interprocess;

/**
 * IpcServer ¨C server for binary IPC.
 * Listens on a named queue, dispatches RPC requests to registered handlers,
 * and allows sending notifications to connected clients.
 */
class IpcServer {
public:
    IpcServer();
    ~IpcServer();

    /**
     * Start the server.
     * @param queue_name      name of the main queue (alnum + underscore only)
     * @param thread_count    number of worker threads (0 = auto)
     * @param max_queue_length maximum pending messages
     * @param max_msg_size    maximum message size for control messages
     * @return true on success
     */
    bool start(const std::string& queue_name, int thread_count,
        size_t max_queue_length, size_t max_msg_size);

    /** Stop the server and clean up. */
    void stop();

    const std::string& get_queue_name() const { return queue_name_; }

    /** Register a binary RPC handler for a function name. */
    int register_binary_reply(const std::string& name,
        ipc_binary_reply_handler h, void* trigger);

    /** Unregister a binary RPC handler. */
    int unregister_binary_reply(const std::string& name);

    /** Register a binary notify handler (client->server). */
    int register_binary_notify(const std::string& name,
        ipc_binary_notify_handler h, void* trigger);

    /** Unregister a binary notify handler. */
    int unregister_binary_notify(const std::string& name);

    /** Send a binary notification to a specific client. */
    int send_notify_binary(const std::string& client_resp_queue,
        const std::string& func, const void* data, size_t size);

private:
    /** Worker thread function ¨C processes incoming messages. */
    void worker_thread_func(std::shared_ptr<ipc::message_queue> mq, int worker_id);

    /** Health monitor thread ¨C checks worker heartbeats. */
    void monitor_thread_func();

    /** Send an RPC response to a client. */
    void send_response(const std::string& resp_queue, uint64_t req_id,
        int status, const std::string& shm_name);

    /** Generate a unique name for a shared memory segment. */
    std::string generate_unique_shm_name();

    std::string queue_name_;
    std::shared_ptr<ipc::message_queue> mq_;

    /** Handlers for binary RPC requests. */
    std::unordered_map<std::string, std::pair<ipc_binary_reply_handler, void*>> binary_reply_handlers_;
    /** Handlers for binary notifications from clients. */
    std::unordered_map<std::string, std::pair<ipc_binary_notify_handler, void*>> binary_notify_handlers_;
    std::mutex handlers_mutex_;

    std::atomic<bool> running_{ false };
    std::vector<std::thread> workers_;
    // Heartbeat timestamps for each worker
    std::vector<std::chrono::steady_clock::time_point> worker_last_active_;
    std::mutex worker_heartbeat_mutex_;

    std::thread monitor_;
    std::atomic<bool> monitor_running_{ false };

    std::mutex cv_mutex_;
    std::condition_variable cv_;

    size_t max_msg_size_;
    size_t max_queue_length_;

    std::atomic<size_t> active_workers_{ 0 };
};