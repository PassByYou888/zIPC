/**
 * @file z_ipc_api.cpp
 * @brief Implementation of the C API for z_ipc.
 *
 * This file manages global server/client instances, forwards calls to the
 * internal C++ implementation classes, and provides logging via a custom
 * streambuf that calls the user?supplied status handler.
 *
 * All exported functions are declared with extern "C" and follow the C calling
 * convention, making them accessible from other languages (Delphi, C, etc.).
 * The API uses integer handles to reference server and client objects, which
 * are stored in global maps protected by a mutex.
 *
 * The status handler (status_call_Handler) is a callback that receives
 * individual characters from std::cout/std::cerr via a custom streambuf.
 * This allows integration with Pascal's logging system (DoStatus).
 */

#include "z_ipc_api.h"
#include "z_ipc_server_impl.h"
#include "z_ipc_client_impl.h"
#include <mutex>
#include <unordered_map>
#include <memory>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <sstream>

 /* Global status handler pointer 每 initially null.
  * It is set by ipc_Set_Status_handler and called by IPC_streambuf.
  * Used to forward library log output to the user's callback.
  */
    static status_handler status_call_Handler = 0;

/**
 * Custom streambuf class that redirects std::cout/std::cerr output to the
 * status handler. Each character (int) is passed to the user callback.
 * This allows the library's internal logging (via std::cerr) to be captured
 * by the application (e.g., Delphi's DoStatus).
 *
 * This streambuf is installed via ipc_Set_Status_handler and restored on
 * shutdown or when a new handler is set.
 */
class IPC_streambuf : public std::streambuf {
protected:
    virtual int_type overflow(int_type c) override {
        if (status_call_Handler != 0) {
            status_call_Handler(c);
        }
        return c;
    }
};

/* Internal log stream that uses the custom streambuf 每 only library-internal
 * logging goes through this stream, not the global cout/cerr.
 */
static IPC_streambuf g_ipc_streambuf;
std::ostream g_ipc_log_stream(&g_ipc_streambuf);

#ifdef ZIPC_ENABLE_LOG
#define ZIPC_LOG(msg) g_ipc_log_stream << "[ZIPC] " << msg << std::endl
#else
#define ZIPC_LOG(msg) ((void)0)
#endif

/* Global storage for server and client objects.
 * All operations that create/destroy or access these maps must hold g_mutex.
 */
static std::mutex g_mutex;
static std::atomic<int> g_next_server_handle{ 1 }; // Next available server handle (starts at 1)
static std::atomic<int> g_next_client_handle{ 1 }; // Next available client handle
static std::unordered_map<int, std::shared_ptr<IpcServer>> g_servers;
static std::unordered_map<int, std::shared_ptr<IpcClient>> g_clients;
static std::atomic<bool> g_shutdown_called{ false }; // Guard to prevent multiple shutdowns

/* Allocate a new server handle (ensures non-zero).
 * Called by ipc_server_create_ex while holding g_mutex.
 */
static int allocate_server_handle() {
    int h = g_next_server_handle.fetch_add(1);
    if (h == 0) h = g_next_server_handle.fetch_add(1);
    return h;
}

/* Allocate a new client handle (ensures non-zero).
 * Called by ipc_client_create while holding g_mutex.
 */
static int allocate_client_handle() {
    int h = g_next_client_handle.fetch_add(1);
    if (h == 0) h = g_next_client_handle.fetch_add(1);
    return h;
}

/* Helper to retrieve a server shared_ptr from its handle.
 * Returns nullptr if the handle is invalid or the server has been destroyed.
 * Used by all server API functions to obtain the IpcServer instance.
 * Called while holding g_mutex (lock_guard ensures).
 */
static std::shared_ptr<IpcServer> get_server(ipc_server_handle_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_servers.find(handle);
    if (it == g_servers.end()) return nullptr;
    return it->second;
}

/* Helper to retrieve a client shared_ptr from its handle.
 * Returns nullptr if the handle is invalid.
 * Used by all client API functions.
 */
static std::shared_ptr<IpcClient> get_client(ipc_client_handle_t handle) {
    std::lock_guard<std::mutex> lock(g_mutex);
    auto it = g_clients.find(handle);
    if (it == g_clients.end()) return nullptr;
    return it->second;
}

/* ======================== Exported C functions ======================== */

extern "C" {

    /**
     * ipc_server_create 每 Create a server with default configuration.
     * @param queue_name   Name of the main message queue.
     * @param thread_count Number of worker threads (0 = auto).
     * @return Server handle (non-zero) on success, 0 on failure.
     *
     * This is a convenience wrapper around ipc_server_create_ex.
     * It fills a default config (max_queue=1000, max_msg=1024) and calls the extended version.
     */
    ipc_server_handle_t ipc_server_create(const char* queue_name, int thread_count) {
        ipc_server_config_t cfg;
        cfg.thread_count = thread_count;
        cfg.max_queue_length = 1000;
        cfg.max_msg_size = 1024;
        return ipc_server_create_ex(queue_name, &cfg);
    }

    /**
     * ipc_server_create_ex 每 Create a server with explicit configuration.
     * @param queue_name   Name of the main queue.
     * @param cfg          Pointer to configuration structure; if NULL, defaults are used.
     * @return Server handle (non-zero) on success, 0 on failure.
     *
     * Flow:
     * 1. Validate queue_name.
     * 2. Copy config and sanitize values (ensure minimums).
     * 3. Instantiate IpcServer and call start().
     * 4. If start succeeds, allocate a new handle and store the shared_ptr in g_servers.
     * 5. Return the handle.
     *
     * Called by applications wanting fine-grained control over queue parameters.
     * The server is not running until start() returns true.
     */
    ipc_server_handle_t ipc_server_create_ex(const char* queue_name,
        const ipc_server_config_t* cfg) {
        if (!queue_name) {
            ZIPC_LOG("ipc_server_create_ex: queue_name is null");
            return 0;
        }
        ipc_server_config_t config;
        if (cfg) config = *cfg;
        else {
            config.thread_count = 0;
            config.max_queue_length = 1000;
            config.max_msg_size = 1024;
        }
        // Protect against invalid values.
        if (config.max_queue_length == 0) config.max_queue_length = 1000;
        if (config.max_msg_size < 64) config.max_msg_size = 1024;
        if (config.thread_count < 0) config.thread_count = 0;

        auto server = std::make_shared<IpcServer>();
        if (!server->start(queue_name, config.thread_count,
            config.max_queue_length, config.max_msg_size)) {
            ZIPC_LOG("ipc_server_create_ex: server start failed for queue '" << queue_name << "'");
            return 0;
        }
        std::lock_guard<std::mutex> lock(g_mutex);
        int handle = allocate_server_handle();
        g_servers[handle] = server;
        ZIPC_LOG("ipc_server_create_ex: queue='" << queue_name << "', handle=" << handle
            << ", threads=" << config.thread_count << ", max_len=" << config.max_queue_length
            << ", max_msg=" << config.max_msg_size);
        return handle;
    }

    /**
     * ipc_server_destroy 每 Destroy a server instance.
     * @param handle Server handle.
     * @return IPC_OK on success, IPC_ERR_INVAL if handle invalid.
     *
     * Flow:
     * 1. Validate handle.
     * 2. Lock g_mutex, retrieve and remove the server from g_servers.
     * 3. Call server->stop() to stop all worker threads and remove the queue.
     * 4. The shared_ptr will delete the server after this call.
     *
     * Typically called when shutting down the server or when it is no longer needed.
     */
    int ipc_server_destroy(ipc_server_handle_t handle) {
        if (handle <= 0) {
            ZIPC_LOG("ipc_server_destroy: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        std::shared_ptr<IpcServer> server;
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            auto it = g_servers.find(handle);
            if (it == g_servers.end()) {
                ZIPC_LOG("ipc_server_destroy: handle " << handle << " not found");
                return IPC_ERR_INVAL;
            }
            server = it->second;
            g_servers.erase(it);
        }
        server->stop();
        ZIPC_LOG("ipc_server_destroy: handle " << handle << " destroyed");
        return IPC_OK;
    }

    /**
     * ipc_server_register_binary_reply 每 Register a binary RPC handler for the server.
     * @param handle   Server handle.
     * @param name     Function name (string) that clients will call.
     * @param handler  Callback function of type ipc_binary_reply_handler.
     * @param trigger  User pointer passed to the handler.
     * @return IPC_OK on success, IPC_ERR_BUSY if name already registered,
     *         IPC_ERR_INVAL on invalid arguments.
     *
     * This handler is invoked by server worker threads when a REQ message with
     * matching 'name' arrives. The handler must produce a reply and allocate
     * the reply buffer using ipc_alloc().
     *
     * The registration is stored in IpcServer's internal map (binary_reply_handlers_).
     * Called by application code after the server has started.
     */
    int ipc_server_register_binary_reply(ipc_server_handle_t handle, const char* name,
        ipc_binary_reply_handler handler, void* trigger) {
        if (!name || !handler) {
            ZIPC_LOG("ipc_server_register_binary_reply: invalid name or handler");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_server_register_binary_reply: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto server = get_server(handle);
        if (!server) {
            ZIPC_LOG("ipc_server_register_binary_reply: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = server->register_binary_reply(name, handler, trigger);
        ZIPC_LOG("ipc_server_register_binary_reply: handle=" << handle << ", name='" << name
            << "', ret=" << ret);
        return ret;
    }

    /**
     * ipc_server_unregister_binary_reply 每 Unregister a previously registered handler.
     * @param handle Server handle.
     * @param name   Function name.
     * @return IPC_OK on success, IPC_ERR_NOT_FOUND if not registered.
     */
    int ipc_server_unregister_binary_reply(ipc_server_handle_t handle, const char* name) {
        if (!name) {
            ZIPC_LOG("ipc_server_unregister_binary_reply: name is null");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_server_unregister_binary_reply: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto server = get_server(handle);
        if (!server) {
            ZIPC_LOG("ipc_server_unregister_binary_reply: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = server->unregister_binary_reply(name);
        ZIPC_LOG("ipc_server_unregister_binary_reply: handle=" << handle << ", name='" << name
            << "', ret=" << ret);
        return ret;
    }

    /**
     * ipc_server_register_binary_notify 每 Register a binary notification handler
     * for client↙server notifications.
     * @param handle   Server handle.
     * @param name     Notification name.
     * @param handler  Callback of type ipc_binary_notify_handler.
     * @param trigger  User pointer.
     * @return IPC_OK on success, IPC_ERR_BUSY if name exists.
     *
     * This handler is called when a NOTIFY message with matching 'name' arrives
     * from a client. Unlike RPC, there is no reply.
     */
    int ipc_server_register_binary_notify(ipc_server_handle_t handle, const char* name,
        ipc_binary_notify_handler handler, void* trigger) {
        if (!name || !handler) {
            ZIPC_LOG("ipc_server_register_binary_notify: invalid name or handler");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_server_register_binary_notify: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto server = get_server(handle);
        if (!server) {
            ZIPC_LOG("ipc_server_register_binary_notify: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = server->register_binary_notify(name, handler, trigger);
        ZIPC_LOG("ipc_server_register_binary_notify: handle=" << handle << ", name='" << name
            << "', ret=" << ret);
        return ret;
    }

    /**
     * ipc_server_unregister_binary_notify 每 Unregister a notification handler.
     */
    int ipc_server_unregister_binary_notify(ipc_server_handle_t handle, const char* name) {
        if (!name) {
            ZIPC_LOG("ipc_server_unregister_binary_notify: name is null");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_server_unregister_binary_notify: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto server = get_server(handle);
        if (!server) {
            ZIPC_LOG("ipc_server_unregister_binary_notify: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = server->unregister_binary_notify(name);
        ZIPC_LOG("ipc_server_unregister_binary_notify: handle=" << handle << ", name='" << name
            << "', ret=" << ret);
        return ret;
    }

    /**
     * ipc_server_send_notify_binary 每 Send a binary notification from the server to a specific client.
     * @param handle             Server handle.
     * @param client_resp_queue  The client's response queue name (obtained from ipc_client_get_resp_queue_name).
     * @param func_name          Notification name (must be registered on the client side).
     * @param data               Payload pointer (may be NULL if size=0).
     * @param size               Payload size.
     * @return IPC_OK on success, error code otherwise.
     *
     * This function creates a shared memory segment for the payload if size>0,
     * then sends a control message over the main queue to the client's response
     * queue with the shared memory name. The client's notify handler will read
     * the data from shared memory.
     *
     * Called by application code to push asynchronous notifications to a specific client.
     */
    int ipc_server_send_notify_binary(ipc_server_handle_t handle,
        const char* client_resp_queue,
        const char* func_name, const void* data, size_t size) {
        if (!client_resp_queue || !func_name) {
            ZIPC_LOG("ipc_server_send_notify_binary: invalid parameters");
            return IPC_ERR_INVAL;
        }
        if (data == nullptr && size != 0) {
            ZIPC_LOG("ipc_server_send_notify_binary: data null but size non-zero");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_server_send_notify_binary: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto server = get_server(handle);
        if (!server) {
            ZIPC_LOG("ipc_server_send_notify_binary: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = server->send_notify_binary(client_resp_queue, func_name, data, size);
        ZIPC_LOG("ipc_server_send_notify_binary: handle=" << handle << ", queue='" << client_resp_queue
            << "', func='" << func_name << "', size=" << size << ", ret=" << ret);
        return ret;
    }

    /* -------------------- Client API -------------------- */

    /**
     * ipc_client_create 每 Create a new client instance.
     * @return Client handle (non-zero) on success, 0 on failure.
     *
     * Allocates an IpcClient object and stores it in g_clients.
     * The client is not yet connected; use ipc_client_connect to attach to a server.
     */
    ipc_client_handle_t ipc_client_create(void) {
        auto client = std::make_shared<IpcClient>();
        std::lock_guard<std::mutex> lock(g_mutex);
        int handle = allocate_client_handle();
        g_clients[handle] = client;
        ZIPC_LOG("ipc_client_create: handle=" << handle);
        return handle;
    }

    /**
     * ipc_client_destroy 每 Destroy a client instance.
     * @param handle Client handle.
     * @return IPC_OK on success, IPC_ERR_INVAL if invalid.
     *
     * Disconnects the client (if connected) and removes it from g_clients.
     */
    int ipc_client_destroy(ipc_client_handle_t handle) {
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_destroy: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        std::shared_ptr<IpcClient> client;
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            auto it = g_clients.find(handle);
            if (it == g_clients.end()) {
                ZIPC_LOG("ipc_client_destroy: handle " << handle << " not found");
                return IPC_ERR_INVAL;
            }
            client = it->second;
            g_clients.erase(it);
        }
        client->disconnect();
        ZIPC_LOG("ipc_client_destroy: handle=" << handle);
        return IPC_OK;
    }

    /**
     * ipc_client_connect 每 Connect the client to a server's main queue.
     * @param handle      Client handle.
     * @param queue_name  Name of the server's main queue.
     * @return IPC_OK on success, IPC_ERR_OPEN if the queue cannot be opened or handshake fails.
     *
     * The client will open the named queue and create its own private response queue.
     * It does NOT perform the RPC handshake here; that is done separately in the
     * higher-level wrapper (Z.Net.Client.IPC). This function only establishes the
     * underlying message queue connections.
     */
    int ipc_client_connect(ipc_client_handle_t handle, const char* queue_name) {
        if (!queue_name) {
            ZIPC_LOG("ipc_client_connect: queue_name is null");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_connect: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_connect: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        bool ok = client->connect(queue_name);
        ZIPC_LOG("ipc_client_connect: handle=" << handle << ", queue='" << queue_name
            << "', result=" << (ok ? "OK" : "FAIL"));
        return ok ? IPC_OK : IPC_ERR_OPEN;
    }

    /**
     * ipc_client_disconnect 每 Disconnect the client from the server.
     * @param handle Client handle.
     * @return IPC_OK on success, IPC_ERR_INVAL if invalid.
     *
     * Closes the response queue and stops the receiver thread.
     */
    int ipc_client_disconnect(ipc_client_handle_t handle) {
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_disconnect: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_disconnect: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        client->disconnect();
        ZIPC_LOG("ipc_client_disconnect: handle=" << handle);
        return IPC_OK;
    }

    /**
     * ipc_client_get_resp_queue_name 每 Get the name of this client's response queue.
     * @param handle Client handle.
     * @return Pointer to a null-terminated string, or NULL if not connected or invalid.
     *
     * This name is used by the server to send notifications to this client.
     * The returned string is owned by the IpcClient object and remains valid until
     * the client is disconnected or destroyed.
     */
    const char* ipc_client_get_resp_queue_name(ipc_client_handle_t handle) {
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_get_resp_queue_name: invalid handle " << handle);
            return nullptr;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_get_resp_queue_name: handle " << handle << " not found");
            return nullptr;
        }
        const char* name = client->get_resp_queue_name();
        ZIPC_LOG("ipc_client_get_resp_queue_name: handle=" << handle << ", name='" << (name ? name : "null") << "'");
        return name;
    }

    /**
     * ipc_client_register_binary_notify 每 Register a handler for server↙client notifications.
     * @param handle   Client handle.
     * @param name     Notification name (must match the name used by server's send function).
     * @param handler  Callback of type ipc_binary_notify_handler.
     * @param trigger  User pointer.
     * @return IPC_OK on success, IPC_ERR_BUSY if already registered.
     *
     * When the server sends a notification with this name, the handler will be
     * invoked in the client's receiver thread.
     */
    int ipc_client_register_binary_notify(ipc_client_handle_t handle, const char* name,
        ipc_binary_notify_handler handler, void* trigger) {
        if (!name || !handler) {
            ZIPC_LOG("ipc_client_register_binary_notify: invalid name or handler");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_register_binary_notify: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_register_binary_notify: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = client->register_binary_notify(name, handler, trigger);
        ZIPC_LOG("ipc_client_register_binary_notify: handle=" << handle << ", name='" << name
            << "', ret=" << ret);
        return ret;
    }

    /**
     * ipc_client_unregister_binary_notify 每 Unregister a notification handler.
     */
    int ipc_client_unregister_binary_notify(ipc_client_handle_t handle, const char* name) {
        if (!name) {
            ZIPC_LOG("ipc_client_unregister_binary_notify: name is null");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_unregister_binary_notify: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_unregister_binary_notify: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = client->unregister_binary_notify(name);
        ZIPC_LOG("ipc_client_unregister_binary_notify: handle=" << handle << ", name='" << name
            << "', ret=" << ret);
        return ret;
    }

    /**
     * ipc_client_call_binary 每 Perform a synchronous binary RPC call.
     * @param handle      Client handle.
     * @param func_name   Function name on the server.
     * @param send_data   Request payload (may be NULL if send_size=0).
     * @param send_size   Payload size.
     * @param out_data    Output pointer that will receive the reply buffer (allocated with ipc_alloc).
     * @param out_size    Output size of the reply.
     * @return IPC_OK on success, error code otherwise (e.g., IPC_ERR_TIMEOUT).
     *
     * Flow:
     * 1. Validate parameters.
     * 2. If send_size>0, create a shared memory segment and copy data.
     * 3. Format a REQ control message with a unique request ID and send it via the main queue.
     * 4. Wait for the response using a std::future with timeout.
     * 5. On response, read the reply from the shared memory named in the RSP message.
     * 6. Return the allocated buffer to the caller (must be freed with ipc_free).
     *
     * Called by application code to execute remote procedures.
     */
    int ipc_client_call_binary(ipc_client_handle_t handle,
        const char* func_name, const void* send_data, size_t send_size,
        void** out_data, size_t* out_size) {
        if (!func_name || !out_data || !out_size) {
            ZIPC_LOG("ipc_client_call_binary: invalid parameters");
            return IPC_ERR_INVAL;
        }
        if (send_data == nullptr && send_size != 0) {
            ZIPC_LOG("ipc_client_call_binary: send_data null but size non-zero");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_call_binary: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        *out_data = nullptr;
        *out_size = 0;

        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_call_binary: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        void* data = nullptr;
        size_t sz = 0;
        int ret = client->call_binary(func_name, send_data, send_size, &data, &sz);
        if (ret == IPC_OK) {
            *out_data = data;
            *out_size = sz;
        }
        ZIPC_LOG("ipc_client_call_binary: handle=" << handle << ", func='" << func_name
            << "', send_size=" << send_size << ", ret=" << ret << ", reply_size=" << sz);
        return ret;
    }

    /**
     * ipc_client_notify_binary 每 Send a binary notification (fire-and-forget) to the server.
     * @param handle      Client handle.
     * @param func_name   Notification name (must be registered on the server).
     * @param send_data   Payload pointer.
     * @param send_size   Payload size.
     * @return IPC_OK on success, error otherwise.
     *
     * Similar to RPC but does not wait for a reply. The server will invoke the
     * corresponding notify handler if registered.
     */
    int ipc_client_notify_binary(ipc_client_handle_t handle,
        const char* func_name, const void* send_data, size_t send_size) {
        if (!func_name) {
            ZIPC_LOG("ipc_client_notify_binary: func_name is null");
            return IPC_ERR_INVAL;
        }
        if (send_data == nullptr && send_size != 0) {
            ZIPC_LOG("ipc_client_notify_binary: send_data null but size non-zero");
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_notify_binary: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_notify_binary: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        int ret = client->notify_binary(func_name, send_data, send_size);
        ZIPC_LOG("ipc_client_notify_binary: handle=" << handle << ", func='" << func_name
            << "', size=" << send_size << ", ret=" << ret);
        return ret;
    }

    /**
     * ipc_client_set_timeout 每 Set the RPC timeout for this client.
     * @param handle        Client handle.
     * @param milliseconds  Timeout in milliseconds (>0).
     * @return IPC_OK on success, IPC_ERR_INVAL if invalid.
     *
     * This timeout affects all subsequent call_binary calls.
     */
    int ipc_client_set_timeout(ipc_client_handle_t handle, int milliseconds) {
        if (milliseconds <= 0) {
            ZIPC_LOG("ipc_client_set_timeout: invalid milliseconds " << milliseconds);
            return IPC_ERR_INVAL;
        }
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_set_timeout: invalid handle " << handle);
            return IPC_ERR_INVAL;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_set_timeout: handle " << handle << " not found");
            return IPC_ERR_INVAL;
        }
        client->set_timeout(milliseconds);
        ZIPC_LOG("ipc_client_set_timeout: handle=" << handle << ", timeout=" << milliseconds << "ms");
        return IPC_OK;
    }

    /**
     * ipc_client_is_connected 每 Check if the client is connected.
     * @param handle Client handle.
     * @return 1 if connected, 0 otherwise.
     */
    int ipc_client_is_connected(ipc_client_handle_t handle) {
        if (handle <= 0) {
            ZIPC_LOG("ipc_client_is_connected: invalid handle " << handle);
            return 0;
        }
        auto client = get_client(handle);
        if (!client) {
            ZIPC_LOG("ipc_client_is_connected: handle " << handle << " not found");
            return 0;
        }
        bool result = client->is_connected();
        ZIPC_LOG("ipc_client_is_connected: handle=" << handle << ", result=" << (result ? "1" : "0"));
        return result ? 1 : 0;
    }

    /* -------------------- Memory management -------------------- */

    /**
     * ipc_alloc 每 Allocate memory that can be passed across the API.
     * @param size Number of bytes.
     * @return Pointer to allocated memory, or NULL on failure.
     *
     * This is a wrapper around malloc(). It is used by reply handlers to allocate
     * the reply buffer, which will be freed by the caller via ipc_free.
     * The allocation is done in the C runtime, making it safe to free from any
     * language that uses the same CRT (or via ipc_free).
     */
    void* ipc_alloc(size_t size) {
        void* p = malloc(size);
        ZIPC_LOG("ipc_alloc: size=" << size << " -> " << p);
        return p;
    }

    /**
     * ipc_free 每 Free memory allocated by ipc_alloc.
     * @param ptr Pointer to free.
     */
    void ipc_free(void* ptr) {
        ZIPC_LOG("ipc_free: ptr=" << ptr);
        if (ptr) free(ptr);
    }

    /* -------------------- Utilities -------------------- */

    /**
     * ipc_Set_Status_handler 每 Install or remove a status/log callback.
     * @param handle  Function pointer (status_handler) or 0 to remove.
     *
     * If handler != 0, installs a custom streambuf that redirects std::cout and
     * std::cerr to the given callback. The callback receives individual characters
     * (int). This allows the library's internal logging to be captured by the host
     * application (e.g., Delphi's DoStatus).
     *
     * If handler == 0, removes the current handler and restores the original streambufs.
     *
     * Can be called multiple times; each call replaces the previous handler.
     */
    void ipc_Set_Status_handler(status_handler handle) {
        status_call_Handler = handle;
        if (handle != 0) {
            ZIPC_LOG("ipc_Set_Status_handler: handler installed");
        }
        else {
            ZIPC_LOG("ipc_Set_Status_handler: handler removed");
        }
    }

    /**
     * ipc_cleanup 每 Remove a named queue from the system.
     * @param queue_name  Name of the queue to remove.
     *
     * This is a convenience function that calls Boost's message_queue::remove.
     * It can be used to manually clean up a queue that may have been left over
     * from a previous run. Typically called before creating a new server.
     */
    void ipc_cleanup(const char* queue_name) {
        if (queue_name) {
            ipc::message_queue::remove(queue_name);
            ZIPC_LOG("ipc_cleanup: removed queue '" << queue_name << "'");
        }
        else {
            ZIPC_LOG("ipc_cleanup: queue_name is null, doing nothing");
        }
    }

    /**
     * ipc_shutdown 每 Shut down the library and release all global resources.
     *
     * This function should be called when the library is no longer needed.
     * It stops all servers and clients, clears the global maps, and restores
     * the standard output streams.
     *
     * It uses g_shutdown_called to prevent multiple invocations.
     */
    void ipc_shutdown() {
        if (g_shutdown_called.exchange(true)) {
            ZIPC_LOG("ipc_shutdown: already called, skipping");
            return;
        }
        // Collect all server and client objects.
        std::vector<std::shared_ptr<IpcServer>> servers;
        std::vector<std::shared_ptr<IpcClient>> clients;
        {
            std::lock_guard<std::mutex> lock(g_mutex);
            for (auto& kv : g_servers) servers.push_back(kv.second);
            for (auto& kv : g_clients) clients.push_back(kv.second);
            g_servers.clear();
            g_clients.clear();
        }
        // Stop all servers and disconnect all clients.
        for (auto& s : servers) s->stop();
        for (auto& c : clients) c->disconnect();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // No need to restore global cout/cerr 每 we never touched them.
        status_call_Handler = 0;

        ZIPC_LOG("ipc_shutdown: completed");
    }

} // extern "C"