/**
 * @file z_ipc_api.h
 * @brief C API for z_ipc ¨C cross process RPC + notification library (binary only).
 *
 * This header defines all error codes, handle types, callback signatures,
 * and functions for creating servers and clients, registering handlers,
 * making RPC calls, and sending notifications.
 *
 * @warning User callbacks (ipc_binary_reply_handler, ipc_binary_notify_handler)
 *          MUST return quickly (preferably < 100 ms) and MUST NOT block.
 *          Blocking callbacks can prevent the library from shutting down
 *          gracefully and may cause resource leaks.
 */

#ifndef Z_IPC_API_H
#define Z_IPC_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

#pragma pack(push)
#pragma pack(1)

    /* ========================== Error Codes ========================== */

#define IPC_OK               0   /**< Success */
#define IPC_ERR_OPEN        -1   /**< Cannot open queue */
#define IPC_ERR_SIZE        -2   /**< Invalid size */
#define IPC_ERR_SEND        -3   /**< Send failed */
#define IPC_ERR_RECEIVE     -4   /**< Receive failed */
#define IPC_ERR_MEMORY      -5   /**< Memory allocation error */
#define IPC_ERR_PERMISSION  -6   /**< Permission denied */
#define IPC_ERR_TIMEOUT     -7   /**< Operation timed out */
#define IPC_ERR_TYPE        -8   /**< Type mismatch */
#define IPC_ERR_NOT_FOUND   -9   /**< Handler or resource not found */
#define IPC_ERR_BUSY        -10  /**< Resource busy (handler already exists) */
#define IPC_ERR_INVAL       -11  /**< Invalid argument */
#define IPC_ERR_RECURSION   -12  /**< Recursive call detected inside callback */
#define IPC_ERR_CANCELED    -13  /**< Operation canceled */
#define IPC_ERR_UNKNOWN     -99  /**< Unknown error */

/* ========================== Callback Types ========================== */

/** Callback for library status/log messages */
    typedef void (*status_handler)(int);

    /**
     * Server-side callback for binary RPC requests.
     * @param trigger    user-supplied pointer
     * @param data       request payload
     * @param size       payload size
     * @param out_reply  output buffer (must be allocated with ipc_alloc)
     * @param out_size   output size
     * @warning Must not block; must return quickly.
     */
    typedef void (*ipc_binary_reply_handler)(void* trigger, const void* data, size_t size,
        void** out_reply, size_t* out_size);

    /**
     * Handler for binary notifications (both client and server sides).
     * @param trigger    user-supplied pointer
     * @param data       notification payload
     * @param size       payload size
     * @warning Must not block; must return quickly.
     */
    typedef void (*ipc_binary_notify_handler)(void* trigger, const void* data, size_t size);

    /* ========================== Handle Types ========================== */

    typedef int ipc_server_handle_t;  /**< Opaque server handle */
    typedef int ipc_client_handle_t;  /**< Opaque client handle */

    /* ========================== Server API ========================== */

    /**
     * Create a server with default configuration.
     * @param queue_name   Name of the main message queue.
     * @param thread_count Number of worker threads (0 = auto).
     * @return Server handle (non-zero) on success, 0 on failure.
     */
    ipc_server_handle_t ipc_server_create(const char* queue_name, int thread_count);

    /**
     * Create a server with explicit parameters.
     * @param queue_name        Name of the main message queue.
     * @param thread_count      Number of worker threads (0 = auto).
     * @param max_queue_length  Maximum number of pending messages in the queue.
     * @param max_msg_size      Maximum size (in bytes) of control messages.
     * @return Server handle (non-zero) on success, 0 on failure.
     */
    ipc_server_handle_t ipc_server_create_ex(const char* queue_name,
        int thread_count, size_t max_queue_length, size_t max_msg_size);

    /** Destroy a server and release resources */
    int ipc_server_destroy(ipc_server_handle_t handle);

    /** Register a binary RPC handler for a function name */
    int ipc_server_register_binary_reply(ipc_server_handle_t handle, const char* name,
        ipc_binary_reply_handler handler, void* trigger);

    /** Unregister a binary RPC handler */
    int ipc_server_unregister_binary_reply(ipc_server_handle_t handle, const char* name);

    /** Register a binary notify handler (client¡úserver notifications) */
    int ipc_server_register_binary_notify(ipc_server_handle_t handle, const char* name,
        ipc_binary_notify_handler handler, void* trigger);

    /** Unregister a binary notify handler */
    int ipc_server_unregister_binary_notify(ipc_server_handle_t handle, const char* name);

    /** Send a binary notification from server to a specific client */
    int ipc_server_send_notify_binary(ipc_server_handle_t handle,
        const char* client_resp_queue,
        const char* func_name, const void* data, size_t size);

    /* ========================== Client API ========================== */

    /** Create a client instance */
    ipc_client_handle_t ipc_client_create(void);

    /** Destroy a client instance */
    int ipc_client_destroy(ipc_client_handle_t handle);

    /** Connect to a server queue */
    int ipc_client_connect(ipc_client_handle_t handle, const char* queue_name);

    /** Disconnect from the server */
    int ipc_client_disconnect(ipc_client_handle_t handle);

    /** Get the name of this client's response queue (for server notifications) */
    const char* ipc_client_get_resp_queue_name(ipc_client_handle_t handle);

    /** Register a binary notify handler (server¡úclient notifications) */
    int ipc_client_register_binary_notify(ipc_client_handle_t handle, const char* name,
        ipc_binary_notify_handler handler, void* trigger);

    /** Unregister a binary notify handler */
    int ipc_client_unregister_binary_notify(ipc_client_handle_t handle, const char* name);

    /** Perform a binary RPC call ¨C sends data and waits for a reply */
    int ipc_client_call_binary(ipc_client_handle_t handle,
        const char* func_name, const void* send_data, size_t send_size,
        void** out_reply, size_t* out_size);

    /** Send a binary notification (client¡úserver) without waiting for reply */
    int ipc_client_notify_binary(ipc_client_handle_t handle,
        const char* func_name, const void* send_data, size_t send_size);

    /** Set the timeout for RPC calls (milliseconds) */
    int ipc_client_set_timeout(ipc_client_handle_t handle, int milliseconds);

    /** Check if the client is connected */
    int ipc_client_is_connected(ipc_client_handle_t handle);

    /* ========================== Memory Management ========================== */

    /** Allocate memory that can be passed across the API (used for reply buffers) */
    void* ipc_alloc(size_t size);

    /** Free memory allocated by ipc_alloc */
    void ipc_free(void* ptr);

    /* ========================== Utilities ========================== */

    /** Install a callback that receives status/log messages */
    void ipc_Set_Status_handler(status_handler handle);

    /** Remove a named queue from the system (cleanup) */
    void ipc_cleanup(const char* queue_name);

    /** Shut down the library and release global resources */
    void ipc_shutdown(void);

#pragma pack(pop)

#ifdef __cplusplus
}
#endif

#endif // Z_IPC_API_H