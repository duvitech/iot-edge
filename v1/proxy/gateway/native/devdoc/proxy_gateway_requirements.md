Module Requirements
===================

## Overview

This is the documentation for a remote module that communicates with other
modules through the Azure IoT Gateway. Remote modules need to implement the
well defined Module Interface. Each remote module will additionally
need to attach and detach from the gateway and will either need to manage
the worker thread manually or use the convenience methods supplied to start
and halt the worker thread.

## References

[Azure IoT Gateway Module Interface](../../../../core/devdoc/module.md)

![High Level Design Diagrams](./media/proxy_gateway_hld.PNG)

## Exposed Types

```c
typedef struct REMOTE_MODULE_TAG * REMOTE_MODULE_HANDLE;
```

## Exposed API

### ProxyGateway_Attach

`ProxyGateway_Attach` will provide the ProxyGateway library with the caller's
"Module API" and the connection id required to communicate with the Azure
IoT Gateway. When `ProxyGateway_Attach` is called the ProxyGateway library will
begin listening for the gateway create message. Once the create message is
received the ProxyGateway library will call `Module_ParseConfigurationFromJson`^,
`Module_Create` and `Module_FreeConfiguration`^.
*^ Assuming the optional API has been provided.*

```c
extern GATEWAY_EXPORT
REMOTE_MODULE_HANDLE
ProxyGateway_Attach (
    const MODULE_API * module_apis,
    const char * const connection_id
);
```

**SRS_PROXY_GATEWAY_027_000: [** *Prerequisite Check* - If the `module_apis` parameter is `NULL`, then `ProxyGateway_Attach` shall do nothing and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_001: [** *Prerequisite Check* - If the `module_apis` version is beyond `MODULE_API_VERSION_1`, then `ProxyGateway_Attach` shall do nothing and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_002: [** *Prerequisite Check* - If the `module_apis` interface fails to provide `Module_Create`, then `ProxyGateway_Attach` shall do nothing and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_003: [** *Prerequisite Check* - If the `module_apis` interface fails to provide `Module_Destroy`, then `ProxyGateway_Attach` shall do nothing and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_004: [** *Prerequisite Check* - If the `module_apis` interface fails to provide `Module_Receive`, then `ProxyGateway_Attach` shall do nothing and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_005: [** *Prerequisite Check* - If the `connection_id` parameter is `NULL`, then `ProxyGateway_Attach` shall do nothing and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_006: [** *Prerequisite Check* - If the `connection_id` parameter is longer than `GATEWAY_CONNECTION_ID_MAX`, then `ProxyGateway_Attach` shall do nothing and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_007: [** `ProxyGateway_Attach` shall allocate the memory required to support its instance data **]**  
**SRS_PROXY_GATEWAY_027_008: [** If memory allocation fails for the instance data, then `ProxyGateway_Attach` shall return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_009: [** `ProxyGateway_Attach` shall allocate the memory required to formulate the connection string to the Azure IoT Gateway **]**  
**SRS_PROXY_GATEWAY_027_010: [** If memory allocation fails for the connection string, then `ProxyGateway_Attach` shall free any previously allocated memory and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_011: [** `ProxyGateway_Attach` shall create a socket for the Azure IoT Gateway command channel by calling `int nn_socket(int domain, int protocol)` with `AF_SP` as the `domain` and `NN_REP` as the `protocol` **]**  
**SRS_PROXY_GATEWAY_027_012: [** If unable to create a socket to the command channel, then `ProxyGateway_Attach` shall free any previously allocated memory and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_013: [** `ProxyGateway_Attach` shall connect to the Azure IoT Gateway command channel by calling `int nn_connect(int s, const char * addr)` with the newly created socket as `s` and the newly formulated connection string as `addr` **]**  
**SRS_PROXY_GATEWAY_027_014: [** If the call to `nn_bind` returns a negative value, then `ProxyGateway_Attach` shall close the socket, free any previously allocated memory and return `NULL` **]**  
**SRS_PROXY_GATEWAY_027_015: [** `ProxyGateway_Attach` shall release the memory required to formulate the connection string **]**  
**SRS_PROXY_GATEWAY_027_016: [** If no errors are encountered, then `ProxyGateway_Attach` shall return a handle to a remote module instance **]**  


### ProxyGateway_Detach

`ProxyGateway_Detach` will cease to interact with the Azure IoT Gateway and
clean up all resources required to maintain the connection with the gateway.

```c
extern GATEWAY_EXPORT
void
ProxyGateway_Detach (
    REMOTE_MODULE_HANDLE remote_module
);
```

**SRS_PROXY_GATEWAY_027_058: [** *Prerequisite Check* - If the `remote_module` parameter is `NULL`, then `ProxyGateway_Detach` shall do nothing **]**  
**SRS_PROXY_GATEWAY_027_059: [** If the worker thread is active, then `ProxyGateway_Detach` shall attempt to halt the worker thread **]**  
**SRS_PROXY_GATEWAY_027_060: [** If unable to halt the worker thread, `ProxyGateway_Detach` shall forcibly free the memory allocated to the worker thread **]**  
**SRS_PROXY_GATEWAY_027_061: [** `ProxyGateway_Detach` shall attempt to notify the Azure IoT Gateway of the detachment **]**  
**SRS_PROXY_GATEWAY_027_062: [** `ProxyGateway_Detach` shall disconnect from the Azure IoT Gateway message channels **]**  
**SRS_PROXY_GATEWAY_027_063: [** `ProxyGateway_Detach` shall shutdown the Azure IoT Gateway control channel by calling `int nn_shutdown(int s, int how)` **]**  
**SRS_PROXY_GATEWAY_027_064: [** `ProxyGateway_Detach` shall close the Azure IoT Gateway control socket by calling `int nn_close(int s)` **]**  
**SRS_PROXY_GATEWAY_027_065: [** `ProxyGateway_Detach` shall free the remaining memory dedicated to its instance data **]**  


### ProxyGateway_DoWork

`ProxyGateway_DoWork` is intended to provide the caller with fine-grain control of work
scheduling, and is provided as an alternative to calling `ProxyGateway_StartWorkerThread`.
`ProxyGateway_DoWork` is a non-blocking call, wherein each call will check for one message
on the command and each message channel connected to the Azure IoT Gateway. In other
words, if multiple messages are queued on a single channel, only the first message of
each channel will be serviced. If the message is intended for the remote module (as
opposed to the ProxyGateway library itself), the ProxyGateway library will pass it along
by calling `Module_Receive` on the remote module.

*NOTE: If `ProxyGateway_StartWorkerThread` has been called, then calling `ProxyGateway_DoWork`
will have no observable effect.*

```c
extern GATEWAY_EXPORT
void
ProxyGateway_DoWork (
    REMOTE_MODULE_HANDLE remote_module
);
```

**SRS_PROXY_GATEWAY_027_026: [** *Prerequisite Check* - If the `remote_module` parameter is `NULL`, then `ProxyGateway_DoWork` shall do nothing] */
**SRS_PROXY_GATEWAY_027_027: [** *Control Channel* - `ProxyGateway_DoWork` shall poll the gateway control channel by calling `int nn_recv(int s, void * buf, size_t len, int flags)` with the control socket for `s`, `NULL` for `buf`, `NN_MSG` for `len` and NN_DONTWAIT for `flags` **]**  
**SRS_PROXY_GATEWAY_027_028: [** *Control Channel* - If no message is available, then `ProxyGateway_DoWork` shall abandon the control channel request **]**  
**SRS_PROXY_GATEWAY_027_066: [** *Control Channel* - If an error occurred when polling the gateway, then `ProxyGateway_DoWork` shall signal the gateway abandon the control channel request **]**  
**SRS_PROXY_GATEWAY_027_029: [** *Control Channel* - If a control message was received, then `ProxyGateway_DoWork` will parse that message by calling `CONTROL_MESSAGE * ControlMessage_CreateFromByteArray(const unsigned char * source, size_t size)` with the buffer received from `nn_recv` as `source` and return value from `nn_recv` as `size` **]**  
**SRS_PROXY_GATEWAY_027_030: [** *Control Channel* - If unable to parse the control message, then `ProxyGateway_DoWork` shall signal the gateway, free any previously allocated memory and abandon the control channel request **]**  
**SRS_PROXY_GATEWAY_027_031: [** *Control Channel* - If the message type is CONTROL_MESSAGE_TYPE_MODULE_CREATE, then `ProxyGateway_DoWork` shall process the create message **]**  
**SRS_PROXY_GATEWAY_027_032: [** *Control Channel* - If the message type is CONTROL_MESSAGE_TYPE_MODULE_START and `Module_Start` was provided, then `ProxyGateway_DoWork` shall call `void Module_Start(MODULE_HANDLE moduleHandle)` **]**  
**SRS_PROXY_GATEWAY_027_033: [** *Control Channel* - If the message type is CONTROL_MESSAGE_TYPE_MODULE_DESTROY, then `ProxyGateway_DoWork` shall call `void Module_Destroy(MODULE_HANDLE moduleHandle)` **]**  
**SRS_PROXY_GATEWAY_027_034: [** *Control Channel* - If the message type is CONTROL_MESSAGE_TYPE_MODULE_DESTROY, then `ProxyGateway_DoWork` shall disconnect from the message channel **]**  
**SRS_PROXY_GATEWAY_027_035: [** *Control Channel* - `ProxyGateway_DoWork` shall free the resources held by the parsed control message by calling `void ControlMessage_Destroy(CONTROL_MESSAGE * message)` using the parsed control message as `message` **]**  
**SRS_PROXY_GATEWAY_027_036: [** *Control Channel* - `ProxyGateway_DoWork` shall free the resources held by the gateway message by calling `int nn_freemsg(void * msg)` with the resulting buffer from the previous call to `nn_recv` **]**  
**SRS_PROXY_GATEWAY_027_037: [** *Message Channel* - `ProxyGateway_DoWork` shall not check for messages, if the message socket is not available **]**  
**SRS_PROXY_GATEWAY_027_038: [** *Message Channel* - `ProxyGateway_DoWork` shall poll each gateway message channel by calling `int nn_recv(int s, void * buf, size_t len, int flags)` with each message socket for `s`, `NULL` for `buf`, `NN_MSG` for `len` and NN_DONTWAIT for `flags` **]**  
**SRS_PROXY_GATEWAY_027_039: [** *Message Channel* - If no message is available or an error occurred, then `ProxyGateway_DoWork` shall abandon the message channel request **]**  
**SRS_PROXY_GATEWAY_027_040: [** *Message Channel* - If a module message was received, then `ProxyGateway_DoWork` will parse that message by calling `MESSAGE_HANDLE Message_CreateFromByteArray(const unsigned char * source, int32_t size)` with the buffer received from `nn_recv` as `source` and return value from `nn_recv` as `size` **]**  
**SRS_PROXY_GATEWAY_027_041: [** *Message Channel* - If unable to parse the module message, then `ProxyGateway_DoWork` shall free any previously allocated memory and abandon the message channel request **]**  
**SRS_PROXY_GATEWAY_027_042: [** *Message Channel* - `ProxyGateway_DoWork` shall pass the structured message to the module by calling `void Module_Receive(MODULE_HANDLE moduleHandle)` using the parsed message as `moduleHandle` **]**  
**SRS_PROXY_GATEWAY_027_043: [** *Message Channel* - `ProxyGateway_DoWork` shall free the resources held by the parsed module message by calling `void Message_Destroy(MESSAGE_HANDLE * message)` using the parsed module message as `message` **]**  
**SRS_PROXY_GATEWAY_027_044: [** *Message Channel* - `ProxyGateway_DoWork` shall free the resources held by the gateway message by calling `int nn_freemsg(void * msg)` with the resulting buffer from the previous call to `nn_recv` **]**  


### ProxyGateway_HaltWorkerThread

`ProxyGateway_HaltWorkerThread` will signal and join the message thread. Once this
method has been invoked, no more messages will be received from the Azure IoT
Gateway without manually calling `ProxyGateway_DoWork`.

```c
extern GATEWAY_EXPORT
int
ProxyGateway_HaltWorkerThread (
    REMOTE_MODULE_HANDLE remote_module
);
```

**SRS_PROXY_GATEWAY_027_045: [** *Prerequisite Check* - If the `remote_module` parameter is `NULL`, then `ProxyGateway_HaltWorkerThread` shall return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_046: [** *Prerequisite Check* - If a worker thread does not exist, then `ProxyGateway_HaltWorkerThread` shall return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_047: [** `ProxyGateway_HaltWorkerThread` shall obtain the thread mutex in order to signal the thread by calling `LOCK_RESULT Lock(LOCK_HANDLE handle)` **]**  
**SRS_PROXY_GATEWAY_027_048: [** If unable to obtain the mutex, then `ProxyGateway_HaltWorkerThread` shall return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_049: [** `ProxyGateway_HaltWorkerThread` shall release the thread mutex upon signalling by calling `LOCK_RESULT Unlock(LOCK_HANDLE handle)` **]**  
**SRS_PROXY_GATEWAY_027_050: [** If unable to release the mutex, then `ProxyGateway_HaltWorkerThread` shall return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_051: [** `ProxyGateway_HaltWorkerThread` shall halt the thread by calling `THREADAPI_RESULT ThreadAPI_Join(THREAD_HANDLE handle, int * res)` **]**  
**SRS_PROXY_GATEWAY_027_052: [** If unable to join the thread, then `ProxyGateway_HaltWorkerThread` shall return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_053: [** `ProxyGateway_HaltWorkerThread` shall free the thread mutex by calling `LOCK_RESULT Lock_Deinit(LOCK_HANDLE handle)` **]**  
**SRS_PROXY_GATEWAY_027_054: [** If unable to free the thread mutex, then `ProxyGateway_HaltWorkerThread` shall ignore the result and continue processing **]**  
**SRS_PROXY_GATEWAY_027_055: [** `ProxyGateway_HaltWorkerThread` shall free the memory allocated to the thread details **]**  
**SRS_PROXY_GATEWAY_027_056: [** If an error is returned from the worker thread, then `ProxyGateway_HaltWorkerThread` shall return the worker thread's error code **]**  
**SRS_PROXY_GATEWAY_027_057: [** If no errors are encountered, then `ProxyGateway_HaltWorkerThread` shall return zero **]**  


### ProxyGateway_StartWorkerThread

`ProxyGateway_StartWorkerThread` is a convenience method which gives control of calling
`ProxyGateway_DoWork` over to the ProxyGateway library. If `ProxyGateway_StartWorkerThread`
has been invoked, then the ProxyGateway library will create a thread to service and deliver
messages from the Azure IoT Gateway to the remote module.

```c
extern GATEWAY_EXPORT
int
ProxyGateway_StartWorkerThread (
    REMOTE_MODULE_HANDLE remote_module
);
```

**SRS_PROXY_GATEWAY_027_017: [** *Prerequisite Check* - If the `remote_module` parameter is `NULL`, then `ProxyGateway_StartWorkerThread` shall return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_018: [** *Prerequisite Check* - If a worker thread already exist for the given handle, then `ProxyGateway_StartWorkerThread` shall do nothing and return zero **]**  
**SRS_PROXY_GATEWAY_027_019: [** `ProxyGateway_StartWorkerThread` shall allocate the memory required to support the worker thread **]**  
**SRS_PROXY_GATEWAY_027_020: [** If memory allocation fails for the worker thread data, then `ProxyGateway_StartWorkerThread` shall return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_021: [** `ProxyGateway_StartWorkerThread` shall create a mutex by calling `LOCK_HANDLE Lock_Init(void)` **]**  
**SRS_PROXY_GATEWAY_027_022: [** If a mutex is unable to be created, then `ProxyGateway_StartWorkerThread` shall free any previously allocated memory and return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_023: [** `ProxyGateway_StartWorkerThread` shall start a worker thread by calling `THREADAPI_RESULT ThreadAPI_Create(&THREAD_HANDLE threadHandle, THREAD_START_FUNC func, void * arg)` with an empty thread handle for `threadHandle`, a function that loops polling the messages for `func`, and `remote_module` for `arg` **]**  
**SRS_PROXY_GATEWAY_027_024: [** If the worker thread failed to start, then `ProxyGateway_StartWorkerThread` shall free any previously allocated memory and return a non-zero value **]**  
**SRS_PROXY_GATEWAY_027_025: [** If no errors are encountered, then `ProxyGateway_StartWorkerThread` shall return zero **]**  

