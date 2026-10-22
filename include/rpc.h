#ifndef RPC_H
#define RPC_H

#include "protocol.h"

/* 状态码 */
#define RPC_OK           0
#define RPC_ERR_METHOD   1

/* RPC 默认接收超时（毫秒）：对方进程卡死/假死时 recv 最多等这么久，避免永久阻塞 */
#define RPC_TIMEOUT_MS   1000

/* 方法处理函数：输入 args，输出 result（malloc 分配） */
typedef int (*rpc_handler_t)(const char *args, char **result);

/* 方法表项 */
typedef struct {
    const char    *name;
    rpc_handler_t  handler;
} rpc_method_t;

/* 注册一个方法 */
void rpc_register(const char *name, rpc_handler_t handler);

/* 分发请求，生成响应 */
void rpc_dispatch(const message_t *req, message_t *resp);

/* 注册所有内置方法 */
void register_all_handlers(void);

/* 客户端接口*/
int rpc_call(int fd, const char *method, const char *args, char **result);

/* 客户端接口（自定义接收超时，毫秒）：辅助类调用（心跳/补拉）用短超时，
 * 避免单线程事件循环里双向同步 RPC 互相等待造成死锁 */
int rpc_call_timeout(int fd, const char *method, const char *args,
                     char **result, int timeout_ms);

#endif