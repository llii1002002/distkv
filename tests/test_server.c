#include <stdio.h>
#include <stdlib.h>
#include "rpc.h"

int main(void) {
    /* 1. 注册方法 */
    register_all_handlers();

    /* 2. 构造一个 add 请求 */
    message_t req = {0};
    req.request_id = 1;
    req.method = "add";
    req.method_len = 3;
    req.args = "3 5";
    req.args_len = 3;

    /* 3. 分发 */
    message_t resp = {0};
    rpc_dispatch(&req, &resp);

    /* 4. 打印响应 */
    printf("request_id = %u\n", resp.request_id);
    printf("status     = %u\n", resp.status);
    printf("method     = %s\n", resp.method);
    printf("args       = %s\n", resp.args);

    /* 5. 释放 */
    message_free(&resp);

    /* 6. 测试不存在的方法 */
    message_t req2 = {0};
    req2.request_id = 2;
    req2.method = "unknown";
    req2.method_len = 7;
    req2.args = "";
    req2.args_len = 0;

    message_t resp2 = {0};
    rpc_dispatch(&req2, &resp2);
    printf("\nunknown 方法:\n");
    printf("status = %u\n", resp2.status);
    printf("args   = %s\n", resp2.args);
    message_free(&resp2);

    return 0;
}