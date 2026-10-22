#include <stdio.h>
#include <stdlib.h>
#include "protocol.h"



 int main(void) {
    /* 1. 构造请求 */
    message_t req = {0};
    req.request_id=1;
    req.status=STATUS_OK;
    req.method="add";
    req.method_len=3;
    req.args="1+2";
    req.args_len=3;

    /* 2. 序列化 */
    char buf[1024];
    int n = serialize(&req, buf, sizeof(buf));
    printf("序列化后 %d 字节\n", n);

    /* 3. 打印字节流 */
    for (int i = 0; i < n; i++) {
        printf("%02x ", (unsigned char)buf[i]);
    }
    printf("\n");

    /* 4. 反序列化 */
    message_t out = {0};
    int consumed = deserialize(buf, n, &out);
    printf("解析消耗 %d 字节\n", consumed);

    /* 5. 验证 */
    printf("request_id  = %u\n",out.request_id);
    printf("status      = %u\n", out.status);
    printf("kmethod     = %s\n", out.method);
    printf("args        = %s\n", out.args);

    /* 6. 释放 */
    message_free(&out);
    return 0;
}
