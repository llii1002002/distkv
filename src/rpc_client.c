#include "rpc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>

/* 请求 ID 计数器 */
static uint32_t g_next_id = 1;

/* 发送带长度前缀的消息，循环发完 */
static int send_message(int fd, const void *payload, uint32_t len) {
    uint32_t len_net = htonl(len);

    const char *p = (const char *)&len_net;
    size_t total = 0;
    while (total < sizeof(len_net)) {
        ssize_t n = send(fd, p + total, sizeof(len_net) - total, 0);
        if (n < 0) { perror("send length"); return -1; }
        total += n;
    }

    p = (const char *)payload;
    total = 0;
    while (total < len) {
        ssize_t n = send(fd, p + total, len - total, 0);
        if (n < 0) { perror("send payload"); return -1; }
        total += n;
    }
    return 0;
}

/* 接收一条完整消息，返回 payload 长度，失败返回 -1 */
static ssize_t recv_message(int fd, char **out) {
    uint32_t len_net;

    /* 收 4 字节长度 */
    size_t got = 0;
    while (got < sizeof(len_net)) {
        ssize_t n = recv(fd, (char *)&len_net + got, sizeof(len_net) - got, 0);
        if (n <= 0) return n;
        got += n;
    }

    uint32_t len = ntohl(len_net);
    if (len == 0 || len > 1024 * 1024) return -1;

    char *buf = malloc(len + 1);
    if (!buf) return -1;

    got = 0;
    while (got < len) {
        ssize_t n = recv(fd, buf + got, len - got, 0);
        if (n <= 0) { free(buf); return n; }
        got += n;
    }
    buf[len] = '\0';
    *out = buf;
    return len;
}

/* 同步调用一个远程方法（默认超时） */
int rpc_call(int fd, const char *method, const char *args, char **result) {
    return rpc_call_timeout(fd, method, args, result, RPC_TIMEOUT_MS);
}

/* 同步调用一个远程方法（自定义超时，毫秒）
 * timeout_ms > 0：SO_RCVTIMEO 限制 recv 等待，超时返回 -1
 * 为什么需要可变超时：单线程事件循环里 handler 做同步 RPC 时，
 * 双方可能互相等（8001 push 等 8002、8002 心跳等 8001）造成死锁。
 * 心跳/补拉这类"辅助调用"用短超时（几百 ms）快速失败，事件循环立即恢复，
 * 就能腾出手处理对方的请求，死锁自然解开 */
int rpc_call_timeout(int fd, const char *method, const char *args,
                     char **result, int timeout_ms) {
    if (!method) return -1;

    /* 0. 设置接收超时 */
    struct timeval tv = {timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /* 1. 构造请求 */
    message_t req = {0};
    req.request_id = g_next_id++;
    req.status     = 0;
    req.method     = (char *)method;
    req.method_len = strlen(method);
    req.args       = (char *)args;
    req.args_len   = args ? strlen(args) : 0;

    /* 2. 序列化 */
    char buf[4096];
    int n = serialize(&req, buf, sizeof(buf));
    if (n < 0) {
        fprintf(stderr, "serialize failed\n");
        return -1;
    }

    /* 3. 发送 */
    if (send_message(fd, buf, n) < 0) {
        fprintf(stderr, "send_message failed\n");
        return -1;
    }

    /* 4. 接收响应 */
    char *resp_buf = NULL;
    ssize_t resp_len = recv_message(fd, &resp_buf);
    if (resp_len <= 0) {
        fprintf(stderr, "recv_message failed\n");
        return -1;
    }

    /* 5. 反序列化 */
    message_t resp = {0};
    if (deserialize(resp_buf, resp_len, &resp) < 0) {
        fprintf(stderr, "deserialize response failed\n");
        free(resp_buf);
        return -1;
    }
    free(resp_buf);

    /* 6. 校验 request_id */
    if (resp.request_id != req.request_id) {
        fprintf(stderr, "request_id mismatch\n");
        message_free(&resp);
        return -1;
    }

    /* 7. 转移 result 所有权 */
    if (result) {
        *result = resp.args;
        resp.args = NULL;
    }
    int status = resp.status;
    message_free(&resp);
    return status;
}