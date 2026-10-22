#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "rpc.h"

#define SERVER_IP "127.0.0.1"

/* 集群节点端口列表（与 cluster.c 一致） */
static const int g_node_ports[] = {8001, 8002, 8003};
static const int g_node_count  = 3;

static int connect_to(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

/* 连接当前端口；失败则轮询节点列表里的其他端口（故障切换）
 * cur_index 记录当前尝试到哪个端口，成功后停在已连接的端口 */
static int connect_with_failover(int *cur_index) {
    for (int tries = 0; tries < g_node_count; tries++) {
        int port = g_node_ports[*cur_index];
        int fd = connect_to(SERVER_IP, port);
        if (fd >= 0) {
            printf("已连接服务端 %s:%d\n\n", SERVER_IP, port);
            return fd;
        }
        printf("[故障切换] 节点 %d 不可用，尝试下一个节点...\n", port);
        *cur_index = (*cur_index + 1) % g_node_count;
    }
    return -1;
}

/* 安全调用：业务错误（status>=0）原样返回；
 * 网络错误（<0）说明节点挂了 → 自动重连下一个节点，重试一次 */
static int safe_call(int *fd, int *cur_index,
                     const char *method, const char *args, char **result) {
    int st = rpc_call(*fd, method, args, result);
    if (st >= 0) return st;

    printf("[故障切换] 节点 %d 连接失效（%s），自动重连...\n",
           g_node_ports[*cur_index], method);
    close(*fd);
    *cur_index = (*cur_index + 1) % g_node_count;
    *fd = connect_with_failover(cur_index);
    if (*fd < 0) {
        fprintf(stderr, "所有节点均不可用，退出\n");
        return -1;
    }
    return rpc_call(*fd, method, args, result);
}

int main(int argc, char *argv[]) {
    int cur_index = 0;              /* 默认从 8001 开始 */
    if (argc >= 2) {
        /* 用户指定起始端口：找到它在列表里的位置 */
        int p = atoi(argv[1]);
        for (int i = 0; i < g_node_count; i++) {
            if (g_node_ports[i] == p) { cur_index = i; break; }
        }
    }

    int fd = connect_with_failover(&cur_index);
    if (fd < 0) {
        fprintf(stderr, "无法连接集群（8001/8002/8003 均不可用）\n");
        return 1;
    }

    char *result = NULL;
    int st;

    /* 1. 调用 echo */
    result = NULL;
    st = safe_call(&fd, &cur_index, "echo", "hello rpc", &result);
    printf("echo: status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    /* 2. 调用 add */
    result = NULL;
    st = safe_call(&fd, &cur_index, "add", "3 5", &result);
    printf("add:  status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    /* 3. 调用 add 非法参数 */
    result = NULL;
    st = safe_call(&fd, &cur_index, "add", "abc", &result);
    printf("add(非法): status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    /* 4. 调用不存在的方法 */
    result = NULL;
    st = safe_call(&fd, &cur_index, "unknown", "", &result);
    printf("unknown: status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    /* 5. 测试 route：看不同 key 落到哪个节点 */
    const char *route_keys[] = {"name", "foo", "user", "test", "hello"};
    for (int i = 0; i < 5; i++) {
        result = NULL;
        st = safe_call(&fd, &cur_index, "route", route_keys[i], &result);
        printf("route %s: %s\n", route_keys[i], result ? result : "(null)");
        free(result);
    }

    result = NULL;
    st = safe_call(&fd, &cur_index, "kv_put", "name alice", &result);
    printf("kv_put: status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    result = NULL;
    st = safe_call(&fd, &cur_index, "kv_get", "name", &result);
    printf("kv_get: status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    result = NULL;
    st = safe_call(&fd, &cur_index, "kv_del", "name", &result);
    printf("kv_del: status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    result = NULL;
    st = safe_call(&fd, &cur_index, "kv_get", "name", &result);
    printf("kv_get after del: status=%d, result=%s\n", st, result ? result : "(null)");
    free(result);

    close(fd);
    return 0;
}
