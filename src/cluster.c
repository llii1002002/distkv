#include "cluster.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "rpc.h"

/* 集群配置：3 个节点 */
#define MAX_NODES 8

typedef struct {
    const char *host;
    int         port;
} node_info_t;

static node_info_t g_nodes[MAX_NODES] = {
    {"127.0.0.1", 8001},
    {"127.0.0.1", 8002},
    {"127.0.0.1", 8003},
};
static int g_node_count = 3;
static int g_my_id = -1;

/* 分片 → 主节点映射（第五步）
 * g_cluster_master[i] = 分片 i 的数据当前由哪个节点服务
 * 初始：每个分片由自己服务（node 0 管分片 0，...）
 * 故障转移后：8002 顶替 8001 → g_cluster_master[0] = 1 */
static int g_cluster_master[MAX_NODES] = {0, 1, 2};

/* 哈希函数：djb2 */
static unsigned int hash_key(const char *key) {
    unsigned int h = 5381;
    while (*key) {
        h = h * 33 + (unsigned char)*key++;
    }
    return h;
}

void cluster_init(int my_port) {
    for (int i = 0; i < g_node_count; i++) {
        if (g_nodes[i].port == my_port) {
            g_my_id = i;
            return;
        }
    }
    fprintf(stderr, "警告：端口 %d 不在集群配置中\n", my_port);
    g_my_id = -1;
}

int cluster_owner(const char *key) {
    return hash_key(key) % g_node_count;
}

int cluster_my_id(void) {
    return g_my_id;
}

int cluster_count(void) {
    return g_node_count;
}

int cluster_node_port(int node_id) {
    if (node_id < 0 || node_id >= g_node_count) return -1;
    return g_nodes[node_id].port;
}

/* 连接到某个节点，返回 fd，失败返回 -1 */
static int connect_to_node(int node_id) {
    if (node_id < 0 || node_id >= g_node_count) return -1;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_nodes[node_id].port);
    inet_pton(AF_INET, g_nodes[node_id].host, &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect_to_node");
        close(fd);
        return -1;
    }
    return fd;
}

int cluster_forward(int node_id, const char *method,
                    const char *args, char **result) {
    return cluster_forward_timeout(node_id, method, args, result, RPC_TIMEOUT_MS);
}

int cluster_forward_timeout(int node_id, const char *method,
                            const char *args, char **result, int timeout_ms) {
    printf("  [转发] %s -> node %d (port %d)\n",
           method, node_id, cluster_node_port(node_id));
    fflush(stdout);

    int fd = connect_to_node(node_id);
    if (fd < 0) {
        if (result) *result = strdup("forward failed: node unreachable");
        return -1;
    }

    int st = rpc_call_timeout(fd, method, args, result, timeout_ms);
    close(fd);
    return st;
}

/* ===== 分片 → 主节点映射（第五步） ===== */

int cluster_master_of(int owner_id) {
    if (owner_id < 0 || owner_id >= g_node_count) return owner_id;
    return g_cluster_master[owner_id];
}

void cluster_set_master(int owner_id, int master_id) {
    if (owner_id < 0 || owner_id >= g_node_count) return;
    if (master_id < 0 || master_id >= g_node_count) return;
    g_cluster_master[owner_id] = master_id;
}

void cluster_master_map_str(char *buf, int len) {
    int n = 0;
    for (int i = 0; i < g_node_count; i++) {
        int w = snprintf(buf + n, len - n, "%s%d:%d",
                         i == 0 ? "" : ",", i, g_cluster_master[i]);
        if (w < 0) break;
        n += w;
    }
}

int cluster_apply_master_map(const char *map) {
    int updated = 0;
    char buf[256];
    strncpy(buf, map, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    /* 按逗号拆 "owner:master" 对 */
    char *save = NULL;
    for (char *tok = strtok_r(buf, ",", &save);
         tok;
         tok = strtok_r(NULL, ",", &save)) {
        int owner = -1, master = -1;
        if (sscanf(tok, "%d:%d", &owner, &master) == 2) {
            if (owner >= 0 && owner < g_node_count &&
                master >= 0 && master < g_node_count &&
                g_cluster_master[owner] != master) {
                printf("  [路由] 分片 %d 的主节点: node %d -> node %d\n",
                       owner, g_cluster_master[owner], master);
                fflush(stdout);
                g_cluster_master[owner] = master;
                updated++;
            }
        }
    }
    return updated;
}

/* 探活（防脑裂第六步）：尝试连接目标节点，成功 0，失败 -1 */
int cluster_ping(int node_id) {
    int fd = connect_to_node(node_id);
    if (fd < 0) return -1;
    close(fd);
    return 0;
}