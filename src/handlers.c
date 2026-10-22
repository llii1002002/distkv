#include "rpc.h"
#include "cluster.h"
#include "repl.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ---------------- KV 存储 ---------------- */
#define MAX_KV  1024
#define MAX_KEY 64
#define MAX_VAL 256

typedef struct {
    char key[MAX_KEY];
    char val[MAX_VAL];
} kv_entry_t;

static kv_entry_t g_kv[MAX_KV];
static int g_kv_count = 0;

/* ---------------- echo / add ---------------- */
static int handle_echo(const char *args, char **result) {
    *result = strdup(args);
    return RPC_OK;
}

static int handle_add(const char *args, char **result) {
    int a, b;
    if (sscanf(args, "%d %d", &a, &b) != 2) {
        *result = strdup("invalid args");
        return RPC_ERR_METHOD;
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%d", a + b);
    *result = strdup(buf);
    return RPC_OK;
}

/* ---------------- 本地 KV（节点间直接调） ---------------- */

/* 直接写入本节点存储（节点间互调 / 复制应用共用） */
int kv_local_put(const char *args, char **result) {
    char key[MAX_KEY], val[MAX_VAL];
    if (sscanf(args, "%63s %255[^\n]", key, val) != 2) {
        *result = strdup("invalid args");
        return RPC_ERR_METHOD;
    }

    for (int i = 0; i < g_kv_count; i++) {
        if (strcmp(g_kv[i].key, key) == 0) {
            strncpy(g_kv[i].val, val, MAX_VAL - 1);
            g_kv[i].val[MAX_VAL - 1] = '\0';
            *result = strdup("updated");
            return RPC_OK;
        }
    }

    if (g_kv_count >= MAX_KV) {
        *result = strdup("store full");
        return RPC_ERR_METHOD;
    }
    strncpy(g_kv[g_kv_count].key, key, MAX_KEY - 1);
    g_kv[g_kv_count].key[MAX_KEY - 1] = '\0';
    strncpy(g_kv[g_kv_count].val, val, MAX_VAL - 1);
    g_kv[g_kv_count].val[MAX_VAL - 1] = '\0';
    g_kv_count++;
    *result = strdup("ok");
    return RPC_OK;
}

/* RPC 入口：节点间转发 / 复制应用共用 kv_local_put */
static int handle_local_kv_put(const char *args, char **result) {
    return kv_local_put(args, result);
}

static int handle_local_kv_get(const char *args, char **result) {
    for (int i = 0; i < g_kv_count; i++) {
        if (strcmp(g_kv[i].key, args) == 0) {
            *result = strdup(g_kv[i].val);
            return RPC_OK;
        }
    }
    *result = strdup("not found");
    return RPC_ERR_METHOD;
}

/* 直接删除本节点存储（节点间互调 / 复制应用共用） */
int kv_local_del(const char *args, char **result) {
    for (int i = 0; i < g_kv_count; i++) {
        if (strcmp(g_kv[i].key, args) == 0) {
            g_kv[i] = g_kv[g_kv_count - 1];
            g_kv_count--;
            *result = strdup("deleted");
            return RPC_OK;
        }
    }
    *result = strdup("not found");
    return RPC_ERR_METHOD;
}

static int handle_local_kv_del(const char *args, char **result) {
    return kv_local_del(args, result);
}

/* ---------------- 对外 KV（带路由 + 主从复制） ---------------- */

/* 从节点禁写：写请求一律拒绝（Redis 风格），防止和主节点数据分叉 */
static int slave_write_denied(char **result) {
    *result = strdup("READONLY You can't write against a read only slave");
    return RPC_ERR_METHOD;
}

static int handle_kv_put(const char *args, char **result) {
    char key[MAX_KEY];
    if (sscanf(args, "%63s", key) != 1) {
        *result = strdup("invalid args");
        return RPC_ERR_METHOD;
    }

    /* 从节点禁写 */
    if (repl_role() == ROLE_SLAVE) {
        return slave_write_denied(result);
    }

    int owner  = cluster_owner(key);
    int master = cluster_master_of(owner);   /* 该分片当前的主节点（可能不是 owner 本身） */
    if (master == cluster_my_id()) {
        /* 1. 本地写 */
        int st = kv_local_put(args, result);
        /* 2. 主节点：把写命令推给所有从节点（半同步） */
        if (st == RPC_OK) {
            char cmd[600];
            snprintf(cmd, sizeof(cmd), "kv_put %s", args);
            repl_broadcast(cmd);
        }
        return st;
    }
    return cluster_forward(master, "local_kv_put", args, result);
}

static int handle_kv_get(const char *args, char **result) {
    char key[MAX_KEY];
    if (sscanf(args, "%63s", key) != 1) {
        *result = strdup("invalid args");
        return RPC_ERR_METHOD;
    }

    int owner  = cluster_owner(key);
    int master = cluster_master_of(owner);
    if (master == cluster_my_id()) {
        return handle_local_kv_get(args, result);
    }
    return cluster_forward(master, "local_kv_get", args, result);
}

static int handle_kv_del(const char *args, char **result) {
    char key[MAX_KEY];
    if (sscanf(args, "%63s", key) != 1) {
        *result = strdup("invalid args");
        return RPC_ERR_METHOD;
    }

    /* 从节点禁写 */
    if (repl_role() == ROLE_SLAVE) {
        return slave_write_denied(result);
    }

    int owner  = cluster_owner(key);
    int master = cluster_master_of(owner);
    if (master == cluster_my_id()) {
        /* 1. 本地删 */
        int st = kv_local_del(args, result);
        /* 2. 主节点：把删除命令推给所有从节点（半同步） */
        if (st == RPC_OK) {
            char cmd[600];
            snprintf(cmd, sizeof(cmd), "kv_del %s", args);
            repl_broadcast(cmd);
        }
        return st;
    }
    return cluster_forward(master, "local_kv_del", args, result);
}

/* ---------------- route 调试方法 ---------------- */
static int handle_route(const char *args, char **result) {
    if (!args || !*args) {
        *result = strdup("usage: route <key>");
        return RPC_ERR_METHOD;
    }
    int owner  = cluster_owner(args);
    int master = cluster_master_of(owner);
    int my     = cluster_my_id();
    char buf[224];
    snprintf(buf, sizeof(buf),
             "key=\"%s\" -> 分片 node %d，主节点 node %d (127.0.0.1:%d), 本节点=node %d%s",
             args, owner, master, cluster_node_port(master), my,
             master == my ? " [本地]" : " [转发]");
    *result = strdup(buf);
    return RPC_OK;
}

/* ---------------- KV 导出（主从复制全量同步用） ---------------- */
/* 清空本地 KV（防脑裂第六步：旧主恢复降级时，先清空旧数据再全量接管新主数据） */
void kv_clear(void) {
    g_kv_count = 0;
}

/* 把本节点全部 KV 打包成命令串：每行一条 "key value"
 * 返回 malloc 字符串到 *out，调用者负责 free */
int kv_export(char **out) {
    if (!out) return -1;
    size_t cap = 1024;
    size_t len = 0;
    char *buf = malloc(cap);
    if (!buf) return -1;
    buf[0] = '\0';

    for (int i = 0; i < g_kv_count; i++) {
        size_t need = strlen(g_kv[i].key) + 1 + strlen(g_kv[i].val) + 1; /* 空格+换行 */
        if (len + need + 1 > cap) {
            while (cap < len + need + 1) cap *= 2;
            char *nb = realloc(buf, cap);
            if (!nb) { free(buf); return -1; }
            buf = nb;
        }
        int n = snprintf(buf + len, cap - len, "%s %s\n", g_kv[i].key, g_kv[i].val);
        if (n < 0) { free(buf); return -1; }
        len += (size_t)n;
    }
    *out = buf;
    return 0;
}

/* ---------------- 注册 ---------------- */
void register_all_handlers(void) {
    rpc_register("echo", handle_echo);
    rpc_register("add",  handle_add);

    rpc_register("route", handle_route);

    rpc_register("kv_put", handle_kv_put);
    rpc_register("kv_get", handle_kv_get);
    rpc_register("kv_del", handle_kv_del);

    rpc_register("local_kv_put", handle_local_kv_put);
    rpc_register("local_kv_get", handle_local_kv_get);
    rpc_register("local_kv_del", handle_local_kv_del);
}