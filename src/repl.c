#include "repl.h"
#include "rpc.h"
#include "cluster.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* handlers.c 导出的内部接口 */
extern int kv_export(char **out);
extern int kv_local_put(const char *args, char **result);
extern int kv_local_del(const char *args, char **result);
extern void kv_clear(void);

/* ---------- 角色 ---------- */
static repl_role_t g_role        = ROLE_MASTER;
static int         g_master_id   = -1;
static int         g_master_port = 0;

/* ---------- 主节点侧：从节点列表 ---------- */
#define MAX_SLAVES 8
static int g_slave_ids[MAX_SLAVES];
static int g_slave_count = 0;

/* ---------- 主节点侧：从节点心跳状态（第四步） ---------- */
static uint64_t g_slave_applied_seq[MAX_SLAVES];  /* 从节点报告的最新 applied_seq */
static time_t   g_slave_last_seen[MAX_SLAVES];    /* 最近一次心跳时间 */
static int      g_slave_alive[MAX_SLAVES];        /* 在线标志 */

#define SLAVE_TIMEOUT 5   /* 超过 5 秒无心跳判失联 */

/* ---------- 主节点侧：复制日志（环形缓冲） ----------
 * 每条写命令带递增序号 seq，从节点凭 seq 判断缺失哪些命令 */
#define REPL_LOG_MAX 1024
typedef struct {
    uint64_t seq;       /* 递增序号 */
    char     cmd[512];  /* 命令原文："kv_put foo 111" / "kv_del foo" */
} repl_log_entry_t;

static repl_log_entry_t g_repl_log[REPL_LOG_MAX];
static uint64_t g_repl_seq   = 0;   /* 最新已分配序号 */
static int      g_repl_head  = 0;   /* 最旧有效项 */
static int      g_repl_tail  = 0;   /* 下一个写入位置 */

/* ---------- 从节点侧：已应用序号 ---------- */
static uint64_t g_applied_seq = 0;

/* ---------- 从节点侧：周期补拉 + 心跳 + 故障检测（第四步） ---------- */
#define REPL_PULL_INTERVAL   2   /* 补拉间隔，秒 */
#define REPL_HB_INTERVAL     1   /* 心跳间隔，秒 */
#define REPL_HB_MAX_MISS     3   /* 连续 3 次心跳失败 → 判定主节点故障 */
#define REPL_AUX_TIMEOUT_MS 300  /* 心跳/补拉等辅助调用的接收超时（毫秒）
                                  * 短超时让事件循环快速恢复，避免和主节点的
                                  * push 广播互相等待形成死锁 */
static time_t g_last_pull = 0;
static time_t g_last_heartbeat = 0;
static int    g_heartbeat_miss = 0;

/*服务器启动时调用一次，决定这个节点是 "主" 还是 "从"*/
void repl_init(int my_port, int slaveof_port) {
    (void)my_port;
    if (slaveof_port > 0) {
        g_role        = ROLE_SLAVE;
        g_master_port = slaveof_port;
        g_master_id   = -1;
        for (int i = 0; i < cluster_count(); i++) {
            if (cluster_node_port(i) == slaveof_port) {
                g_master_id = i;
                break;
            }
        }
        if (g_master_id < 0) {
            fprintf(stderr, "[复制] 警告:master 端口 %d 不在集群配置中\n", slaveof_port);
        }
    } else {
        g_role = ROLE_MASTER;
    }
}
/*更改全区变量逻辑，外部调用链接*/
repl_role_t repl_role(void) { return g_role; }
int repl_master_id(void) { return g_master_id; }

/* ==================== 复制日志 ==================== */

/* 追加一条命令到复制日志，分配新 seq，返回该 seq */
static uint64_t repl_log_append(const char *cmd) {
    g_repl_log[g_repl_tail].seq = ++g_repl_seq;
    strncpy(g_repl_log[g_repl_tail].cmd, cmd,
            sizeof(g_repl_log[g_repl_tail].cmd) - 1);
    g_repl_log[g_repl_tail].cmd[sizeof(g_repl_log[g_repl_tail].cmd) - 1] = '\0';
    g_repl_tail = (g_repl_tail + 1) % REPL_LOG_MAX;
    if (g_repl_tail == g_repl_head) {
        g_repl_head = (g_repl_head + 1) % REPL_LOG_MAX;   /* 覆盖最旧 */
    }
    return g_repl_seq;
}

/* ==================== 主节点侧 RPC ==================== */

/* 全量同步：第一行 "SEQ n"，之后每行 "key value" */
static int handle_repl_full_sync(const char *args, char **result) {
    (void)args;

    char *kv = NULL;
    if (kv_export(&kv) < 0) {
        *result = strdup("export failed");
        return RPC_ERR_METHOD;
    }

    char seq_line[64];
    snprintf(seq_line, sizeof(seq_line), "SEQ %llu\n",
             (unsigned long long)g_repl_seq);

    size_t total = strlen(seq_line) + strlen(kv) + 1;
    char *buf = malloc(total);
    if (!buf) { free(kv); *result = strdup("oom"); return RPC_ERR_METHOD; }
    snprintf(buf, total, "%s%s", seq_line, kv);
    free(kv);
    *result = buf;
    return RPC_OK;
}

/* 从节点注册：主节点把它记入从节点列表（幂等，重复注册去重） */
static int handle_repl_handshake(const char *args, char **result) {
    int slave_id = atoi(args);
    if (slave_id < 0 || slave_id >= cluster_count()) {
        *result = strdup("invalid slave id");
        return RPC_ERR_METHOD;
    }
    for (int i = 0; i < g_slave_count; i++) {
        if (g_slave_ids[i] == slave_id) {
            *result = strdup("already registered");
            return RPC_OK;
        }
    }
    if (g_slave_count >= MAX_SLAVES) {
        *result = strdup("slave list full");
        return RPC_ERR_METHOD;
    }
    g_slave_ids[g_slave_count++] = slave_id;
    g_slave_applied_seq[slave_id] = 0;
    g_slave_last_seen[slave_id]   = time(NULL);
    g_slave_alive[slave_id]       = 1;
    printf("  [复制] 从节点 node %d 注册成功（共 %d 个从）\n", slave_id, g_slave_count);
    fflush(stdout);
    *result = strdup("ok");
    return RPC_OK;
}

/* 心跳：从节点报告自己的 applied_seq，主节点记录在线状态和滞后（lag）
 * args: "<my_id> <applied_seq>" */
static int handle_repl_heartbeat(const char *args, char **result) {
    int slave_id;
    unsigned long long seq;
    if (sscanf(args, "%d %llu", &slave_id, &seq) != 2) {
        *result = strdup("bad hb");
        return RPC_ERR_METHOD;
    }
    for (int i = 0; i < g_slave_count; i++) {
        if (g_slave_ids[i] == slave_id) {
            if (!g_slave_alive[slave_id]) {
                printf("  [复制] 从节点 node %d 恢复在线\n", slave_id);
                fflush(stdout);
            }
            g_slave_applied_seq[slave_id] = seq;
            g_slave_last_seen[slave_id]   = time(NULL);
            g_slave_alive[slave_id]       = 1;
            break;
        }
    }
    *result = strdup("ok");
    return RPC_OK;
}

/* 增量补拉：返回 seq > offset 的所有命令，每行 "seq cmd" */
static int handle_repl_pull(const char *args, char **result) {
    uint64_t offset = strtoull(args, NULL, 10);

    size_t cap = 4096, len = 0;
    char *buf = malloc(cap);
    if (!buf) { *result = strdup("oom"); return RPC_ERR_METHOD; }
    buf[0] = '\0';

    int i = g_repl_head;
    while (i != g_repl_tail) {
        if (g_repl_log[i].seq > offset) {
            size_t need = len + 32 + strlen(g_repl_log[i].cmd) + 1;
            if (need > cap) {
                cap = need * 2;
                char *nb = realloc(buf, cap);
                if (!nb) { free(buf); *result = strdup("oom"); return RPC_ERR_METHOD; }
                buf = nb;
            }
            int n = snprintf(buf + len, cap - len, "%llu %s\n",
                             (unsigned long long)g_repl_log[i].seq,
                             g_repl_log[i].cmd);
            if (n > 0) len += (size_t)n;
        }
        i = (i + 1) % REPL_LOG_MAX;
    }
    *result = buf;
    return RPC_OK;
}

/* 复制状态（调试）：
 * MASTER: role=MASTER slaves=N [node1: alive, lag=0] ...
 * SLAVE:  role=SLAVE applied_seq=N master=0(port 8001) */
static int handle_repl_status(const char *args, char **result) {
    (void)args;
    char buf[256];
    if (g_role == ROLE_MASTER) {
        int n = snprintf(buf, sizeof(buf), "role=MASTER slaves=%d", g_slave_count);
        for (int i = 0; i < g_slave_count; i++) {
            int sid = g_slave_ids[i];
            long idle = (long)difftime(time(NULL), g_slave_last_seen[sid]);
            long lag  = (long)(g_repl_seq - g_slave_applied_seq[sid]);
            snprintf(buf + n, sizeof(buf) - n, " [node%d: %s, lag=%ld, %.0fs]",
                     sid, g_slave_alive[sid] ? "alive" : "DEAD", lag,
                     g_slave_alive[sid] ? (double)idle : 0.0);
            /* 简化显示：失联只显示 DEAD */
        }
    } else {
        snprintf(buf, sizeof(buf), "role=SLAVE applied_seq=%llu master=%d(port %d)",
                 (unsigned long long)g_applied_seq, g_master_id, g_master_port);
    }
    *result = strdup(buf);
    return RPC_OK;
}

/* ==================== 从节点侧：应用命令 ==================== */

/* 应用一条带 seq 的命令："<seq> kv_put key val" / "<seq> kv_del key"
 * seq <= applied_seq 时跳过（幂等：重复推/拉不影响结果） */
static int apply_repl_cmd(const char *line, char **result) {
    unsigned long long seq;   /* sscanf 用 %llu 读取 */
    char method[16];
    char key[64];
    char val[256];

    if (sscanf(line, "%llu %15s %63s %255[^\n]", &seq, method, key, val) == 4) {
        if (seq <= g_applied_seq) { *result = strdup("dup"); return RPC_OK; }
        if (strcmp(method, "kv_put") == 0) {
            char cmd[512];
            snprintf(cmd, sizeof(cmd), "%s %s", key, val);
            int st = kv_local_put(cmd, result);
            g_applied_seq = seq;   /* 无论如何都推进，避免死循环重拉 */
            return st;
        }
    } else if (sscanf(line, "%llu %15s %63s", &seq, method, key) == 3) {
        if (seq <= g_applied_seq) { *result = strdup("dup"); return RPC_OK; }
        if (strcmp(method, "kv_del") == 0) {
            int st = kv_local_del(key, result);
            g_applied_seq = seq;
            return st;
        }
    }
    *result = strdup("bad repl cmd");
    return RPC_ERR_METHOD;
}

/* RPC：主节点推来的实时命令 */
static int handle_repl_apply(const char *args, char **result) {
    return apply_repl_cmd(args, result);
}

/* ==================== 主节点侧：广播 ==================== */

/* 把一条写命令推给所有从节点
 * 流程：进复制日志（分配 seq）→ 带 seq 推给每个从（半同步）
 * ≥1 个确认即视为成功；0 个确认打印警告但写仍成功（降级异步，稍后 pull 补上）
 * 返回确认的从节点数 */
int repl_broadcast(const char *cmd) {
    uint64_t seq = repl_log_append(cmd);

    char buf[600];
    snprintf(buf, sizeof(buf), "%llu %s", (unsigned long long)seq, cmd);

    int ack = 0;
    for (int i = 0; i < g_slave_count; i++) {
        char *res = NULL;
        int st = cluster_forward(g_slave_ids[i], "repl_apply", buf, &res);
        printf("  [复制] push seq=%llu -> node %d: %s\n",
               (unsigned long long)seq, g_slave_ids[i],
               st == RPC_OK ? "ack" : "FAILED");
        fflush(stdout);
        if (st == RPC_OK) ack++;
        free(res);
    }
    if (ack < 1) {
        printf("  [复制] 警告：没有从节点确认（共 %d 个从），seq=%llu 未复制（稍后可由 pull 补上）\n",
               g_slave_count, (unsigned long long)seq);
        fflush(stdout);
    }
    return ack;
}

/* ==================== 从节点侧：全量 + 补拉 ==================== */

/* 从节点：启动时向主节点拉全量，逐条写入本地，并设置 applied_seq */
int repl_sync_from_master(void) {
    if (g_role != ROLE_SLAVE || g_master_id < 0) return -1;

    char *data = NULL;
    printf("[复制] 向 node %d (port %d) 拉取全量数据...\n",
           g_master_id, g_master_port);
    fflush(stdout);

    int st = cluster_forward(g_master_id, "repl_full_sync", "", &data);
    if (st < 0 || !data || !*data) {
        printf("[复制] 全量同步失败：master 未启动\n");
        free(data);
        return -1;
    }

    /* 第一行 "SEQ n"，之后每行 "key value" */
    char *save = NULL;
    char *first = strtok_r(data, "\n", &save);
    if (first && strncmp(first, "SEQ ", 4) == 0) {
        g_applied_seq = strtoull(first + 4, NULL, 10);
    }

    int n = 0;
    for (char *line = strtok_r(NULL, "\n", &save);
         line;
         line = strtok_r(NULL, "\n", &save)) {
        char *res = NULL;
        kv_local_put(line, &res);
        free(res);
        n++;
    }
    free(data);
    printf("[复制] 全量同步完成，共 %d 条 (applied_seq=%llu)\n",
           n, (unsigned long long)g_applied_seq);
    return 0;
}

/* 从节点：向主节点注册自己 */
int repl_register_to_master(void) {
    if (g_role != ROLE_SLAVE || g_master_id < 0) return -1;

    char buf[16];
    snprintf(buf, sizeof(buf), "%d", cluster_my_id());

    char *res = NULL;
    int st = cluster_forward(g_master_id, "repl_handshake", buf, &res);
    printf("[复制] 向主节点注册自己: %s\n",
           (st == RPC_OK && res) ? res : "failed");
    fflush(stdout);
    free(res);
    return st == RPC_OK ? 0 : -1;
}

/* 从节点：向主节点拉缺失命令（seq > applied_seq）并应用 */
int repl_pull_from_master(void) {
    if (g_role != ROLE_SLAVE || g_master_id < 0) return -1;

    char offset[32];
    snprintf(offset, sizeof(offset), "%llu", (unsigned long long)g_applied_seq);

    char *data = NULL;
    int st = cluster_forward_timeout(g_master_id, "repl_pull", offset, &data,
                                     REPL_AUX_TIMEOUT_MS);
    if (st < 0 || !data || !*data) {
        free(data);
        return 0;   /* master 不可达或无新命令 */
    }

    int n = 0;
    char *save = NULL;
    for (char *line = strtok_r(data, "\n", &save);
         line;
         line = strtok_r(NULL, "\n", &save)) {
        char *res = NULL;
        apply_repl_cmd(line, &res);
        free(res);
        n++;
    }
    free(data);
    if (n > 0) {
        printf("[复制] pull 补到 %d 条 (applied_seq=%llu)\n",
               n, (unsigned long long)g_applied_seq);
        fflush(stdout);
    }
    return n;
}

/* ==================== 从节点侧：心跳 + 故障转移（第四步） ==================== */

/* 从节点：向主节点发一次心跳（带自己的 applied_seq），返回 0 成功 -1 失败 */
static int heartbeat_to_master(void) {
    char args[64];
    snprintf(args, sizeof(args), "%d %llu",
             cluster_my_id(), (unsigned long long)g_applied_seq);
    char *res = NULL;
    int st = cluster_forward_timeout(g_master_id, "repl_heartbeat", args, &res,
                                     REPL_AUX_TIMEOUT_MS);
    free(res);
    return st == RPC_OK ? 0 : -1;
}

/* 从节点：主节点失联，自动提升为新主（禁写解除，心跳/补拉停发）
 * 提升后更新"分片→主节点"映射（自己顶替原主的分片）并广播给集群 */
static void promote_to_master(void) {
    int failed_owner = g_master_id;   /* 原主节点 = 我们顶替的分片 */

    g_role      = ROLE_MASTER;
    g_master_id = -1;

    /* 更新本节点路由表：分片 failed_owner 的主节点变成自己 */
    cluster_set_master(failed_owner, cluster_my_id());

    printf("  [复制] !!! 主节点失联（连续 %d 次心跳失败），从节点提升为 MASTER，禁写解除\n",
           REPL_HB_MAX_MISS);
    printf("  [复制] !!! 分片 %d 的主节点: node %d -> node %d（本节点）\n",
           failed_owner, failed_owner, cluster_my_id());
    printf("  [复制] !!! 注意：若原主恢复会出现双主脑裂，请人工处理\n");
    fflush(stdout);

    /* 向集群其他节点广播新的映射表 */
    repl_announce_master_map();
}

/* 把自己当前的分片映射广播给集群所有其他节点 */
void repl_announce_master_map(void) {
    char map[128];
    cluster_master_map_str(map, sizeof(map));

    printf("  [路由] 广播映射表: %s\n", map);
    fflush(stdout);

    for (int i = 0; i < cluster_count(); i++) {
        if (i == cluster_my_id()) continue;
        char *res = NULL;
        int st = cluster_forward(i, "repl_notify_master", map, &res);
        printf("  [路由] 通知 node %d: %s\n",
               i, st == RPC_OK ? "ok" : "FAILED");
        fflush(stdout);
        free(res);
    }
}

/* 接收方：应用广播来的分片映射 */
static int handle_repl_notify_master(const char *args, char **result) {
    cluster_apply_master_map(args);
    *result = strdup("ok");
    return RPC_OK;
}

/* ==================== 防脑裂（第六步） ==================== */

/* 探活请求（仲裁 A）：args = 要探的节点 id
 * 被问方亲自尝试连接目标：连得上回 "alive"，连不上回 "dead"
 * 注意：connect 成功只证明内核在监听（进程假死时也成功）——
 * 对教学足够：宁可把假死判成 alive（不提升），也不在无证词时冒险当主 */
static int handle_repl_probe(const char *args, char **result) {
    int target = atoi(args);
    if (cluster_ping(target) == 0)
        *result = strdup("alive");
    else
        *result = strdup("dead");
    return RPC_OK;
}

/* 返回本节点当前的分片映射表（恢复检测 B 用） */
static int handle_repl_map(const char *args, char **result) {
    (void)args;
    char map[128];
    cluster_master_map_str(map, sizeof(map));
    *result = strdup(map);
    return RPC_OK;
}

/* 提升前仲裁：询问集群其他节点"主节点是否真的不可达"
 * 只要有 ≥1 个第三方证实 dead（自己 1 票 + 它 1 票 = 3 节点多数派 2 票）→ 允许提升
 * 有人证实 alive（说明是网络分区）→ 返回 0，不提升
 * 问不到任何第三方（都连不上）→ 返回 0，无证词不冒险 */
static int repl_probe_confirm_dead(int master_id) {
    char target[16];
    snprintf(target, sizeof(target), "%d", master_id);

    int dead_votes = 0;
    for (int i = 0; i < cluster_count(); i++) {
        if (i == cluster_my_id()) continue;
        char *res = NULL;
        int st = cluster_forward(i, "repl_probe", target, &res);
        if (st == RPC_OK && res) {
            if (strcmp(res, "dead") == 0) {
                dead_votes++;
                printf("  [复制] 仲裁：node %d 证实主节点 node %d 不可达（dead）\n",
                       i, master_id);
            } else {
                printf("  [复制] 仲裁：node %d 证实主节点 node %d 仍可达（alive）\n",
                       i, master_id);
                free(res);
                return 0;   /* 有人证明主还活着 → 是网络分区，不提升 */
            }
        }
        free(res);
    }
    return dead_votes >= 1;   /* ≥1 第三方 dead = 多数派（自己+它）允许提升 */
}

/* 被顶替的旧主：降级为指定节点的从节点（防脑裂 B）
 * 只做状态翻转 + 清空旧数据；注册和全量交给 server.c 统一的从节点启动流程 */
int repl_demote_to_slave(int new_master_id) {
    if (new_master_id < 0 || new_master_id >= cluster_count()) return -1;
    g_role        = ROLE_SLAVE;
    g_master_id   = new_master_id;
    g_master_port = cluster_node_port(new_master_id);
    g_slave_count = 0;          /* 不再是主：清空从列表，停止广播 */
    kv_clear();                 /* 清空旧数据，避免和新主数据残留混在一起 */
    printf("  [复制] !!! 检测到本节点已被顶替，自动降级为 node %d 的从节点\n",
           new_master_id);
    fflush(stdout);
    return 0;
}

/* 恢复检测（防脑裂 B）：启动时"先听后说"
 * 遍历其他节点调 repl_map，应用对方映射；
 * 若发现自己负责的分片已被接管 → 自动降级（原主变新主的从） */
void repl_check_demote_on_startup(void) {
    if (g_role != ROLE_MASTER) return;   /* 从节点无需检查 */

    for (int i = 0; i < cluster_count(); i++) {
        if (i == cluster_my_id()) continue;
        char *map = NULL;
        int st = cluster_forward(i, "repl_map", "", &map);
        if (st != RPC_OK || !map) {
            free(map);
            continue;                     /* 该节点不在线，跳过 */
        }
        cluster_apply_master_map(map);    /* 先听后说：应用集群最新路由 */
        free(map);
    }

    /* 检查：我原本负责的分片，现在主还是我吗？ */
    int my_owner = cluster_my_id();
    if (cluster_master_of(my_owner) != my_owner) {
        repl_demote_to_slave(cluster_master_of(my_owner));
    }
}

/* 统一周期入口（server 主循环每轮调用）
 * SLAVE：每秒心跳（失败累计，连续 3 次提升）+ 每 2 秒补拉
 * MASTER：每秒检查从节点是否失联 */
void repl_tick(void) {
    time_t now = time(NULL);

    if (g_role == ROLE_SLAVE && g_master_id >= 0) {
        /* 心跳：每秒一次 */
        if (now - g_last_heartbeat >= REPL_HB_INTERVAL) {
            g_last_heartbeat = now;
            if (heartbeat_to_master() < 0) {
                if (++g_heartbeat_miss >= REPL_HB_MAX_MISS) {
                    /* 防脑裂 A：不立即提升，先仲裁——向集群求证主是否真的不可达 */
                    if (repl_probe_confirm_dead(g_master_id)) {
                        promote_to_master();
                        return;
                    } else {
                        g_heartbeat_miss = 0;
                        printf("  [复制] 仲裁：集群仍可见主节点 node %d，判定为网络分区，暂不提升\n",
                               g_master_id);
                        fflush(stdout);
                    }
                }
            } else {
                g_heartbeat_miss = 0;
            }
        }
        /* 补拉：每 2 秒一次 */
        if (now - g_last_pull >= REPL_PULL_INTERVAL) {
            g_last_pull = now;
            repl_pull_from_master();
        }
    } else if (g_role == ROLE_MASTER) {
        /* 失联检测：每秒一次 */
        static time_t last_check = 0;
        if (now - last_check >= 1) {
            last_check = now;
            for (int i = 0; i < g_slave_count; i++) {
                int sid = g_slave_ids[i];
                if (g_slave_alive[sid] &&
                    difftime(now, g_slave_last_seen[sid]) > SLAVE_TIMEOUT) {
                    g_slave_alive[sid] = 0;
                    long lag = (long)(g_repl_seq - g_slave_applied_seq[sid]);
                    printf("  [复制] 从节点 node %d 失联（>%ds 无心跳），lag=%ld\n",
                           sid, SLAVE_TIMEOUT, lag);
                    fflush(stdout);
                }
            }
        }
    }
}

void register_repl_handlers(void) {
    rpc_register("repl_full_sync",  handle_repl_full_sync);
    rpc_register("repl_handshake",  handle_repl_handshake);
    rpc_register("repl_apply",      handle_repl_apply);
    rpc_register("repl_pull",       handle_repl_pull);
    rpc_register("repl_heartbeat",  handle_repl_heartbeat);
    rpc_register("repl_notify_master", handle_repl_notify_master);
    rpc_register("repl_probe",      handle_repl_probe);
    rpc_register("repl_map",        handle_repl_map);
    rpc_register("repl_status",     handle_repl_status);
}
