#ifndef REPL_H
#define REPL_H

/* 主从复制
 *
 * 第一步（已实现）：
 *   - 角色：默认 ROLE_MASTER；--slaveof <master_port> → ROLE_SLAVE
 *   - 从节点启动时通过 repl_full_sync 拉取主节点全量数据
 *
 * 第二步（已实现）：
 *   - 从节点启动时通过 repl_handshake 向主节点注册
 *   - 主节点每次写操作通过 repl_apply 实时推给从节点（半同步）
 *   - 从节点禁写（写请求返回 READONLY）
 *
 * 第三步（已实现）：
 *   - 主节点维护复制日志，每条命令带递增序号 seq
 *   - 从节点记录 applied_seq，重复序号幂等跳过
 *   - 从节点周期 repl_pull(offset) 补齐缺失命令（断线兜底）
 *
 * 第四步（已实现）：
 *   - 从节点每秒向主节点发 repl_heartbeat（带 applied_seq）
 *   - 主节点记录从节点在线状态和复制滞后（lag），失联超时打印告警
 *   - 从节点心跳连续失败 → 判定主节点故障 → 自动提升为 MASTER（禁写解除）
 */

typedef enum {
    ROLE_MASTER = 0,
    ROLE_SLAVE  = 1,
} repl_role_t;

/* 设置角色：slaveof_port <= 0 表示主节点；> 0 表示从节点，主节点端口为 slaveof_port */
void repl_init(int my_port, int slaveof_port);

/* 当前角色 */
repl_role_t repl_role(void);

/* 从节点：主节点的 node_id（仅 ROLE_SLAVE 有效，无效返回 -1） */
int repl_master_id(void);

/* 从节点：向主节点拉取全量数据并写入本地存储，返回 0 成功，-1 失败 */
int repl_sync_from_master(void);

/* 从节点：向主节点注册自己（repl_handshake），返回 0 成功，-1 失败 */
int repl_register_to_master(void);

/* 主节点：把一条写命令推给所有从节点（半同步），返回确认的从节点数
 * 命令格式："kv_put key val" / "kv_del key" */
int repl_broadcast(const char *cmd);

/* 从节点：向主节点拉取缺失命令（seq > applied_seq）并应用，返回补到的条数 */
int repl_pull_from_master(void);

/* 心跳 + 补拉 + 故障检测的统一周期入口（server 主循环每轮调用）
 * SLAVE：每秒心跳、每 2 秒补拉、连续失败自动提升
 * MASTER：每秒检查从节点失联 */
void repl_tick(void);

/* 把本节点的"分片→主节点"映射广播给集群所有其他节点
 * （启动时和故障转移提升时调用，让路由表收敛） */
void repl_announce_master_map(void);

/* ===== 防脑裂（第六步） ===== */

/* 恢复检测（保险 B）：启动时"先听后说"——查询集群其他节点的映射表，
 * 若发现自己负责的分片已被接管，则自动降级为接管者的从节点 */
void repl_check_demote_on_startup(void);

/* 被顶替的旧主：降级为指定节点的从节点（清空旧数据，交给启动流程做注册+全量） */
int repl_demote_to_slave(int new_master_id);

/* 注册复制相关 RPC 方法（server 启动时调用） */
void register_repl_handlers(void);

#endif
