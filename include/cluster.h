#ifndef CLUSTER_H
#define CLUSTER_H

/* 初始化集群：传入自己的端口 */
void cluster_init(int my_port);

/* 计算 key 归属的节点 ID，返回 0..count-1 */
int  cluster_owner(const char *key);

/* 当前节点的 ID */
int  cluster_my_id(void);

/* 节点总数 */
int  cluster_count(void);

/* 返回指定节点的端口 */
int  cluster_node_port(int node_id);

/* 转发请求到指定节点
 * 返回 0 成功，非 0 失败
 * 成功后 *result 是 malloc 的字符串，调用者 free
 */
int  cluster_forward(int node_id, const char *method,const char *args, char **result);

/* 转发（自定义接收超时，毫秒）：心跳/补拉等辅助调用用短超时快速失败，
 * 避免单线程事件循环里双向同步 RPC 互相等待造成死锁 */
int  cluster_forward_timeout(int node_id, const char *method,
                             const char *args, char **result, int timeout_ms);

/* ===== 分片 → 主节点映射（第五步：路由表更新） ===== */

/* 返回分片 owner_id 当前服务的主节点
 * 初始每个分片由自己服务；故障转移后可能指向别的主节点 */
int  cluster_master_of(int owner_id);

/* 更新分片 owner_id 的服务主节点 */
void cluster_set_master(int owner_id, int master_id);

/* 把当前映射序列化成 "0:1,1:1,2:2" 格式，写入 buf */
void cluster_master_map_str(char *buf, int len);

/* 解析 "0:1,1:1,2:2" 并应用（广播接收方调用），返回更新的分片数 */
int  cluster_apply_master_map(const char *map);

/* 探活：尝试连接指定节点并立即关闭
 * 成功返回 0（端口在监听），失败 -1
 * 注意：connect 成功只能证明内核在监听，不能证明进程在响应——
 * 对教学足够（进程真死必然 connect 失败，进程假死则保守判活，宁可不提升） */
int  cluster_ping(int node_id);

#endif