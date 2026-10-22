# 项目文档：分布式键值存储系统

下面帮你把目前做过的东西整理成一份**完整的项目笔记**。可以直接保存为 `docs/design.md`，以后回顾或写简历都用得上。

---

## 一、项目概述

用 C 语言从零实现一个**分布式键值存储系统**，支持多节点、数据分片、RPC 远程调用。

**技术栈**：

- 语言：C（C11）
- 系统：Linux（WSL2 Ubuntu）
- 网络：TCP + epoll
- 通信：自定义 RPC 协议（长度前缀 + 二进制序列化）
- 存储：内存数组（`g_kv[]`，最大 1024 条）
- 路由：简单取模哈希（`hash % 节点数`）

> **注意**：当前实现的是**简单取模哈希**路由，不是一致性哈希。文档表述以代码为准（见第六节）。

---

## 二、系统架构

```
┌─────────────────────────────────────────────────────────┐
│                        客户端                            │
│  kv_client → rpc_call → 序列化 → 发送 → 接收 → 反序列化  │
└─────────────────────────────────────────────────────────┘
                          ↕ TCP
┌─────────────────────────────────────────────────────────┐
│                    分布式节点集群                         │
│                                                          │
│  节点0 (8001)      节点1 (8002)      节点2 (8003)        │
│  ┌─────────┐      ┌─────────┐      ┌─────────┐         │
│  │ epoll   │      │ epoll   │      │ epoll   │         │
│  │ 协议层  │      │ 协议层  │      │ 协议层  │         │
│  │ RPC分发 │      │ RPC分发 │      │ RPC分发 │         │
│  │ KV存储  │      │ KV存储  │      │ KV存储  │         │
│  │ 路由    │←────→│ 路由    │←────→│ 路由    │         │
│  └─────────┘      └─────────┘      └─────────┘         │
└─────────────────────────────────────────────────────────┘
```

**节点 ID 由端口决定**：`cluster_init(port)` 根据进程启动时传入的端口（8001/8002/8003）确定自己是节点 0/1/2。

---

## 三、模块划分

### 3.1 序列化层（`protocol.c` / `protocol.h`）

**职责**：把内存结构转成字节流，以及反向操作。

**消息格式**：

```
[RequestID 4B][Status 1B][MethodLen 4B][Method][ArgsLen 4B][Args]
```

| 字段 | 长度 | 说明 |
|---|---|---|
| RequestID | 4B 网络序 | 请求编号 |
| Status | 1B | 0 成功，非 0 错误 |
| MethodLen | 4B 网络序 | 方法名字节数 |
| Method | 变长 | 方法名 |
| ArgsLen | 4B 网络序 | 参数字节数 |
| Args | 变长 | 参数 |

**关键函数**：

| 函数 | 作用 |
|---|---|
| `serialize` | 结构体 → 字节流 |
| `deserialize` | 字节流 → 结构体 |
| `message_free` | 释放动态分配的内存 |

**关键点**：

- 用 `htonl` / `ntohl` 转换字节序
- 用 `memcpy` 避免对齐问题
- 长度字段要检查合法性（`method_len` / `args_len` 超过 1MB 直接拒绝）
- 出错路径要 `free` 已分配的内存，并把指针置 NULL（`deserialize` 开头先清空 `out`）

### 3.2 网络层（`server.c`）

**职责**：用 epoll 处理多客户端并发。

**服务端流程**：

```
main(argc, argv):
    port = argv[1]（默认 8888，但集群只认 8001/8002/8003）
    cluster_init(port)          ← 确定自己的节点 ID
    register_all_handlers()     ← 注册所有 RPC 方法
    socket → setsockopt → bind → listen → set_nonblocking
    epoll_create1 → epoll_ctl(ADD listen_fd)
    while (1):
        epoll_wait
        if fd == listen_fd: accept → 加入 epoll
        else: handle_read → 反序列化 → rpc_dispatch → 序列化响应 → 发送
```

**关键点**：

- 所有 socket 都要设非阻塞（`fcntl O_NONBLOCK`）
- 用状态机处理长度前缀协议（`STATE_READ_LEN` / `STATE_READ_PAYLOAD`）
- 每个连接维护独立的 `connection_t`，存在 `epoll_event.data.ptr`
- 断开时先 `EPOLL_CTL_DEL`，再 `close`

**已知问题（代码里存在，暂不影响学习使用）**：

| 问题 | 位置 | 影响 |
|---|---|---|
| `STATE_READ_LEN` 阶段没有累加已读字节数，半包时半截长度会被丢弃 | `server.c` 的 `handle_read` | 高并发/慢网络下可能解析错乱 |
| 响应缓冲区写死 4096 字节 | `handle_read` 内 `char resp_buf[4096]` | 超大响应会被静默丢弃 |
| server 端 `send_message` 遇 `EAGAIN` 直接忙转（未注册 `EPOLLOUT`） | `server.c` | 发送阻塞时 CPU 空转 |

### 3.3 RPC 层（`rpc_server.c` / `rpc_client.c` / `rpc.h`）

**职责**：让客户端像调本地函数一样调远程函数。

**服务端：方法表 + 分发**

```c
typedef int (*rpc_handler_t)(const char *args, char **result);
typedef struct {
    const char    *name;
    rpc_handler_t  handler;
} rpc_method_t;
```

- `rpc_register(name, handler)`：注册方法（方法表 `g_methods[]` 最多 32 个）
- `rpc_dispatch(req, resp)`：根据 `req->method` 线性查找并调用
- 方法的返回结果用 `malloc`/`strdup` 分配，调用者负责 `free`

**客户端：同步调用**

```c
int rpc_call(int fd, const char *method, const char *args, char **result);
```

流程：生成自增 request_id → 序列化 → 发送（长度前缀 + payload）→ 接收 → 反序列化 → 校验 ID 配对 → 返回结果。

### 3.4 业务层（`handlers.c`）

**职责**：实现具体的 RPC 方法，分为「对外接口（带路由）」和「本地接口（节点内执行）」两套。

| 方法 | 类型 | 作用 |
|---|---|---|
| `echo` | 演示 | 原样返回参数 |
| `add` | 演示 | 解析两个整数相加 |
| `route` | 调试 | 输入 key，返回它归哪个节点（本地还是转发） |
| `kv_put` | 对外 | 算 owner，自己管则本地写，否则转发 |
| `kv_get` | 对外 | 算 owner，自己管则本地读，否则转发 |
| `kv_del` | 对外 | 算 owner，自己管则本地删，否则转发 |
| `local_kv_put` | 本地 | 直接写入本节点存储（节点间互调） |
| `local_kv_get` | 本地 | 直接读本节点存储 |
| `local_kv_del` | 本地 | 直接删本节点存储 |

`route` 输出示例：

```
route name: key="name" -> node 1 (127.0.0.1:8002), 本节点=node 0 [转发]
```

**KV 存储实现**：固定数组模拟哈希表。

```c
#define MAX_KV  1024   // 最多 1024 条
#define MAX_KEY 64
#define MAX_VAL 256

typedef struct {
    char key[MAX_KEY];
    char val[MAX_VAL];
} kv_entry_t;

static kv_entry_t g_kv[MAX_KV];
static int g_kv_count = 0;   // 当前条数
```

- 查找：线性扫描 `g_kv[]` 比较 key
- 覆盖：key 已存在则更新 val
- 删除：把最后一条搬到被删位置（`g_kv[i] = g_kv[g_kv_count-1]`），`g_kv_count--`
- 满时返回 `"store full"`

### 3.5 集群层（`cluster.c` / `cluster.h`）

**职责**：管理节点列表、哈希路由、节点间转发。

**核心函数**：

| 函数 | 作用 |
|---|---|
| `cluster_init(port)` | 根据端口确定自己的节点 ID（找不到返回 -1 并告警） |
| `cluster_owner(key)` | 算 key 归哪个节点（取模哈希） |
| `cluster_my_id()` | 当前节点 ID |
| `cluster_count()` | 节点总数 |
| `cluster_node_port(node_id)` | 返回指定节点的端口 |
| `cluster_forward(node_id, method, args, result)` | 转发请求到指定节点 |

**哈希函数**（djb2）：

```c
unsigned int h = 5381;
while (*key) h = h * 33 + (unsigned char)*key++;
```

**路由规则（取模哈希，当前实现）**：

```c
int cluster_owner(const char *key) {
    return hash_key(key) % g_node_count;   // 0..2
}
```

**节点配置**（硬编码在 `cluster.c`，`MAX_NODES = 8`）：

```c
static node_info_t g_nodes[] = {
    {"127.0.0.1", 8001},   // 节点 0
    {"127.0.0.1", 8002},   // 节点 1
    {"127.0.0.1", 8003},   // 节点 2
};
static int g_node_count = 3;
```

**转发机制**：`cluster_forward` 每次调用都会**新建一条 TCP 连接**（`connect_to_node`），`rpc_call` 收发一条消息后立刻 `close`。简单但每次都有握手开销，后续可做连接池。

**调试辅助（已实现）**：

- 转发时打印日志：`[转发] local_kv_put -> node 1 (port 8002)`（`cluster_forward` 内）
- 目标节点不可达时返回错误信息：`"forward failed: node unreachable"`，而不是空串

### 3.6 复制层（`repl.c` / `repl.h`）—— 主从复制

**职责**：给节点加角色（主/从），实现全量同步（第一步）+ 增量同步（第二步）+ 从节点禁写 + 复制日志与断线补拉（第三步）+ 心跳检测与故障转移（第四步）+ 路由表更新与客户端故障切换（第五步）。

**角色**：

- 默认 `ROLE_MASTER`（主节点，数据源）
- 启动参数 `--slaveof <master_port>` → `ROLE_SLAVE`（从节点，启动时注册 + 拉全量）

**核心函数**：

| 函数 | 作用 |
|---|---|
| `repl_init(port, slaveof_port)` | 设置角色；`slaveof_port > 0` 时找到主节点 ID |
| `repl_role()` | 返回当前角色 |
| `repl_master_id()` | 从节点的主节点 ID |
| `repl_sync_from_master()` | 从节点启动时向主节点拉全量数据并逐条写入本地 |
| `repl_register_to_master()` | 从节点向主节点注册自己（`repl_handshake`） |
| `repl_broadcast(cmd)` | 主节点把一条写命令推给所有从节点（半同步），返回确认数 |
| `repl_pull_from_master()` | 从节点向主节点拉 `seq > applied_seq` 的缺失命令并应用 |
| `repl_tick()` | 统一周期入口：心跳（每秒）+ 补拉（每 2 秒）+ 失联检测 + 故障提升 |
| `register_repl_handlers()` | 注册复制相关 RPC 方法 |

**新增 RPC 方法**：

| 方法 | 方向 | 作用 |
|---|---|---|
| `repl_full_sync` | 从 → 主 | 主节点把整个 `g_kv[]` 打包成命令串返回（每行 `key value`） |
| `repl_handshake` | 从 → 主 | 从节点注册自己，主节点记入从列表（幂等去重） |
| `repl_apply` | 主 → 从 | 主节点把一条写命令推给从节点，从节点本地执行 |
| `repl_pull(offset)` | 从 → 主 | 主节点返回复制日志里 `seq > offset` 的所有命令（每行 `seq cmd`） |
| `repl_heartbeat` | 从 → 主 | 从节点报告 `my_id + applied_seq`，主节点记录在线状态和 lag |
| `repl_status` | 任意 | 调试：主节点显示从列表健康度；从节点显示角色和进度 |
| `repl_notify_master` | 任意 | 广播/应用"分片→主节点"映射表（路由表更新） |

**第一步：全量同步流程**（从节点启动时一次性拷贝）

```
从节点启动 (--slaveof 8001)
  ↓
repl_register_to_master()      ← 先注册（主节点记入从列表）
  ↓
cluster_forward(master_id, "repl_full_sync", "", &data)   ← 复用现有转发
  ↓
主节点 kv_export() 打包 g_kv → "foo 111\nbar 222\n..."
  ↓
从节点按行 strtok_r 拆分 → kv_local_put 逐条写入
  ↓
打印 "[复制] 全量同步完成，共 N 条"
```

**第二步：增量同步 + 半同步**（每次写操作实时推给从节点）

```
client → 主节点: kv_put foo 111
  ↓
主节点 handle_kv_put:
  1. 从节点禁写检查（ROLE_SLAVE → READONLY 拒绝）
  2. 算 owner；owner == 自己 → kv_local_put 写本地
  3. repl_broadcast("kv_put foo 111")
       ↓
    遍历从列表 → cluster_forward(slave_id, "repl_apply", cmd)
       ↓
    从节点 handle_repl_apply 解析命令 → kv_local_put / kv_local_del
  ↓
返回 client: "ok"
```

**半同步语义**：主节点等所有从节点尝试推完，**≥1 个确认**即算复制成功；**0 个确认**打印警告 `没有从节点确认`，但**不阻塞写**（降级为异步，写仍成功）——这是"半同步"与"强同步"的区别，也是生产系统（MySQL 半同步、Redis 半同步）的做法。

**从节点禁写**：`kv_put` / `kv_del` 在 `ROLE_SLAVE` 节点上直接拒绝，返回：

```
READONLY You can't write against a read only slave
```

防止从节点被直接写入导致和主节点数据分叉（Redis 同款语义）。

**关键设计**：

- 从节点复用 `kv_local_put` / `kv_local_del`（`handlers.c` 导出）应用数据，存储逻辑只有一份
- 主节点用 `kv_export()`（`handlers.c`）打包全量，不新发明序列化格式
- `repl_apply` 命令格式 `"kv_put key val"` / `"kv_del key"`，**幂等**（重复执行结果不变，为断线补拉打基础）
- 同步失败最多重试 3 次（间隔 2 秒），避免主节点还没起好
- 广播只发生在 `owner == 本节点` 的写路径上，转发链上不会重复广播

**第三步：复制日志 + 断线补拉**（命令流带递增序号，从节点按 `applied_seq` 补齐）

主节点维护**复制日志**（环形缓冲），每条写命令带**递增序号 seq**；从节点记录自己已应用到的位置 `applied_seq`。正常时主节点实时 push；从节点错过命令时（断线、push 失败），靠周期 `repl_pull(offset)` 补拉。

**消息格式升级**（命令一律带 seq 前缀）：

```
repl_apply:  "3 kv_put foo 111"            ← seq + 空格 + 命令
repl_pull:   args = applied_seq，返回 "4 kv_put kv3 444\n5 kv_del bar\n..."
full_sync:   首行 "SEQ 3"，后续每行 "key value"
```

**补拉流程**（从节点断线恢复）：

```
主节点写 → 复制日志(seq=6) → push 失败（从节点不可达）→ 警告"稍后可由 pull 补上"
  ↓
从节点恢复后，server 主循环每 2 秒调 repl_pull_tick()
  ↓
repl_pull(applied_seq=5) → 主节点返回 "6 kv_put beta 666"
  ↓
从节点 apply_repl_cmd 应用，applied_seq 推进到 6
```

**关键点**：

- 命令幂等：`seq <= applied_seq` 时直接跳过（返回 `dup`），重复推/拉不会重复写
- 全量同步也带 SEQ：从节点全量后 `applied_seq = 主节点当前 seq`，之后只补增量
- 主推 + 从拉双通道：push 是实时加速，pull 是可靠兜底（生产系统如 MySQL binlog 也是从库主动拉）
- 复制日志是环形缓冲（1024 条），太旧的命令被覆盖后从节点只能重新全量（本步未做自动重全量）
- 这正是 **Raft 日志复制** 的雏形：命令日志 + 序号 + 从节点已应用位置

**第四步：心跳检测 + 故障转移**（探活 + 主挂了从顶上）

**心跳**：从节点每秒向主节点发 `repl_heartbeat <my_id> <applied_seq>`，主节点记录每个从节点的**在线状态**和**复制滞后（lag = 主节点最新 seq - 从节点 applied_seq）**。

```
从节点                   主节点
  |  repl_heartbeat 1 5  → 记录 node1: alive, applied_seq=5
  |  (每秒一次)            lag = 最新seq - 5
  |<--- ok
```

**故障检测（两方向）**：

| 谁检测 | 怎么检测 | 动作 |
|---|---|---|
| 主节点 | 某从节点 > 5 秒无心跳 | 打印 `从节点 node 1 失联`，status 显示 DEAD |
| 从节点 | 心跳连续失败 3 次（约 3 秒） | 判定主节点故障 → **自动提升为 MASTER** |

**故障转移流程**（从节点自动顶上）：

```
主节点 8001 宕机
  ↓
从节点 8002 心跳连续 3 次失败（connect 失败）
  ↓
promote_to_master():
  g_role = MASTER, 禁写解除, 心跳/补拉停发
  ↓
打印 "[复制] !!! 主节点失联，从节点提升为 MASTER，禁写解除"
  ↓
8002 继续服务：已复制的数据不丢，可直接读写
```

**关键点**：

- **数据不丢的根基**：前三步把 8001 的写命令实时复制到 8002，故障发生时 8002 已持有全部数据（lag≈0），所以"从顶上"才有意义
- **脑裂风险**：若原主 8001 恢复，会出现**双主同时写**（分叉）。真实系统用租约（lease）/仲裁（quorum）解决，本步仅打印提示、不自动处理——这是面试必问点
- 半同步升级为"**根据心跳判断从节点是否值得等**"：从节点失联时，主节点写不阻塞（已有降级逻辑），心跳让这个判断有依据
- 本步解决的是"**从节点视角**"的故障转移；"**客户端视角**"（自动重连到新主、路由表更新）在第五步完成

**第五步：客户端故障切换 + 路由表更新**（从节点视角 → 客户端视角）

**核心问题**：第四步 8002 提升为新主后，集群里其他节点（8003）和客户端都不知道——8003 还在把 owner=0 的请求转发给已挂的 8001，client 也还死连 8001。

**方案：分片 → 主节点映射表**（Redis Cluster slot 映射的雏形）：

```c
/* cluster.c：每个分片的数据当前由哪个节点服务 */
static int g_cluster_master[MAX_NODES] = {0, 1, 2};   /* 初始：分片 i 由 node i 服务 */

int cluster_master_of(int owner_id);      /* 分片 owner 当前的主节点 */
void cluster_set_master(int owner_id, int master_id);
void cluster_master_map_str(char *buf, int len);      /* 序列化 "0:1,1:1,2:2" */
int  cluster_apply_master_map(const char *map);       /* 应用广播来的映射 */
```

**故障转移后的传播流程**：

```
8001 宕机
  ↓
8002 心跳失败 → promote_to_master()
  ↓
8002 更新自己映射：cluster_set_master(0, 1)   ← 分片 0 的数据现在由我服务
  ↓
8002 广播映射表 "0:1,1:1,2:2" 给所有其他节点（repl_notify_master RPC）
  ↓
8003 收到 → cluster_apply_master_map → "分片 0 的主节点: node 0 -> node 1"
```

**路由层改造**（handlers.c）：`kv_put/kv_get/kv_del` 的转发目标从 `owner` 改为 `cluster_master_of(owner)`：

```c
int owner  = cluster_owner(key);
int master = cluster_master_of(owner);   /* 该分片当前的主节点（可能是 node 1 而不是 0） */
if (master == cluster_my_id()) {
    /* 我是该分片的主 → 本地写 + 广播到从节点 */
} else {
    return cluster_forward(master, "local_kv_put", args, result);
}
```

> 必须用 `master == my_id` 判断而不是 `owner == my_id`：8002 提升后分片 0 的主是 1（自己），否则会把自己转发给自己 → 死循环。

**客户端故障切换**（client.c）：

```c
static const int g_node_ports[] = {8001, 8002, 8003};

connect_with_failover(&index):   /* 连当前端口失败 → 轮询下一个 */
safe_call(&fd, &index, method, args):
    st = rpc_call(...);
    if (st >= 0) return st;           /* 业务错误（0/1）正常返回 */
    /* st < 0 = 网络错误 → 节点挂了 → 重连下一个节点并重试一次 */
```

**启动收敛**：节点启动时广播一次自己的映射表（`repl_announce_master_map`），让后启动的节点也能拿到最新路由。

**关键点**：

- 映射表是"**哪个分片由谁服务**"的单一事实来源：分片归属（hash）不变，变的只是"服务主节点"
- 广播是尽力而为：节点没起就打印 FAILED，不影响后续（下次启动/提升会再广播）
- 与真实系统对照：Redis Cluster 用 slot→node 映射 + gossip 协议传播；本步是它的简化版
- **剩余缺口**：脑裂防护（原主恢复）、映射表的持久化/重拉、client 无感知的自动发现（真实系统用服务发现/配置中心）

---

## 四、数据流示例

### 4.1 客户端 PUT（数据在本地节点）

```
客户端 → 节点0: kv_put bbb 222
   ↓
节点0 算 hash("bbb") % 3 = 0
节点0 的 my_id = 0，等于 owner
   ↓
节点0 直接调 local_kv_put
   ↓
存到 g_kv[]，返回 "ok"
```

### 4.2 客户端 PUT（数据在其他节点）

```
客户端 → 节点0: kv_put aaa 111
   ↓
节点0 算 hash("aaa") % 3 = 1
节点0 的 my_id = 0，不等于 owner
   ↓
节点0 通过 cluster_forward 新建连接连节点1
节点0 → 节点1: local_kv_put aaa 111
   ↓
节点1 存到 g_kv[]，返回 "ok"
   ↓
节点1 → 节点0: "ok"
节点0 → 客户端: "ok"
```

### 4.3 客户端 GET（跨节点）

```
客户端 → 节点2: kv_get aaa
   ↓
节点2 算 hash("aaa") % 3 = 1
节点2 转发给节点1
   ↓
节点1 本地查到 aaa = 111
   ↓
节点1 → 节点2: "111"
节点2 → 客户端: "111"
```

---

## 五、项目目录结构

```
distkv/
├── Makefile
├── include/
│   ├── protocol.h      # 序列化接口
│   ├── rpc.h           # RPC 接口
│   ├── cluster.h       # 集群管理
│   └── repl.h          # 主从复制（角色 + 全量/增量同步 + 禁写）
├── src/
│   ├── protocol.c      # 序列化实现
│   ├── rpc_server.c    # RPC 服务端（方法表 + 分发）
│   ├── rpc_client.c    # RPC 客户端（rpc_call）
│   ├── handlers.c      # 业务方法实现（KV + 路由 + 导出）
│   ├── cluster.c       # 集群管理 + 转发
│   ├── repl.c          # 主从复制（角色 + 全量/增量同步 + 禁写 + 防脑裂仲裁/降级）
│   ├── server.c        # 主服务端（epoll，支持端口和 --slaveof 参数）
│   └── client.c        # 测试客户端（支持端口参数）
├── tests/
│   ├── test_protocol.c # 协议测试
│   └── test_server.c   # RPC 分发测试
├── bin/                # 编译产物
└── docs/
    └── design.md       # 本文档
```

---

## 六、关键设计决策

| 决策 | 原因 |
|---|---|
| 二进制序列化而非 JSON | 学习协议原理，性能更好 |
| 长度前缀协议 | 解决 TCP 粘包问题 |
| 4 字节长度 | 支持最大 4GB 消息，足够用 |
| request_id | 匹配请求和响应，为异步做准备 |
| 方法表 + 函数指针 | 加方法只需 `rpc_register` |
| 对外方法 + 本地方法 | 路由和存储分离，转发时避免死循环 |
| 硬编码节点列表 | 简化初期复杂度，后续可改成配置文件 |
| **取模哈希（而非一致性哈希）** | **学习阶段简化实现；代价是加节点时数据基本全量迁移，生产环境应换一致性哈希** |

---

## 七、已实现的功能

- ✅ 文件 I/O、进程、线程、IPC、信号量
- ✅ epoll 多客户端服务器
- ✅ 长度前缀协议（解决粘包）
- ✅ 二进制序列化 / 反序列化
- ✅ RPC 框架（方法注册、分发、远程调用）
- ✅ 单机 KV 存储（put / get / del）
- ✅ 多节点启动（`./bin/server 8001/8002/8003`，端口决定节点 ID）
- ✅ 节点 ID 显示（启动时打印自己是几号 + 集群节点列表）
- ✅ `route` 调试方法（查 key 归哪个节点）
- ✅ 简单取模哈希路由（`hash % 节点数`）
- ✅ 节点间转发（`cluster_forward`，带日志和失败提示）
- ✅ 主从复制第一步：角色（`--slaveof`）+ 启动时全量同步（`repl_full_sync`）
- ✅ 主从复制第二步：增量同步（`repl_apply` + 半同步确认）+ 从节点禁写（READONLY）
- ✅ 主从复制第三步：复制日志（递增 seq）+ `applied_seq` + 断线补拉（`repl_pull`）+ `repl_status` 调试
- ✅ 主从复制第四步：心跳检测（`repl_heartbeat` + 失联/恢复检测）+ 故障转移（从节点自动提升 MASTER）
- ✅ 主从复制第五步：分片→主节点映射表（`cluster_master_of`）+ 提升广播映射（`repl_notify_master`）+ 路由感知新主 + 客户端故障重连（`connect_with_failover` / `safe_call`）
- ✅ 主从复制第六步：防脑裂（提升前仲裁 `repl_probe` + 恢复自动降级 `repl_check_demote_on_startup` / `repl_demote_to_slave`）+ RPC 超时（SO_RCVTIMEO，短超时辅助调用）+ SIGPIPE 修复
---

## 七点五、防脑裂（第六步）详解

**脑裂是什么**：主节点"假死"（网络分区 / 进程卡死但内核还活着）时，从节点心跳失败就提升，
原主恢复后出现**两个 MASTER**，数据各写各的、永远分叉。本步用两道保险解决
（对 3 节点集群够用；生产环境应上租约 / Quorum / Raft）。

### 保险 A：提升前仲裁（`repl_probe_confirm_dead`）

从节点心跳连续 3 次失败后**不立即提升**，先向集群第三方节点求证："主节点真的不可达吗？"

- 新 RPC `repl_probe <target>`：被问方**亲自 connect 目标**，连得上回 `alive`，连不上回 `dead`
- 判定规则：
  - **≥1 个第三方证实 dead**（自己 1 票 + 它 1 票 = 3 节点中的 2 票多数派）→ 允许提升
  - **有人证实 alive**（说明是网络分区、主还活着）→ 暂不提升、重置 miss 计数
  - **问不到任何第三方**（都连不上）→ 无证词不冒险
- 关键设计：`connect` 成功只证明**内核在监听**（进程假死时也成功），所以宁可把假死判成
  alive 也不冒险当主——"错误的保守比错误的激进安全"

### 保险 B：恢复自动降级（`repl_check_demote_on_startup`）

原主恢复（重启）时**先听后说**：

1. 遍历集群其他节点调 `repl_map`，拿它们当前的分片映射并应用
2. 若发现自己负责的分片已被接管（`cluster_master_of(own) != own`）→
   调 `repl_demote_to_slave(接管者)` 自动降级：清空从列表 + `kv_clear()` 清旧数据，
   交给 server.c 统一的从节点启动流程做**注册 + 全量同步**，数据从新主补齐
3. 确认自己仍是主之后，才 `repl_announce_master_map()` 广播映射
   （防止旧映射污染集群路由）

### 配套修复（本步踩的两个大坑）

| 坑 | 现象 | 修复 |
|---|---|---|
| 双向同步 RPC 死锁 | 8001 在 handler 里同步等 8002 的 push 响应，8002 又在等 8001 的心跳响应，互相等 → 请求积压超时、从节点心跳误判 | `rpc_call_timeout`（SO_RCVTIMEO）；心跳/补拉等辅助调用用短超时（300ms）快速失败，事件循环立刻恢复处理对方请求 |
| SIGPIPE 崩溃 | 心跳超时的从节点已 close，主节点迟到的响应 `send` 触发 SIGPIPE → 整个进程崩溃、端口消失、被误判为 dead | `signal(SIGPIPE, SIG_IGN)`，`send` 只返回 EPIPE 由 `send_message` 正常处理 |

### E2E 验证（`repl_test6.sh` 三场景全绿）

- **场景 A 真死**（`kill 8001`）→ 8002 仲裁拿 dead 证词 → 正常提升，禁写解除
- **场景 B 假死**（`SIGSTOP 8001`，内核还在监听）→ 8002 仲裁拿 alive 证词 → **暂不提升**，仍 SLAVE
- **场景 C 原主恢复** → 8001 先听后说 → 自动降级为 8002 的从节点 → 全量补齐 → 禁写生效（双主消除）


---

## 八、待实现的功能

| 功能 | 目的 | 难度 |
|---|---|---|
| 路由表持久化 | 节点重启后恢复最新映射（当前靠启动广播 + 恢复时"先听后说"） | 中 |
| 服务发现/配置中心 | 客户端无感知地发现当前主节点 | 高 |
| Raft 共识 | 强一致性 | 很高 |
| 持久化 | 重启不丢数据 | 中 |
| 配置文件 | 动态节点列表 | 低 |
| 连接池 | 复用节点间连接，减少握手开销 | 中 |
| 日志系统 | 调试和监控 | 低 |
| 性能优化 | 批量写、零拷贝 | 高 |

---

## 九、学习收获

通过这个项目，我掌握了：

### 9.1 系统编程

- 文件描述符的本质
- 进程/线程的创建和同步
- 进程间通信（管道、共享内存、信号、信号量）
- 网络编程（socket、字节序、粘包）

### 9.2 高性能 I/O

- 阻塞 vs 非阻塞
- IO 多路复用（epoll）
- 事件驱动编程
- 状态机处理协议

### 9.3 分布式系统

- 序列化与协议设计
- RPC 框架原理
- 数据分片
- 取模哈希路由（及与一致性哈希的对比）
- 节点间通信与请求转发

### 9.4 工程能力

- Makefile 组织多文件项目
- 头文件和源文件分离
- 模块化设计
- 调试工具（gdb、valgrind、ss）

---

## 十、遇到的典型问题和解决方案

| 问题 | 原因 | 解决 |
|---|---|---|
| `recv: Transport endpoint is not connected` | 对 `listen_fd` 调了 `recv` | 用 `else` 分支区分 listen_fd 和 client_fd |
| 粘包 | TCP 是字节流 | 长度前缀协议 |
| 字节序错误 | x86 是小端 | `htonl` / `ntohl` |
| `double free` | 出错路径没置 NULL | `deserialize` 开头清空 out，出错用 `message_free` |
| `implicit declaration of 'malloc'` | 缺头文件 | `#include <stdlib.h>` |
| 内存泄漏 | 忘了 free | 每个 `malloc` 都要有对应 `free` |
| 节点ID = -1 | 忘了调 `cluster_init`，或端口不在集群配置 | 在 `main` 开头调 `cluster_init(port)`，且必须传 8001/8002/8003 |
| SIGPIPE 导致进程崩溃 | 对端关闭后 `send` 触发 SIGPIPE | `signal(SIGPIPE, SIG_IGN)`，网络服务器标配 |
| 双向同步 RPC 互相等待（死锁） | 单线程事件循环里 handler 做同步 RPC，双方互相等响应 | RPC 超时参数（SO_RCVTIMEO）+ 心跳/补拉用短超时（300ms） |
| **client 拿到空结果** | key 路由到未启动的节点，`cluster_forward` connect 失败 | 三个节点都要启动；转发失败现在会返回 `"forward failed: node unreachable"` |

---

## 十一、如何运行

### 编译

```bash
cd ~/projects/distkv
make
```

### 启动 3 个节点（必须传端口参数，端口决定节点 ID）

```bash
# 终端 1 → 节点 0
./bin/server 8001
# 终端 2 → 节点 1
./bin/server 8002
# 终端 3 → 节点 2
./bin/server 8003
```

启动后每个终端会打印自己的身份和集群列表：

```
节点 0/3 启动，监听端口 8001
  集群节点: [0]=127.0.0.1:8001, [1]=127.0.0.1:8002, [2]=127.0.0.1:8003
节点启动，监听端口 8001...
```

> 注意：直接跑 `./bin/server`（默认 8888）不在集群配置里，`cluster_my_id()` 会返回 -1，路由会出错。

### 运行客户端（也支持端口参数）

```bash
# 终端 4，连 8001
./bin/client 8001
```

客户端内置测试序列：

```bash
echo hello rpc        # 原样返回
add 3 5               # → 8
add abc               # → invalid args
unknown               # → method not found
route name            # → key="name" -> node 1 (127.0.0.1:8002), 本节点=node 0 [转发]
route foo             # → key="foo" -> node 0 (127.0.0.1:8001), 本节点=node 0 [本地]
kv_put name alice     # 存储（按 key 路由到节点 1）
kv_get name           # 读取 → alice
kv_del name           # 删除 → deleted
kv_get name           # → not found
```

client 会对 5 个 key（name/foo/user/test/hello）各打一次 `route`，直观看到不同 key 落到不同节点。

### 主从复制（角色 + 全量同步 + 增量同步 + 断线补拉 + 心跳与故障转移 + 路由表更新）

```bash
# 终端 1：主节点
./bin/server 8001
# 终端 2：从节点（复制 8001）
./bin/server 8002 --slaveof 8001
# 终端 3：普通节点（可选，保证集群路由完整）
./bin/server 8003
```

从节点启动后会看到：

```
节点 1/3 启动，监听端口 8002 [SLAVE]
  复制目标: node 0 (port 8001)
节点启动，监听端口 8002...
  [转发] repl_handshake -> node 0 (port 8001)
[复制] 向主节点注册自己: ok
[复制] 向 node 0 (port 8001) 拉取全量数据...
  [转发] repl_full_sync -> node 0 (port 8001)
[复制] 全量同步完成，共 N 条 (applied_seq=N)
```

主节点每次写（`kv_put`/`kv_del`）后推送日志（带 seq）：

```
  [复制] push seq=4 -> node 1: ack
```

从节点断线期间主节点继续写（push 失败）会提示：

```
  [复制] push seq=6 -> node 1: FAILED
  [复制] 警告：没有从节点确认（共 1 个从），seq=6 未复制（稍后可由 pull 补上）
```

从节点恢复后每 2 秒自动补拉缺失命令（无新命令时静默）：

```
[复制] pull 补到 2 条 (applied_seq=8)
```

查复制状态（调试）：

```bash
./bin/client 8002     # 或直接连任意节点
# 通过 RPC 调 repl_status → role=SLAVE applied_seq=8
```

从节点被直接写会被拒绝：

```
READONLY You can't write against a read only slave
```

**验证**：主节点上写的数据，在从节点上能直接读到（直连从节点调 `local_kv_get`，不走路由）；从节点禁写。

**心跳与故障转移**（第四步）：

- 主节点查从节点健康度：`repl_status` → `role=MASTER slaves=1 [node1: alive, lag=0, 0s]`
- 从节点查自己：`repl_status` → `role=SLAVE applied_seq=5 master=0(port 8001)`
- 从节点挂掉 > 5 秒 → 主节点打印 `[复制] 从节点 node 1 失联（>5s 无心跳），lag=0`
- 从节点重启 → 主节点打印 `[复制] 从节点 node 1 恢复在线`
- 主节点挂掉 → 从节点心跳连续 3 次失败 → 自动提升：

```
  [复制] !!! 主节点失联（连续 3 次心跳失败），从节点提升为 MASTER，禁写解除
  [复制] !!! 注意：若原主恢复会出现双主脑裂，请人工处理
```

提升后新主 `repl_status` → `role=MASTER`，可直接读写（已复制的数据不丢）。

**路由表更新与客户端故障切换**（第五步）：

- 8002 提升后自动广播映射表：`[路由] 广播映射表: 0:1,1:1,2:2`，并通知集群其他节点
- 8003 收到后打印：`[路由] 分片 0 的主节点: node 0 -> node 1`，之后 owner=0 的请求转发到 8002：

```
[转发] local_kv_get -> node 0 (port 8001)   ← 提升前：转发到旧主 8001
[转发] local_kv_get -> node 1 (port 8002)   ← 提升后：转发到新主 8002
```

- `route` 输出现在带主节点信息：

```
route foo: key="foo" -> 分片 node 0，主节点 node 1 (127.0.0.1:8002), 本节点=node 1 [本地]
```

- client 故障切换：连已挂节点会自动轮询重连（`./bin/client 8001` 在 8001 挂掉时自动切到 8002）：

```
connect: Connection refused
[故障切换] 节点 8001 不可用，尝试下一个节点...
已连接服务端 127.0.0.1:8002
```

### 查看节点状态

```bash
ss -tlnp | grep -E '8001|8002|8003'   # 查看监听
```

---

## 十二、下一步计划

1. **短期**：~~脑裂防护~~（已实现：提升前仲裁 + 恢复自动降级）；接下来可做**路由表持久化**（重启不丢映射）。
2. **中期**：复制日志覆盖后自动重新全量。
3. **长期**：Raft 共识，实现强一致性。
4. **可选**：持久化、配置文件、连接池、性能优化。

> 已完成：节点 ID 显示、`route` 调试方法、转发失败提示、转发日志、主从复制（角色 + 全量同步 + 增量同步 + 从节点禁写 + 复制日志 + 断线补拉 + 心跳检测 + 故障转移 + 路由表更新 + 客户端故障切换）。

---

## 十三、参考资料

| 主题 | 资源 |
|---|---|
| 系统编程 | 《Linux/UNIX 系统编程手册》 |
| 网络编程 | 《UNIX 网络编程 卷1》 |
| 分布式 | 《数据密集型应用系统设计》 |
| RPC | gRPC 文档 |
| 一致性 | MIT 6.824 |
| epoll | `man 7 epoll` |

---

## 十四、项目总结

这个项目从一个简单的 TCP echo 服务器开始，逐步演进为：

- 支持多客户端并发（epoll）
- 有完整协议栈（序列化 + RPC）
- 能跨多节点分片存储（取模哈希路由 + 转发）

**核心收获**：理解了分布式系统不是"高深莫测"的，而是由很多简单的机制组合而成——网络通信、序列化、路由、转发。

**下一步的关键**：从"能跑"到"可靠"——复制、容错、一致性。

---

## 保存方式

把这份笔记保存到 `~/projects/distkv/docs/design.md`：

```bash
cd ~/projects/distkv
code docs/design.md
```

把上面内容粘进去，保存。以后每次回顾、写简历、面试准备，都能直接翻出来看。
