# distkv — 分布式 KV 存储（C 语言）

distkv 是一个用 C 语言从零实现的分布式 Key-Value 存储系统（教学/实践项目）。它在单线程事件循环上实现了 3 节点集群 + 主从复制，覆盖了分布式系统最核心的几个主题：

- 分片路由（key 按哈希归属节点）
- 自定义二进制协议与 RPC 方法表
- 主从复制（全量同步 + 增量复制 + 断线补拉）
- 心跳检测与主节点故障自动提升
- 防脑裂（启动降级检查 + 路由表广播收敛）

## 核心特性

| 特性 | 说明 |
|---|---|
| 3 节点集群 | 节点端口 8001/8002/8003，key 按哈希分片到节点 |
| 主从复制 | `--slaveof <master_port>` 启动从节点；启动时全量同步，运行期实时半同步推送 |
| 复制日志 | 每条写命令带递增序号 seq；从节点按 applied_seq 幂等应用，断线后按 seq 补拉 |
| 从节点禁写 | 写请求返回 READONLY，读请求照常服务 |
| 心跳与故障转移 | 从节点每秒心跳（携带 applied_seq）；连续失败自动提升为 MASTER |
| 防脑裂 | 启动时"先听后说"检查降级；分片→主节点映射广播收敛；忽略 SIGPIPE |
| RPC 超时 | 默认 1s；心跳/补拉等辅助调用用短超时，避免单线程双向同步 RPC 互相等待造成死锁 |
| 客户端故障切换 | 按 8001 → 8002 → 8003 顺序尝试，连接/调用失败自动切换下一个节点 |

## 目录结构

```
distkv/
├── src/                      # 服务端与客户端源码
│   ├── server.c              # 主循环（epoll 事件驱动）
│   ├── rpc_server.c          # RPC 服务端（监听/接收/分发）
│   ├── rpc_client.c          # RPC 客户端（发送/接收，支持超时）
│   ├── protocol.c            # 二进制协议序列化/反序列化
│   ├── handlers.c            # RPC 方法实现（echo/add/route/kv_put/kv_get/kv_del）
│   ├── cluster.c             # 分片路由、请求转发、节点探活
│   ├── repl.c                # 主从复制（全量/增量/心跳/故障转移/防脑裂）
│   └── client.c              # 命令行演示客户端（自动故障切换）
├── include/                  # 头文件
│   ├── protocol.h            # 消息结构与命令类型
│   ├── rpc.h                 # RPC 方法表与超时
│   ├── cluster.h             # 分片路由与主节点映射
│   └── repl.h                # 复制角色与同步接口
├── docs/
│   └── design.md             # 详细设计文档
├── tests/
│   ├── test_protocol.c       # 协议往返测试
│   └── test_server.c         # 服务端方法测试
├── Makefile
└── .gitignore                # 排除 bin/ 与 *.o
```

## 构建

依赖：Linux + gcc + make（项目在 WSL/Ubuntu 上开发调试）。

```bash
make          # 生成 bin/server bin/client bin/test_protocol bin/test_server
make clean    # 清理编译产物
```

## 运行

**1. 启动 3 节点集群（开 3 个终端）**

```bash
# 终端 1：主节点
./bin/server 8001

# 终端 2：从节点（挂到 8001）
./bin/server 8002 --slaveof 8001

# 终端 3：从节点（挂到 8001）
./bin/server 8003 --slaveof 8001
```

> 说明：server 默认端口是 8888；集群演示时用 8001/8002/8003 显式指定，与客户端的节点列表一致。

**2. 运行客户端**（自动连接 8001/8002/8003，故障时自动切换）

```bash
./bin/client
```

客户端会依次演示：echo、add、route（查看 key 分片归属）、kv_put、kv_get、kv_del。

**3. 运行测试**

```bash
./bin/test_protocol   # 协议序列化往返测试
./bin/test_server     # 服务端方法测试
```

## RPC 方法

| 方法 | 参数 | 说明 |
|---|---|---|
| echo | 任意字符串 | 原样返回 |
| add | "a b" | 返回 a + b |
| route | key | 返回该 key 归属的节点 ID |
| kv_put | "key value" | 写入 KV；写操作触发主从半同步广播 |
| kv_get | key | 读取 KV |
| kv_del | key | 删除 KV |

## 文档

详细设计（协议格式、集群分片、复制流程、防脑裂的演进过程）见 [docs/design.md](docs/design.md)。

## 说明

这是一个以"把分布式原理真正跑起来"为目标的学习项目，代码注释里记录了每一步的设计决策（例如：为什么辅助 RPC 要用短超时避免死锁、为什么 connect 成功不等于进程存活、防脑裂为什么要"先听后说"）。

License：暂无（学习项目，如需开源请自行补充 LICENSE 文件）。
