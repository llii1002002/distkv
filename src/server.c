#include "rpc.h"
#include "cluster.h"
#include "repl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <signal.h>

#define BUF_SIZE   1024
#define MAX_EVENTS 64
#define MAX_PAYLOAD (1024*1024)

//连接状态
typedef enum{
    STATE_READ_LEN,
    STATE_READ_PAYLOAD,
}recv_state_t;

typedef struct 
{
    int fd;
    recv_state_t state;
    uint32_t payload_len;
    char *payload;
    size_t payload_recv;
}connection_t;

static int send_message(int fd,const void  *payload,uint32_t len){
    uint32_t len_net=htonl(len);
    ssize_t n=send(fd,&len_net,sizeof(len_net),0);
    if(n!=sizeof(len_net)){
        perror("send length");
        return -1;
    }
    const char *p=payload;
    size_t total=0;
    while (total<len)
    {
        n=send(fd,p+total,len-total,0);
        if(n<0){
            if (errno==EAGAIN||errno==EWOULDBLOCK)
            {
                continue;
            }
            perror("send payload");
            return -1;
        }
        total += n;
    }
    return 0;
}
//处理连读事件
//返回0正常-1关闭
static int handle_read(connection_t *conn){
    while(1){
        if(conn->state==STATE_READ_LEN){
            uint32_t len_net;
            ssize_t n=recv(conn->fd,&len_net,sizeof(len_net),0);
            if(n<0){
                if(errno==EAGAIN||errno==EWOULDBLOCK)return 0;
                perror("recv len");
                return  -1;
            }
            if(n==0)return -1;
            if(n!=sizeof(len_net)){
                continue;
            }
            conn->payload_len=ntohl(len_net);
            if(conn->payload_len==0||conn->payload_len>MAX_PAYLOAD){
                fprintf(stderr,"非法长度： %u\n",conn->payload_len);
                return -1;
            }
            conn->payload=malloc(conn->payload_len+1);
            if(!conn->payload){
                
                perror("malloc payload");
                return -1;
            }
            conn->payload_recv=0;
            conn->state=STATE_READ_PAYLOAD;
        }else{
            size_t need=conn->payload_len-conn->payload_recv;
            ssize_t n=recv(conn->fd,conn->payload+conn->payload_recv,need,0);
            if(n<0){
                if(errno==EAGAIN||errno==EWOULDBLOCK)return 0;
                perror("recv payload");
                return -1;
            }
            if(n==0)return -1;
            conn->payload_recv+=n;
            if(conn->payload_recv==conn->payload_len){
                /* 收到完整消息 */
                printf("fd=%d 收到完整消息(%u 字节)\n", conn->fd, conn->payload_len);
                /* 反序列化 */
                message_t req = {0};
                if (deserialize(conn->payload, conn->payload_len, &req) < 0) {
                    fprintf(stderr, "反序列化失败\n");
                    free(conn->payload);
                    conn->payload = NULL;
                    conn->state = STATE_READ_LEN;
                    continue;
                }

                /* 分发 */
                message_t resp = {0};
                rpc_dispatch(&req, &resp);

                /* 序列化响应并发送 */
                char resp_buf[4096];
                int resp_len = serialize(&resp, resp_buf, sizeof(resp_buf));
                if (resp_len > 0) {
                    send_message(conn->fd, resp_buf, resp_len);
                }

                /* 释放 */
                message_free(&req);
                message_free(&resp);
                free(conn->payload);
                conn->payload = NULL;
                conn->state = STATE_READ_LEN;
            }
        }
    }
}




static int set_nonblocking(int fd){
    int flags=fcntl(fd,F_GETFL,0);
    if(flags<0) return -1;
    return fcntl(fd,F_SETFL,flags | O_NONBLOCK);
}

int main(int argc, char *argv[]){
    setvbuf(stdout, NULL, _IONBF, 0);   /* 让 printf 立即输出，不缓冲 */

    /* 忽略 SIGPIPE（防脑裂第六步关键修复）：
     * 网络服务器的标准做法。对方（如心跳超时的从节点）关闭连接后，
     * 我们的 send 会触发 SIGPIPE 导致整个进程崩溃；
     * 忽略后 send 只返回 EPIPE（-1），由 send_message 正常处理，进程存活 */
    signal(SIGPIPE, SIG_IGN);

    int port = 8888;   /* 默认端口 */
    int slaveof_port = 0;   /* 主从复制：--slaveof <master_port> */
    if (argc >= 2) {
        port = atoi(argv[1]);
    }
    for (int i = 2; i + 1 < argc; i++) {
        if (strcmp(argv[i], "--slaveof") == 0) {
            slaveof_port = atoi(argv[i + 1]);
        }
    }
    int listen_fd,epoll_fd;
    struct sockaddr_in server_addr;
    struct epoll_event ev,events[MAX_EVENTS];
    cluster_init(port);
    repl_init(port, slaveof_port);
    printf("节点 %d/%d 启动，监听端口 %d [%s]\n",
           cluster_my_id(), cluster_count(), port,
           repl_role() == ROLE_MASTER ? "MASTER" : "SLAVE");
    if (repl_role() == ROLE_SLAVE) {
        printf("  复制目标: node %d (port %d)\n", repl_master_id(), slaveof_port);
    }
    printf("  集群节点:");
    for (int i = 0; i < cluster_count(); i++) {
        printf(" [%d]=127.0.0.1:%d", i, cluster_node_port(i));
    }
    printf("\n");
    register_all_handlers();
    register_repl_handlers();

    /* 防脑裂 B：先听后说——启动时先查询集群，若发现自己已被顶替则自动降级为从 */
    repl_check_demote_on_startup();

    /* 确认自己仍是主（或集群无接管者）后，才广播映射让路由收敛 */
    repl_announce_master_map();

    listen_fd = socket(AF_INET,SOCK_STREAM,0);
    if (listen_fd<0)
    {
        perror("socker");
        return 1;
    }
    
    int opt = 1;
    setsockopt(listen_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    //绑定地址
    memset(&server_addr,0,sizeof(server_addr));
    server_addr.sin_family=AF_INET;
    server_addr.sin_addr.s_addr=htonl(INADDR_ANY);
    server_addr.sin_port=htons(port);

    if(bind(listen_fd,(struct sockaddr *)&server_addr,sizeof(server_addr))<0){
        perror("bind"); return 1;
    }

//监听
    if(listen(listen_fd,128)<0){
        perror("listen");return 1;
    }

//设为非阻塞
    set_nonblocking(listen_fd);


//创建epoll实例
    epoll_fd = epoll_create1(0);
    if(epoll_fd<0){perror("epoll_create1"); return 1;}

//把监听socket加入epoll
    ev.events=EPOLLIN;
    ev.data.fd=listen_fd;
    if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,listen_fd,&ev)<0){
        perror("epoll_ctl:listen_fd");return 1;
    }

    printf("节点启动，监听端口 %d...\n",port);

    /* 从节点：先向主节点注册，再拉全量（最多重试 3 次） */
    if (repl_role() == ROLE_SLAVE) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            repl_register_to_master();   /* 幂等：重复注册会被主节点去重 */
            if (repl_sync_from_master() == 0) break;
            if (attempt < 3) {
                printf("[复制] 2 秒后重试 (%d/3)...\n", attempt);
                sleep(2);
            } else {
                printf("[复制] 重试失败，作为空从节点启动\n");
            }
        }
    }

    //主循环
    while (1)
    {
        /* 500ms 超时：让主/从节点周期性执行心跳、补拉、失联检测（repl_tick） */
        int nready=epoll_wait(epoll_fd,events,MAX_EVENTS,500);
        if(nready<0){
                if(errno==EINTR) continue;
                perror("epoll_wait");
                break;
        }
        //遍历就绪事件
        for(int i=0;i<nready;i++){
            int fd=events[i].data.fd;

            //处理新连接
            if(fd==listen_fd){
                struct sockaddr_in client_addr;
                socklen_t client_len=sizeof(client_addr);
                int client_fd=accept(listen_fd,(struct sockaddr *)&client_addr,&client_len);
                if(client_fd<0){
                    if(errno==EAGAIN||errno==EWOULDBLOCK)continue;
                    perror("accept");
                    continue;
                }
                //打印客户信息 
                set_nonblocking(client_fd);
                
                connection_t *conn = calloc(1, sizeof(connection_t));
                if (!conn) { close(client_fd); continue; }
                conn->fd = client_fd;
                conn->state = STATE_READ_LEN;

                char ip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &client_addr.sin_addr, ip, sizeof(ip));
                printf("新客户端：%s:%d, fd=%d\n",
                       ip, ntohs(client_addr.sin_port), client_fd);

                /* data.ptr 存 connection_t 指针 */
                ev.events = EPOLLIN;
                ev.data.ptr = conn;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    perror("epoll_ctl: client_fd");
                    close(client_fd);
                    free(conn);
                }
            }else {
                connection_t *conn = events[i].data.ptr;
                if (handle_read(conn) < 0) {
                    printf("fd=%d 断开连接\n", conn->fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn->fd, NULL);
                    close(conn->fd);
                    if (conn->payload) free(conn->payload);
                    free(conn);
                }
            }
        }
        /* 心跳 + 补拉 + 失联检测 + 故障提升（repl_tick） */
        repl_tick();
    }
    //退出清理
    close(epoll_fd);
    close(listen_fd);
    return 0;
}