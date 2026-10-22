#include <stdlib.h>
#include "protocol.h"
#include <string.h>
#include <arpa/inet.h>


/* 序列化 */
int serialize(const message_t *msg, char *buf, size_t buf_size) {
    size_t need = 4 + 1+ 4 + msg->method_len + 4 + msg->args_len;
    if (buf_size < need) return -1;
    size_t offset = 0;

    //request_len（4字节，网络序）
    uint32_t id_net=htonl(msg->request_id);
    memcpy(buf + offset,&id_net,4);
    offset += 4;

    /* 1 字节 status */
    buf[offset++] = (char)msg->status;


    /* 4 字节 method_len（网络序） */
    uint32_t mlen_net = htonl((uint32_t)msg->method_len);
    memcpy(buf + offset, &mlen_net, 4);
    offset += 4;

    /* method */
    if (msg->method_len > 0) {
        memcpy(buf + offset, msg->method, msg->method_len);
        offset += msg->method_len;
    }

    /* 4 字节 args_len（网络序） */
    uint32_t alen_net = htonl((uint32_t)msg->args_len);
    memcpy(buf + offset, &alen_net, 4);
    offset += 4;

    /* args */
    if (msg->args_len > 0) {
        memcpy(buf + offset, msg->args, msg->args_len);
        offset += msg->args_len;
    }

    return (int)offset;
}

/* 反序列化 */
int deserialize(const char *buf, size_t buf_len, message_t *out) {
    
        //结构体清空，防止野指针；
        out->method=NULL;
        out->args=NULL;
        out->method_len=0;
        out->args_len=0;
    
    //buf_len4字节，可以判断阻拦
    if (buf_len < 4) return -1;

    
    size_t offset = 0;

    /* request_id (4byte,网络序)*/
    if(buf_len<offset+4) return -1;
    uint32_t id_net;
    memcpy(&id_net,buf+offset,4);
    offset += 4;
    out->request_id=ntohl(id_net);

    //status
    if(buf_len<offset+1) return -1;
    out->status = (uint8_t)buf[offset++];

    /* method_len */
    if (buf_len < offset + 4) return -1;
    uint32_t mlen_net;
    memcpy(&mlen_net, buf + offset, 4);
    offset += 4;
    uint32_t method_len = ntohl(mlen_net);

    /* 合法性检查：防止恶意长度 */
    if (method_len > 1024 * 1024) return -1;
    if (buf_len < offset + method_len) return -1;

    /* method：分配并拷贝 */
    out->method = malloc(method_len + 1);
    if (!out->method) return -1;
    memcpy(out->method, buf + offset, method_len);
    out->method[method_len] = '\0';
    out->method_len = method_len;
    offset += method_len;

    /* args_len */
    if (buf_len < offset + 4) {
        free(out->method);
        out->method = NULL;
        return -1;
    }
    uint32_t args_len_net;
    memcpy(&args_len_net, buf + offset, 4);
    offset += 4;
    uint32_t args_len = ntohl(args_len_net);

    if (args_len > 1024 * 1024) {
        free(out->method);
        out->method = NULL;
        return -1;
    }
    if (buf_len < offset + args_len) {
        free(out->method);
        out->method = NULL;
        return -1;
    }

    /* args */
    out->args = malloc(args_len + 1);
    if (!out->args) {
        free(out->method);
        out->method=NULL;
        return -1;
    }
    memcpy(out->args, buf + offset, args_len);
    out->args[args_len] = '\0';
    out->args_len = args_len;
    offset += args_len;

    return (int)offset;
}

/* 释放 */
void message_free(message_t *msg) {
    if (msg->method)   { free(msg->method);   msg->method = NULL; }
    if (msg->args) { free(msg->args); msg->args = NULL; }
    msg->method_len = 0;
    msg->args_len = 0;
}