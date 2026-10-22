#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stddef.h>

/* 命令类型 */
#define CMD_PUT  0x01
#define CMD_GET  0x02
#define CMD_DEL  0x03
#define CMD_OK   0x80
#define CMD_ERR  0x81

#define STATUS_OK    0
#define STATUS_ERROR 1

/* 消息结构 */
typedef struct {
    uint32_t request_id;
    uint8_t status;
    char *method;
    size_t method_len;
    char *args;
    size_t args_len;
} message_t;

int  serialize(const message_t *msg, char *buf, size_t buf_size);
int  deserialize(const char *buf, size_t buf_len, message_t *out);
void message_free(message_t *msg);

#endif