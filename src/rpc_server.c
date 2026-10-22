#include "rpc.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define MAX_METHODS 32

static rpc_method_t g_methods[MAX_METHODS];
static int          g_method_count = 0;

void rpc_register(const char *name, rpc_handler_t handler) {
    if (g_method_count >= MAX_METHODS) return;
    g_methods[g_method_count].name    = name;
    g_methods[g_method_count].handler = handler;
    g_method_count++;
}

static rpc_handler_t find_handler(const char *name) {
    for (int i = 0; i < g_method_count; i++) {
        if (strcmp(g_methods[i].name, name) == 0) {
            return g_methods[i].handler;
        }
    }
    return NULL;
}

void rpc_dispatch(const message_t *req, message_t *resp) {
    memset(resp, 0, sizeof(*resp));
    resp->request_id = req->request_id;

    rpc_handler_t h = find_handler(req->method);
    if (!h) {
        resp->status = RPC_ERR_METHOD;
        resp->method = strdup(req->method);
        resp->method_len = req->method_len;
        resp->args = strdup("method not found");
        resp->args_len = strlen(resp->args);
        return;
    }

    char *result = NULL;
    int status = h(req->args, &result);

    resp->status = (uint8_t)status;
    resp->method = strdup(req->method);
    resp->method_len = req->method_len;
    if (result) {
        resp->args = result;
        resp->args_len = strlen(result);
    } else {
        resp->args = strdup("");
        resp->args_len = 0;
    }
}