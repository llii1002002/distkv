CC      = gcc
CFLAGS  = -Wall -g -Iinclude
LDFLAGS = -pthread

all: bin/server bin/client bin/test_protocol bin/test_server

bin/server: src/server.c src/protocol.c src/rpc_server.c src/rpc_client.c src/handlers.c src/cluster.c src/repl.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

bin/client: src/client.c src/protocol.c src/rpc_client.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

bin/test_protocol: tests/test_protocol.c src/protocol.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

bin/test_server: tests/test_server.c src/rpc_server.c src/rpc_client.c src/handlers.c src/cluster.c src/repl.c src/protocol.c
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f bin/server bin/client bin/test_protocol bin/test_server

.PHONY: all clean