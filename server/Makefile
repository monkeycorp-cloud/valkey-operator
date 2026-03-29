TARGET  = valkey-operator-module.so
SRCS    = module.c health.c bootstrap.c failover.c topology.c cluster_safe.c node_ready.c gate.c slot_stats.c node_state.c
CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -fPIC -std=c99 -I. -D_POSIX_C_SOURCE=200809L
LDFLAGS = -shared -o $(TARGET)

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(SRCS)

clean:
	rm -f $(TARGET)
