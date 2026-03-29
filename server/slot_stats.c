/**
 * slot_stats.c — OPERATOR.SLOT.STATS command implementation.
 *
 * Returns per-shard memory and key statistics for this node without any
 * expensive operation (no SCAN, no KEYS). All data comes from:
 *   - INFO all       : keyspace keys, used_memory, role, replication offset
 *   - CLUSTER NODES  : slot ranges owned by this node ("myself" line)
 *
 * The operator calls this on every primary to detect slot imbalance across
 * shards and trigger a valkey-cli --cluster rebalance Job when needed.
 *
 * Returns a flat array of 10 elements (5 key-value pairs):
 *   ["keys",         N,
 *    "memory_bytes", N,
 *    "role",         "primary" | "replica",
 *    "slots",        "0-5460 10000-10922",
 *    "repl_offset",  N]
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>

#define INFO_BUF_SIZE (64 * 1024)   /* INFO all can be large */
#define NODES_BUF_SIZE (32 * 1024)
#define SLOTS_BUF_SIZE 256

static long long parse_field_ll(const char *info, const char *field) {
    const char *p = strstr(info, field);
    if (!p) return -1;
    p += strlen(field);
    if (*p != ':') return -1;
    return atoll(p + 1);
}

static int parse_field_str(const char *info, const char *field, char *buf, size_t buflen) {
    const char *p = strstr(info, field);
    if (!p) return -1;
    p += strlen(field);
    if (*p != ':') return -1;
    p++;
    size_t i = 0;
    while (*p && *p != '\r' && *p != '\n' && i < buflen - 1) {
        buf[i++] = *p++;
    }
    buf[i] = '\0';
    return 0;
}

/* extract_keyspace_keys parses "db0:keys=N,expires=..." from INFO keyspace.
 * Returns 0 if db0 is absent (no keys in this shard). */
static long long extract_keyspace_keys(const char *info) {
    const char *p = strstr(info, "db0:keys=");
    if (!p) return 0;
    p += strlen("db0:keys=");
    return atoll(p);
}

/* extract_myself_slots parses the CLUSTER NODES output and collects all
 * slot ranges from the line that contains the "myself" flag.
 * Result is written into buf as space-separated ranges, e.g. "0-5460 10000-10922".
 * Returns 0 on success, -1 if the myself line is not found. */
static int extract_myself_slots(const char *nodes, char *buf, size_t buflen) {
    buf[0] = '\0';
    const char *line = nodes;
    while (line && *line) {
        /* Find end of line. */
        const char *eol = strstr(line, "\n");
        size_t linelen = eol ? (size_t)(eol - line) : strlen(line);

        /* Check if this line contains "myself". */
        char linecopy[1024];
        if (linelen >= sizeof(linecopy)) {
            linelen = sizeof(linecopy) - 1;
        }
        memcpy(linecopy, line, linelen);
        linecopy[linelen] = '\0';

        if (strstr(linecopy, " myself") || strstr(linecopy, ",myself")) {
            /* CLUSTER NODES format (space-separated fields):
             * <id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv>
             * <config-epoch> <link-state> [slot range ...]
             * Slot ranges start at field index 8 (0-based). */
            int field = 0;
            const char *p = linecopy;
            /* Skip 8 fields. */
            while (field < 8 && *p) {
                while (*p && *p != ' ') p++;
                while (*p == ' ') p++;
                field++;
            }
            /* Remaining tokens are slot ranges or [import/migrate]. */
            size_t written = 0;
            while (*p) {
                const char *tok_end = p;
                while (*tok_end && *tok_end != ' ') tok_end++;
                size_t tok_len = (size_t)(tok_end - p);

                /* Skip slot-migration markers starting with '['. */
                if (*p != '[' && tok_len > 0) {
                    if (written > 0 && written < buflen - 1) {
                        buf[written++] = ' ';
                    }
                    size_t copy = tok_len;
                    if (written + copy >= buflen) {
                        copy = buflen - written - 1;
                    }
                    memcpy(buf + written, p, copy);
                    written += copy;
                }
                p = tok_end;
                while (*p == ' ') p++;
            }
            buf[written] = '\0';
            return 0;
        }

        if (!eol) break;
        line = eol + 1;
    }
    return -1;
}

int OperatorSlotStats_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    /* --- Call INFO all --- */
    ValkeyModuleCallReply *info_reply =
        ValkeyModule_Call(ctx, "INFO", "c", "all");
    if (!info_reply ||
        ValkeyModule_CallReplyType(info_reply) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_ReplyWithError(ctx, "ERR failed to call INFO all");
        if (info_reply) ValkeyModule_FreeCallReply(info_reply);
        return VALKEYMODULE_OK;
    }
    size_t info_len;
    const char *info_ptr = ValkeyModule_CallReplyStringPtr(info_reply, &info_len);

    /* Copy into a local buffer so we can free the reply before calling CLUSTER NODES. */
    char *info = (char *)ValkeyModule_Alloc(info_len + 1);
    if (!info) {
        ValkeyModule_FreeCallReply(info_reply);
        ValkeyModule_ReplyWithError(ctx, "ERR out of memory");
        return VALKEYMODULE_OK;
    }
    memcpy(info, info_ptr, info_len);
    info[info_len] = '\0';
    ValkeyModule_FreeCallReply(info_reply);

    /* Parse INFO all fields. */
    long long keys        = extract_keyspace_keys(info);
    long long memory      = parse_field_ll(info, "used_memory");
    if (memory < 0) memory = 0;

    char role_raw[16] = "unknown";
    parse_field_str(info, "role", role_raw, sizeof(role_raw));
    const char *role_label = (strcmp(role_raw, "master") == 0) ? "primary" : "replica";

    /* Replication offset: use master_repl_offset for primaries, slave_repl_offset for replicas. */
    long long repl_offset = parse_field_ll(info, "master_repl_offset");
    if (strcmp(role_raw, "slave") == 0) {
        long long slave_off = parse_field_ll(info, "slave_repl_offset");
        if (slave_off >= 0) repl_offset = slave_off;
    }
    if (repl_offset < 0) repl_offset = 0;

    ValkeyModule_Free(info);

    /* --- Call CLUSTER NODES --- */
    ValkeyModuleCallReply *nodes_reply =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "NODES");
    if (!nodes_reply ||
        ValkeyModule_CallReplyType(nodes_reply) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_ReplyWithError(ctx, "ERR failed to call CLUSTER NODES");
        if (nodes_reply) ValkeyModule_FreeCallReply(nodes_reply);
        return VALKEYMODULE_OK;
    }
    size_t nodes_len;
    const char *nodes_ptr = ValkeyModule_CallReplyStringPtr(nodes_reply, &nodes_len);

    char *nodes_buf = (char *)ValkeyModule_Alloc(nodes_len + 1);
    if (!nodes_buf) {
        ValkeyModule_FreeCallReply(nodes_reply);
        ValkeyModule_ReplyWithError(ctx, "ERR out of memory");
        return VALKEYMODULE_OK;
    }
    memcpy(nodes_buf, nodes_ptr, nodes_len);
    nodes_buf[nodes_len] = '\0';
    ValkeyModule_FreeCallReply(nodes_reply);

    char slots[SLOTS_BUF_SIZE] = "";
    extract_myself_slots(nodes_buf, slots, sizeof(slots));
    ValkeyModule_Free(nodes_buf);

    /* --- Build response: flat array of 10 elements (5 key-value pairs) --- */
    ValkeyModule_ReplyWithArray(ctx, 10);

    ValkeyModule_ReplyWithCString(ctx, "keys");
    ValkeyModule_ReplyWithLongLong(ctx, keys);

    ValkeyModule_ReplyWithCString(ctx, "memory_bytes");
    ValkeyModule_ReplyWithLongLong(ctx, memory);

    ValkeyModule_ReplyWithCString(ctx, "role");
    ValkeyModule_ReplyWithCString(ctx, role_label);

    ValkeyModule_ReplyWithCString(ctx, "slots");
    ValkeyModule_ReplyWithCString(ctx, slots);

    ValkeyModule_ReplyWithCString(ctx, "repl_offset");
    ValkeyModule_ReplyWithLongLong(ctx, repl_offset);

    return VALKEYMODULE_OK;
}
