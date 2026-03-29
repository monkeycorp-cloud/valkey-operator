/**
 * node_state.c — OPERATOR.NODE.STATE command implementation.
 *
 * Exposes the persistent cluster role of this node as recorded in nodes.conf
 * (via INFO replication + CLUSTER NODES). Used by the operator after a brutal
 * restart to restore CLUSTER REPLICATE assignments without heuristics.
 *
 * Unlike CLUSTER NODES (which reflects the current gossip view, potentially
 * stale after an IP change), this command reads the node's own internal state
 * which is always accurate for the local node.
 *
 * Syntax:
 *   OPERATOR.NODE.STATE
 *
 * Returns a flat array of 6 elements:
 *   [0] "role"        [1] "primary" | "replica"
 *   [2] "master_id"   [3] "<40-char node ID>" | "" (empty if primary)
 *   [4] "master_addr" [5] "ip:port" | ""            (empty if primary)
 *
 * The master_id is extracted from the "myself" line in CLUSTER NODES —
 * specifically the 7th field (masterID) which nodes.conf persists across restarts.
 * The master_addr is built from master_host:master_port in INFO replication.
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define MAX_FIELD_LEN 128

static int parse_field_str_ns(const char *info, const char *field,
                               char *buf, size_t buflen) {
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

/**
 * parse_myself_line parses the "myself" line from CLUSTER NODES output and
 * extracts both the masterID (field 3) and whether the node has its own slots
 * (fields 8+, present only for primaries with assigned slots).
 *
 * CLUSTER NODES line format:
 *   <id> <addr> <flags> <masterID> <ping> <pong> <epoch> <link-state> [slots...]
 *   field: 0     1       2          3       4      5       6      7       8+
 *
 * master_id is set to "" if the node is a primary ("-") or if not found.
 * has_own_slots is set to 1 if field 8+ is present and non-empty, 0 otherwise.
 */
static void parse_myself_line(ValkeyModuleCtx *ctx,
                               char *master_id, size_t master_id_len,
                               int *has_own_slots) {
    *has_own_slots = 0;
    if (master_id_len > 0) master_id[0] = '\0';

    ValkeyModuleCallReply *r =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "NODES");
    if (!r || ValkeyModule_CallReplyType(r) != VALKEYMODULE_REPLY_STRING) {
        if (r) ValkeyModule_FreeCallReply(r);
        return;
    }

    size_t len;
    const char *nodes = ValkeyModule_CallReplyStringPtr(r, &len);
    const char *nodes_end = nodes + len;

    const char *line = nodes;
    while (line && line < nodes_end && *line) {
        /* Find the end of this line so we only search within it.
         * strstr(line, "myself") would search the entire remaining buffer,
         * falsely matching the first line when "myself" appears later. */
        const char *eol = (const char *)memchr(line, '\n', (size_t)(nodes_end - line));
        if (!eol) eol = nodes_end;
        size_t line_len = (size_t)(eol - line);
        /* Check if "myself" appears within this line only. */
        int found_myself = 0;
        if (line_len >= 6) {
            for (size_t i = 0; i + 6 <= line_len; i++) {
                if (memcmp(line + i, "myself", 6) == 0) {
                    found_myself = 1;
                    break;
                }
            }
        }
        if (!found_myself) {
            goto next_line;
        }

        {
            const char *p = line;
            int field_idx = 0;
            const char *field_start = p;

            /* Bound the inner loop to the buffer returned by the module API. */
            while (p < nodes_end && *p && *p != '\n' && *p != '\r') {
                if (*p == ' ') {
                    size_t flen = (size_t)(p - field_start);

                    if (field_idx == 3 && flen > 0 && flen < master_id_len) {
                        strncpy(master_id, field_start, flen);
                        master_id[flen] = '\0';
                        if (strcmp(master_id, "-") == 0) master_id[0] = '\0';
                    }
                    if (field_idx == 7) {
                        /* Field 8+ contains slot ranges ("0-5460", "10923-16383") or
                         * migration markers ("[slot->-nodeID]" or "[slot-<-nodeID]").
                         * A node with only migration markers has no stable slot ownership.
                         * has_own_slots=1 only when at least one non-marker token exists. */
                        const char *s = p + 1;
                        while (s < nodes_end && *s == ' ') s++;
                        int found_real_slot = 0;
                        while (s < nodes_end && *s && *s != '\n' && *s != '\r') {
                            /* Skip migration markers: tokens starting with '[' */
                            if (*s == '[') {
                                while (s < nodes_end && *s && *s != ']' && *s != '\n') s++;
                                if (*s == ']') s++;
                            } else if (*s != ' ') {
                                found_real_slot = 1;
                                break;
                            } else {
                                s++;
                            }
                        }
                        *has_own_slots = found_real_slot;
                        break;
                    }

                    field_idx++;
                    field_start = p + 1;
                }
                p++;
            }
        }
        break;

next_line:
        /* Use memchr to stay within the buffer bounds returned by the module API. */
        line = (const char *)memchr(line, '\n', (size_t)(nodes_end - line));
        if (line) line++;
    }

    ValkeyModule_FreeCallReply(r);
}

int OperatorNodeState_Command(ValkeyModuleCtx *ctx,
                               ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    /* --- INFO replication --- */
    ValkeyModuleCallReply *repl_r =
        ValkeyModule_Call(ctx, "INFO", "c", "replication");
    if (!repl_r ||
        ValkeyModule_CallReplyType(repl_r) != VALKEYMODULE_REPLY_STRING) {
        if (repl_r) ValkeyModule_FreeCallReply(repl_r);
        ValkeyModule_ReplyWithError(ctx, "ERR failed to call INFO replication");
        return VALKEYMODULE_OK;
    }

    size_t rlen;
    const char *rinfo = ValkeyModule_CallReplyStringPtr(repl_r, &rlen);

    char raw_role[16] = "";
    char master_host[MAX_FIELD_LEN] = "";
    char master_port[16] = "";

    parse_field_str_ns(rinfo, "role", raw_role, sizeof(raw_role));
    parse_field_str_ns(rinfo, "master_host", master_host, sizeof(master_host));
    parse_field_str_ns(rinfo, "master_port", master_port, sizeof(master_port));

    ValkeyModule_FreeCallReply(repl_r);

    /* Normalize role to "primary" / "replica". */
    const char *role = "";
    if (strcmp(raw_role, "master") == 0) {
        role = "primary";
    } else if (strcmp(raw_role, "slave") == 0) {
        role = "replica";
    } else {
        /* Role not yet determined — node still initialising. */
        role = "unknown";
    }

    /* --- master_addr: build from master_host:master_port --- */
    char master_addr[MAX_FIELD_LEN + 16] = "";
    if (master_host[0] != '\0' && master_port[0] != '\0') {
        snprintf(master_addr, sizeof(master_addr), "%s:%s", master_host, master_port);
    }

    /* --- master_id + has_slots: both extracted from CLUSTER NODES "myself" line.
     * has_slots=1 means this node owns slot ranges in CLUSTER NODES.
     * A primary with has_slots=0 after a brutal restart is a former replica
     * that Valkey auto-promoted to standalone primary. */
    char master_id[64] = "";
    int has_own_slots = 0;
    parse_myself_line(ctx, master_id, sizeof(master_id), &has_own_slots);

    /* --- Build flat key-value array response --- */
    ValkeyModule_ReplyWithArray(ctx, 8);
    ValkeyModule_ReplyWithCString(ctx, "role");
    ValkeyModule_ReplyWithCString(ctx, role);
    ValkeyModule_ReplyWithCString(ctx, "master_id");
    ValkeyModule_ReplyWithCString(ctx, master_id);
    ValkeyModule_ReplyWithCString(ctx, "master_addr");
    ValkeyModule_ReplyWithCString(ctx, master_addr);
    ValkeyModule_ReplyWithCString(ctx, "has_slots");
    ValkeyModule_ReplyWithLongLong(ctx, (long long)has_own_slots);

    return VALKEYMODULE_OK;
}
