/**
 * topology.c — OPERATOR.TOPOLOGY.SET command implementation.
 *
 * Allows the operator to push cluster topology directly to a node,
 * replacing the external orchestration of CLUSTER MEET + CLUSTER REPLICATE.
 *
 * The node applies the topology autonomously:
 *   1. Issues CLUSTER MEET for each unknown peer.
 *   2. Issues CLUSTER REPLICATE <primary_node_id> if role=replica.
 *
 * Syntax:
 *   OPERATOR.TOPOLOGY.SET <json>
 *
 * JSON schema:
 * {
 *   "peers":        ["ip:port", ...],   // all cluster members to MEET
 *   "role":         "primary"|"replica",
 *   "primary_addr": "ip:port"           // required when role=replica
 * }
 *
 * Returns an array of three elements:
 *   [0] int  : 1 = success, 0 = failure
 *   [1] str  : "ok" or error description
 *   [2] int  : number of CLUSTER MEET commands issued
 *
 * Idempotent: CLUSTER MEET on an already-known peer is a no-op in Valkey.
 * CLUSTER REPLICATE on the current primary is also a no-op.
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#define MAX_PEERS 64
#define MAX_ADDR_LEN 64

/* --- Minimal JSON parser for our fixed schema --- */

/**
 * json_extract_string copies the value of a JSON string field into buf.
 * Handles: "field": "value"
 * Returns 0 on success, -1 if not found.
 */
static int json_extract_string(const char *json, const char *field,
                                char *buf, size_t buflen) {
    /* Build search pattern: "field": " */
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "\"%s\"", field);
    const char *p = strstr(json, pattern);
    if (!p) return -1;
    p += strlen(pattern);
    /* Skip whitespace and colon */
    while (*p == ' ' || *p == '\t' || *p == ':') p++;
    if (*p != '"') return -1;
    p++; /* skip opening quote */
    size_t i = 0;
    while (*p && *p != '"' && i < buflen - 1) {
        buf[i++] = *p++;
    }
    buf[i] = '\0';
    return (*p == '"') ? 0 : -1;
}

/**
 * json_extract_array_strings parses a JSON string array field.
 * Fills addrs[] with up to max_count entries.
 * Returns the number of entries found, or -1 on error.
 */
static int json_extract_array_strings(const char *json, const char *field,
                                       char addrs[][MAX_ADDR_LEN], int max_count) {
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "\"%s\"", field);
    const char *p = strstr(json, pattern);
    if (!p) return -1;
    p += strlen(pattern);
    while (*p == ' ' || *p == '\t' || *p == ':') p++;
    if (*p != '[') return -1;
    p++; /* skip '[' */

    int count = 0;
    while (*p && *p != ']' && count < max_count) {
        while (*p == ' ' || *p == '\t' || *p == '\n' || *p == ',') p++;
        if (*p != '"') break;
        p++; /* skip '"' */
        size_t i = 0;
        while (*p && *p != '"' && i < MAX_ADDR_LEN - 1) {
            addrs[count][i++] = *p++;
        }
        addrs[count][i] = '\0';
        if (*p == '"') p++;
        count++;
    }
    return count;
}

/**
 * split_addr splits "ip:port" into separate ip and port strings.
 * Returns 0 on success, -1 on invalid format.
 */
static int split_addr(const char *addr, char *ip, size_t ip_len,
                       char *port, size_t port_len) {
    const char *colon = strrchr(addr, ':');
    if (!colon) return -1;
    size_t ip_part_len = (size_t)(colon - addr);
    if (ip_part_len >= ip_len) return -1;
    strncpy(ip, addr, ip_part_len);
    ip[ip_part_len] = '\0';
    strncpy(port, colon + 1, port_len - 1);
    port[port_len - 1] = '\0';
    return 0;
}

/**
 * is_already_replica_of returns 1 if this node is already replicating
 * primary_ip, 0 otherwise (including on error — safe fallback to REPLICATE).
 */
static int is_already_replica_of(ValkeyModuleCtx *ctx, const char *primary_ip) {
    ValkeyModuleCallReply *r = ValkeyModule_Call(ctx, "INFO", "c", "replication");
    if (!r || ValkeyModule_CallReplyType(r) != VALKEYMODULE_REPLY_STRING) {
        if (r) ValkeyModule_FreeCallReply(r);
        return 0;
    }
    size_t len;
    const char *info = ValkeyModule_CallReplyStringPtr(r, &len);

    /* Check role:slave */
    int is_slave = (strstr(info, "\nrole:slave\r") != NULL ||
                    strstr(info, "\nrole:slave\n") != NULL);

    /* Check master_host:<primary_ip> — handle both \r\n and \n line endings. */
    char needle[MAX_ADDR_LEN + 16];
    snprintf(needle, sizeof(needle), "\nmaster_host:%s\r", primary_ip);
    int correct_master = (strstr(info, needle) != NULL);
    if (!correct_master) {
        snprintf(needle, sizeof(needle), "\nmaster_host:%s\n", primary_ip);
        correct_master = (strstr(info, needle) != NULL);
    }

    ValkeyModule_FreeCallReply(r);
    return is_slave && correct_master;
}

/**
 * get_node_id_for_addr fetches the node ID of a peer at ip:port
 * by issuing CLUSTER NODES and scanning for a matching address.
 * Returns 0 on success (node_id filled), -1 if not found.
 */
static int get_node_id_for_addr(ValkeyModuleCtx *ctx, const char *target_addr,
                                  char *node_id, size_t node_id_len) {
    ValkeyModuleCallReply *reply =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "NODES");
    if (!reply ||
        ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_STRING) {
        if (reply) ValkeyModule_FreeCallReply(reply);
        return -1;
    }
    size_t len;
    const char *nodes = ValkeyModule_CallReplyStringPtr(reply, &len);

    /* Each line: <id> <ip:port@busport[,hostname]> <flags> ... */
    const char *line = nodes;
    int found = -1;
    while (line && *line) {
        /* Extract node ID (first field). */
        const char *id_end = strchr(line, ' ');
        if (!id_end) break;
        size_t id_len = (size_t)(id_end - line);

        /* Extract addr field (second field): ip:port@busport */
        const char *addr_start = id_end + 1;
        const char *addr_end = strchr(addr_start, ' ');
        if (!addr_end) break;

        /* addr_start to first '@' gives us ip:port */
        const char *at = strchr(addr_start, '@');
        size_t addr_part_len = at ? (size_t)(at - addr_start)
                                  : (size_t)(addr_end - addr_start);
        char addr_buf[MAX_ADDR_LEN];
        if (addr_part_len < sizeof(addr_buf)) {
            strncpy(addr_buf, addr_start, addr_part_len);
            addr_buf[addr_part_len] = '\0';

            if (strcmp(addr_buf, target_addr) == 0) {
                if (id_len < node_id_len) {
                    strncpy(node_id, line, id_len);
                    node_id[id_len] = '\0';
                    found = 0;
                }
                break;
            }
        }

        /* Advance to next line. */
        line = strchr(addr_end, '\n');
        if (line) line++;
    }

    ValkeyModule_FreeCallReply(reply);
    return found;
}

int OperatorTopologySet_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc < 2) {
        ValkeyModule_ReplyWithError(ctx, "ERR usage: OPERATOR.TOPOLOGY.SET <json>");
        return VALKEYMODULE_OK;
    }

    size_t json_len;
    const char *json = ValkeyModule_StringPtrLen(argv[1], &json_len);

    /* --- Parse JSON --- */
    char role[16] = "";
    char primary_addr[MAX_ADDR_LEN] = "";
    char peers[MAX_PEERS][MAX_ADDR_LEN];

    json_extract_string(json, "role", role, sizeof(role));
    json_extract_string(json, "primary_addr", primary_addr, sizeof(primary_addr));
    int peer_count = json_extract_array_strings(json, "peers", peers, MAX_PEERS);
    if (peer_count < 0) peer_count = 0;

    if (role[0] == '\0') {
        ValkeyModule_ReplyWithArray(ctx, 3);
        ValkeyModule_ReplyWithLongLong(ctx, 0);
        ValkeyModule_ReplyWithCString(ctx, "missing required field: role");
        ValkeyModule_ReplyWithLongLong(ctx, 0);
        return VALKEYMODULE_OK;
    }

    if (strcmp(role, "replica") == 0 && primary_addr[0] == '\0') {
        ValkeyModule_ReplyWithArray(ctx, 3);
        ValkeyModule_ReplyWithLongLong(ctx, 0);
        ValkeyModule_ReplyWithCString(ctx, "missing required field: primary_addr (required for replica)");
        ValkeyModule_ReplyWithLongLong(ctx, 0);
        return VALKEYMODULE_OK;
    }

    /* --- Step 1: CLUSTER MEET unknown peers only --- */

    /* Build a set of already-known peer ip:port from CLUSTER NODES to avoid
     * issuing redundant MEET commands that cause link churn ("Freeing outbound
     * link ... after receiving a MEET packet from this known node"). */
    char known_addrs[MAX_PEERS][MAX_ADDR_LEN];
    int known_count = 0;
    {
        ValkeyModuleCallReply *nr =
            ValkeyModule_Call(ctx, "CLUSTER", "c", "NODES");
        if (nr && ValkeyModule_CallReplyType(nr) == VALKEYMODULE_REPLY_STRING) {
            size_t nlen;
            const char *nodes = ValkeyModule_CallReplyStringPtr(nr, &nlen);
            const char *line = nodes;
            const char *end = nodes + nlen;
            while (line && line < end && *line && known_count < MAX_PEERS) {
                /* Skip node ID (field 0). */
                const char *sp = memchr(line, ' ', (size_t)(end - line));
                if (!sp || sp >= end) break;
                /* Field 1: ip:port@busport[,hostname] */
                const char *addr_start = sp + 1;
                const char *addr_end = memchr(addr_start, '@', (size_t)(end - addr_start));
                if (addr_end && addr_end < end) {
                    size_t alen = (size_t)(addr_end - addr_start);
                    if (alen > 0 && alen < MAX_ADDR_LEN) {
                        memcpy(known_addrs[known_count], addr_start, alen);
                        known_addrs[known_count][alen] = '\0';
                        known_count++;
                    }
                }
                /* Next line. */
                const char *nl = memchr(line, '\n', (size_t)(end - line));
                line = nl ? nl + 1 : NULL;
            }
            ValkeyModule_FreeCallReply(nr);
        } else if (nr) {
            ValkeyModule_FreeCallReply(nr);
        }
    }

    int meet_count = 0;
    for (int i = 0; i < peer_count; i++) {
        /* Skip peers already known in CLUSTER NODES. */
        int already_known = 0;
        for (int k = 0; k < known_count; k++) {
            if (strcmp(peers[i], known_addrs[k]) == 0) {
                already_known = 1;
                break;
            }
        }
        if (already_known) continue;

        char ip[MAX_ADDR_LEN], port[16];
        if (split_addr(peers[i], ip, sizeof(ip), port, sizeof(port)) != 0) {
            continue;
        }
        ValkeyModuleCallReply *meet_reply =
            ValkeyModule_Call(ctx, "CLUSTER", "ccc", "MEET", ip, port);
        if (meet_reply) {
            meet_count++;
            ValkeyModule_FreeCallReply(meet_reply);
        }
    }

    /* --- Step 2: CLUSTER REPLICATE if replica --- */
    if (strcmp(role, "replica") == 0) {
        /* Pre-check: skip REPLICATE if already replicating the correct primary.
         * Avoids ERR from Valkey when the node is already in the right state. */
        char primary_ip[MAX_ADDR_LEN] = "";
        char primary_port[16] = "";
        split_addr(primary_addr, primary_ip, sizeof(primary_ip),
                   primary_port, sizeof(primary_port));
        if (primary_ip[0] != '\0' && is_already_replica_of(ctx, primary_ip)) {
            ValkeyModule_ReplyWithArray(ctx, 3);
            ValkeyModule_ReplyWithLongLong(ctx, 1);
            ValkeyModule_ReplyWithCString(ctx, "ok");
            ValkeyModule_ReplyWithLongLong(ctx, meet_count);
            return VALKEYMODULE_OK;
        }

        /* Resolve primary node ID from CLUSTER NODES. */
        char node_id[64] = "";
        if (get_node_id_for_addr(ctx, primary_addr, node_id, sizeof(node_id)) != 0) {
            /* Primary not yet known via gossip — operator should retry. */
            ValkeyModule_ReplyWithArray(ctx, 3);
            ValkeyModule_ReplyWithLongLong(ctx, 0);
            ValkeyModule_ReplyWithCString(ctx,
                "primary node ID not yet known — retry after gossip converges");
            ValkeyModule_ReplyWithLongLong(ctx, meet_count);
            return VALKEYMODULE_OK;
        }

        ValkeyModuleCallReply *repl_reply =
            ValkeyModule_Call(ctx, "CLUSTER", "cc", "REPLICATE", node_id);
        if (!repl_reply ||
            ValkeyModule_CallReplyType(repl_reply) == VALKEYMODULE_REPLY_ERROR) {
            size_t err_len;
            const char *err_str = repl_reply
                ? ValkeyModule_CallReplyStringPtr(repl_reply, &err_len)
                : "call failed";
            ValkeyModule_ReplyWithArray(ctx, 3);
            ValkeyModule_ReplyWithLongLong(ctx, 0);
            ValkeyModule_ReplyWithCString(ctx, err_str ? err_str : "CLUSTER REPLICATE failed");
            ValkeyModule_ReplyWithLongLong(ctx, meet_count);
            if (repl_reply) ValkeyModule_FreeCallReply(repl_reply);
            return VALKEYMODULE_OK;
        }
        if (repl_reply) ValkeyModule_FreeCallReply(repl_reply);
    }

    ValkeyModule_ReplyWithArray(ctx, 3);
    ValkeyModule_ReplyWithLongLong(ctx, 1);
    ValkeyModule_ReplyWithCString(ctx, "ok");
    ValkeyModule_ReplyWithLongLong(ctx, meet_count);
    return VALKEYMODULE_OK;
}
