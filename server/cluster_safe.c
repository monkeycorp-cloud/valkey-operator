/**
 * cluster_safe.c — OPERATOR.CLUSTER.SAFE command implementation.
 *
 * Answers: "is it safe to stop this node right now?"
 *
 * Called from the PreStop hook before OPERATOR.FAILOVER.PREPARE to prevent
 * a rolling update from causing CLUSTERDOWN by stopping nodes too quickly.
 *
 * A node is safe to stop when ALL of the following are true:
 *   1. cluster_state = ok        — cluster is not already degraded
 *   2. If this node is a primary:
 *      a. connected_slaves >= 1  — at least one replica can take over
 *      b. At least 2 other primaries are reachable (not fail/pfail) so the
 *         cluster retains a majority after this node stops
 *
 * Replicas are always safe to stop (no slots, no quorum impact).
 *
 * Usage:
 *   OPERATOR.CLUSTER.SAFE
 *
 * Returns an array of two elements:
 *   [0] int  : 1 = safe to stop, 0 = not safe
 *   [1] str  : reason string
 *
 * PreStop hook pattern:
 *   until OPERATOR.CLUSTER.SAFE returns [1, ...]; do sleep 1; done
 *   OPERATOR.FAILOVER.PREPARE <timeout_ms>
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>

#define MAX_NODES 128

static long long parse_field_ll(const char *info, const char *field) {
    const char *p = strstr(info, field);
    if (!p) return -1;
    p += strlen(field);
    if (*p != ':') return -1;
    return atoll(p + 1);
}

static int parse_field_str(const char *info, const char *field,
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

/* reply_safe emits the [ok, message] array. */
static void reply_safe(ValkeyModuleCtx *ctx, int safe, const char *msg) {
    ValkeyModule_ReplyWithArray(ctx, 2);
    ValkeyModule_ReplyWithLongLong(ctx, safe);
    ValkeyModule_ReplyWithCString(ctx, msg);
}

int OperatorClusterSafe_Command(ValkeyModuleCtx *ctx,
                                 ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    /* --- Step 1: CLUSTER INFO — check overall cluster state --- */
    ValkeyModuleCallReply *cluster_r =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "INFO");
    if (!cluster_r ||
        ValkeyModule_CallReplyType(cluster_r) != VALKEYMODULE_REPLY_STRING) {
        if (cluster_r) ValkeyModule_FreeCallReply(cluster_r);
        reply_safe(ctx, 0, "ERR failed to call CLUSTER INFO");
        return VALKEYMODULE_OK;
    }
    size_t clen;
    const char *cinfo = ValkeyModule_CallReplyStringPtr(cluster_r, &clen);

    char cluster_state[16] = "";
    parse_field_str(cinfo, "cluster_state", cluster_state, sizeof(cluster_state));
    ValkeyModule_FreeCallReply(cluster_r);

    if (strcmp(cluster_state, "ok") != 0) {
        reply_safe(ctx, 0, "cluster_state is not ok — already degraded");
        return VALKEYMODULE_OK;
    }

    /* --- Step 2: INFO replication — determine local role --- */
    ValkeyModuleCallReply *repl_r =
        ValkeyModule_Call(ctx, "INFO", "c", "replication");
    if (!repl_r ||
        ValkeyModule_CallReplyType(repl_r) != VALKEYMODULE_REPLY_STRING) {
        if (repl_r) ValkeyModule_FreeCallReply(repl_r);
        reply_safe(ctx, 0, "ERR failed to call INFO replication");
        return VALKEYMODULE_OK;
    }
    size_t rlen;
    const char *rinfo = ValkeyModule_CallReplyStringPtr(repl_r, &rlen);

    char role[16] = "";
    parse_field_str(rinfo, "role", role, sizeof(role));
    long long connected_slaves = parse_field_ll(rinfo, "connected_slaves");
    ValkeyModule_FreeCallReply(repl_r);

    /* Replicas have no slots and do not affect quorum — always safe. */
    if (strcmp(role, "slave") == 0) {
        reply_safe(ctx, 1, "replica — safe to stop");
        return VALKEYMODULE_OK;
    }

    /* --- Step 3: primary checks --- */

    /* No replica available — failover impossible after stop. */
    if (connected_slaves <= 0) {
        reply_safe(ctx, 0, "primary has no connected replica — failover not possible");
        return VALKEYMODULE_OK;
    }

    /* --- Step 4: CLUSTER NODES — count healthy primaries excluding self --- */
    ValkeyModuleCallReply *nodes_r =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "NODES");
    if (!nodes_r ||
        ValkeyModule_CallReplyType(nodes_r) != VALKEYMODULE_REPLY_STRING) {
        if (nodes_r) ValkeyModule_FreeCallReply(nodes_r);
        reply_safe(ctx, 0, "ERR failed to call CLUSTER NODES");
        return VALKEYMODULE_OK;
    }
    size_t nlen;
    const char *nodes = ValkeyModule_CallReplyStringPtr(nodes_r, &nlen);

    /*
     * Count primaries that are:
     *   - not myself
     *   - flagged as master
     *   - not in fail or pfail state
     *   - not noaddr
     * These are the primaries that will remain after this node stops.
     * We need at least 2 to maintain a cluster majority (total >= 3 shards).
     */
    int healthy_other_primaries = 0;
    const char *line = nodes;
    while (line && *line) {
        const char *nl = strchr(line, '\n');
        size_t line_len = nl ? (size_t)(nl - line) : strlen(line);

        /* Skip empty lines. */
        if (line_len == 0) {
            line = nl ? nl + 1 : NULL;
            continue;
        }

        /* Extract flags field (3rd space-separated field). */
        const char *p = line;
        int spaces = 0;
        const char *flags_start = NULL;
        const char *flags_end = NULL;
        while (p < line + line_len) {
            if (*p == ' ') {
                spaces++;
                if (spaces == 2) flags_start = p + 1;
                if (spaces == 3) { flags_end = p; break; }
            }
            p++;
        }
        if (!flags_start) {
            line = nl ? nl + 1 : NULL;
            continue;
        }
        if (!flags_end) flags_end = line + line_len;

        /* Copy flags into a local buffer for easy strstr. */
        char flags[64] = "";
        size_t flen = (size_t)(flags_end - flags_start);
        if (flen >= sizeof(flags)) flen = sizeof(flags) - 1;
        strncpy(flags, flags_start, flen);
        flags[flen] = '\0';

        /* Skip self. */
        if (strstr(flags, "myself")) {
            line = nl ? nl + 1 : NULL;
            continue;
        }

        /* Must be master, not failing, not missing address. */
        if (!strstr(flags, "master")) {
            line = nl ? nl + 1 : NULL;
            continue;
        }
        if (strstr(flags, "fail") || strstr(flags, "noaddr")) {
            line = nl ? nl + 1 : NULL;
            continue;
        }

        healthy_other_primaries++;
        line = nl ? nl + 1 : NULL;
    }
    ValkeyModule_FreeCallReply(nodes_r);

    /*
     * We need at least 2 other healthy primaries so that after this node stops,
     * the remaining 2 form a majority (2 out of 3 total shards).
     * If the cluster has more shards, the threshold scales accordingly but
     * 2 is the safe minimum for a standard 3-shard deployment.
     */
    if (healthy_other_primaries < 2) {
        char msg[128];
        snprintf(msg, sizeof(msg),
            "only %d other healthy primary(s) reachable — stopping would risk quorum loss",
            healthy_other_primaries);
        reply_safe(ctx, 0, msg);
        return VALKEYMODULE_OK;
    }

    reply_safe(ctx, 1, "safe to stop");
    return VALKEYMODULE_OK;
}
