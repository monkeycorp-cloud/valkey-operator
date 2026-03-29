/**
 * health.c — OPERATOR.HEALTH command implementation.
 *
 * Returns an enriched health snapshot of the local Valkey node:
 *   - cluster_state       : "ok" or "fail"
 *   - slots_assigned      : number of hash slots owned by this node
 *   - slots_ok            : number of hash slots in ok state
 *   - gossip_converged    : 1 if all known peers are reachable, 0 otherwise
 *   - repl_lag_bytes      : replication lag in bytes (0 for primaries)
 *   - connected_replicas  : number of connected replicas (primaries only)
 *   - role                : "primary" or "replica"
 *   - master_link_status  : "up", "down", or "" for primaries
 *
 * Unlike CLUSTER INFO + INFO replication (two round-trips), OPERATOR.HEALTH
 * returns all fields in a single call, reducing operator polling overhead.
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>

/**
 * parse_field extracts a numeric value from a Valkey INFO-style string.
 * Format: "field_name:value\r\n"
 * Returns -1 if the field is not found.
 */
static long long parse_field_ll(const char *info, const char *field) {
    const char *p = strstr(info, field);
    if (!p) return -1;
    p += strlen(field);
    if (*p != ':') return -1;
    return atoll(p + 1);
}

/**
 * parse_field_str copies the value of a string field into buf (max buflen bytes).
 * Returns 0 on success, -1 if not found.
 */
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

/**
 * OPERATOR.HEALTH
 *
 * Returns a flat map (array of key-value pairs) with the health snapshot.
 * Compatible with both RESP2 (array) and RESP3 (map).
 */
int OperatorHealth_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    /* --- Gather CLUSTER INFO --- */
    ValkeyModuleCallReply *cluster_info_reply =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "INFO");
    if (!cluster_info_reply ||
        ValkeyModule_CallReplyType(cluster_info_reply) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_ReplyWithError(ctx, "ERR failed to call CLUSTER INFO");
        if (cluster_info_reply) ValkeyModule_FreeCallReply(cluster_info_reply);
        return VALKEYMODULE_OK;
    }
    size_t cluster_info_len;
    const char *cluster_info_str =
        ValkeyModule_CallReplyStringPtr(cluster_info_reply, &cluster_info_len);

    /* Parse cluster fields. */
    char cluster_state[16] = "unknown";
    parse_field_str(cluster_info_str, "cluster_state", cluster_state, sizeof(cluster_state));

    long long slots_assigned = parse_field_ll(cluster_info_str, "cluster_slots_assigned");
    long long slots_ok       = parse_field_ll(cluster_info_str, "cluster_slots_ok");
    long long cluster_known_nodes = parse_field_ll(cluster_info_str, "cluster_known_nodes");
    long long cluster_size        = parse_field_ll(cluster_info_str, "cluster_size");

    if (slots_assigned < 0) slots_assigned = 0;
    if (slots_ok       < 0) slots_ok       = 0;

    ValkeyModule_FreeCallReply(cluster_info_reply);

    /* --- Gather INFO replication --- */
    ValkeyModuleCallReply *repl_reply =
        ValkeyModule_Call(ctx, "INFO", "c", "replication");
    if (!repl_reply ||
        ValkeyModule_CallReplyType(repl_reply) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_ReplyWithError(ctx, "ERR failed to call INFO replication");
        if (repl_reply) ValkeyModule_FreeCallReply(repl_reply);
        return VALKEYMODULE_OK;
    }
    size_t repl_len;
    const char *repl_str = ValkeyModule_CallReplyStringPtr(repl_reply, &repl_len);

    char role[16] = "unknown";
    parse_field_str(repl_str, "role", role, sizeof(role));

    /* Normalize role to operator conventions. */
    char role_label[16];
    if (strcmp(role, "master") == 0) {
        strncpy(role_label, "primary", sizeof(role_label));
    } else if (strcmp(role, "slave") == 0) {
        strncpy(role_label, "replica", sizeof(role_label));
    } else {
        strncpy(role_label, role, sizeof(role_label));
    }

    long long connected_replicas = parse_field_ll(repl_str, "connected_slaves");
    if (connected_replicas < 0) connected_replicas = 0;

    /* Replication lag: master_repl_offset - slave_repl_offset (replicas only). */
    long long repl_lag = 0;
    if (strcmp(role, "slave") == 0) {
        long long master_offset = parse_field_ll(repl_str, "master_repl_offset");
        long long slave_offset  = parse_field_ll(repl_str, "slave_repl_offset");
        if (master_offset > 0 && slave_offset >= 0) {
            repl_lag = master_offset - slave_offset;
            if (repl_lag < 0) repl_lag = 0;
        }
    }

    char master_link_status[16] = "";
    if (strcmp(role, "slave") == 0) {
        parse_field_str(repl_str, "master_link_status", master_link_status,
                        sizeof(master_link_status));
    }

    ValkeyModule_FreeCallReply(repl_reply);

    /* --- Gossip convergence heuristic ---
     * A node is considered converged when:
     *   - cluster_state = ok
     *   - cluster_known_nodes == cluster_size * (1 + replicasPerShard)
     *     (we can't know replicasPerShard here, so we use a simpler proxy:
     *      cluster_size > 0 and no node in pfail/fail state)
     * Here we use: converged = (cluster_state == "ok") && (cluster_size > 0)
     * The operator can do the exact headcount check from status.nodes.
     */
    int gossip_converged = (strcmp(cluster_state, "ok") == 0 && cluster_size > 0) ? 1 : 0;

    /* --- Build response: flat array of key-value pairs (16 fields = 32 elements) --- */
    ValkeyModule_ReplyWithArray(ctx, 16 * 2);

    ValkeyModule_ReplyWithCString(ctx, "cluster_state");
    ValkeyModule_ReplyWithCString(ctx, cluster_state);

    ValkeyModule_ReplyWithCString(ctx, "slots_assigned");
    ValkeyModule_ReplyWithLongLong(ctx, slots_assigned);

    ValkeyModule_ReplyWithCString(ctx, "slots_ok");
    ValkeyModule_ReplyWithLongLong(ctx, slots_ok);

    ValkeyModule_ReplyWithCString(ctx, "gossip_converged");
    ValkeyModule_ReplyWithLongLong(ctx, gossip_converged);

    ValkeyModule_ReplyWithCString(ctx, "repl_lag_bytes");
    ValkeyModule_ReplyWithLongLong(ctx, repl_lag);

    ValkeyModule_ReplyWithCString(ctx, "connected_replicas");
    ValkeyModule_ReplyWithLongLong(ctx, connected_replicas);

    ValkeyModule_ReplyWithCString(ctx, "role");
    ValkeyModule_ReplyWithCString(ctx, role_label);

    ValkeyModule_ReplyWithCString(ctx, "master_link_status");
    ValkeyModule_ReplyWithCString(ctx, master_link_status);

    ValkeyModule_ReplyWithCString(ctx, "cluster_known_nodes");
    ValkeyModule_ReplyWithLongLong(ctx, cluster_known_nodes < 0 ? 0 : cluster_known_nodes);

    ValkeyModule_ReplyWithCString(ctx, "cluster_size");
    ValkeyModule_ReplyWithLongLong(ctx, cluster_size < 0 ? 0 : cluster_size);

    /* Padding fields reserved for future use — keeps the array size stable
     * so existing operator parsers do not break when new fields are added. */
    ValkeyModule_ReplyWithCString(ctx, "reserved_1");
    ValkeyModule_ReplyWithCString(ctx, "");

    ValkeyModule_ReplyWithCString(ctx, "reserved_2");
    ValkeyModule_ReplyWithCString(ctx, "");

    ValkeyModule_ReplyWithCString(ctx, "reserved_3");
    ValkeyModule_ReplyWithCString(ctx, "");

    ValkeyModule_ReplyWithCString(ctx, "reserved_4");
    ValkeyModule_ReplyWithCString(ctx, "");

    ValkeyModule_ReplyWithCString(ctx, "reserved_5");
    ValkeyModule_ReplyWithCString(ctx, "");

    ValkeyModule_ReplyWithCString(ctx, "reserved_6");
    ValkeyModule_ReplyWithCString(ctx, "");

    return VALKEYMODULE_OK;
}
