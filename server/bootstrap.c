/**
 * bootstrap.c — OPERATOR.BOOTSTRAP.READY command implementation.
 *
 * Answers: "is this node ready to participate in --cluster create?"
 *
 * A node is bootstrap-ready when ALL of the following are true:
 *   1. server_mode = cluster  — running in cluster mode (INFO server)
 *   2. cluster_slots_assigned = 0 — no slots owned (not already part of a cluster)
 *   3. cluster_known_nodes <= 1  — no stale peers in nodes.conf (only self)
 *
 * Note: Valkey 9 removed the cluster_enabled field from CLUSTER INFO output.
 * We check server_mode from INFO server instead.
 *
 * Returns an array of two elements:
 *   [0] int  : 1 if ready, 0 if not
 *   [1] str  : reason string ("ready", or explanation why not ready)
 *
 * Used by the operator bootstrap Job readiness check and by reconcileBootstrapJob
 * to gate --cluster create until all nodes report ready.
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>

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

int OperatorBootstrapReady_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    /* --- INFO server: check server_mode=cluster ---
     * Valkey 9 removed cluster_enabled from CLUSTER INFO; use server_mode instead. */
    ValkeyModuleCallReply *server_r =
        ValkeyModule_Call(ctx, "INFO", "c", "server");
    if (!server_r ||
        ValkeyModule_CallReplyType(server_r) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_ReplyWithError(ctx, "ERR failed to call INFO server");
        if (server_r) ValkeyModule_FreeCallReply(server_r);
        return VALKEYMODULE_OK;
    }
    size_t server_len;
    const char *server_info = ValkeyModule_CallReplyStringPtr(server_r, &server_len);
    char server_mode[16] = "";
    parse_field_str(server_info, "server_mode", server_mode, sizeof(server_mode));
    ValkeyModule_FreeCallReply(server_r);

    /* --- CLUSTER INFO --- */
    ValkeyModuleCallReply *reply =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "INFO");
    if (!reply ||
        ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_STRING) {
        ValkeyModule_ReplyWithError(ctx, "ERR failed to call CLUSTER INFO");
        if (reply) ValkeyModule_FreeCallReply(reply);
        return VALKEYMODULE_OK;
    }
    size_t len;
    const char *info = ValkeyModule_CallReplyStringPtr(reply, &len);

    long long slots_assigned = parse_field_ll(info, "cluster_slots_assigned");
    long long known_nodes    = parse_field_ll(info, "cluster_known_nodes");

    ValkeyModule_FreeCallReply(reply);

    /* --- Evaluate conditions --- */
    ValkeyModule_ReplyWithArray(ctx, 2);

    if (strcmp(server_mode, "cluster") != 0) {
        ValkeyModule_ReplyWithLongLong(ctx, 0);
        ValkeyModule_ReplyWithCString(ctx, "server_mode is not cluster");
        return VALKEYMODULE_OK;
    }

    if (slots_assigned > 0) {
        ValkeyModule_ReplyWithLongLong(ctx, 0);
        ValkeyModule_ReplyWithCString(ctx, "node already owns slots — already part of a cluster");
        return VALKEYMODULE_OK;
    }

    /* known_nodes > 1 means stale peers from a previous nodes.conf. */
    if (known_nodes > 1) {
        ValkeyModule_ReplyWithLongLong(ctx, 0);
        ValkeyModule_ReplyWithCString(ctx, "stale peers in nodes.conf — CLUSTER RESET SOFT required");
        return VALKEYMODULE_OK;
    }

    ValkeyModule_ReplyWithLongLong(ctx, 1);
    ValkeyModule_ReplyWithCString(ctx, "ready");
    return VALKEYMODULE_OK;
}
