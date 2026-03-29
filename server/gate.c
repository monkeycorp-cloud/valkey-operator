/**
 * gate.c — Cluster redirection gate for Kubernetes rolling updates.
 *
 * Problem: when a Valkey pod restarts, it rejoins cluster gossip via nodes.conf
 * immediately. Peer nodes may send MOVED redirections pointing clients to this
 * pod before the operator reconciler has applied ACLs (~3s window).
 *
 * Solution: set CLUSTER_FLAG_NO_REDIRECTION at startup so this node does not
 * issue MOVED responses based on potentially stale topology while warming up.
 * A 500ms timer polls ACL LIST; when an active application user is found,
 * the flag is cleared and the node participates fully in cluster routing.
 *
 * Note: NO_REDIRECTION prevents this node from redirecting clients to other
 * nodes, but does not prevent other nodes from sending MOVED to this node.
 * The OPERATOR.NODE.READY readiness probe (FailureThreshold:1) gates
 * Kubernetes endpoints and is the primary protection mechanism.
 *
 * Thread safety: timer callback runs in the Valkey main thread — no locking
 * needed for acl_ready.
 */

#include "valkeymodule.h"
#include <string.h>

/* Set to 1 once the timer detects an active application user in ACL LIST. */
static volatile int acl_ready = 0;

/* Timer ID returned by ValkeyModule_CreateTimer. 0 when inactive. */
static ValkeyModuleTimerID acl_poll_timer_id = 0;

static void acl_poll_timer_cb(ValkeyModuleCtx *ctx, void *data);

/* gate_check_acl_ready — scan ACL LIST for an active non-default, non-operator
 * user. Returns 1 if found, 0 otherwise.
 * Mirrors the same logic in node_ready.c. */
static int gate_check_acl_ready(ValkeyModuleCtx *ctx) {
    ValkeyModuleCallReply *acl_r =
        ValkeyModule_Call(ctx, "ACL", "c", "LIST");
    if (!acl_r ||
        ValkeyModule_CallReplyType(acl_r) != VALKEYMODULE_REPLY_ARRAY) {
        if (acl_r) ValkeyModule_FreeCallReply(acl_r);
        return 0;
    }

    size_t count = ValkeyModule_CallReplyLength(acl_r);
    int found = 0;
    size_t i;
    for (i = 0; i < count && !found; i++) {
        ValkeyModuleCallReply *entry =
            ValkeyModule_CallReplyArrayElement(acl_r, i);
        if (!entry ||
            ValkeyModule_CallReplyType(entry) != VALKEYMODULE_REPLY_STRING)
            continue;
        size_t elen;
        const char *estr = ValkeyModule_CallReplyStringPtr(entry, &elen);

        if (strncmp(estr, "user default ", 13) == 0) continue;
        if (strncmp(estr, "user operator ", 14) == 0) continue;

        if (strstr(estr, " on ") ||
            (elen > 8 && strncmp(estr + elen - 3, " on", 3) == 0)) {
            found = 1;
        }
    }
    ValkeyModule_FreeCallReply(acl_r);
    return found;
}

/* acl_poll_timer_cb — fires every 500ms while the gate is armed.
 * Clears NO_REDIRECTION once ACLs are detected. */
static void acl_poll_timer_cb(ValkeyModuleCtx *ctx, void *data) {
    (void)data;
    acl_poll_timer_id = 0;

    if (gate_check_acl_ready(ctx)) {
        acl_ready = 1;
        ValkeyModule_SetClusterFlags(ctx, VALKEYMODULE_CLUSTER_FLAG_NONE);
        ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_NOTICE,
            "operator-gate: ACLs applied — cluster redirection re-enabled");
    } else {
        acl_poll_timer_id =
            ValkeyModule_CreateTimer(ctx, 500, acl_poll_timer_cb, NULL);
    }
}

/* Gate_Init — called from ValkeyModule_OnLoad.
 * Sets NO_REDIRECTION and arms the 500ms timer. */
int Gate_Init(ValkeyModuleCtx *ctx) {
    acl_ready = 0;

    ValkeyModule_SetClusterFlags(ctx, VALKEYMODULE_CLUSTER_FLAG_NO_REDIRECTION);

    /* Check immediately for hot-reload case (ACLs already present). */
    if (gate_check_acl_ready(ctx)) {
        acl_ready = 1;
        ValkeyModule_SetClusterFlags(ctx, VALKEYMODULE_CLUSTER_FLAG_NONE);
        ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_NOTICE,
            "operator-gate: ACLs already present at load time — gate not armed");
        return VALKEYMODULE_OK;
    }

    acl_poll_timer_id =
        ValkeyModule_CreateTimer(ctx, 500, acl_poll_timer_cb, NULL);

    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_NOTICE,
        "operator-gate: armed — NO_REDIRECTION until ACLs are applied");

    return VALKEYMODULE_OK;
}
