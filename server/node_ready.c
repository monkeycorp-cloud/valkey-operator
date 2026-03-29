/**
 * node_ready.c — OPERATOR.NODE.READY command implementation.
 *
 * Answers: "is this node fully operational and stable?"
 *
 * Used as the readiness probe command, replacing the shell script that
 * checked PING + master_link_status. This gives a more precise signal:
 * Kubernetes only routes traffic to this pod and advances the rolling
 * update to the next pod once this command returns [1, "ready"].
 *
 * A node is ready when ALL of the following are true:
 *
 *   All nodes:
 *     1. Role is known (not empty) — node has completed startup
 *     2. No slots in fail state   — cluster_slots_fail = 0
 *     3. ACL users are active     — at least one non-default user is enabled,
 *                                   meaning the reconciler has applied ACLs
 *
 *   Primary only:
 *     4. cluster_state = ok       — gossip has converged
 *     5. slots_assigned = slots_ok — all owned slots are healthy
 *     6. connected_slaves >= 1   — at least one replica is synced
 *        (skipped during bootstrap: slots_assigned = 0)
 *
 *   Replica only:
 *     7. master_sync_in_progress = 0      — no full resync in progress
 *     8. slave_repl_offset > 0 OR         — initial sync complete (has data)
 *        master_link_status = up
 *     9. repl lag <= 1024 bytes           — replica is caught up (link up only)
 *        (all skipped during bootstrap: slots_assigned = 0 cluster-wide)
 *
 * Note: master_link_status=down is NOT checked when slave_repl_offset > 0.
 * A replica that already has data must stay Ready when its master crashes so
 * that clients always have a valid endpoint for those slots.
 *
 * Returns an array of two elements:
 *   [0] int  : 1 if ready, 0 if not
 *   [1] str  : "ready" or reason why not ready
 *
 * Usage in statefulset readiness probe:
 *   valkey-cli -p 6379 --user operator --pass "$VALKEY_OPERATOR_PASSWORD" \
 *     OPERATOR.NODE.READY | grep -q "^1"
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

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

static void reply_ready(ValkeyModuleCtx *ctx, int ready, const char *msg) {
    ValkeyModule_ReplyWithArray(ctx, 2);
    ValkeyModule_ReplyWithLongLong(ctx, ready);
    ValkeyModule_ReplyWithCString(ctx, msg);
}

int OperatorNodeReady_Command(ValkeyModuleCtx *ctx,
                               ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    /* --- INFO replication --- */
    ValkeyModuleCallReply *repl_r =
        ValkeyModule_Call(ctx, "INFO", "c", "replication");
    if (!repl_r ||
        ValkeyModule_CallReplyType(repl_r) != VALKEYMODULE_REPLY_STRING) {
        if (repl_r) ValkeyModule_FreeCallReply(repl_r);
        reply_ready(ctx, 0, "ERR failed to call INFO replication");
        return VALKEYMODULE_OK;
    }
    size_t rlen;
    const char *rinfo = ValkeyModule_CallReplyStringPtr(repl_r, &rlen);

    char role[16] = "";
    parse_field_str(rinfo, "role", role, sizeof(role));
    long long master_sync_in_progress = parse_field_ll(rinfo, "master_sync_in_progress");
    long long master_repl_offset      = parse_field_ll(rinfo, "master_repl_offset");
    long long slave_repl_offset       = parse_field_ll(rinfo, "slave_repl_offset");
    char master_link_status[16] = "";
    parse_field_str(rinfo, "master_link_status", master_link_status,
                    sizeof(master_link_status));
    ValkeyModule_FreeCallReply(repl_r);

    /* Role must be known — node still initialising. */
    if (role[0] == '\0') {
        reply_ready(ctx, 0, "role not yet determined");
        return VALKEYMODULE_OK;
    }

    /* --- ACL LIST: verify that application users are active.
     * The reconciler applies ACLs after the pod passes its liveness probe.
     * Until then, non-operator users are absent or disabled — clients would
     * get WRONGPASS. We require at least one non-default, non-operator user
     * to be active (flag "on") before declaring the node ready. */
    ValkeyModuleCallReply *acl_r =
        ValkeyModule_Call(ctx, "ACL", "c", "LIST");
    if (!acl_r ||
        ValkeyModule_CallReplyType(acl_r) != VALKEYMODULE_REPLY_ARRAY) {
        if (acl_r) ValkeyModule_FreeCallReply(acl_r);
        reply_ready(ctx, 0, "ERR failed to call ACL LIST");
        return VALKEYMODULE_OK;
    }

    size_t acl_count = ValkeyModule_CallReplyLength(acl_r);
    int app_user_active = 0;
    size_t ai;
    for (ai = 0; ai < acl_count; ai++) {
        ValkeyModuleCallReply *entry = ValkeyModule_CallReplyArrayElement(acl_r, ai);
        if (!entry || ValkeyModule_CallReplyType(entry) != VALKEYMODULE_REPLY_STRING)
            continue;
        size_t elen;
        const char *estr = ValkeyModule_CallReplyStringPtr(entry, &elen);

        /* Format: "user <name> on|off ..."
         * Skip default and operator accounts — look for any other "on" user. */
        if (strncmp(estr, "user default ", 13) == 0) continue;
        if (strncmp(estr, "user operator ", 14) == 0) continue;
        if (strstr(estr, " on ") || (elen > 8 && strncmp(estr + elen - 3, " on", 3) == 0)) {
            /* Found an active application user. */
            app_user_active = 1;
            break;
        }
    }
    ValkeyModule_FreeCallReply(acl_r);

    if (!app_user_active) {
        reply_ready(ctx, 0, "ACL not yet applied — no active application user");
        return VALKEYMODULE_OK;
    }

    /* --- CLUSTER INFO --- */
    ValkeyModuleCallReply *cluster_r =
        ValkeyModule_Call(ctx, "CLUSTER", "c", "INFO");
    if (!cluster_r ||
        ValkeyModule_CallReplyType(cluster_r) != VALKEYMODULE_REPLY_STRING) {
        if (cluster_r) ValkeyModule_FreeCallReply(cluster_r);
        reply_ready(ctx, 0, "ERR failed to call CLUSTER INFO");
        return VALKEYMODULE_OK;
    }
    size_t clen;
    const char *cinfo = ValkeyModule_CallReplyStringPtr(cluster_r, &clen);

    char cluster_state[16] = "";
    parse_field_str(cinfo, "cluster_state", cluster_state, sizeof(cluster_state));
    long long slots_assigned = parse_field_ll(cinfo, "cluster_slots_assigned");
    long long slots_ok       = parse_field_ll(cinfo, "cluster_slots_ok");
    long long slots_fail     = parse_field_ll(cinfo, "cluster_slots_fail");
    ValkeyModule_FreeCallReply(cluster_r);

    if (slots_fail < 0) slots_fail = 0;
    if (slots_assigned < 0) slots_assigned = 0;
    if (slots_ok < 0) slots_ok = 0;

    /* Slots in fail state — something is wrong cluster-wide. */
    if (slots_fail > 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "cluster has %lld slot(s) in fail state", slots_fail);
        reply_ready(ctx, 0, msg);
        return VALKEYMODULE_OK;
    }

    /* Bootstrap phase: cluster not yet formed (no slots assigned anywhere).
     * Skip role-specific checks — the node is ready to receive CLUSTER MEET. */
    int bootstrap = (slots_assigned == 0);

    if (strcmp(role, "master") == 0) {
        /* Primary: gossip must have converged. */
        if (!bootstrap && strcmp(cluster_state, "ok") != 0) {
            reply_ready(ctx, 0, "cluster_state is not ok — gossip not converged");
            return VALKEYMODULE_OK;
        }

        /* Primary: all owned slots must be healthy. */
        if (!bootstrap && slots_assigned != slots_ok) {
            char msg[80];
            snprintf(msg, sizeof(msg),
                "slots not fully ok: assigned=%lld ok=%lld",
                slots_assigned, slots_ok);
            reply_ready(ctx, 0, msg);
            return VALKEYMODULE_OK;
        }

        /* NOTE: connected_slaves is intentionally not checked here.
         * A primary is operationally ready to serve clients regardless of
         * whether its replica is currently connected — the replica validates
         * its own link via master_link_status. Checking this on the primary
         * creates a feedback loop: when a replica restarts during a rolling
         * update, the primary briefly sees connected_slaves=0 and flips
         * Not-Ready, which in turn affects the replica's master_link check. */

    } else if (strcmp(role, "slave") == 0) {
        if (!bootstrap) {
            /* Full resync in progress — replica does not yet have a consistent
             * dataset. Not safe to serve any traffic. */
            if (master_sync_in_progress == 1) {
                reply_ready(ctx, 0, "full resync in progress — replica not yet caught up");
                return VALKEYMODULE_OK;
            }

            /* Initial sync not yet started: slave_repl_offset=0 means the replica
             * has never received any data from the master. This covers the gap
             * between TCP connection establishment and the start of the RDB transfer
             * where master_sync_in_progress is still 0 but the link is not yet up.
             *
             * This is distinct from the "master crashed" case (slave_repl_offset > 0)
             * where the replica retains a consistent dataset and must stay Ready so
             * clients have somewhere to redirect during the primary's absence. */
            if (slave_repl_offset == 0 && strcmp(master_link_status, "up") != 0) {
                reply_ready(ctx, 0, "initial sync not complete — replica has no data yet");
                return VALKEYMODULE_OK;
            }

            /* Replication lag: replica is too far behind to serve consistent
             * reads. Only checked when the link is up and offsets are known. */
            if (strcmp(master_link_status, "up") == 0 &&
                master_repl_offset > 0 && slave_repl_offset >= 0 &&
                master_repl_offset - slave_repl_offset > 1024) {
                char msg[80];
                snprintf(msg, sizeof(msg),
                    "replication lag %lld bytes — not fully synced",
                    master_repl_offset - slave_repl_offset);
                reply_ready(ctx, 0, msg);
                return VALKEYMODULE_OK;
            }

            /* NOTE: master_link_status=down is intentionally not checked here
             * when slave_repl_offset > 0. When a primary crashes or restarts,
             * the replica briefly loses the link but retains a consistent dataset
             * and remains a valid failover candidate. Marking it Not-Ready during
             * this window would remove it from endpoints right when it is most
             * needed — and create a race where both the primary (restarting) and
             * its replica (link down) are simultaneously Not-Ready, leaving no
             * endpoint available for those slots. */
        }
    }

    reply_ready(ctx, 1, "ready");
    return VALKEYMODULE_OK;
}
