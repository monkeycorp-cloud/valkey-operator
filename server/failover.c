/**
 * failover.c — OPERATOR.FAILOVER.PREPARE command implementation.
 *
 * Called from the PreStop hook AFTER the shell script has already sent
 * CLUSTER FAILOVER to a replica via valkey-cli. This command handles:
 *   1. CLIENT PAUSE WRITE — buffer client writes during the swap
 *   2. WAIT 1 500 — confirm replication to at least one replica
 *   3. CLIENT UNPAUSE — lift the pause, clients receive MOVED
 *
 * All three operations execute atomically in the Valkey main thread —
 * no fork, no exec, no network round-trips between steps.
 *
 * CLUSTER FAILOVER is NOT issued here — it is a replica-only command and
 * must be sent to the replica by the caller (PreStop shell script).
 *
 * Behaviour:
 *   - Replica : immediate success — nothing to hand off.
 *   - Primary : CLIENT PAUSE WRITE, WAIT 1 500, CLIENT UNPAUSE.
 *               Total duration: ~500ms (WAIT cap).
 *
 * Arguments (optional):
 *   OPERATOR.FAILOVER.PREPARE [timeout_ms]
 *   timeout_ms : CLIENT PAUSE WRITE duration in ms (default: 5000)
 *                Acts as a safety cap — UNPAUSE is issued before that.
 *
 * Returns an array of three elements:
 *   [0] int  : 1 = success, 0 = failure
 *   [1] str  : "ok", "replica — no action needed", or error description
 *   [2] int  : elapsed milliseconds
 */

#include "valkeymodule.h"
#include <string.h>
#include <stdlib.h>
#include <time.h>

#define DEFAULT_TIMEOUT_MS 5000

/* --- Helpers --- */

static long long elapsed_ms(struct timespec *start) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (long long)(now.tv_sec - start->tv_sec) * 1000
         + (now.tv_nsec - start->tv_nsec) / 1000000;
}

static void reply_result(ValkeyModuleCtx *ctx, int ok, const char *msg, long long ms) {
    ValkeyModule_ReplyWithArray(ctx, 3);
    ValkeyModule_ReplyWithLongLong(ctx, ok);
    ValkeyModule_ReplyWithCString(ctx, msg);
    ValkeyModule_ReplyWithLongLong(ctx, ms);
}

static int parse_field_str(const char *info, const char *field, char *buf, size_t buflen) {
    const char *p = strstr(info, field);
    if (!p) return -1;
    p += strlen(field);
    if (*p != ':') return -1;
    p++;
    size_t i = 0;
    while (*p && *p != '\r' && *p != '\n' && i < buflen - 1)
        buf[i++] = *p++;
    buf[i] = '\0';
    return 0;
}

static long long parse_field_ll(const char *info, const char *field) {
    const char *p = strstr(info, field);
    if (!p) return -1;
    p += strlen(field);
    if (*p != ':') return -1;
    return atoll(p + 1);
}

/* --- Init --- */

int Failover_Init(ValkeyModuleCtx *ctx) {
    (void)ctx;
    return VALKEYMODULE_OK;
}

/* --- Command --- */

int OperatorFailoverPrepare_Command(ValkeyModuleCtx *ctx,
                                    ValkeyModuleString **argv, int argc) {
    long long timeout_ms = DEFAULT_TIMEOUT_MS;
    if (argc >= 2) {
        if (ValkeyModule_StringToLongLong(argv[1], &timeout_ms) != VALKEYMODULE_OK
                || timeout_ms <= 0) {
            timeout_ms = DEFAULT_TIMEOUT_MS;
        }
    }

    /* Check current role via INFO replication. */
    ValkeyModuleCallReply *repl_r =
        ValkeyModule_Call(ctx, "INFO", "c", "replication");
    if (!repl_r ||
        ValkeyModule_CallReplyType(repl_r) != VALKEYMODULE_REPLY_STRING) {
        if (repl_r) ValkeyModule_FreeCallReply(repl_r);
        reply_result(ctx, 0, "ERR failed to call INFO replication", 0);
        return VALKEYMODULE_OK;
    }
    size_t rlen;
    const char *rstr = ValkeyModule_CallReplyStringPtr(repl_r, &rlen);

    char role[16] = "";
    parse_field_str(rstr, "role", role, sizeof(role));
    long long connected_slaves = parse_field_ll(rstr, "connected_slaves");
    ValkeyModule_FreeCallReply(repl_r);

    /* Replica — nothing to do. */
    if (strcmp(role, "slave") == 0) {
        reply_result(ctx, 1, "replica — no action needed", 0);
        return VALKEYMODULE_OK;
    }

    if (strcmp(role, "master") != 0) {
        reply_result(ctx, 0, "unknown role — cannot prepare failover", 0);
        return VALKEYMODULE_OK;
    }

    if (connected_slaves <= 0) {
        reply_result(ctx, 0, "no connected replica — failover not possible", 0);
        return VALKEYMODULE_OK;
    }

    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

    /* Pause client writes. WRITE modifier preserves the replication channel
     * so REPLCONF GETACK can flow freely during the cooperative handshake. */
    char pause_str[32];
    snprintf(pause_str, sizeof(pause_str), "%lld", timeout_ms);
    ValkeyModuleCallReply *pause_r =
        ValkeyModule_Call(ctx, "CLIENT", "ccc", "PAUSE", pause_str, "WRITE");
    if (pause_r) ValkeyModule_FreeCallReply(pause_r);

    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_NOTICE,
        "operator-failover: CLIENT PAUSE WRITE %lldms", timeout_ms);

    /* WAIT 1 500: confirm the new primary has received the last writes
     * before lifting the pause. 500ms cap — non-fatal if no replica acks. */
    ValkeyModuleCallReply *wait_r =
        ValkeyModule_Call(ctx, "WAIT", "cc", "1", "500");
    if (wait_r) ValkeyModule_FreeCallReply(wait_r);

    /* Lift write pause — clients receive MOVED and redirect transparently. */
    ValkeyModuleCallReply *up =
        ValkeyModule_Call(ctx, "CLIENT", "c", "UNPAUSE");
    if (up) ValkeyModule_FreeCallReply(up);

    long long ms = elapsed_ms(&start);
    ValkeyModule_Log(ctx, VALKEYMODULE_LOGLEVEL_NOTICE,
        "operator-failover: done in %lldms", ms);

    reply_result(ctx, 1, "ok", ms);
    return VALKEYMODULE_OK;
}
