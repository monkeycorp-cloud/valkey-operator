/**
 * module.c — entry point for the valkey-operator module.
 *
 * Registers all OPERATOR.* commands with the Valkey module API.
 * Loaded via: loadmodule /usr/local/lib/valkey-operator-module.so
 */

#include "valkeymodule.h"

/* Forward declarations from other translation units. */
int Gate_Init(ValkeyModuleCtx *ctx);
int Failover_Init(ValkeyModuleCtx *ctx);
int OperatorHealth_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int OperatorBootstrapReady_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int OperatorFailoverPrepare_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int OperatorTopologySet_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int OperatorClusterSafe_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int OperatorNodeReady_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int OperatorSlotStats_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);
int OperatorNodeState_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx,
                        ValkeyModuleString **argv,
                        int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "operator", 1, VALKEYMODULE_APIVER_1) ==
        VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* ACL readiness gate — must be armed before any command is registered.
     * Blocks non-operator AUTH with LOADING error until ACLs are applied,
     * preventing WRONGPASS on clients redirected here via MOVED during startup. */
    if (Gate_Init(ctx) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* Failover event subscription — subscribes to ReplicationRoleChanged so
     * OPERATOR.FAILOVER.PREPARE can unblock the waiting client via event (~5ms)
     * instead of relying on the BlockClient timeout. Non-fatal. */
    Failover_Init(ctx);

    /* OPERATOR.HEALTH — single-call health snapshot for the operator reconciler.
     * Replaces two separate round-trips (CLUSTER INFO + INFO replication).
     * Read-only, no key access, safe to run on any node at any time. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.health",
            OperatorHealth_Command,
            "readonly fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* OPERATOR.BOOTSTRAP.READY — checks whether this node is ready for --cluster create.
     * Returns [1, "ready"] or [0, "<reason>"].
     * Used by the operator to gate bootstrap until all nodes are in a clean state. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.bootstrap.ready",
            OperatorBootstrapReady_Command,
            "readonly fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* OPERATOR.FAILOVER.PREPARE [timeout_ms]
     * Called from the PreStop hook AFTER the shell script has sent CLUSTER FAILOVER
     * to a replica. Issues CLIENT PAUSE WRITE, blocks the client (event loop free),
     * unblocks on ReplicationRoleChanged, issues WAIT 1 500 + UNPAUSE.
     * Uses ValkeyModule_BlockClient — no nanosleep, event loop never blocked. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.failover.prepare",
            OperatorFailoverPrepare_Command,
            "readonly fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* OPERATOR.TOPOLOGY.SET <json>
     * Pushes cluster topology to this node: CLUSTER MEET all peers, then
     * CLUSTER REPLICATE <primary> if role=replica. Idempotent.
     * Returns [ok, message, meet_count]. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.topology.set",
            OperatorTopologySet_Command,
            "write fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }


    /* OPERATOR.CLUSTER.SAFE
     * Checks whether this node can be stopped without risking CLUSTERDOWN.
     * Called from the PreStop hook before OPERATOR.FAILOVER.PREPARE.
     * Replicas always return safe. Primaries check connected replicas and
     * the number of other healthy primaries remaining after this node stops.
     * Returns [ok, message]. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.cluster.safe",
            OperatorClusterSafe_Command,
            "readonly fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* OPERATOR.NODE.READY
     * Readiness probe replacement — returns [1, "ready"] only when the node
     * is fully operational: gossip converged, slots healthy, replication up.
     * Kubernetes advances the rolling update to the next pod only when this
     * returns 1, making minReadySeconds redundant. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.node.ready",
            OperatorNodeReady_Command,
            "readonly fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* OPERATOR.SLOT.STATS
     * Returns per-shard stats for this node: key count, memory, role, slot
     * ranges, and replication offset. O(1) — no SCAN.
     * Called by the operator on each primary to detect slot imbalance and
     * trigger valkey-cli --cluster rebalance when memory skew exceeds threshold. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.slot.stats",
            OperatorSlotStats_Command,
            "readonly fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    /* OPERATOR.NODE.STATE
     * Returns the persistent cluster role of this node as recorded in nodes.conf:
     * role (primary/replica), master_id, and master_addr.
     * Used by the operator after a brutal restart to restore CLUSTER REPLICATE
     * assignments without heuristics, even when cluster_state:fail. */
    if (ValkeyModule_CreateCommand(ctx,
            "operator.node.state",
            OperatorNodeState_Command,
            "readonly fast",
            0, 0, 0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    return VALKEYMODULE_OK;
}
