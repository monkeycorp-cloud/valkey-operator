# Reconciliation Flows — Valkey Operator

This document maps all reconciliation flows handled by the operator. Two families are distinguished:

- **Family 1** — normal cluster operation (steady state + expected failure modes)
- **Family 2** — corner cases (exceptional events, rare operational mistakes)

The main reconcile loop runs every 30 s (`SyncPeriod`). Steps 9–11 are deferred when `isStatefulSetStable()` returns false (during rolling updates or scale events).

---

## Family 1 — Healthy cluster in production

### 1A. Nominal cycle (everything is fine)

No Valkey-modifying commands are emitted. All reconcilers are read-only.

```
reconcileClusterTopology  → state:ok, slots:16384 → syncRoleLabels (no-op)
reconcileClusterHealth    → all nodes ok           → no conditions raised
reconcileOrphanReplicas   → no standalones         → no-op
reconcileShardStats       → skew < threshold        → no-op
reconcileReplicaRoles     → gossip-recovery-done absent → no-op (guard)
```

**Invariant:** No mutating commands issued in a healthy cycle.

---

### 1B. Pod crash / restart

**Sub-case 1: fast restart (< 30 s), same IP**

- `reconcileClusterHealth` → `stale-since` annotation placed on the node
- 30 s grace period → no `CLUSTER FORGET`
- Pod comes back → annotation cleared → `ShardUnderReplicated` cleared
- **No destructive command emitted**

**Sub-case 2: slow restart (> 30 s) or IP change**

- `forgetStaleNodes` → `CLUSTER FORGET` after 30 s if node is `fail+disconnected`
- `reconcileOrphanReplicas` → pod comes back standalone (`cluster_known_nodes ≤ 1`)
  - `OPERATOR.TOPOLOGY.SET role=replica` → `CLUSTER MEET` + `CLUSTER REPLICATE`
- `ShardUnderReplicated` condition raised after 30 s grace if not resolved

**Critical guards:** `forgetStaleNodes` is skipped when:
- Any pod has a `DeletionTimestamp` (eviction / manual delete)
- A rolling update is in progress (`CurrentRevision ≠ UpdateRevision`)
- A scale operation is in progress (`sts.Status.Replicas ≠ totalPods`)

---

### 1C. Network flap (pod temporarily unreachable)

- `queryClusterInfo` times out → operator falls back to another pod as seed
- `reconcileClusterHealth` → node in `pfail`, not yet `fail` → no `CLUSTER FORGET`
- If flap exceeds `cluster-node-timeout` → Valkey marks the node `fail` automatically
- `forgetStaleNodes` → 30 s grace → `CLUSTER FORGET` if still failing
- **Guard:** rolling update in progress → `CLUSTER FORGET` skipped

---

### 1D. Rolling update (image change)

- `isStatefulSetStable()` → detects `CurrentRevision ≠ UpdateRevision` → defers Steps 9–11
- PreStop hook (primary pod only):
  1. `OPERATOR.CLUSTER.SAFE` — safety check (CLUSTERDOWN risk)
  2. `CLUSTER FAILOVER` sent to best replica (returns immediately, handshake begins)
  3. `OPERATOR.FAILOVER.PREPARE <timeout_ms>` — `CLIENT PAUSE WRITE` + `WAIT 1 500` + `CLIENT UNPAUSE`, executed atomically in the Valkey event loop
  4. Poll `INFO replication` until `role:slave` (up to 5 s) — ensures `nodes.conf` is saved as slave on PVC so the pod restarts as replica, not standalone master
- PreStop hook (replica pod): waits for `OPERATOR.CLUSTER.SAFE`, then exits (no failover needed)
- Pod restarts → readiness probe (`OPERATOR.NODE.READY`) blocks traffic during resync
- `forgetStaleNodes` → **skipped** during rolling update
- `reconcileOrphanReplicas` → **skipped** (StatefulSet unstable)
- When all pods are ready → `isStatefulSetStable()=true` → normal cycle resumes

---

### 1E. Node drain (GKE upgrade / kubectl cordon)

- `reconcileNodeDrainFailover` → detects primary on cordoned node
- `pickBestReplicaForShard` → selects best replica (lag + zone)
- `CLUSTER FAILOVER` issued on the selected replica
- Polls 15 s for confirmation → updates role labels
- Requeues after 2 s
- **Guard:** only if pod is `Running` and not terminating

---

### 1F. ShardImbalanced (memory skew)

- `reconcileShardStats` → `OPERATOR.SLOT.STATS` on each primary
- 60 s grace period via `valkey.io/shard-imbalanced-since` annotation
- If `spec.rebalance.enabled=true` → `reconcileRebalance` → one-shot `valkey-cli --cluster rebalance` Job
- Job: `BackoffLimit=0`, `TTLSecondsAfterFinished=300`

---

### 1G. CrashLoop (corrupted RDB)

- `reconcileCrashLoopRecovery` → triggers when `RestartCount ≥ 3` and CrashLoopBackOff > 3 min
- Cleanup Job: deletes `dump.rdb` + `nodes.conf` on the PVC
- Pod is deleted only after `Job.Active > 0` (PVC mount prevents concurrent writes)
- Pod restarts clean → `reconcileOrphanReplicas` re-joins it as a replica

---

## Family 2 — Corner cases

### 2A. Brutal restart with IP change (Docker restart, Kind node reboot)

All pods restart simultaneously. `nodes.conf` is stale (old IPs). Each pod auto-promotes to standalone primary (Valkey cannot reach its former master).

**Phase 1 — `cluster_state:fail`**

- `reconcileClusterTopology` → `state=fail`, `slots>0`
- `gossipRecoveryTimedOut` → `valkey.io/gossip-recovery-start` annotation placed
- Natural convergence wait (30–120 s depending on shard count)
- If timeout → `resetGossipRecoveryAnnotation` (clears `start` only, does NOT set `done`) → `reconcileGossipRecovery`
  - `OPERATOR.TOPOLOGY.SET role=primary` on all running pods (triggers `CLUSTER MEET` with fresh IPs)

**Phase 2 — `cluster_state:ok`, replicas lost**

- `clearGossipRecoveryAnnotation` → clears `start` AND sets `valkey.io/gossip-recovery-done=true`
- `reconcileClusterHealth` → `ShardUnderReplicated=True` (30 s grace)
- `reconcileReplicaRoles` (gated on `gossip-recovery-done`):
  1. Wait until `cluster_known_nodes ≤ expected` — ghost `handshake disconnected` entries expire after ~50 s (see P3 below)
  2. Clear `gossip-recovery-done` annotation
  3. Parse `CLUSTER NODES` gossip view directly — identify `master` nodes with no slot ranges and no `fail`/`noaddr` flags
  4. Resolve pod by IP (`podByIP` map from ready pods)
  5. `OPERATOR.TOPOLOGY.SET role=replica primary_addr=<ip:port>` → `CLUSTER REPLICATE`
  6. If any pod assignment failed (gossip not yet converged) → re-set `gossip-recovery-done` for retry next cycle

**Population overlap with `reconcileOrphanReplicas`:**
After brutal restart, each pod knows its former peers (stale `nodes.conf`) → `cluster_known_nodes > 1` → `reconcileOrphanReplicas` skips them. The two functions target disjoint populations. No conflict.

---

### 2B. Accidental pod deletion (`kubectl delete pod --all`)

- StatefulSet recreates pods with the same ordinals and PVCs
- `nodes.conf` intact on PV → pods rejoin the cluster normally
- If `nodes.conf` is stale (IP changed) → handled as case 2A

**If PVCs are also deleted:**

- Pods restart without `nodes.conf` → standalone (`cluster_known_nodes=1`)
- `reconcileOrphanReplicas` → `CLUSTER MEET` + `CLUSTER REPLICATE`
- **Edge case:** if all pods are simultaneously deleted → no seed available for `fetchClusterNodesWithSeed` → no re-join possible
  → `reconcileBootstrapJob` detects `slots=0` → **full `CLUSTER RESET` + re-bootstrap**

---

### 2C. Accidental scale to zero (`kubectl scale sts --replicas=0`)

- `isStatefulSetStable()=false` → all repair steps deferred
- Pods terminated → `reconcileClusterHealth` → nodes `fail` → `CLUSTER FORGET` skipped while scale in progress; issued after 30 s once stable
- On scale-up → new pods without `nodes.conf` → standalone
- `reconcileOrphanReplicas` → normal re-integration (if quorum still available)
- **If all primaries were forgotten** → unassigned slots → re-bootstrap triggered

---

### 2D. Initial bootstrap (fresh cluster)

- All pods standalone (`cluster_known_nodes=1`)
- `reconcileBootstrapJob` → `CLUSTER RESET SOFT` + `valkey-cli --cluster create`
- Post-bootstrap → `reconcileClusterTopology` → `state:ok` → `syncRoleLabels`
- `gossip-recovery-done` is never set → `reconcileReplicaRoles` does not run ✓
- `reconcileOrphanReplicas` → pods already clustered → no-op ✓

---

## Guards and interference table

| Flow | Guard | Interference risk |
|------|-------|-------------------|
| `forgetStaleNodes` | Skip if pod has `DeletionTimestamp`; skip during rolling update; skip during scale operation | Restarting or scaling pod briefly looks stale — guards are correct |
| `reconcileOrphanReplicas` | `cluster_known_nodes ≤ 1` (standalone) | Does not target 2A pods (known_nodes > 1) |
| `reconcileReplicaRoles` | `gossip-recovery-done` annotation | **Critical:** without this guard, runs on fresh clusters and causes `ERR CLUSTER REPLICATE on a node with assigned slots` |
| `reconcileBootstrapJob` | Skipped if `alreadyFormed=true` (slots > 0) | 2A short-circuits before this path |
| `reconcileShardStats` | Requires ≥ 2 primaries Ready | No-op when cluster is degraded |
| `reconcileRebalance` | `ShardImbalanced=True` + `spec.rebalance.enabled=true` | One-shot Job, auto-cleaned after TTL |
| `reconcileCrashLoopRecovery` | 3 min CrashLoopBackOff + `RestartCount ≥ 3` | Deletes `nodes.conf` → pod restarts standalone → picked up by orphan repair |

---

## Known edge cases and fixes

### P1 — Migration markers in `CLUSTER NODES` slot field

**Problem:** The "myself" line slot field (field 8+) can contain migration markers
(`[slot->-nodeID]`, `[slot-<-nodeID]`) in addition to real slot ranges. A node
participating in a migration has markers but may own no stable slots. Counting
markers as slots produces a false positive for `has_slots`.

**Fix (server/node_state.c `parse_myself_line`):** Skip any token starting with
`[` when scanning field 8+. Only set `has_own_slots=1` if at least one
non-marker, non-space token is found.

---

### P2 — No retry when gossip not yet converged

**Problem:** `OPERATOR.TOPOLOGY.SET` may reply "primary node ID not yet known" if
gossip has not fully converged. The `gossip-recovery-done` annotation was cleared
at the start of `reconcileReplicaRoles`, so the failed assignment had no way to
trigger a retry on the next cycle.

**Fix (internal/controller/repair.go):** Track `pendingPods` count. If > 0 after
the assignment loop, re-set `gossip-recovery-done=true` before returning. The next
reconcile cycle will retry only the remaining pods.

---

### P3 — Ghost nodes after gossip recovery (`cluster_known_nodes` inflated)

**Problem:** `reconcileGossipRecovery` issues `CLUSTER MEET` with fresh IPs. Old
stale-IP entries remain as `handshake disconnected` in the gossip table for
approximately `cluster-node-timeout × 10` (≈ 50 s with the default 5 000 ms
timeout). These inflate `cluster_known_nodes` above the expected pod count.
`CLUSTER FORGET` cannot purge them (no node ID assigned to handshaking nodes).
Acting during this window risks assigning replicas to the wrong primary.

**Fix (internal/controller/repair.go):** At the start of `reconcileReplicaRoles`,
compare `cluster_known_nodes` against the expected pod count. If inflated, return
without clearing the annotation — the function retries on the next cycle once
ghost nodes have expired naturally.

---

### P4 — PVC-persisted `nodes.conf` causes stale `has_slots` during rolling update

**Problem:** After a rolling update, a restarted node reads its `nodes.conf` from the PVC.
This file may still list the slots the node owned before failover. `OPERATOR.NODE.STATE` reads
`has_slots` from the `myself` line in `CLUSTER NODES`, which reflects the **local** view at
startup — not yet converged with the cluster gossip. The operator sees `has_slots=1` and skips
the node in `reconcileReplicaRoles`, leaving it as a master without slots indefinitely (4 masters
instead of 3).

**Fix (`internal/controller/repair.go` — `reconcileReplicaRoles`):** Use the `CLUSTER NODES`
gossip view (queried from a stable seed pod) as the authoritative source. A node appearing as
`master` with an empty slot field in the gossip view is a genuine candidate for reassignment —
regardless of what its local `nodes.conf` says. `OPERATOR.NODE.STATE` is no longer used here.

**Fix (`internal/controller/repair.go` — `pickMasterForReplica`):** Skip masters with an empty
slot field (`n.slots == ""`). A master without slots cannot accept `CLUSTER REPLICATE` — Valkey
returns `ERR To set a master the node must be empty and without assigned slots`.

---

## Annotation lifecycle

| Annotation | Set by | Cleared by | Purpose |
|------------|--------|------------|---------|
| `valkey.io/gossip-recovery-start` | `gossipRecoveryTimedOut` | `clearGossipRecoveryAnnotation`, `resetGossipRecoveryAnnotation` | Tracks when gossip recovery started (timer) |
| `valkey.io/gossip-recovery-done` | `clearGossipRecoveryAnnotation` (post-recovery) | `reconcileReplicaRoles` (on start, re-set on retry) | Gates `reconcileReplicaRoles` to post-recovery context only |
| `valkey.io/stale-since` | `reconcileClusterHealth` | `reconcileClusterHealth` (node comes back) | Grace period before `CLUSTER FORGET` |
| `valkey.io/last-reset-time` | `resetAllNodes` | `clearClusterNotOKAnnotation` (cluster ok) | 5 min cooldown between `CLUSTER RESET SOFT` operations |
| `valkey.io/shard-imbalanced-since` | `reconcileShardStats` | `reconcileShardStats` (skew resolved) | 60 s grace before rebalance Job |

---

## Key files

| File | Role |
|------|------|
| `internal/controller/valkeycluster_controller.go` | Main reconcile loop, step ordering, global guards |
| `internal/controller/cluster.go` | Bootstrap, gossip recovery, annotation helpers |
| `internal/controller/health.go` | Conditions, stale node cleanup, under-replication detection |
| `internal/controller/repair.go` | Orphan repair (`reconcileOrphanReplicas`), replica role restoration (`reconcileReplicaRoles`) |
| `internal/controller/recovery.go` | CrashLoop recovery |
| `internal/controller/failover.go` | Node drain and rolling update failover |
| `server/node_state.c` | `OPERATOR.NODE.STATE` — exposes `has_slots` from local `CLUSTER NODES` "myself" line (note: reflects PVC-persisted nodes.conf at startup, not yet converged gossip) |
| `server/topology.c` | `OPERATOR.TOPOLOGY.SET` — idempotent `CLUSTER MEET` + `CLUSTER REPLICATE` |
