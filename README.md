# Valkey Operator

A production-grade Kubernetes operator for managing [Valkey](https://valkey.io) clusters in cluster mode (sharded, hash slots).

Built with [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime) v0.23, targeting Kubernetes 1.34+.

## Features

### Cluster lifecycle
- **Valkey Cluster mode** — 3+ primaries, configurable replicas per shard, 16384 hash slots distributed via `valkey-cli --cluster create`
- **Idempotent bootstrap** — one-shot Kubernetes Job with PING wait, idempotency check, and stale `nodes.conf` reset; annotation-gated so it never runs twice
- **ACL baked into `valkey.conf`** — operator account exists from the first millisecond; no unauthenticated window during bootstrap; `default` account disabled
- **Role label sync** — `valkey.io/role=primary|replica` labels kept in sync from `CLUSTER NODES` each reconcile cycle

### Zero-downtime operations
- **Zero-downtime rolling updates** — `CLUSTER FAILOVER` issued from the PreStop hook before SIGTERM; role swap confirmed via `INFO replication`; `CLIENT PAUSE` buffers writes during the handover window
- **WAIT after failover** — `WAIT 1 500` issued after role swap to guarantee at least one replica has acknowledged the final writes before the pod exits
- **StatefulSet stability guard** — cluster topology commands deferred while a rolling update or scale is in progress
- **GKE nodepool upgrade awareness** — detects cordoned nodes (`Unschedulable=true`) and triggers failover before drain
- **`cluster-allow-reads-when-down yes`** — replicas continue serving reads even when `cluster_state=fail`, preventing client errors during transient failures

### ACL readiness gate (`gate.c`)
- **`CLUSTER_FLAG_NO_REDIRECTION` at startup** — the node does not issue MOVED responses based on potentially stale topology while warming up (~3s after restart)
- **500ms ACL poll timer** — `ACL LIST` polled every 500ms; `NO_REDIRECTION` is cleared as soon as an active application user is detected, confirming the reconciler has applied ACLs
- **Zero WRONGPASS on rolling updates** — clients are never routed to a node that cannot authenticate them; the gate and the `OPERATOR.NODE.READY` readiness probe work in tandem

### Resilience and self-healing
- **Corrupted RDB recovery** — detects pods stuck in `CrashLoopBackOff` (≥3 restarts, >3 min) due to a corrupt RDB; deletes `dump.rdb` and `nodes.conf` via a recovery Job then recreates the pod for clean resync from primary
- **Stale node cleanup** — `CLUSTER FORGET` with 30s grace period, skipped during rolling updates and pod evictions
- **Under-replication grace period** — `ShardUnderReplicated` condition only raised after 30s of confirmed under-replication to avoid false positives during gossip convergence
- **Orphan replica reintegration** — idempotent: `OPERATOR.TOPOLOGY.SET` skips `CLUSTER REPLICATE` if the pod already replicates the correct primary

### Readiness probe design
- **`OPERATOR.NODE.READY` module command** — single call replacing multi-step shell scripts; checks role, gossip state, slot health, and ACL presence
- **`FailureThreshold: 1`** — single failure immediately removes the pod from endpoints, minimising the window where a restarted pod (with lost in-memory ACLs) receives traffic
- **`SuccessThreshold: 1`** — no artificial delay before marking Ready; the gate and probe invariants are sufficient — a higher threshold would leave the pod in cluster gossip but absent from endpoints (a race condition)
- **Primary probe** — checks `cluster_state=ok` and `slots_ok == slots_assigned`; does not check `connected_slaves` (avoids a feedback loop where primary and replica flip simultaneously)
- **Replica probe** — blocks only on `master_sync_in_progress=1` (inconsistent dataset) or replication lag > 1024 bytes with link up; does not fail when `master_link_status=down` so the replica stays Ready as a failover candidate when its primary crashes

### Observability
- **Structured status conditions** — `Available`, `ClusterDegraded`, `ShardUnderReplicated`, `ShardColocated`, `RollingUpdate` conditions with reasons and messages
- **Rolling update phase** — `status.phase=Updating` and `RollingUpdate` condition show progress (`N/M pods updated`) visible in `kubectl get vc -o wide`
- **Kubernetes Events** — warning events for degraded states, failovers, stale node cleanup, and RDB recovery
- **Prometheus metrics** — `redis_exporter` sidecar with dedicated ACL user, optional ServiceMonitor for Prometheus Operator
- **Colocation detection** — warns (`ShardColocated` condition) when a primary and its replica share the same Kubernetes **node** (same-zone is unavoidable with 3 zones / 6 pods and is not flagged)

### Security
- **ACL users** — dedicated `operator`, `metrics`, and application accounts per CRD spec; `default` account disabled at startup
- **masterauth / masteruser** — replica→primary RDB sync uses operator credentials baked into `valkey.conf`; no unauthenticated replication window
- **Secret-based credentials** — all passwords injected via Kubernetes Secrets, never stored in plain text in the CR
- **Rootless pods** — Valkey containers run as UID/GID 999 (`valkey` user), `RunAsNonRoot=true`, `AllowPrivilegeEscalation=false`, all Linux capabilities dropped; operator runs as UID 65532

### Placement and topology
- **Topology-aware placement** — hard `PodAntiAffinity` on `kubernetes.io/hostname`; soft zone spread via `TopologySpreadConstraints`
- **Topology-aware election** — replica scoring combines replication lag and zone preference with configurable weight (0–100)
- **PodDisruptionBudget** — `maxUnavailable: 1` to protect against aggressive infrastructure maintenance

### Configuration
- **Structured config** — `maxmemory` auto-calculated from pod memory limits × ratio, all common parameters exposed in CRD
- **`io-threads` auto-tuning** — automatically derived from `resources.limits.cpu` (< 4 CPUs → disabled, 4 → 2, 8 → 4, 16+ → 8); overridable via `spec.config.ioThreads`
- **CustomConfig escape hatch** — raw `valkey.conf` lines appended last, override any structured value
- **Config hash rolling update** — pod template annotation tracks config changes; any config update triggers a StatefulSet rolling update automatically
- **ACL hash reconciliation** — `valkey.io/acl-hash` annotation skips ACL apply on pods stable >60s when credentials are unchanged; reduces N connections per reconcile cycle on large clusters
- **Leader election** — safe multi-replica operator deployment via Kubernetes Lease

## Requirements

- Kubernetes 1.34+
- Prometheus Operator (optional, for ServiceMonitor)

## Quick Start

### 1. Create the operator secrets

```bash
kubectl create secret generic valkey-operator-secret \
  --from-literal=password=$(openssl rand -base64 32)

kubectl create secret generic valkey-metrics-secret \
  --from-literal=password=$(openssl rand -base64 32)

kubectl create secret generic valkey-app-readwrite-secret \
  --from-literal=password=$(openssl rand -base64 32)
```

### 2. Install the CRD and deploy the operator

```bash
make install   # installs the CRD
make deploy    # deploys the operator (namespace: valkey-operator-system)
```

Or with a custom image:

```bash
IMG=ghcr.io/your-org/valkey-operator:v1.0.0 make docker-build docker-push deploy
```

### 3. Create a ValkeyCluster

```bash
kubectl apply -f config/samples/cache_v1alpha1_valkeycluster.yaml
```

```bash
kubectl get valkeycluster
NAME             PHASE     READY   CLUSTER   SLOTS   AGE
valkey-sample    Running   6       ok        16384   2m
```

## CRD Reference

```yaml
apiVersion: cache.valkey.io/v1alpha1
kind: ValkeyCluster
metadata:
  name: valkey-sample
spec:
  # Cluster topology
  shards: 3                  # number of primary shards
  replicasPerShard: 1        # replicas per shard (0 = no replication)
  image: valkey/valkey:9.0.3-alpine

  # Resources
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi

  storage:
    size: 10Gi
    # storageClassName: fast-ssd  # optional

  port: 6379
  clusterNodeTimeout: 2000       # ms, default 2000
  terminationGracePeriodSeconds: 60

  # Operator account — full cluster management privileges
  operatorSecret:
    name: valkey-operator-secret
    key: password

  # Structured Valkey configuration
  config:
    maxmemoryPolicy: allkeys-lru
    maxmemoryRatio: 80       # maxmemory = limits.memory * 80%
    hz: 20
    lazyfree: true
    tcpKeepalive: 300
    # ioThreads: 4           # optional override; auto-derived from CPU limit when unset
    #   < 4 CPUs → disabled, 4 → 2, 8 → 4, 16+ → 8 (cap)

  # Raw config appended last (overrides structured values)
  # customConfig: |
  #   latency-monitor-threshold 10

  # Application ACL accounts (default account is disabled).
  # cluster|slots, cluster|shards and command are required by Redis/Valkey
  # cluster client libraries (redis-py, phpredis) for slot map discovery
  # and command introspection at connection init.
  aclUsers:
    - name: app-readwrite
      passwordSecret:
        name: valkey-app-readwrite-secret
        key: password
      keyPatterns: ["~app:*"]
      commands: "+@read +@write -@dangerous +command +cluster|slots +cluster|shards"

  # Prometheus metrics sidecar
  metrics:
    enabled: true
    image: oliver006/redis_exporter:latest
    metricsSecret:
      name: valkey-metrics-secret
      key: password
    serviceMonitor:
      enabled: true
      labels:
        release: kube-prometheus-stack
      interval: 30s
      scrapeTimeout: 10s

  # Topology-aware placement and failover
  topology:
    nodeTopologyKey: topology.kubernetes.io/zone
    nodeSpreadPolicy: Hard    # Hard | Soft | None
    zoneSpreadPolicy: Hard
    avoidSameZoneAsFailed: true
    electionTopologyWeight: 70  # 0=lag only, 100=topology only
    podAssignments:
      - podIndex: 0
        preferredValues: ["zone-a"]
      - podIndex: 1
        preferredValues: ["zone-b"]
      - podIndex: 2
        preferredValues: ["zone-c"]
```

## Status and Conditions

```bash
kubectl get valkeycluster valkey-sample -o yaml
```

```yaml
status:
  phase: Running          # Pending | Initializing | Running | Degraded | Scaling | Updating
  readyReplicas: 6
  clusterState: ok
  slotsOk: 16384
  nodesOk: 6
  podRoles:
    valkey-sample-0: primary
    valkey-sample-1: primary
    valkey-sample-2: primary
    valkey-sample-3: replica
    valkey-sample-4: replica
    valkey-sample-5: replica
  podTopology:
    valkey-sample-0: zone-a
    valkey-sample-1: zone-b
    valkey-sample-2: zone-c
  conditions:
    - type: Available
      status: "True"
      reason: ClusterReady
    - type: ClusterDegraded
      status: "False"
      reason: ClusterOK
    - type: ShardUnderReplicated
      status: "False"
      reason: ReplicationOK
    - type: ShardColocated
      status: "False"
      reason: TopologyOK
    - type: RollingUpdate
      status: "False"
      reason: UpdateComplete
      message: "all 6 pods on current revision"
```

## Rolling Update Behaviour

The operator achieves zero client errors during rolling updates through a layered approach:

1. **Safety check** — `OPERATOR.CLUSTER.SAFE` verifies the cluster can tolerate this pod stopping without risking CLUSTERDOWN.
2. **Cooperative failover** — `CLUSTER FAILOVER` is sent to the best replica first; the replica becomes primary via cooperative handshake with the current primary.
3. **Atomic write pause** — `OPERATOR.FAILOVER.PREPARE` executes atomically in the Valkey event loop: `CLIENT PAUSE WRITE` (buffers client writes) → `WAIT 1 500` (confirms at least one replica received the last writes, typically 1–5 ms) → `CLIENT UNPAUSE`.
4. **Role confirmation** — the PreStop script polls `INFO replication` until `role:slave` is confirmed (up to 5 s). This ensures `nodes.conf` is saved as slave on the PVC, so the pod restarts as replica rather than standalone master.
5. **ACL readiness gate** — the restarted pod sets `CLUSTER_FLAG_NO_REDIRECTION` at module load time, preventing MOVED responses until the reconciler has applied ACLs (~500ms–3s). Combined with `OPERATOR.NODE.READY` (`FailureThreshold:1`), this eliminates WRONGPASS errors on rolling updates.
6. **SIGTERM** — only delivered after the PreStop hook returns, by which time the new primary is active and clients have been rerouted.

**Why this order matters:** `CLUSTER FAILOVER` is sent first so that the cooperative handshake can proceed in parallel while `OPERATOR.FAILOVER.PREPARE` atomically pauses, waits, and unpauses — minimising the write-pause window to the replication lag (typically < 5 ms).

### GKE Nodepool Upgrades

The operator watches for `node.Spec.Unschedulable=true` (set by GKE when cordoning a node before drain) and triggers the same failover path — without waiting for the StatefulSet rolling update signal.

The `PodDisruptionBudget` (`maxUnavailable: 1`) prevents GKE from evicting more than one pod at a time across the cluster.

## PHP / Python Client Configuration

Client libraries require a small set of `@dangerous` commands for cluster initialisation. These are read-only topology queries and must be explicitly granted:

```yaml
commands: "+@read +@write -@dangerous +command +cluster|slots +cluster|shards"
```

- `command` — `redis-py` v7+ introspects supported commands on every new connection
- `cluster|slots` / `cluster|shards` — slot map discovery used by `phpredis` and `redis-py`

**phpredis** (PHP-FPM with persistent connections):

```php
$redis = new RedisCluster(
    null,
    ['valkey-sample-headless.default.svc.cluster.local:6379'],
    1.5,  // connect timeout
    1.5,  // read timeout
    true, // persistent connections (pconnect)
    null,
    ['auth' => ['app-readwrite', $_ENV['APP_PASSWORD']]],
);

// Route all traffic to primaries — avoids stale reads and MOVED retries
$redis->setOption(
    RedisCluster::OPT_SLAVE_FAILOVER,
    RedisCluster::FAILOVER_NONE
);
```

**redis-py** (Python):

```python
from redis.cluster import RedisCluster, ClusterNode

client = RedisCluster(
    startup_nodes=[ClusterNode("valkey-sample-0.valkey-sample-internal.default.svc.cluster.local", 6379)],
    username="app-readwrite",
    password=os.environ["APP_PASSWORD"],
    decode_responses=True,
    skip_full_coverage_check=True,
)
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ValkeyCluster CR                      │
└────────────────────────┬────────────────────────────────┘
                         │ reconciles
                         ▼
┌─────────────────────────────────────────────────────────┐
│                  Operator (2 replicas)                   │
│                  leader-election via Lease               │
│                                                          │
│  ConfigMap → Services → StatefulSet → PDB               │
│  → ACLs → ClusterTopology → Health → Status             │
└─────────┬───────────────────────────────────────────────┘
          │ manages
          ▼
┌─────────────────────────────────────────────────────────┐
│              Valkey Cluster (6 pods)                     │
│         + valkey-operator-module.so (C module)           │
│                                                          │
│  zone-a: pod-0 (primary-0)  pod-5 (replica-2)           │
│  zone-b: pod-1 (primary-1)  pod-3 (replica-0)           │
│  zone-c: pod-2 (primary-2)  pod-4 (replica-1)           │
│                                                          │
│  OPERATOR.NODE.READY    — readiness probe                │
│  OPERATOR.FAILOVER.PREPARE — PreStop hook failover       │
│  OPERATOR.CLUSTER.SAFE  — PreStop safety check           │
│  OPERATOR.HEALTH        — reconciler health snapshot     │
│  gate.c                 — ACL readiness gate             │
│                                                          │
│  16384 hash slots distributed across 3 shards           │
└─────────────────────────────────────────────────────────┘
```

## Valkey Module (`server/`)

The operator ships a native Valkey module (`valkey-operator-module.so`) embedded in the custom server image. It exposes the following commands:

| Command | Flags | Description |
|---|---|---|
| `OPERATOR.NODE.READY` | readonly | Readiness probe: checks role, gossip, slots, ACLs. Returns `[1, "ready"]` or `[0, "<reason>"]` |
| `OPERATOR.FAILOVER.PREPARE [timeout_ms]` | write | PreStop hook: `CLIENT PAUSE WRITE` → `WAIT 1 500` → `CLIENT UNPAUSE`, executed atomically in the Valkey event loop. Returns `[1, "ok", elapsed_ms]` or an error. |
| `OPERATOR.CLUSTER.SAFE` | readonly | PreStop safety check: safe to stop without risking CLUSTERDOWN |
| `OPERATOR.HEALTH` | readonly | Single-call health snapshot for the reconciler (replaces two round-trips) |
| `OPERATOR.BOOTSTRAP.READY` | readonly | Bootstrap gate: node is clean and ready for `--cluster create` |
| `OPERATOR.TOPOLOGY.SET <json>` | write | Pushes topology: `CLUSTER MEET` all peers, `CLUSTER REPLICATE` if replica (idempotent: skips REPLICATE if already replicating the correct primary) |

The module also registers an ACL readiness gate (`gate.c`) at load time: sets `CLUSTER_FLAG_NO_REDIRECTION` and polls `ACL LIST` every 500ms until application users are present, then clears the flag.

## Development

```bash
# Generate CRD manifests and deepcopy methods
make generate manifests

# Build
make build

# Run locally against current kubeconfig
make run

# Run unit tests (no cluster required)
make test

# Lint
make lint
```

### Building the Valkey module

```bash
cd module
make                          # builds valkey-operator-module.so
docker build -t valkey-operator-server:dev .   # embeds .so into Valkey image
```

### Local kind cluster

```bash
kind load docker-image valkey-operator-server:dev --name <cluster-name>
kubectl rollout restart statefulset/valkey-sample
```

### Unit tests

Pure unit tests — no Kubernetes cluster, no network required:

```bash
go test ./internal/controller/... -v
```

Coverage includes: `aclHash`, `buildACLRules`, `shouldSkipACLPod`, `parseClusterInfoInt`, `parseClusterInfoField`, `stripVerbatimPrefix`, `convergenceTimeoutFor`.

### Local development logs

```bash
# Human-readable console output
go run ./cmd/main.go --zap-devel=true
```

Production deployments emit JSON structured logs, compatible with GKE Cloud Logging, Datadog, and Grafana Loki.

## Roadmap

- [ ] Resharding (slot rebalancing)
- [ ] Backup/restore via ValkeyBackup CRD
- [ ] MutatingAdmissionWebhook for topology-aware pod scheduling post rolling-update
- [ ] Multi-cluster federation

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

Copyright 2024 MonkeyCorp Cloud contributors.
