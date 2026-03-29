# Testing with kind

This guide walks through setting up a local Kubernetes cluster with [kind](https://kind.sigs.k8s.io/)
to test the Valkey operator end-to-end, including rolling updates, node drain simulation, and
topology-aware failover.

## Prerequisites

```bash
brew install kind kubectl go
```

Verify:
```bash
kind version    # >= 0.20
kubectl version --client
go version      # >= 1.25
```

---

## 1. Create the cluster

The cluster simulates 3 availability zones with one worker per zone, matching a typical
production topology (3 shards × 1 replica = 6 Valkey pods across 3 nodes).

```bash
cat <<EOF | kind create cluster --name valkey-dev --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
    labels:
      topology.kubernetes.io/zone: zone-a
  - role: worker
    labels:
      topology.kubernetes.io/zone: zone-b
  - role: worker
    labels:
      topology.kubernetes.io/zone: zone-c
EOF
```

Verify the nodes are ready with their zone labels:

```bash
kubectl get nodes -L topology.kubernetes.io/zone
```

```
NAME                       STATUS   ROLES           ZONE
valkey-dev-control-plane   Ready    control-plane
valkey-dev-worker          Ready    <none>          zone-a
valkey-dev-worker2         Ready    <none>          zone-b
valkey-dev-worker3         Ready    <none>          zone-c
```

---

## 2. Install the CRD

```bash
make install
```

Verify:
```bash
kubectl get crd valkeyclusters.cache.valkey.io
```

---

## 3. Create the secrets

```bash
kubectl create secret generic valkey-operator-secret \
  --from-literal=password=devpassword

kubectl create secret generic valkey-app-readwrite-secret \
  --from-literal=password=devpassword
```

---

## 4. Run the operator locally

Running the operator as a local process (no Docker build required) gives the fastest
feedback loop — just restart the process after a code change.

```bash
go run ./cmd/main.go \
  --zap-devel=true \
  --leader-elect=false
```

> `--leader-elect=false` disables the Kubernetes Lease requirement when running a single
> local instance. `--zap-devel=true` enables human-readable console logs.

Leave this terminal open. Open a second terminal for the next steps.

---

## 5. Deploy a ValkeyCluster

```bash
kubectl apply -f config/samples/cache_v1alpha1_valkeycluster.yaml
```

Watch the cluster come up:

```bash
kubectl get valkeycluster -w
```

```
NAME             PHASE          READY   CLUSTER   SLOTS   AGE
valkey-sample    Initializing   0       0s
valkey-sample    Initializing   3       30s
valkey-sample    Running        6       ok        16384   90s
```

Check pod placement across zones:

```bash
kubectl get pods -o wide
```

```
NAME              READY   STATUS    NODE
valkey-sample-0   2/2     Running   valkey-dev-worker    (zone-a)
valkey-sample-1   2/2     Running   valkey-dev-worker2   (zone-b)
valkey-sample-2   2/2     Running   valkey-dev-worker3   (zone-c)
valkey-sample-3   2/2     Running   valkey-dev-worker2   (zone-b)
valkey-sample-4   2/2     Running   valkey-dev-worker3   (zone-c)
valkey-sample-5   2/2     Running   valkey-dev-worker    (zone-a)
```

Check topology-aware shard placement (primary and its replica should be in different zones):

```bash
kubectl get valkeycluster valkey-sample -o jsonpath='{.status.podRoles}' | jq
kubectl get valkeycluster valkey-sample -o jsonpath='{.status.podTopology}' | jq
```

Check conditions:

```bash
kubectl get valkeycluster valkey-sample -o jsonpath='{.status.conditions}' | jq '.[] | {type,status,reason}'
```

```json
{ "type": "Available",            "status": "True",  "reason": "ClusterReady"      }
{ "type": "ClusterDegraded",      "status": "False", "reason": "ClusterOK"         }
{ "type": "ShardUnderReplicated", "status": "False", "reason": "ReplicationOK"     }
{ "type": "ShardColocated",       "status": "False", "reason": "TopologyOK"        }
```

---

## 6. Build and load the custom Valkey image

The operator requires a custom Valkey image with the operator module embedded.
With kind, images must be built locally and loaded into the cluster — no registry needed.

```bash
# Build natively for the current platform (linux/arm64 on Apple Silicon,
# linux/amd64 on x86). Docker Desktop handles the Linux/macOS binary format
# difference automatically — the .dockerignore excludes the local macOS .so.
docker build -t valkey-operator-server:dev ./module

# Load into every kind node (kind uses the same arch as Docker Desktop)
kind load docker-image valkey-operator-server:dev --name valkey-dev
```

> **Multi-arch release** (push to a registry for production):
> ```bash
> docker buildx build \
>   --platform linux/amd64,linux/arm64 \
>   --push \
>   -t ghcr.io/geoffrey/valkey-operator-server:9.0.3 \
>   ./module
> ```
> The resulting image manifest contains both architectures — Docker pulls the
> correct variant automatically on amd64 or arm64 nodes.

Verify the image is available on the nodes:
```bash
docker exec valkey-dev-worker crictl images | grep valkey-operator-server
```

Update the sample CR to use the local image:
```yaml
spec:
  image: valkey-operator-server:dev
```

### Verify the module loads

```bash
# Connect to any pod and check the module is loaded
kubectl exec valkey-sample-0 -c valkey -- \
  valkey-cli -p 6379 --no-auth-warning --user operator --pass devpassword \
  MODULE LIST
```

Expected output includes:
```
1) 1) "name"
   2) "operator"
   3) "ver"
   4) (integer) 1
```

### Test OPERATOR.HEALTH

```bash
kubectl exec valkey-sample-0 -c valkey -- \
  valkey-cli -p 6379 --no-auth-warning --user operator --pass devpassword \
  OPERATOR.HEALTH
```

Expected (primary pod):
```
 1) "cluster_state"
 2) "ok"
 3) "slots_assigned"
 4) (integer) 5461
 5) "slots_ok"
 6) (integer) 5461
 7) "gossip_converged"
 8) (integer) 1
 9) "repl_lag_bytes"
10) (integer) 0
11) "connected_replicas"
12) (integer) 1
13) "role"
14) "primary"
15) "master_link_status"
16) ""
...
```

### Test OPERATOR.BOOTSTRAP.READY

On a healthy cluster, all nodes should report not-ready (already have slots):
```bash
kubectl exec valkey-sample-0 -c valkey -- \
  valkey-cli -p 6379 --no-auth-warning --user operator --pass devpassword \
  OPERATOR.BOOTSTRAP.READY
```

Expected:
```
1) (integer) 0
2) "node already owns slots — already part of a cluster"
```

### Test OPERATOR.HEALTH.STREAM

Start the stream on one pod and subscribe from another terminal:

```bash
# Terminal 1 — subscribe to events
kubectl exec valkey-sample-0 -c valkey -- \
  valkey-cli -p 6379 --no-auth-warning --user operator --pass devpassword \
  SUBSCRIBE __operator__:events

# Terminal 2 — start the stream
kubectl exec valkey-sample-0 -c valkey -- \
  valkey-cli -p 6379 --no-auth-warning --user operator --pass devpassword \
  OPERATOR.HEALTH.STREAM START
```

Then trigger a role change (e.g. delete a primary pod) and watch Terminal 1:
```
1) "message"
2) "__operator__:events"
3) "{\"event\":\"role_changed\",\"node_id\":\"abc123...\",\"from\":\"primary\",\"to\":\"replica\"}"
```

Stop the stream:
```bash
kubectl exec valkey-sample-0 -c valkey -- \
  valkey-cli -p 6379 --no-auth-warning --user operator --pass devpassword \
  OPERATOR.HEALTH.STREAM STOP
```

### Test OPERATOR.FAILOVER.PREPARE

On a primary pod:
```bash
kubectl exec valkey-sample-0 -c valkey -- \
  valkey-cli -p 6379 --no-auth-warning --user operator --pass devpassword \
  OPERATOR.FAILOVER.PREPARE 5000
```

Expected (successful failover):
```
1) (integer) 1
2) "ok"
3) (integer) 342   # elapsed ms
```

On a replica pod:
```
1) (integer) 1
2) "replica — no action needed"
3) (integer) 0
```

---

## 7. Test scenarios



### 6.1 Connect to the cluster

```bash
kubectl run valkey-cli --image=valkey/valkey:9.0.3-alpine --rm -it --restart=Never -- \
  valkey-cli -h valkey-sample-headless -p 6379 \
    --no-auth-warning -u operator -a devpassword \
    CLUSTER INFO
```

### 6.2 StatefulSet rolling update

Trigger a rolling update by changing the image version:

```bash
kubectl patch valkeycluster valkey-sample \
  --type=merge -p '{"spec":{"image":"valkey/valkey:9.0.3-alpine"}}'
```

Or edit the sample and re-apply. Watch the failover events in real time:

```bash
# Terminal 1 — watch pods
kubectl get pods -w

# Terminal 2 — watch events
kubectl get events --sort-by='.lastTimestamp' -w

# Terminal 3 — operator logs (already running)
```

Expected sequence:
1. Operator detects rolling update reaching a primary pod
2. `CLUSTER FAILOVER` issued on best replica (different zone)
3. Role labels updated on pods
4. SIGTERM sent to old primary
5. Restarted pod waits for `master_link_status=up` before becoming Ready
6. StatefulSet proceeds to next pod once `OPERATOR.NODE.READY` returns 1

### 6.3 Node drain (GKE nodepool simulation)

This simulates what GKE does before recycling a node during a nodepool upgrade.

```bash
# Step 1: cordon the node (sets Unschedulable=true — this is what GKE does first)
kubectl cordon valkey-dev-worker

# Step 2: watch the operator detect the cordoned node and trigger failover
kubectl get events --field-selector reason=NodeDrainFailover -w
```

Expected event:
```
REASON             MESSAGE
NodeDrainFailover  Primary pod valkey-sample-0 is on unschedulable node
                   valkey-dev-worker — triggering pro-active failover
```

```bash
# Step 3: drain the node (evicts pods)
kubectl drain valkey-dev-worker \
  --ignore-daemonsets \
  --delete-emissary-dir \
  --timeout=120s

# Step 4: verify the cluster is still healthy
kubectl get valkeycluster valkey-sample

# Step 5: restore the node
kubectl uncordon valkey-dev-worker
```

### 6.4 PodDisruptionBudget

Verify the PDB is in place:

```bash
kubectl get pdb valkey-sample
```

```
NAME            MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS
valkey-sample   N/A             1                 1
```

Test that the PDB blocks a second simultaneous eviction:

```bash
# Evict one pod manually
kubectl delete pod valkey-sample-3

# Immediately try to evict another — should be blocked by the PDB
kubectl delete pod valkey-sample-4
# Expected: blocked until valkey-sample-3 is back Ready
```

### 6.5 Stale node cleanup

Simulate a pod that disappears without a clean shutdown (network partition / OOM kill):

```bash
# Force-delete a replica pod (bypasses graceful termination)
kubectl delete pod valkey-sample-5 --force --grace-period=0

# The node will appear as "fail" in CLUSTER NODES
# Watch the operator wait 30s before issuing CLUSTER FORGET
kubectl get events --field-selector reason=NodeForgotten -w
```

### 6.6 Health conditions

Manually scale down replicas to trigger `ShardUnderReplicated`:

```bash
# Temporarily delete a replica
kubectl delete pod valkey-sample-3

# Condition should flip within 10s (requeueInterval)
kubectl get valkeycluster valkey-sample \
  -o jsonpath='{.status.conditions[?(@.type=="ShardUnderReplicated")]}' | jq
```

```json
{
  "type": "ShardUnderReplicated",
  "status": "True",
  "reason": "MissingReplica",
  "message": "shards under-replicated: abc12345(0/1 replicas)"
}
```

Once the pod is rescheduled and synced, the condition returns to `False`.

### 6.7 Colocation detection

Manually move a replica to the same node as its primary to trigger `ShardColocated`:

```bash
# Find which node hosts primary shard-0
kubectl get pod valkey-sample-0 -o jsonpath='{.spec.nodeName}'
# e.g. valkey-dev-worker (zone-a)

# Cordon the other nodes to force the replica onto the same node
kubectl cordon valkey-dev-worker2
kubectl cordon valkey-dev-worker3

# Delete the replica — it will reschedule onto the only available node (zone-a)
kubectl delete pod valkey-sample-3

# Check the ShardColocated condition
kubectl get valkeycluster valkey-sample \
  -o jsonpath='{.status.conditions[?(@.type=="ShardColocated")]}' | jq
```

Restore:
```bash
kubectl uncordon valkey-dev-worker2
kubectl uncordon valkey-dev-worker3
```

---

## 7. Cleanup

```bash
kubectl delete -f config/samples/cache_v1alpha1_valkeycluster.yaml
kubectl delete secret valkey-operator-secret valkey-metrics-secret valkey-app-readwrite-secret
make uninstall
kind delete cluster --name valkey-dev
```

---

## Troubleshooting

### Pods stuck in Pending

The `Hard` spread constraints require exactly 3 worker nodes (one per zone). If pods are
pending, check:

```bash
kubectl describe pod valkey-sample-0 | grep -A5 Events
```

If you have fewer than 3 nodes, set `nodeSpreadPolicy: Disabled` and `zoneSpreadPolicy: Disabled`
in the sample spec.

### Operator cannot reach Valkey pods

When running the operator locally (`go run`), it connects to pod IPs directly. Make sure
your local machine can reach the kind pod network:

```bash
# kind usually sets up routing automatically on Linux
# On macOS, use docker network inspect kind to find the subnet
ping $(kubectl get pod valkey-sample-0 -o jsonpath='{.status.podIP}')
```

If unreachable on macOS, use [docker-mac-net-connect](https://github.com/chipmk/docker-mac-net-connect):

```bash
brew install chipmk/tap/docker-mac-net-connect
sudo brew services start chipmk/tap/docker-mac-net-connect
```

### Bootstrap stuck at Initializing

The cluster bootstrap requires all pods to be Ready simultaneously. Check:

```bash
kubectl get pods
kubectl logs valkey-sample-0 -c valkey
kubectl describe pod valkey-sample-0
```

Common causes: secret missing, image pull error, insufficient resources on kind nodes.
