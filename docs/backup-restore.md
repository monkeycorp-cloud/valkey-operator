# Backup & Restore

## Principes

Each Valkey shard owns a contiguous range of the 16384 hash slots. Each primary pod
stores its shard data in `/data/dump.rdb` on its PVC. A complete backup is one
`dump.rdb` file per shard (replicas hold identical data and can be skipped).

`status.nodes` exposes the live mapping `podName → slots → nodeId` and is updated
every reconcile cycle (~10 s). This mapping is the key to restoring data to the
correct shard after a cluster rebuild.

## Backup procedure

1. For each node in `status.nodes` where `role=primary`:
   - Issue `BGSAVE` and wait for `lastsave` to increment (poll `LASTSAVE`).
   - Copy `/data/dump.rdb` from the pod PVC to external storage.
   - Store the file alongside its metadata: `nodeId`, `podName`, `slots`.

```
backup/
  manifest.json          # { "clusterName": "...", "timestamp": "...", "shards": [...] }
  shard-0-5460.rdb       # primary owning slots 0-5460
  shard-5461-10922.rdb
  shard-10923-16383.rdb
```

`manifest.json` example:
```json
{
  "clusterName": "mycluster",
  "timestamp": "2026-03-27T10:00:00Z",
  "shards": [
    { "nodeId": "abc123", "podName": "mycluster-0", "slots": "0-5460",       "file": "shard-0-5460.rdb" },
    { "nodeId": "def456", "podName": "mycluster-3", "slots": "5461-10922",   "file": "shard-5461-10922.rdb" },
    { "nodeId": "ghi789", "podName": "mycluster-6", "slots": "10923-16383",  "file": "shard-10923-16383.rdb" }
  ]
}
```

## Restore procedure

The restore always targets a **running, empty cluster** — never raw PVCs.
This avoids the slot-mapping problem of case 2 (cluster rebuild from PVCs alone).

### Step 1 — bootstrap a fresh cluster

Create (or recreate) the ValkeyCluster CR with the same number of shards as the
backup. Wait for `status.clusterState=ok` and all nodes bootstrapped.

### Step 2 — match backup shards to live pods

`--cluster create` always assigns slots in the same order for a given shard count:
- shard 0 → 0-5460
- shard 1 → 5461-10922
- shard 2 → 10923-16383

Read `status.nodes` to get the current `podName` for each slot range. Cross-reference
with `manifest.json` to identify which `.rdb` file goes to which pod.

### Step 3 — restore each shard

For each primary pod:

```bash
# Copy the rdb file onto the pod PVC
kubectl cp shard-0-5460.rdb <namespace>/<podName>:/data/dump.rdb

# Reload data from disk without restarting the process
kubectl exec -n <namespace> <podName> -- \
  valkey-cli -p 6379 --user operator --pass <password> DEBUG RELOAD
```

`DEBUG RELOAD` flushes memory, reloads `dump.rdb` from disk, and resumes serving
traffic — no pod restart required, no cluster disruption.

### Step 4 — verify

```bash
kubectl exec -n <namespace> <podName> -- \
  valkey-cli -p 6379 --user operator --pass <password> DBSIZE
```

Check that key counts match expectations across all primaries.

## Constraints

- Shard count must be identical between backup and restore cluster.
- If shard count changes, slot ranges change → manual remapping required (out of scope).
- `DEBUG RELOAD` is a blocking operation — clients will see a brief latency spike
  during the reload window. Restore one shard at a time to limit blast radius.
- Replicas resync automatically from their primary after `DEBUG RELOAD` via RDB transfer.
