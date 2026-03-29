package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

const (
	// getClusterKnownNodesTimeout is used when polling CLUSTER INFO for orphan detection.
	getClusterKnownNodesTimeout = 3 * time.Second
)

// reconcileOrphanReplicas detects pods that are running standalone (not part of
// the cluster) and re-integrates them:
//
//  1. Detect: pod is Running + reachable but absent from CLUSTER NODES on any
//     cluster member (or present as noaddr/disconnected with no slots).
//  2. CLUSTER MEET: instruct the orphan to join the cluster via a seed node.
//  3. Wait: poll until the node appears in CLUSTER NODES with a valid ID.
//  4. CLUSTER REPLICATE: assign it to the shard that is most under-replicated.
//
// This covers the pod-recreated-after-RDB-recovery case where nodes.conf was
// deleted and the pod restarted as a standalone node.
func (r *ValkeyClusterReconciler) reconcileOrphanReplicas(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return fmt.Errorf("resolving operator secret: %w", err)
	}
	creds := aclCredentials{username: operatorUsername, password: operatorPassword}

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return err
	}

	// Collect running pods with an IP.
	var runningPods []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" && pod.DeletionTimestamp == nil {
			runningPods = append(runningPods, pod)
		}
	}
	if len(runningPods) == 0 {
		return nil
	}

	logger.Info("Orphan repair: checking running pods", "count", len(runningPods))

	// Fetch CLUSTER NODES from any ready cluster member to get the current topology.
	nodesRaw, seedPod, err := r.fetchClusterNodesWithSeed(ctx, vc, creds, runningPods)
	if err != nil || nodesRaw == "" {
		// No reachable cluster member — nothing to repair yet.
		return nil
	}

	// Build a set of IPs and hostnames known to the cluster.
	knownIPs, knownHostnames := buildKnownAddrSets(nodesRaw)

	svcName := internalServiceName(vc)
	port := effectivePort(vc)

	for _, pod := range runningPods {
		if pod.Name == seedPod.Name {
			continue
		}

		stableDNS := fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod.Name, svcName, pod.Namespace)
		_, knownByIP := knownIPs[pod.Status.PodIP]
		_, knownByHostname := knownHostnames[stableDNS]

		if knownByIP || knownByHostname {
			continue // Pod is already a cluster member.
		}

		// Pod is not in CLUSTER NODES — check if it is reachable and standalone.
		podAddr := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
		role, err := r.getPodClusterRole(ctx, podAddr, creds)
		if err != nil {
			// Not reachable yet — skip, will retry on next reconcile.
			logger.Info("Orphan candidate not reachable yet", "pod", pod.Name, "err", err)
			continue
		}

		if role != rolePrimary {
			// Already a replica of some cluster — not orphaned.
			continue
		}

		// Verify it's truly standalone: CLUSTER INFO should show cluster_known_nodes <= 1.
		knownNodes, err := r.getClusterKnownNodes(ctx, podAddr, creds)
		if err != nil || knownNodes > 1 {
			// Part of a different cluster view or unreachable — skip.
			continue
		}

		logger.Info("Orphan pod detected — re-integrating into cluster",
			"pod", pod.Name, "podIP", pod.Status.PodIP)
		r.Recorder.Eventf(vc, corev1.EventTypeWarning, "OrphanRepair",
			"Pod %s is standalone (not in cluster) — issuing CLUSTER MEET + REPLICATE", pod.Name)

		if err := r.reintegratePod(ctx, vc, pod, seedPod, nodesRaw, creds); err != nil {
			logger.Error(err, "Failed to reintegrate orphan pod", "pod", pod.Name)
			// Non-fatal — retry on next reconcile.
			continue
		}

		logger.Info("Orphan pod reintegrated", "pod", pod.Name)
		r.Recorder.Eventf(vc, corev1.EventTypeNormal, "OrphanRepaired",
			"Pod %s successfully reintegrated into cluster", pod.Name)
	}

	return nil
}

// reintegratePod uses OPERATOR.TOPOLOGY.SET to rejoin an orphan node into the
// cluster. The module handles CLUSTER MEET + gossip propagation + CLUSTER REPLICATE
// internally — no external polling loop required.
func (r *ValkeyClusterReconciler) reintegratePod(
	ctx context.Context,
	vc *cachev1alpha1.ValkeyCluster,
	orphan, seed *corev1.Pod,
	nodesRaw string,
	creds aclCredentials,
) error {
	logger := log.FromContext(ctx).WithValues("orphan", orphan.Name, "seed", seed.Name)
	port := effectivePort(vc)
	orphanAddr := fmt.Sprintf("%s:%d", orphan.Status.PodIP, port)

	// Pick the primary for this replica.
	masterID, err := pickMasterForReplica(nodesRaw, int(vc.Spec.ReplicasPerShard))
	if err != nil {
		return fmt.Errorf("picking master for replica: %w", err)
	}

	// Resolve the primary IP from nodesRaw.
	primaryIP := ""
	for _, n := range parseClusterNodes(nodesRaw) {
		if n.id == masterID {
			primaryIP = n.ip
			break
		}
	}
	if primaryIP == "" {
		return fmt.Errorf("could not resolve IP for primary %s", masterID[:8])
	}

	// Build peer list: all cluster members this node should MEET.
	peers := []string{}
	for _, n := range parseClusterNodes(nodesRaw) {
		if n.ip != "" && !strings.Contains(n.flags, "noaddr") {
			peers = append(peers, fmt.Sprintf("%s:%d", n.ip, port))
		}
	}

	// Build JSON payload for OPERATOR.TOPOLOGY.SET.
	peersJSON := "["
	for i, p := range peers {
		if i > 0 {
			peersJSON += ","
		}
		peersJSON += fmt.Sprintf("%q", p)
	}
	peersJSON += "]"

	payload := fmt.Sprintf(
		`{"peers":%s,"role":"replica","primary_addr":"%s:%d"}`,
		peersJSON, primaryIP, port,
	)

	oc, err := valkeyClient(orphanAddr, creds.username, creds.password)
	if err != nil {
		return fmt.Errorf("connecting to orphan: %w", err)
	}
	defer oc.Close()

	tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
	defer cancel()

	resp, err := oc.Do(tctx, oc.B().Arbitrary("OPERATOR.TOPOLOGY.SET").Args(payload).Build()).ToArray()
	if err != nil {
		return fmt.Errorf("OPERATOR.TOPOLOGY.SET failed: %w", err)
	}
	if len(resp) < 2 {
		return fmt.Errorf("unexpected response from OPERATOR.TOPOLOGY.SET")
	}

	ok, _ := resp[0].ToInt64()
	msg, _ := resp[1].ToString()

	if ok != 1 {
		// Primary not yet known via gossip — operator will retry on next reconcile.
		logger.Info("OPERATOR.TOPOLOGY.SET not ready yet", "reason", msg)
		return fmt.Errorf("topology set not ready: %s", msg)
	}

	logger.Info("Orphan reintegrated via OPERATOR.TOPOLOGY.SET",
		"masterID", masterID[:8], "response", msg)
	return nil
}

// pickMasterForReplica returns the node ID of the primary with the fewest
// replicas (most under-replicated). Ties are broken by node ID for determinism.
func pickMasterForReplica(nodesRaw string, expectedReplicas int) (string, error) {
	nodes := parseClusterNodes(nodesRaw)

	// Count current replicas per primary.
	replicaCount := make(map[string]int)
	for _, n := range nodes {
		if strings.Contains(n.flags, "slave") && n.masterID != "-" && n.masterID != "" {
			replicaCount[n.masterID]++
		}
	}

	var bestID string
	bestCount := expectedReplicas + 1 // start above max so any primary qualifies

	for _, n := range nodes {
		if !strings.Contains(n.flags, "master") {
			continue
		}
		if strings.Contains(n.flags, "fail") || strings.Contains(n.flags, "noaddr") {
			continue
		}
		// Only primaries with slots are valid replication targets.
		// A master with no slots cannot accept CLUSTER REPLICATE.
		if n.slots == "" {
			continue
		}
		count := replicaCount[n.id]
		if count < bestCount || (count == bestCount && n.id < bestID) {
			bestCount = count
			bestID = n.id
		}
	}

	if bestID == "" {
		return "", fmt.Errorf("no healthy primary found in CLUSTER NODES")
	}
	return bestID, nil
}

// fetchClusterNodesWithSeed fetches CLUSTER NODES from the first ready pod and
// returns both the raw output and the pod used as seed.
func (r *ValkeyClusterReconciler) fetchClusterNodesWithSeed(
	ctx context.Context,
	vc *cachev1alpha1.ValkeyCluster,
	creds aclCredentials,
	pods []*corev1.Pod,
) (string, *corev1.Pod, error) {
	for _, pod := range pods {
		if !isPodReady(pod) || pod.Status.PodIP == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, effectivePort(vc))
		c, err := valkeyClient(addr, creds.username, creds.password)
		if err != nil {
			continue
		}
		tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
		raw, err := c.Do(tctx, c.B().ClusterNodes().Build()).ToString()
		cancel()
		c.Close()
		if err == nil && raw != "" {
			return raw, pod, nil
		}
	}
	return "", nil, fmt.Errorf("no reachable cluster member found")
}

// getClusterKnownNodes returns the cluster_known_nodes value from CLUSTER INFO.
func (r *ValkeyClusterReconciler) getClusterKnownNodes(ctx context.Context, addr string, creds aclCredentials) (int64, error) {
	c, err := valkeyClient(addr, creds.username, creds.password)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	info, err := c.Do(tctx, c.B().ClusterInfo().Build()).ToString()
	if err != nil {
		return 0, err
	}
	return parseClusterInfoInt(info, "cluster_known_nodes"), nil
}

// nodeState holds the result of OPERATOR.NODE.STATE for a single pod.
type nodeState struct {
	role       string // "primary", "replica", or "unknown"
	masterID   string // empty if primary
	masterAddr string // "ip:port", empty if primary
	hasSlots   int64  // 1 if this node owns slot ranges in CLUSTER NODES, 0 otherwise
}

// reconcileReplicaRoles detects pods that are primary with no slots and assigns
// them as replicas. This covers two scenarios:
//   - Brutal restart: Valkey auto-promotes all nodes to standalone primaries.
//   - Failed rolling update: PreStop failover did not execute, former replicas
//     lost their primary and self-promoted.
//
// In both cases, former replicas have role=primary but has_slots=0 as reported
// by OPERATOR.NODE.STATE. The ShardUnderReplicated condition gates this function
// to avoid unnecessary work on healthy clusters.
//
// nodesRaw is re-fetched after each successful assignment so pickMasterForReplica
// sees updated replica counts and avoids assigning two pods to the same primary.
//
// Stateless — no annotations needed. Retries naturally on the next reconcile
// cycle as long as ShardUnderReplicated remains true.
func (r *ValkeyClusterReconciler) reconcileReplicaRoles(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	// Only act when ShardUnderReplicated is True.
	underReplicated := false
	for _, cond := range vc.Status.Conditions {
		if cond.Type == "ShardUnderReplicated" && cond.Status == "True" {
			underReplicated = true
			break
		}
	}
	if !underReplicated {
		return nil
	}

	operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return fmt.Errorf("resolving operator secret: %w", err)
	}
	creds := aclCredentials{username: operatorUsername, password: operatorPassword}

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return err
	}

	var readyPods []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if isPodReady(pod) && pod.Status.PodIP != "" {
			readyPods = append(readyPods, pod)
		}
	}
	if len(readyPods) == 0 {
		return nil
	}

	port := effectivePort(vc)

	// Initial CLUSTER NODES fetch for peer list and primary resolution.
	nodesRaw, _, err := r.fetchClusterNodesWithSeed(ctx, vc, creds, readyPods)
	if err != nil || nodesRaw == "" {
		return nil
	}

	// Wait for ghost nodes to expire before acting.
	// After gossip recovery or rolling update, stale handshake entries from old
	// IPs may inflate cluster_known_nodes. Acting while inflated would assign
	// replicas to the wrong primaries. Handshakes expire after ~cluster-node-timeout*10.
	//
	// Query from a stable primary (one with slots), not readyPods[0]. New pods
	// joining during a rolling update see ghost nodes from the replaced pod's
	// old IP; stable primaries have a converged gossip view.
	expectedNodes := int64(totalPods(vc))
	if len(readyPods) > 0 {
		stableAddr := ""
		for _, n := range parseClusterNodes(nodesRaw) {
			if strings.Contains(n.flags, "master") && n.slots != "" {
				for _, pod := range readyPods {
					if pod.Status.PodIP == n.ip {
						stableAddr = fmt.Sprintf("%s:%d", n.ip, port)
						break
					}
				}
			}
			if stableAddr != "" {
				break
			}
		}
		if stableAddr == "" {
			stableAddr = fmt.Sprintf("%s:%d", readyPods[0].Status.PodIP, port)
		}
		knownNodes, knErr := r.getClusterKnownNodes(ctx, stableAddr, creds)
		if knErr != nil {
			logger.Info("Cannot query cluster_known_nodes — deferring replica assignment", "err", knErr)
			return nil
		}
		if knownNodes > expectedNodes {
			logger.Info("Ghost nodes still present — waiting for expiry before replica assignment",
				"knownNodes", knownNodes, "expected", expectedNodes)
			return nil
		}
	}

	// Build IP → pod map for fast lookup.
	podByIP := make(map[string]*corev1.Pod, len(readyPods))
	for _, pod := range readyPods {
		podByIP[pod.Status.PodIP] = pod
	}

	// Detect masters without slots directly from CLUSTER NODES — the authoritative
	// gossip view. OPERATOR.NODE.STATE is not used here because a restarting node
	// reads its PVC-persisted nodes.conf and may report has_slots=1 for slots it
	// no longer owns in the cluster view.
	for _, n := range parseClusterNodes(nodesRaw) {
		if !strings.Contains(n.flags, "master") {
			continue
		}
		if strings.Contains(n.flags, "fail") || strings.Contains(n.flags, "noaddr") {
			continue
		}
		if n.slots != "" {
			continue // has slots — valid primary, not a candidate
		}

		pod, ok := podByIP[n.ip]
		if !ok {
			continue // not a pod we manage
		}

		podAddr := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
		logger.Info("Primary with no slots detected — assigning as replica", "pod", pod.Name)

		// Re-fetch nodesRaw to get up-to-date replica counts after any
		// previous assignment in this loop iteration.
		nodesRaw, _, err = r.fetchClusterNodesWithSeed(ctx, vc, creds, readyPods)
		if err != nil || nodesRaw == "" {
			break
		}

		masterID, err := pickMasterForReplica(nodesRaw, int(vc.Spec.ReplicasPerShard))
		if err != nil {
			logger.Info("No healthy primary to assign replica to", "err", err)
			break
		}

		primaryIP := ""
		for _, n := range parseClusterNodes(nodesRaw) {
			if n.id == masterID {
				primaryIP = n.ip
				break
			}
		}
		if primaryIP == "" {
			continue
		}

		peers := []string{}
		for _, n := range parseClusterNodes(nodesRaw) {
			if n.ip != "" && !strings.Contains(n.flags, "noaddr") {
				peers = append(peers, fmt.Sprintf("%s:%d", n.ip, port))
			}
		}
		peersJSON := "["
		for i, p := range peers {
			if i > 0 {
				peersJSON += ","
			}
			peersJSON += fmt.Sprintf("%q", p)
		}
		peersJSON += "]"

		payload := fmt.Sprintf(
			`{"peers":%s,"role":"replica","primary_addr":"%s:%d"}`,
			peersJSON, primaryIP, port,
		)

		c, err := valkeyClient(podAddr, creds.username, creds.password)
		if err != nil {
			logger.Info("Cannot connect to pod for replica assignment", "pod", pod.Name)
			continue
		}

		tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
		resp, err := c.Do(tctx, c.B().Arbitrary("OPERATOR.TOPOLOGY.SET").Args(payload).Build()).ToArray()
		cancel()
		c.Close()

		if err != nil {
			logger.Info("OPERATOR.TOPOLOGY.SET failed", "pod", pod.Name, "err", err)
			continue
		}
		if len(resp) < 2 {
			continue
		}

		statusOk, _ := resp[0].ToInt64()
		msg, _ := resp[1].ToString()

		if statusOk == 1 {
			logger.Info("Replica role assigned", "pod", pod.Name,
				"primaryID", masterID[:min8(masterID)], "primaryIP", primaryIP)
			r.Recorder.Eventf(vc, corev1.EventTypeNormal, "ReplicaRoleRestored",
				"Pod %s assigned as replica of %s",
				pod.Name, masterID[:min8(masterID)])
		} else {
			logger.Info("OPERATOR.TOPOLOGY.SET not ready yet — will retry next cycle",
				"pod", pod.Name, "reason", msg)
		}
	}

	return nil
}

// min8 returns min(8, len(s)) for safe truncation in log messages.
func min8(s string) int {
	if len(s) < 8 {
		return len(s)
	}
	return 8
}
