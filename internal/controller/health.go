package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

const (
	// staleNodeTimeout is how long a node must be in fail/disconnected state
	// before the operator issues CLUSTER FORGET to clean it up.
	staleNodeTimeout = 30 * time.Second

	// staleNodeAnnotationPrefix is the annotation key prefix used to track
	// the first time a node was observed as stale.
	// Full key: staleNodeAnnotationPrefix + nodeID
	staleNodeAnnotationPrefix = "valkey.io/stale-since."
)

// clusterNodeState holds parsed state for a single node from CLUSTER NODES.
type clusterNodeState struct {
	id       string
	ip       string // from cluster-announce-ip (used for MOVED/ASK to clients)
	hostname string // from cluster-announce-hostname (stable DNS, stored in nodes.conf)
	flags    string // e.g. "master", "slave", "fail", "disconnected"
	masterID string // "-" if primary
	slots    string // slot ranges, empty for replicas
}

// reconcileClusterHealth checks the health of the Valkey cluster and:
//   - Updates ClusterDegraded / ShardUnderReplicated conditions in status
//   - Emits Kubernetes Events for degraded states
//   - Issues CLUSTER FORGET for stale nodes (fail + disconnected > staleNodeTimeout)
func (r *ValkeyClusterReconciler) reconcileClusterHealth(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return fmt.Errorf("resolving operator secret: %w", err)
	}
	creds := aclCredentials{username: operatorUsername, password: operatorPassword}

	// Find a reachable pod to query.
	nodesRaw, clusterInfo, err := r.fetchClusterState(ctx, vc, creds)
	if err != nil {
		logger.Error(err, "Cannot reach any Valkey node — cluster may be fully down")
		setCondition(vc, metav1.Condition{
			Type:               "ClusterDegraded",
			Status:             metav1.ConditionTrue,
			Reason:             "Unreachable",
			Message:            "No Valkey node is reachable",
			ObservedGeneration: vc.Generation,
		})
		r.Recorder.Event(vc, corev1.EventTypeWarning, "ClusterUnreachable", "No Valkey node is reachable")
		return nil
	}

	nodes := parseClusterNodes(nodesRaw)
	clusterState := parseClusterInfoField(clusterInfo, "cluster_state")

	// Update status.nodes from CLUSTER NODES output.
	// Map pod IP → pod name for enrichment.
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err == nil {
		ipToPodName := make(map[string]string, len(podList.Items))
		for i := range podList.Items {
			p := &podList.Items[i]
			if p.Status.PodIP != "" {
				ipToPodName[p.Status.PodIP] = p.Name
			}
		}
		nodeStatuses := make([]cachev1alpha1.ValkeyNodeStatus, 0, len(nodes))
		for _, n := range nodes {
			if strings.Contains(n.flags, "noaddr") {
				continue
			}
			role := "replica"
			if strings.Contains(n.flags, "master") {
				role = "primary"
			}
			masterID := n.masterID
			if masterID == "-" {
				masterID = ""
			}
			nodeStatuses = append(nodeStatuses, cachev1alpha1.ValkeyNodeStatus{
				NodeID:   n.id,
				PodName:  ipToPodName[n.ip],
				IP:       n.ip,
				Role:     role,
				MasterID: masterID,
				Slots:    n.slots,
			})
		}
		vc.Status.Nodes = nodeStatuses
	}

	// --- Check 1: cluster_state ---
	// Treat empty as unknown (parse failure / transient) — do not fire false positive.
	if clusterState != "" && clusterState != "ok" {
		msg := fmt.Sprintf("cluster_state=%s", clusterState)
		logger.Info("Cluster state degraded", "state", clusterState)
		setCondition(vc, metav1.Condition{
			Type:               "ClusterDegraded",
			Status:             metav1.ConditionTrue,
			Reason:             "ClusterStateFail",
			Message:            msg,
			ObservedGeneration: vc.Generation,
		})
		r.Recorder.Event(vc, corev1.EventTypeWarning, "ClusterDegraded", msg)

		// Detect stale gossip after scale-to-0/up or full pod restart:
		// nodes.conf contains old IPs that are no longer valid, peers never
		// reconnect, and cluster_state stays fail indefinitely.
		// After convergenceTimeoutFor(vc), force CLUSTER RESET SOFT on all pods
		// and re-run the bootstrap sequence to reform the cluster.
		if r.clusterNotOKTimedOut(ctx, vc) {
			logger.Info("Cluster stuck in fail state — nodes.conf has stale IPs, forcing reset and re-bootstrap")
			r.Recorder.Event(vc, corev1.EventTypeWarning, "StaleGossip",
				"Cluster stuck in fail state after full restart — forcing CLUSTER RESET SOFT and re-bootstrap")

			podList := &corev1.PodList{}
			if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err == nil {
				readyPods := make([]*corev1.Pod, 0, len(podList.Items))
				for i := range podList.Items {
					if isPodReady(&podList.Items[i]) && podList.Items[i].Status.PodIP != "" {
						readyPods = append(readyPods, &podList.Items[i])
					}
				}
				operatorPassword, _ := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
				creds2 := aclCredentials{username: operatorUsername, password: operatorPassword}
				_ = r.resetAllNodes(ctx, vc, readyPods, creds2)
			}

			r.clearClusterNotOKAnnotation(ctx, vc)
		}
	} else {
		r.clearClusterNotOKAnnotation(ctx, vc)
		setCondition(vc, metav1.Condition{
			Type:               "ClusterDegraded",
			Status:             metav1.ConditionFalse,
			Reason:             "ClusterOK",
			Message:            "cluster_state=ok",
			ObservedGeneration: vc.Generation,
		})
	}

	// --- Check 2: shards under-replicated ---
	// Raise the condition immediately so reconcileReplicaRoles can act without delay.
	// No grace period needed: the condition is cleared as soon as replicas are assigned,
	// and transient gossip convergence noise is handled by the ghost-node check in
	// reconcileReplicaRoles itself.
	underReplicated := detectUnderReplicatedShards(nodes, int(vc.Spec.ReplicasPerShard))
	if len(underReplicated) > 0 {
		msg := fmt.Sprintf("shards under-replicated: %s", strings.Join(underReplicated, ", "))
		logger.Info("Under-replicated shards detected", "shards", underReplicated)
		setCondition(vc, metav1.Condition{
			Type:               "ShardUnderReplicated",
			Status:             metav1.ConditionTrue,
			Reason:             "MissingReplica",
			Message:            msg,
			ObservedGeneration: vc.Generation,
		})
		r.Recorder.Event(vc, corev1.EventTypeWarning, "ShardUnderReplicated", msg)
	} else {
		setCondition(vc, metav1.Condition{
			Type:               "ShardUnderReplicated",
			Status:             metav1.ConditionFalse,
			Reason:             "ReplicationOK",
			Message:            fmt.Sprintf("All shards have %d replica(s)", vc.Spec.ReplicasPerShard),
			ObservedGeneration: vc.Generation,
		})
	}

	// --- Check 3: stale nodes (fail/disconnected) → CLUSTER FORGET ---
	if err := r.forgetStaleNodes(ctx, vc, nodes, creds); err != nil {
		logger.Error(err, "Failed to forget stale nodes")
		// Non-fatal.
	}

	// --- Check 4: primary/replica colocation on the same node or zone ---
	r.detectShardColocation(ctx, vc, nodes)

	return nil
}

// detectShardColocation checks whether any primary and one of its replicas share
// the same Kubernetes node or topology zone. This degrades fault tolerance since
// losing that node/zone would take down both the primary and its replica.
// Emits a ShardColocated warning event and sets a condition when detected.
func (r *ValkeyClusterReconciler) detectShardColocation(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, nodes []clusterNodeState) {
	logger := log.FromContext(ctx)

	topologyKey := "topology.kubernetes.io/zone"
	if vc.Spec.Topology != nil && vc.Spec.Topology.NodeTopologyKey != "" {
		topologyKey = vc.Spec.Topology.NodeTopologyKey
	}

	// Build node ID → clusterNodeState for replica→primary lookup.
	nodeByID := make(map[string]clusterNodeState, len(nodes))
	for _, n := range nodes {
		nodeByID[n.id] = n
	}

	// Resolve IP or hostname → (nodeName, zone) from the pod list.
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return
	}

	type podPlacement struct {
		nodeName string
		zone     string
	}
	// Index by both IP and hostname to handle clusters with or without announce-hostname.
	keyToPlacement := make(map[string]podPlacement, len(podList.Items)*2)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		placement := podPlacement{nodeName: pod.Spec.NodeName}
		node := &corev1.Node{}
		if err := r.Client.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err == nil {
			placement.zone = node.Labels[topologyKey]
		}
		if pod.Status.PodIP != "" {
			keyToPlacement[pod.Status.PodIP] = placement
		}
		// Stable DNS name matches cluster-announce-hostname format.
		svcName := internalServiceName(vc)
		stableDNS := fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod.Name, svcName, pod.Namespace)
		keyToPlacement[stableDNS] = placement
	}

	// nodePlacement looks up a clusterNodeState by IP first, then hostname.
	nodePlacement := func(n clusterNodeState) (podPlacement, bool) {
		if p, ok := keyToPlacement[n.ip]; ok {
			return p, true
		}
		if n.hostname != "" {
			if p, ok := keyToPlacement[n.hostname]; ok {
				return p, true
			}
		}
		return podPlacement{}, false
	}

	// For each replica, compare its placement with its primary's placement.
	var colocated []string
	for _, n := range nodes {
		if !strings.Contains(n.flags, "slave") || n.masterID == "-" || n.masterID == "" {
			continue
		}
		primary, ok := nodeByID[n.masterID]
		if !ok {
			continue
		}

		replicaPlacement, replicaKnown := nodePlacement(n)
		primaryPlacement, primaryKnown := nodePlacement(primary)
		if !replicaKnown || !primaryKnown {
			continue
		}

		// Only flag same-node colocation — losing a node takes down both primary
		// and replica simultaneously. Same-zone colocation with different nodes is
		// unavoidable with 3 zones / 6 pods and does not warrant an alert.
		if replicaPlacement.nodeName != "" && replicaPlacement.nodeName == primaryPlacement.nodeName {
			colocated = append(colocated, fmt.Sprintf("primary %s + replica %s on node %s",
				primary.id[:8], n.id[:8], replicaPlacement.nodeName))
		}
	}

	if len(colocated) > 0 {
		msg := fmt.Sprintf("shard colocation detected (reduced fault tolerance): %s", strings.Join(colocated, "; "))
		logger.Info("Shard colocation detected", "details", colocated)
		setCondition(vc, metav1.Condition{
			Type:               "ShardColocated",
			Status:             metav1.ConditionTrue,
			Reason:             "PrimaryReplicaColocated",
			Message:            msg,
			ObservedGeneration: vc.Generation,
		})
		r.Recorder.Event(vc, corev1.EventTypeWarning, "ShardColocated", msg)
	} else {
		setCondition(vc, metav1.Condition{
			Type:               "ShardColocated",
			Status:             metav1.ConditionFalse,
			Reason:             "TopologyOK",
			Message:            "No primary/replica colocation detected",
			ObservedGeneration: vc.Generation,
		})
	}
}

// fetchClusterState queries CLUSTER NODES and CLUSTER INFO from any ready pod.
func (r *ValkeyClusterReconciler) fetchClusterState(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, creds aclCredentials) (nodesRaw, clusterInfo string, err error) {
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return "", "", err
	}

	for i := range podList.Items {
		pod := &podList.Items[i]
		if !isPodReady(pod) || pod.Status.PodIP == "" {
			continue
		}

		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, effectivePort(vc))
		c, err := valkeyClient(addr, creds.username, creds.password)
		if err != nil {
			continue
		}

		tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		results := c.DoMulti(tctx,
			c.B().ClusterNodes().Build(),
			c.B().ClusterInfo().Build(),
		)
		cancel()
		c.Close()

		nodes, err1 := results[0].ToString()
		info, err2 := results[1].ToString()
		if err1 != nil || err2 != nil || nodes == "" || info == "" {
			continue
		}
		return nodes, info, nil
	}
	return "", "", fmt.Errorf("no reachable pod found")
}

// detectUnderReplicatedShards returns the node IDs of primaries that have
// fewer than expectedReplicas replicas in the cluster.
func detectUnderReplicatedShards(nodes []clusterNodeState, expectedReplicas int) []string {
	// Count replicas per primary.
	replicaCount := make(map[string]int)
	for _, n := range nodes {
		if strings.Contains(n.flags, "slave") && n.masterID != "-" && n.masterID != "" {
			replicaCount[n.masterID]++
		}
	}

	var underReplicated []string
	for _, n := range nodes {
		if !strings.Contains(n.flags, "master") {
			continue
		}
		if strings.Contains(n.flags, "fail") || strings.Contains(n.flags, "noaddr") {
			continue // Skip nodes already in failed state.
		}
		count := replicaCount[n.id]
		if count < expectedReplicas {
			underReplicated = append(underReplicated, fmt.Sprintf("%s(%d/%d replicas)", n.id[:8], count, expectedReplicas))
		}
	}
	return underReplicated
}

// forgetStaleNodes issues CLUSTER FORGET for nodes that have been in a
// fail/disconnected state for longer than staleNodeTimeout.
// It is skipped entirely during a StatefulSet rolling update to avoid
// mistakenly forgetting pods that are legitimately restarting.
func (r *ValkeyClusterReconciler) forgetStaleNodes(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, nodes []clusterNodeState, creds aclCredentials) error {
	logger := log.FromContext(ctx)

	// Guard 1: skip during rolling updates — a pod being replaced looks stale
	// (fail + unknown IP) but is just restarting. Forgetting it would force a
	// full re-MEET/re-REPLICATE once it comes back.
	sts := &appsv1.StatefulSet{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: statefulSetName(vc), Namespace: vc.Namespace}, sts); err == nil {
		if sts.Status.CurrentRevision != sts.Status.UpdateRevision {
			logger.Info("Rolling update in progress — skipping CLUSTER FORGET")
			return nil
		}
	}

	// Collect live pod IPs to distinguish truly lost nodes from restarting ones.
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return err
	}

	// Guard 2: skip if any pod is currently terminating (eviction, node drain).
	// A pod being evicted looks stale (fail + IP disappearing) but will be
	// rescheduled — forgetting it would force a full re-MEET/re-REPLICATE.
	for i := range podList.Items {
		if podList.Items[i].DeletionTimestamp != nil {
			logger.Info("Pod terminating — skipping CLUSTER FORGET")
			return nil
		}
	}

	knownIPs := make(map[string]struct{}, len(podList.Items))
	for i := range podList.Items {
		if podList.Items[i].Status.PodIP != "" {
			knownIPs[podList.Items[i].Status.PodIP] = struct{}{}
		}
	}

	now := time.Now()

	// Single snapshot before all annotation mutations — one patch at the end
	// covers both first-seen timestamps and forgotten-node cleanup in a single
	// API call, preventing divergence from a double-patch pattern.
	patch := client.MergeFrom(vc.DeepCopy())
	annotationsChanged := false

	var staleIDs []string
	for _, n := range nodes {
		isFailing := strings.Contains(n.flags, "fail") || strings.Contains(n.flags, "noaddr")
		_, ipKnown := knownIPs[n.ip]

		annotationKey := staleNodeAnnotationPrefix + n.id

		if !isFailing || ipKnown {
			// Node is healthy or its IP is still known — remove stale annotation if present.
			if vc.Annotations != nil {
				if _, exists := vc.Annotations[annotationKey]; exists {
					delete(vc.Annotations, annotationKey)
					annotationsChanged = true
				}
			}
			continue
		}

		// Enforce staleNodeTimeout via annotation timestamp.
		// Record first observation time; only forget after the timeout elapses.
		if vc.Annotations == nil {
			vc.Annotations = make(map[string]string)
		}
		firstSeenStr, tracked := vc.Annotations[annotationKey]
		if !tracked {
			vc.Annotations[annotationKey] = now.UTC().Format(time.RFC3339)
			annotationsChanged = true
			logger.Info("Stale node first observed — waiting for timeout",
				"nodeID", n.id[:8], "timeout", staleNodeTimeout)
			continue
		}

		firstSeen, err := time.Parse(time.RFC3339, firstSeenStr)
		if err != nil || now.Sub(firstSeen) < staleNodeTimeout {
			// Not yet timed out.
			continue
		}

		staleIDs = append(staleIDs, n.id)
	}

	// Remove annotations for nodes we are about to forget.
	for _, id := range staleIDs {
		delete(vc.Annotations, staleNodeAnnotationPrefix+id)
		annotationsChanged = true
	}

	// Single patch: persists first-seen timestamps, cleared healthy nodes, and
	// forgotten-node annotation removals atomically.
	if annotationsChanged {
		if err := r.Patch(ctx, vc, patch); err != nil {
			logger.Info("Failed to patch stale-node annotations", "err", err)
		}
	}

	if len(staleIDs) == 0 {
		return nil
	}

	// Issue CLUSTER FORGET from every healthy node.
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !isPodReady(pod) || pod.Status.PodIP == "" {
			continue
		}

		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, effectivePort(vc))
		func() {
			c, err := valkeyClient(addr, creds.username, creds.password)
			if err != nil {
				return
			}
			defer c.Close()

			tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
			defer cancel()
			for _, staleID := range staleIDs {
				if err := c.Do(tctx, c.B().ClusterForget().NodeId(staleID).Build()).Error(); err != nil {
					// Node may already have been forgotten — not fatal.
					logger.Info("CLUSTER FORGET skipped", "nodeID", staleID[:8], "err", err)
				} else {
					logger.Info("CLUSTER FORGET issued", "nodeID", staleID[:8], "pod", pod.Name)
					r.Recorder.Eventf(vc, corev1.EventTypeNormal, "NodeForgotten",
						"Stale cluster node %s forgotten from pod %s", staleID[:8], pod.Name)
				}
			}
		}()
	}

	return nil
}
