// Copyright 2026 MonkeyCorp Cloud contributors
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// reconcileNodeDrainFailover detects primary pods running on Unschedulable nodes
// (cordoned by GKE before a nodepool upgrade or manual drain) and triggers a
// pro-active CLUSTER FAILOVER before SIGTERM arrives.
//
// GKE cordons a node (Unschedulable=true) before draining it. This gives the
// operator a window of several seconds to move the primary role away cleanly.
// Returns (true, nil) when a failover was initiated and a short requeue is needed.
func (r *ValkeyClusterReconciler) reconcileNodeDrainFailover(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) (bool, error) {
	logger := log.FromContext(ctx)

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return false, err
	}

	for i := range podList.Items {
		pod := &podList.Items[i]

		// Only act on primary pods.
		if pod.Labels == nil || pod.Labels[roleLabelKey] != rolePrimary {
			continue
		}

		// Pod must be running and not already terminating.
		if pod.DeletionTimestamp != nil || pod.Spec.NodeName == "" {
			continue
		}

		// Check if the node is unschedulable (cordoned by GKE or manually).
		node := &corev1.Node{}
		if err := r.Client.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
			continue
		}
		if !node.Spec.Unschedulable {
			continue
		}

		logger.Info("Primary pod on unschedulable node — initiating pro-active CLUSTER FAILOVER",
			"pod", pod.Name,
			"node", pod.Spec.NodeName,
		)
		r.Recorder.Eventf(vc, corev1.EventTypeWarning, "NodeDrainFailover",
			"Primary pod %s is on unschedulable node %s — triggering pro-active failover",
			pod.Name, pod.Spec.NodeName,
		)

		operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
		if err != nil {
			return false, fmt.Errorf("resolving operator secret: %w", err)
		}
		creds := aclCredentials{username: operatorUsername, password: operatorPassword}
		if err := r.clusterFailover(ctx, vc, pod, creds); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// clusterFailover selects the best replica for the given primary's shard and
// issues CLUSTER FAILOVER (cooperative handover). It then waits for the role
// swap to be confirmed via CLUSTER NODES before returning.
//
// CLUSTER FAILOVER (without FORCE/TAKEOVER) is the safest path:
//   - The current primary stops accepting writes and waits for the replica to catch up.
//   - The replica becomes primary only once fully synced — zero data loss.
//   - Takes ~100-300ms instead of cluster-node-timeout (2000ms default).
func (r *ValkeyClusterReconciler) clusterFailover(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, primaryPod *corev1.Pod, creds aclCredentials) error {
	logger := log.FromContext(ctx).WithValues("primaryPod", primaryPod.Name)

	// Find the best replica for this primary's shard using topology-aware scoring.
	bestReplica, err := r.pickBestReplicaForShard(ctx, vc, primaryPod, creds)
	if err != nil {
		return fmt.Errorf("picking best replica for shard: %w", err)
	}
	logger.Info("Selected replica for CLUSTER FAILOVER", "replica", bestReplica.Name)

	// Issue CLUSTER FAILOVER on the chosen replica.
	// The replica contacts the primary, which cooperates by stopping writes.
	replicaAddr := fmt.Sprintf("%s:%d", bestReplica.Status.PodIP, effectivePort(vc))
	rc, err := valkeyClient(replicaAddr, creds.username, creds.password)
	if err != nil {
		return fmt.Errorf("connecting to replica %s: %w", bestReplica.Name, err)
	}
	defer rc.Close()

	tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
	defer cancel()

	// Verify the target is still a replica before issuing CLUSTER FAILOVER.
	// The automatic election may have already completed during pod termination.
	role, _ := r.getPodClusterRole(tctx, replicaAddr, creds)
	if role == rolePrimary {
		logger.Info("Target pod is already primary — failover already completed", "pod", bestReplica.Name)
		return nil
	}

	if err := rc.Do(tctx, rc.B().ClusterFailover().Build()).Error(); err != nil {
		return fmt.Errorf("CLUSTER FAILOVER on %s: %w", bestReplica.Name, err)
	}

	logger.Info("CLUSTER FAILOVER issued, waiting for role swap confirmation")

	// Poll CLUSTER NODES until the replica appears as master.
	if err := r.waitForFailoverComplete(ctx, vc, bestReplica, primaryPod, creds); err != nil {
		return fmt.Errorf("waiting for failover confirmation: %w", err)
	}

	// Update role labels immediately so the operator status is accurate.
	_ = r.applyPodRoleLabel(ctx, bestReplica, rolePrimary)
	_ = r.removePodRoleLabel(ctx, primaryPod)

	logger.Info("Cluster failover complete", "newPrimary", bestReplica.Name, "oldPrimary", primaryPod.Name)
	return nil
}

// waitForFailoverComplete does a single non-blocking check of CLUSTER NODES to
// verify whether the failover has completed. Returns nil if the new primary has
// assumed the master role, or an error if not yet confirmed.
//
// The caller (reconcileNodeDrainFailover) already returns requeued=true with a
// 2-second requeue, so repeated confirmation attempts happen via normal reconcile
// cycles rather than a blocking sleep loop — keeping the reconcile goroutine free.
func (r *ValkeyClusterReconciler) waitForFailoverComplete(
	ctx context.Context,
	vc *cachev1alpha1.ValkeyCluster,
	newPrimary, oldPrimary *corev1.Pod,
	creds aclCredentials,
) error {
	// Query from a third pod to avoid bias if the old primary is already gone.
	pollAddr, err := r.findPollAddr(ctx, vc, newPrimary, oldPrimary, creds)
	if err != nil {
		// Fall back to the new primary itself.
		pollAddr = fmt.Sprintf("%s:%d", newPrimary.Status.PodIP, effectivePort(vc))
	}

	c, err := valkeyClient(pollAddr, creds.username, creds.password)
	if err != nil {
		return fmt.Errorf("cannot connect to poll node: %w", err)
	}
	defer c.Close()

	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	nodesRaw, err := c.Do(tctx, c.B().ClusterNodes().Build()).ToString()
	if err != nil {
		return fmt.Errorf("CLUSTER NODES query failed: %w", err)
	}

	// Match by IP first (cluster-announce-ip), hostname as fallback.
	entries := parseNodeRoles(nodesRaw)
	svcName := internalServiceName(vc)
	stableDNS := fmt.Sprintf("%s.%s.%s.svc.cluster.local", newPrimary.Name, svcName, newPrimary.Namespace)
	for _, e := range entries {
		if e.ip == newPrimary.Status.PodIP || (e.hostname != "" && e.hostname == stableDNS) {
			if e.role == rolePrimary {
				return nil
			}
			return fmt.Errorf("failover not yet confirmed: %s still has role %q", newPrimary.Name, e.role)
		}
	}
	return fmt.Errorf("failover not confirmed: %s not found in CLUSTER NODES", newPrimary.Name)
}

// findPollAddr returns the address of any ready pod that is neither oldPrimary nor newPrimary.
func (r *ValkeyClusterReconciler) findPollAddr(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, exclude1, exclude2 *corev1.Pod, creds aclCredentials) (string, error) {
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return "", err
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Name == exclude1.Name || pod.Name == exclude2.Name {
			continue
		}
		if isPodReady(pod) && pod.Status.PodIP != "" {
			return fmt.Sprintf("%s:%d", pod.Status.PodIP, effectivePort(vc)), nil
		}
	}
	return "", fmt.Errorf("no available poll pod found")
}

// pickBestReplicaForShard selects the best replica of the given primary's shard
// using topology-aware scoring when configured, falling back to lag-only otherwise.
func (r *ValkeyClusterReconciler) pickBestReplicaForShard(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, primaryPod *corev1.Pod, creds aclCredentials) (*corev1.Pod, error) {
	// Resolve the primary's cluster node ID to identify its replicas.
	primaryAddr := fmt.Sprintf("%s:%d", primaryPod.Status.PodIP, effectivePort(vc))
	primaryNodeID, err := r.getNodeID(ctx, primaryAddr, creds)
	if err != nil {
		return nil, fmt.Errorf("getting node ID of primary: %w", err)
	}

	// Get all replica pods for this shard from CLUSTER NODES.
	replicaPods, err := r.findReplicaPodsForMaster(ctx, vc, primaryNodeID, primaryPod, creds)
	if err != nil {
		return nil, err
	}
	if len(replicaPods) == 0 {
		return nil, fmt.Errorf("no replicas found for primary %s (node %s)", primaryPod.Name, primaryNodeID)
	}

	if vc.Spec.Topology != nil {
		return r.pickBestReplicaTopologyAware(ctx, vc, primaryPod.Name, creds, replicaPods)
	}
	return r.pickBestReplicaByLag(ctx, vc, creds, replicaPods)
}

// getNodeID returns the cluster node ID of a pod via CLUSTER MYID.
func (r *ValkeyClusterReconciler) getNodeID(ctx context.Context, addr string, creds aclCredentials) (string, error) {
	c, err := valkeyClient(addr, creds.username, creds.password)
	if err != nil {
		return "", err
	}
	defer c.Close()

	tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
	defer cancel()

	id, err := c.Do(tctx, c.B().ClusterMyid().Build()).ToString()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(stripVerbatimPrefix(id)), nil
}

// findReplicaPodsForMaster returns all ready pods that are replicas of the given master node ID.
// primaryPod is explicitly excluded from the result to guard against self-selection.
func (r *ValkeyClusterReconciler) findReplicaPodsForMaster(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, masterNodeID string, primaryPod *corev1.Pod, creds aclCredentials) ([]*corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return nil, err
	}

	// Get CLUSTER NODES from any ready pod.
	var nodesRaw string
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
		tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
		raw, err := c.Do(tctx, c.B().ClusterNodes().Build()).ToString()
		cancel()
		c.Close()
		if err == nil {
			nodesRaw = raw
			break
		}
	}

	if nodesRaw == "" {
		return nil, fmt.Errorf("could not retrieve CLUSTER NODES")
	}

	// Build lookup sets for both IP and hostname of replicas belonging to masterNodeID.
	replicaIPs, replicaHostnames := parseReplicaAddrsForMaster(nodesRaw, masterNodeID)

	svcName := internalServiceName(vc)
	// Match to pods — exclude the primary itself and terminating/non-running pods.
	// Do not filter on isPodReady: a replica may not have passed its readiness
	// probe yet but still be reachable and able to receive CLUSTER FAILOVER.
	// CLUSTER NODES is the source of truth for replica eligibility.
	// Require PodRunning to avoid selecting Pending/Unknown pods that cannot
	// accept connections.
	var result []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Name == primaryPod.Name {
			continue
		}
		if pod.DeletionTimestamp != nil || pod.Status.PodIP == "" {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}
		_, matchIP := replicaIPs[pod.Status.PodIP]
		stableDNS := fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod.Name, svcName, pod.Namespace)
		_, matchHostname := replicaHostnames[stableDNS]
		if matchIP || matchHostname {
			result = append(result, &podList.Items[i])
		}
	}
	return result, nil
}

// pickBestReplicaByLag selects the replica with the highest replication offset
// (smallest lag) from a given set of candidate pods.
func (r *ValkeyClusterReconciler) pickBestReplicaByLag(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, creds aclCredentials, candidates []*corev1.Pod) (*corev1.Pod, error) {
	type result struct {
		pod    *corev1.Pod
		offset int64
	}
	ch := make(chan result, len(candidates))

	for _, pod := range candidates {
		go func(p *corev1.Pod) {
			addr := fmt.Sprintf("%s:%d", p.Status.PodIP, effectivePort(vc))
			offset, err := getReplicaOffset(ctx, addr, creds)
			if err != nil {
				offset = -1
			}
			ch <- result{pod: p, offset: offset}
		}(pod)
	}

	var best *corev1.Pod
	var bestOffset int64 = -1
	for i := 0; i < len(candidates); i++ {
		res := <-ch
		if res.offset > bestOffset {
			bestOffset = res.offset
			best = res.pod
		}
	}

	if best == nil {
		return nil, fmt.Errorf("no valid replica candidates found")
	}
	return best, nil
}

// getReplicaOffset returns the replication offset of a replica pod.
// Higher offset = more up-to-date.
// getPodClusterRole returns "primary" or "replica" for a pod via INFO replication.
func (r *ValkeyClusterReconciler) getPodClusterRole(ctx context.Context, addr string, creds aclCredentials) (string, error) {
	c, err := valkeyClient(addr, creds.username, creds.password)
	if err != nil {
		return "", err
	}
	defer c.Close()

	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	info, err := c.Do(tctx, c.B().Info().Section("replication").Build()).ToString()
	if err != nil {
		return "", err
	}

	for _, line := range strings.Split(stripVerbatimPrefix(info), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "role:") {
			val := strings.TrimPrefix(line, "role:")
			val = strings.TrimSpace(val)
			if val == "master" {
				return rolePrimary, nil
			}
			return roleReplica, nil
		}
	}
	return "", fmt.Errorf("role field not found in INFO replication")
}

func getReplicaOffset(ctx context.Context, addr string, creds aclCredentials) (int64, error) {
	c, err := valkeyClient(addr, creds.username, creds.password)
	if err != nil {
		return -1, err
	}
	defer c.Close()

	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	info, err := c.Do(tctx, c.B().Info().Section("replication").Build()).ToString()
	if err != nil {
		return -1, err
	}

	offset := parseIntField(info, "slave_repl_offset:")
	if offset < 0 {
		offset = parseIntField(info, "master_repl_offset:")
	}
	return offset, nil
}

// removePodRoleLabel removes the valkey.io/role label from a pod.
func (r *ValkeyClusterReconciler) removePodRoleLabel(ctx context.Context, pod *corev1.Pod) error {
	patchData := []byte(fmt.Sprintf(`{"metadata":{"labels":{"%s":null}}}`, roleLabelKey))
	return r.Client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, patchData))
}
