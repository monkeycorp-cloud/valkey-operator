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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

const (
	roleLabelKey       = "valkey.io/role"
	rolePrimary        = "primary"
	roleReplica        = "replica"
	clusterTimeout     = 10 * time.Second
	bootstrapJobSuffix = "-bootstrap"
	bootstrapSASuffix  = "-bootstrap"
)

// reconcileClusterTopology is the main cluster state reconciler.
// Before bootstrap: creates a Job that runs valkey-cli --cluster create.
// After bootstrap: syncs role labels on pods.
//
// Returns isBootstrapped=true when the cluster is fully formed (cluster_state=ok,
// slots assigned). This is determined by querying Valkey directly on every reconcile —
// no annotation is used so there is no risk of stale or lost state.
func (r *ValkeyClusterReconciler) reconcileClusterTopology(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) (isBootstrapped bool, err error) {
	logger := log.FromContext(ctx)

	operatorPassword, resolveErr := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if resolveErr != nil {
		return false, fmt.Errorf("resolving operator secret: %w", resolveErr)
	}
	creds := aclCredentials{username: operatorUsername, password: operatorPassword}

	// Collect all ready pods.
	podList := &corev1.PodList{}
	if listErr := r.Client.List(ctx, podList,
		podSelector(vc),
		client.InNamespace(vc.Namespace),
	); listErr != nil {
		return false, listErr
	}

	readyPods := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if isPodReady(pod) && pod.Status.PodIP != "" {
			readyPods = append(readyPods, pod)
		}
	}

	logger.Info("Cluster topology: pods collected", "ready", len(readyPods), "total", len(podList.Items))

	if len(readyPods) == 0 {
		logger.Info("No ready pods — waiting before querying cluster state")
		return false, nil
	}

	// Determine cluster state directly from Valkey — no annotation dependency.
	// The cluster is bootstrapped when cluster_state=ok and slots are assigned.
	clusterState, slotsAssigned, queryErr := r.queryClusterInfo(ctx, vc, readyPods, creds)
	logger.Info("CLUSTER INFO result", "state", clusterState, "slotsAssigned", slotsAssigned, "err", queryErr)

	if queryErr == nil && slotsAssigned > 0 && clusterState == "ok" {
		// Cluster is healthy — sync role labels and report bootstrapped.
		r.clearClusterNotOKAnnotation(ctx, vc)
		if syncErr := r.syncRoleLabels(ctx, vc, readyPods, creds); syncErr != nil {
			logger.Info("Failed to sync role labels", "error", syncErr)
		}
		return true, nil
	}

	// Cluster has slots but gossip has not converged — typical after a brutal restart
	// where pod IPs changed. The nodes.conf carries stale IPs that are no longer
	// reachable, so gossip cannot self-heal. Push current IPs via OPERATOR.TOPOLOGY.SET
	// (CLUSTER MEET only, no REPLICATE) to kick-start convergence.
	// This is handled before reconcileBootstrapJob to avoid a destructive CLUSTER RESET.
	if queryErr == nil && slotsAssigned > 0 && clusterState == "fail" {
		if r.clusterNotOKTimedOut(ctx, vc) {
			logger.Info("Gossip convergence timed out — pushing topology to all running pods")
			r.clearClusterNotOKAnnotation(ctx, vc)
			return false, r.reconcileGossipRecovery(ctx, vc, creds)
		}
		logger.Info("cluster_state:fail with slots — waiting for gossip self-recovery",
			"timeout", convergenceTimeoutFor(vc))
		return false, nil
	}

	logger.Info("Cluster not yet bootstrapped — running bootstrap state machine",
		"clusterState", clusterState, "slotsAssigned", slotsAssigned, "queryErr", queryErr)
	// Cluster not yet formed or not healthy — run bootstrap state machine.
	return false, r.reconcileBootstrapJob(ctx, vc, readyPods)
}

const (
	// annotationClusterNotOKSince is set when we first observe slots>0 + state!=ok.
	// Used by both the bootstrap and gossip recovery paths to measure how long
	// the cluster has been degraded. Cleared when cluster_state returns to ok.
	annotationClusterNotOKSince = "valkey.io/cluster-not-ok-since"

	// annotationLastResetTime records when the last CLUSTER RESET SOFT was
	// issued. Used as a cooldown to prevent repeated resets every
	// convergenceTimeout cycle if the cluster stays in fail state.
	annotationLastResetTime = "valkey.io/last-reset-time"

	// resetCooldown is the minimum interval between two CLUSTER RESET SOFT
	// operations. Gives the cluster time to reform after a reset before
	// attempting another one.
	resetCooldown = 5 * time.Minute
)

// reconcileBootstrapJob drives the bootstrap state machine entirely from Go.
//
// The Job script is intentionally minimal: it only runs `valkey-cli --cluster create`.
// All convergence checks and idempotency decisions are made here in the reconciler
// so they benefit from the normal requeue loop instead of a blocking shell sleep.
//
// State machine (evaluated on every reconcile when cluster is not yet formed):
//
//  1. slots > 0, state ≠ ok  → gossip converging; wait up to convergenceTimeout,
//     then force CLUSTER RESET SOFT + bootstrap (handles scale-to-0 / CR recreate).
//  2. Not all pods Ready → wait.
//  3. slots = 0, all pods ready → fresh cluster; CLUSTER RESET SOFT + bootstrap Job.
//
// Note: the slots > 0, state = ok case is handled upstream in reconcileClusterTopology
// before this function is called.
func (r *ValkeyClusterReconciler) reconcileBootstrapJob(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, readyPods []*corev1.Pod) error {
	logger := log.FromContext(ctx)

	total := int(totalPods(vc))

	operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return fmt.Errorf("resolving operator secret: %w", err)
	}
	creds := aclCredentials{username: operatorUsername, password: operatorPassword}

	if len(readyPods) < total {
		logger.Info("Waiting for all pods before bootstrap", "ready", len(readyPods), "total", total)
		return nil
	}

	// Query OPERATOR.BOOTSTRAP.READY from all pods to determine readiness.
	// A pod reports ready when: cluster-enabled=1, slots_assigned=0, known_nodes<=1.
	allReady, notReadyReasons, err := r.allNodesBootstrapReady(ctx, vc, readyPods, creds)
	if err != nil {
		// Transient — pods may not yet accept connections.
		logger.Info("Could not query bootstrap readiness, will retry", "error", err)
		return nil
	}

	if !allReady {
		// Check if all nodes have slots but gossip hasn't converged yet.
		alreadyFormed := true
		for _, reason := range notReadyReasons {
			if reason != "node already owns slots — already part of a cluster" {
				alreadyFormed = false
				break
			}
		}
		if alreadyFormed {
			// Cluster has slots but cluster_state!=ok — gossip converging.
			logger.Info("Cluster slots assigned but state not yet ok — waiting for gossip convergence")
			return nil
		}

		// Check for stale peers requiring CLUSTER RESET SOFT.
		hasStalePeers := false
		for _, reason := range notReadyReasons {
			if reason == "stale peers in nodes.conf — CLUSTER RESET SOFT required" {
				hasStalePeers = true
				break
			}
		}

		if hasStalePeers {
			if !r.clusterNotOKTimedOut(ctx, vc) {
				logger.Info("Some nodes have stale peers — waiting for convergence",
					"timeout", convergenceTimeoutFor(vc))
				return nil
			}
			logger.Info("Gossip convergence timed out — forcing CLUSTER RESET SOFT on all pods")
			// Clear annotation AFTER the reset succeeds so the timer is not lost
			// on a transient failure — next cycle will retry without waiting again.
			if err := r.resetAllNodes(ctx, vc, readyPods, creds); err != nil {
				logger.Error(err, "Failed to reset nodes — will retry")
				return nil
			}
			r.clearClusterNotOKAnnotation(ctx, vc)
			// After reset all nodes will be ready — fall through to bootstrap Job.
		} else {
			logger.Info("Not all nodes bootstrap-ready yet — waiting", "reasons", notReadyReasons)
			return nil
		}
	}

	// All nodes bootstrap-ready (slots=0, no stale peers) — run bootstrap Job.

	// Check if a bootstrap Job is already running or completed.
	jobName := vc.Name + bootstrapJobSuffix
	job := &batchv1.Job{}
	err = r.Client.Get(ctx, client.ObjectKey{Name: jobName, Namespace: vc.Namespace}, job)

	if err == nil {
		if job.Status.Succeeded > 0 {
			// Job succeeded — delete it. On next reconcile, queryClusterInfo will
			// return state=ok and reconcileClusterTopology will report isBootstrapped=true.
			logger.Info("Bootstrap Job succeeded — cluster will be detected as bootstrapped on next reconcile")
			_ = r.Client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
			return nil
		}
		if job.Spec.BackoffLimit != nil && job.Status.Failed > *job.Spec.BackoffLimit {
			logger.Error(fmt.Errorf("bootstrap job failed %d times", job.Status.Failed),
				"Bootstrap Job exhausted retries — deleting for retry on next cycle")
			_ = r.Client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
			return nil
		}
		logger.Info("Bootstrap Job in progress",
			"active", job.Status.Active,
			"succeeded", job.Status.Succeeded,
			"failed", job.Status.Failed)
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("getting bootstrap job: %w", err)
	}

	// Issue CLUSTER RESET SOFT on every pod before --cluster create.
	// Safe on fresh pods (no-op), required when PVCs carry stale cluster state
	// (e.g. CR recreated, scale-to-0 with stale nodes.conf that did not converge).
	if err := r.resetAllNodes(ctx, vc, readyPods, creds); err != nil {
		logger.Error(err, "Failed to reset nodes before bootstrap — will retry")
		return nil
	}

	newJob := r.buildBootstrapJob(vc)
	if err := r.Client.Create(ctx, newJob); err != nil {
		return fmt.Errorf("creating bootstrap job: %w", err)
	}
	logger.Info("Created bootstrap Job", "job", jobName)
	return nil
}

// convergenceTimeoutFor returns how long to wait for gossip convergence before
// giving up and forcing a CLUSTER RESET SOFT.
//
// The timeout is 10× the cluster-node-timeout: that is the time Valkey needs
// to detect a failed node and elect a new primary. After a full restart, gossip
// should converge well within one detection cycle. If it has not converged after
// 10 cycles, the nodes.conf is from a different cluster incarnation.
// Minimum is 30s, maximum is 120s to avoid waiting too long on slow clusters.
func convergenceTimeoutFor(vc *cachev1alpha1.ValkeyCluster) time.Duration {
	nodeTimeoutMs := vc.Spec.ClusterNodeTimeout
	if nodeTimeoutMs <= 0 {
		nodeTimeoutMs = 2000 // default cluster-node-timeout: 2000ms
	}
	d := time.Duration(nodeTimeoutMs) * time.Millisecond * 10
	if d < 30*time.Second {
		d = 30 * time.Second
	}
	if d > 120*time.Second {
		d = 120 * time.Second
	}
	return d
}

// clusterNotOKTimedOut returns true when the cluster has been in a non-ok state
// for longer than convergenceTimeoutFor(vc). Sets the cluster-not-ok-since
// annotation on the first call so subsequent calls can measure elapsed time.
// Used by both the bootstrap and gossip recovery paths.
func (r *ValkeyClusterReconciler) clusterNotOKTimedOut(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) bool {
	if vc.Annotations == nil {
		vc.Annotations = make(map[string]string)
	}

	start, exists := vc.Annotations[annotationClusterNotOKSince]
	if !exists {
		patch := client.MergeFrom(vc.DeepCopy())
		vc.Annotations[annotationClusterNotOKSince] = time.Now().UTC().Format(time.RFC3339)
		if err := r.Client.Patch(ctx, vc, patch); err != nil {
			// Revert in-memory so the timer starts only once persisted.
			delete(vc.Annotations, annotationClusterNotOKSince)
		}
		return false
	}

	t, err := time.Parse(time.RFC3339, start)
	if err != nil {
		return false
	}
	return time.Since(t) >= convergenceTimeoutFor(vc)
}

// clearClusterNotOKAnnotation removes the cluster-not-ok-since and
// last-reset-time annotations. Called when cluster_state returns to ok.
func (r *ValkeyClusterReconciler) clearClusterNotOKAnnotation(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) {
	if vc.Annotations == nil {
		return
	}
	_, hasNotOK := vc.Annotations[annotationClusterNotOKSince]
	_, hasReset := vc.Annotations[annotationLastResetTime]
	if !hasNotOK && !hasReset {
		return
	}
	patch := client.MergeFrom(vc.DeepCopy())
	delete(vc.Annotations, annotationClusterNotOKSince)
	delete(vc.Annotations, annotationLastResetTime)
	if err := r.Client.Patch(ctx, vc, patch); err != nil {
		log.FromContext(ctx).Error(err, "Failed to clear cluster recovery annotations")
	}
}

// queryClusterInfo queries CLUSTER INFO from all ready pods and returns the most
// informative result: the pod reporting the highest slot count, preferring
// state=ok when slots are equal. Querying all pods avoids false negatives from
// a minority-partition pod that responds but reports an empty or stale state.
func (r *ValkeyClusterReconciler) queryClusterInfo(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, pods []*corev1.Pod, creds aclCredentials) (string, int64, error) {
	type result struct {
		state string
		slots int64
	}
	ch := make(chan result, len(pods))

	for _, pod := range pods {
		go func(p *corev1.Pod) {
			addr := fmt.Sprintf("%s:%d", p.Status.PodIP, effectivePort(vc))
			c, err := valkeyClient(addr, creds.username, creds.password)
			if err != nil {
				ch <- result{}
				return
			}
			tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			info, err := c.Do(tctx, c.B().ClusterInfo().Build()).ToString()
			cancel()
			c.Close()
			if err != nil {
				ch <- result{}
				return
			}
			ch <- result{
				state: parseClusterInfoField(info, "cluster_state"),
				slots: parseClusterInfoInt(info, "cluster_slots_assigned"),
			}
		}(pod)
	}

	var bestState string
	var bestSlots int64
	responded := 0
	for i := 0; i < len(pods); i++ {
		r := <-ch
		if r.slots == 0 && r.state == "" {
			continue
		}
		responded++
		// Prefer the pod with the most slots; break ties by preferring state=ok.
		if r.slots > bestSlots || (r.slots == bestSlots && r.state == "ok") {
			bestSlots = r.slots
			bestState = r.state
		}
	}

	if responded == 0 {
		return "", 0, fmt.Errorf("no responsive pod found")
	}
	return bestState, bestSlots, nil
}

// allNodesBootstrapReady queries OPERATOR.BOOTSTRAP.READY on all pods in parallel.
// Returns (true, nil, nil) when every pod reports ready.
// Returns (false, reasons, nil) when at least one pod is not ready — reasons
// contains the per-pod reason strings for logging and decision making.
func (r *ValkeyClusterReconciler) allNodesBootstrapReady(
	ctx context.Context,
	vc *cachev1alpha1.ValkeyCluster,
	pods []*corev1.Pod,
	creds aclCredentials,
) (bool, []string, error) {
	type result struct {
		ready  bool
		reason string
	}
	ch := make(chan result, len(pods))

	for _, pod := range pods {
		go func(p *corev1.Pod) {
			addr := fmt.Sprintf("%s:%d", p.Status.PodIP, effectivePort(vc))
			c, err := valkeyClient(addr, creds.username, creds.password)
			if err != nil {
				ch <- result{false, "unreachable"}
				return
			}
			tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			resp, err := c.Do(tctx, c.B().Arbitrary("OPERATOR.BOOTSTRAP.READY").Build()).ToArray()
			cancel()
			c.Close()
			if err != nil || len(resp) < 2 {
				ch <- result{false, "command error: " + err.Error()}
				return
			}
			ready, _ := resp[0].ToInt64()
			reason, _ := resp[1].ToString()
			ch <- result{ready == 1, reason}
		}(pod)
	}

	var notReadyReasons []string
	for i := 0; i < len(pods); i++ {
		res := <-ch
		if !res.ready {
			notReadyReasons = append(notReadyReasons, res.reason)
		}
	}

	if len(notReadyReasons) > 0 {
		return false, notReadyReasons, nil
	}
	return true, nil, nil
}

// resetAllNodes issues CLUSTER RESET SOFT on every reachable pod.
// SOFT reset clears cluster membership (node-id, known peers, slot assignments)
// while preserving data in dump.rdb, so --cluster create can form a clean cluster
// even when pods hold stale nodes.conf from a previous cluster incarnation.
// Pods that cannot be reached are skipped — they will be reset on a later attempt.
// A cooldown annotation prevents repeated resets if the cluster stays in fail state.
func (r *ValkeyClusterReconciler) resetAllNodes(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, pods []*corev1.Pod, creds aclCredentials) error {
	logger := log.FromContext(ctx)

	// Cooldown: skip if a reset was issued recently.
	if vc.Annotations != nil {
		if lastStr, ok := vc.Annotations[annotationLastResetTime]; ok {
			if last, err := time.Parse(time.RFC3339, lastStr); err == nil && time.Since(last) < resetCooldown {
				logger.Info("CLUSTER RESET SOFT skipped — cooldown active",
					"lastReset", lastStr, "cooldown", resetCooldown)
				return nil
			}
		}
	}

	for _, pod := range pods {
		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, effectivePort(vc))
		func() {
			c, err := valkeyClient(addr, creds.username, creds.password)
			if err != nil {
				logger.Info("Cannot connect to pod for reset — skipping", "pod", pod.Name, "error", err)
				return
			}
			defer c.Close()

			tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if err := c.Do(tctx, c.B().ClusterReset().Soft().Build()).Error(); err != nil {
				logger.Error(err, "CLUSTER RESET SOFT failed", "pod", pod.Name)
			} else {
				logger.Info("CLUSTER RESET SOFT issued", "pod", pod.Name)
			}
		}()
	}

	// Record reset timestamp for cooldown.
	if vc.Annotations == nil {
		vc.Annotations = make(map[string]string)
	}
	patch := client.MergeFrom(vc.DeepCopy())
	vc.Annotations[annotationLastResetTime] = time.Now().UTC().Format(time.RFC3339)
	if err := r.Client.Patch(ctx, vc, patch); err != nil {
		logger.Info("Failed to record reset timestamp", "err", err)
	}

	return nil
}

// reconcileBootstrapServiceAccount ensures a dedicated ServiceAccount exists for
// the bootstrap Job. The SA has no RBAC bindings — the Job only calls valkey-cli
// over the network and needs no Kubernetes API access. Using a dedicated SA
// (instead of the namespace default) prevents the Job pod from inheriting any
// RBAC permissions that may be bound to the default SA by cluster policies.
func (r *ValkeyClusterReconciler) reconcileBootstrapServiceAccount(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vc.Name + bootstrapSASuffix,
			Namespace: vc.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		sa.Labels = commonLabels(vc)
		// Disable auto-mount of the API server token — the Job has no need for it.
		f := false
		sa.AutomountServiceAccountToken = &f
		return controllerutil.SetControllerReference(vc, sa, r.Scheme)
	})
	return err
}

// buildBootstrapJob constructs the bootstrap Job spec.
// The script is intentionally minimal: wait for PING on all pods, then run
// `--cluster create`. All convergence and idempotency logic has been moved to
// reconcileBootstrapJob so the Job only ever runs on a fresh cluster.
func (r *ValkeyClusterReconciler) buildBootstrapJob(vc *cachev1alpha1.ValkeyCluster) *batchv1.Job {
	port := effectivePort(vc)
	shards := int(vc.Spec.Shards)
	replicasPerShard := int(vc.Spec.ReplicasPerShard)
	total := shards * (1 + replicasPerShard)
	svcName := internalServiceName(vc)

	// Build the host list using StatefulSet DNS names:
	// <name>-0.<headless-svc>.<ns>.svc.cluster.local:port ...
	var hostList strings.Builder
	for i := 0; i < total; i++ {
		if i > 0 {
			hostList.WriteString(" ")
		}
		fmt.Fprintf(&hostList, "%s-%d.%s.%s.svc.cluster.local:%d",
			vc.Name, i, svcName, vc.Namespace, port)
	}

	script := fmt.Sprintf(`#!/bin/sh
set -e

HOSTS="%s"
CLI="valkey-cli --no-auth-warning --user operator --pass $VALKEY_OPERATOR_PASSWORD"

echo "==> Waiting for all pods to respond to PING..."
for host in $HOSTS; do
  echo -n "  Waiting for $host... "
  until $CLI -h ${host%%:*} -p ${host##*:} PING 2>/dev/null | grep -q PONG; do
    sleep 2
  done
  echo "OK"
done

echo "==> Running valkey-cli --cluster create..."
$CLI --cluster create $HOSTS \
  --cluster-replicas %d \
  --cluster-yes

echo "==> Bootstrap complete."
`, hostList.String(), replicasPerShard)

	backoffLimit := int32(3)
	ttl := int32(300)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vc.Name + bootstrapJobSuffix,
			Namespace: vc.Namespace,
			Labels:    commonLabels(vc),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: bootstrapJobLabels(vc),
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyOnFailure,
					ServiceAccountName:           vc.Name + bootstrapSASuffix,
					AutomountServiceAccountToken: func() *bool { f := false; return &f }(),
					Containers: []corev1.Container{
						{
							Name:            "bootstrap",
							Image:           vc.Spec.Image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"sh", "-c", script},
							Env: []corev1.EnvVar{
								{
									Name: "VALKEY_OPERATOR_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &vc.Spec.OperatorSecret,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Set owner reference so the Job is GC'd when the ValkeyCluster CR is deleted.
	_ = controllerutil.SetControllerReference(vc, job, r.Scheme)
	return job
}

// syncRoleLabels reads CLUSTER NODES and applies valkey.io/role labels on pods.
// It tries each ready pod in turn as seed until one returns a consistent view —
// this avoids stale data from a pod that just restarted and has not yet converged
// its gossip state.
func (r *ValkeyClusterReconciler) syncRoleLabels(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, pods []*corev1.Pod, creds aclCredentials) error {
	logger := log.FromContext(ctx)
	if len(pods) == 0 {
		return nil
	}

	port := effectivePort(vc)
	expectedNodes := int(totalPods(vc))

	var nodesRaw string
	var lastErr error
	for _, seed := range pods {
		addr := fmt.Sprintf("%s:%d", seed.Status.PodIP, port)
		c, err := valkeyClient(addr, creds.username, creds.password)
		if err != nil {
			lastErr = err
			continue
		}

		tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
		raw, err := c.Do(tctx, c.B().ClusterNodes().Build()).ToString()
		cancel()
		c.Close()

		if err != nil {
			lastErr = err
			continue
		}

		// Validate gossip convergence: the seed must know all expected nodes.
		// A recently restarted pod may only know itself until gossip converges.
		knownNodes := 0
		for _, line := range strings.Split(raw, "\n") {
			if strings.TrimSpace(line) != "" {
				knownNodes++
			}
		}
		if knownNodes < expectedNodes {
			lastErr = fmt.Errorf("seed %s knows only %d/%d nodes — gossip not converged, trying next seed",
				seed.Name, knownNodes, expectedNodes)
			continue
		}

		nodesRaw = raw
		break
	}

	if nodesRaw == "" {
		if lastErr != nil {
			return lastErr
		}
		return fmt.Errorf("no seed returned a complete cluster view")
	}
	logger.Info("Role label sync: got cluster nodes view")

	// Build lookup maps for both IP and hostname to handle clusters with or
	// without cluster-announce-hostname configured.
	entries := parseNodeRoles(nodesRaw)
	ipToRole := make(map[string]string, len(entries))
	hostnameToRole := make(map[string]string, len(entries))
	for _, e := range entries {
		if e.ip != "" {
			ipToRole[e.ip] = e.role
		}
		if e.hostname != "" {
			hostnameToRole[e.hostname] = e.role
		}
	}

	svcName := internalServiceName(vc)
	var labelErrs []string
	for _, pod := range pods {
		role, ok := ipToRole[pod.Status.PodIP]
		if !ok && pod.Status.PodIP == "" {
			// Fall back to hostname match when IP is not in CLUSTER NODES
			// (e.g. cluster-announce-hostname configured without announce-ip).
			stableDNS := fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod.Name, svcName, pod.Namespace)
			role, ok = hostnameToRole[stableDNS]
		}
		if !ok {
			logger.Info("Role label sync: pod not found in CLUSTER NODES — skipping", "pod", pod.Name)
			continue
		}
		// Collect errors rather than short-circuit so all pods are labeled even if
		// one patch fails (transient API server error).
		if err := r.applyPodRoleLabel(ctx, pod, role); err != nil {
			logger.Info("Role label sync: failed to patch pod", "pod", pod.Name, "err", err)
			labelErrs = append(labelErrs, fmt.Sprintf("%s: %v", pod.Name, err))
			continue
		}
		logger.Info("Role label synced", "pod", pod.Name, "role", role)
	}
	if len(labelErrs) > 0 {
		return fmt.Errorf("role label sync partial failure: %s", strings.Join(labelErrs, "; "))
	}
	return nil
}

// nodeRoleEntry holds the role and both identifiers for a cluster node.
type nodeRoleEntry struct {
	role     string // rolePrimary or roleReplica
	ip       string
	hostname string // empty if cluster-announce-hostname not configured
}

// applyPodRoleLabel sets the valkey.io/role label on a pod.
func (r *ValkeyClusterReconciler) applyPodRoleLabel(ctx context.Context, pod *corev1.Pod, role string) error {
	if pod.Labels != nil && pod.Labels[roleLabelKey] == role {
		return nil
	}
	patchData := []byte(fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s"}}}`, roleLabelKey, role))
	return r.Client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, patchData))
}

// buildGossipPeersJSON returns a JSON array of "ip:port" strings for the given pods.
func buildGossipPeersJSON(pods []*corev1.Pod, port int32) string {
	s := "["
	for i, pod := range pods {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%q", fmt.Sprintf("%s:%d", pod.Status.PodIP, port))
	}
	s += "]"
	return s
}

// reconcileGossipRecovery pushes OPERATOR.TOPOLOGY.SET to every Running pod to
// force CLUSTER MEET with current IPs. This repairs gossip after a brutal restart
// where pod IPs changed and nodes.conf carries stale addresses.
//
// We use role="primary" for all pods so that topology.c only issues CLUSTER MEET
// and never CLUSTER REPLICATE — preserving the existing primary/replica assignments
// that are encoded in nodes.conf on each pod.
//
// We target Running pods (not just Ready) because the readiness probe fails
// precisely due to cluster_state:fail, which is what we are trying to fix.
func (r *ValkeyClusterReconciler) reconcileGossipRecovery(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, creds aclCredentials) error {
	logger := log.FromContext(ctx)

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return err
	}

	// Collect all Running pods with an IP — including non-Ready ones.
	var runningPods []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" && pod.DeletionTimestamp == nil {
			runningPods = append(runningPods, pod)
		}
	}

	if len(runningPods) == 0 {
		logger.Info("Gossip recovery: no running pods found — waiting")
		return nil
	}

	port := effectivePort(vc)

	// Build peer list with all current pod IPs and push to every running pod.
	// role=primary ensures topology.c issues only CLUSTER MEET, never CLUSTER REPLICATE.
	payload := fmt.Sprintf(`{"peers":%s,"role":"primary"}`, buildGossipPeersJSON(runningPods, port))

	successCount := 0
	for _, pod := range runningPods {
		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
		func() {
			c, err := valkeyClient(addr, creds.username, creds.password)
			if err != nil {
				logger.Info("Gossip recovery: cannot connect to pod — skipping", "pod", pod.Name, "err", err)
				return
			}
			defer c.Close()

			tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
			defer cancel()
			resp, err := c.Do(tctx, c.B().Arbitrary("OPERATOR.TOPOLOGY.SET").Args(payload).Build()).ToArray()

			if err != nil {
				logger.Info("Gossip recovery: OPERATOR.TOPOLOGY.SET failed", "pod", pod.Name, "err", err)
				return
			}
			if len(resp) >= 2 {
				ok, _ := resp[0].ToInt64()
				msg, _ := resp[1].ToString()
				logger.Info("Gossip recovery: OPERATOR.TOPOLOGY.SET issued",
					"pod", pod.Name, "ok", ok, "msg", msg)
				if ok == 1 {
					successCount++
				}
			}
		}()
	}

	logger.Info("Gossip recovery: topology push complete",
		"pods", len(runningPods), "success", successCount)

	// If no pod accepted the TOPOLOGY.SET, the cluster is not progressing.
	// Return an error so the caller knows the push did not help — the timeout
	// timer was already cleared by clearClusterNotOKAnnotation so the next
	// cycle will restart the timer and wait before retrying.
	if successCount == 0 {
		return fmt.Errorf("gossip recovery: no pod accepted OPERATOR.TOPOLOGY.SET (%d pods tried)", len(runningPods))
	}
	return nil
}
