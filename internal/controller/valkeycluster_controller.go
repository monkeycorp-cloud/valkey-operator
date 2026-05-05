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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

const (
	finalizerName   = "cache.valkey.io/finalizer"
	requeueInterval = 3 * time.Second
)

// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.valkey.io,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.valkey.io,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.valkey.io,resources=valkeyclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete

// ValkeyClusterReconciler reconciles a ValkeyCluster object.
type ValkeyClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// Reconcile is the main reconciliation loop.
func (r *ValkeyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	vc := &cachev1alpha1.ValkeyCluster{}
	if err := r.Get(ctx, req.NamespacedName, vc); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !vc.DeletionTimestamp.IsZero() {
		logger.Info("ValkeyCluster marked for deletion — running finalizer cleanup")
		return r.handleDeletion(ctx, vc)
	}

	if !controllerutil.ContainsFinalizer(vc, finalizerName) {
		logger.Info("Adding finalizer")
		controllerutil.AddFinalizer(vc, finalizerName)
		if err := r.Update(ctx, vc); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if vc.Status.Phase == "" {
		logger.Info("Initialising cluster phase to Pending")
		vc.Status.Phase = cachev1alpha1.PhasePending
		if err := r.Status().Update(ctx, vc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Capture status patch base after early returns and the initial phase
	// Update so the resourceVersion matches the server and the final Patch
	// does not conflict.
	statusPatch := client.MergeFrom(vc.DeepCopy())

	// Step 1: ConfigMap.
	if err := r.reconcileConfigMap(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile ConfigMap")
		r.Recorder.Eventf(vc, corev1.EventTypeWarning, "ConfigMapFailed", "Failed to reconcile ConfigMap: %v", err)
		return ctrl.Result{}, err
	}

	// Step 2a: Bootstrap ServiceAccount — dedicated SA with no RBAC for the bootstrap Job.
	if err := r.reconcileBootstrapServiceAccount(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile bootstrap ServiceAccount")
		return ctrl.Result{}, err
	}

	// Step 2b: PodDisruptionBudget.
	if err := r.reconcilePDB(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile PDB")
		// Non-fatal — PDB is a best-effort safety net.
	}

	// Step 2c: Internal headless Service.
	if err := r.reconcileInternalService(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile internal Service")
		return ctrl.Result{}, err
	}

	// Step 2d: Client headless Service.
	if err := r.reconcileClientService(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile client Service")
		return ctrl.Result{}, err
	}

	// Step 3: StatefulSet.
	if err := r.reconcileStatefulSet(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile StatefulSet")
		return ctrl.Result{}, err
	}

	// Step 4: Metrics Service + ServiceMonitor.
	if err := r.reconcileMetrics(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile metrics")
		// Non-fatal.
	}

	// Step 5: ACL users — applied to all Running pods (not just Ready) so that
	// a pod can pass its readiness probe which checks for an active app user.
	if err := r.reconcileACLs(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile ACLs")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Step 6: CrashLoop recovery.
	if err := r.reconcileCrashLoopRecovery(ctx, vc); err != nil {
		logger.Error(err, "Failed to reconcile crash loop recovery")
		// Non-fatal — recovery is best-effort.
	}

	// Step 7: Cluster topology — queries CLUSTER INFO to determine bootstrap state.
	isBootstrapped, err := r.reconcileClusterTopology(ctx, vc)
	if err != nil {
		logger.Error(err, "Failed to reconcile cluster topology")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	logger.Info("Cluster topology reconciled", "isBootstrapped", isBootstrapped)

	// Steps 8–11 require a fully formed cluster.
	if isBootstrapped {
		// Step 8: Cluster health — conditions, Events, stale node cleanup.
		// Safe during rolling updates: read-only observation + condition updates.
		// forgetStaleNodes has its own guards (DeletionTimestamp + revision check).
		if err := r.reconcileClusterHealth(ctx, vc); err != nil {
			logger.Error(err, "Failed to reconcile cluster health")
			// Non-fatal.
		}

		// Step 9: Replica role restoration — fix pods that lost their replica
		// role after a brutal restart (nodes.conf stale replication state).
		// Safe during rolling updates: only issues CLUSTER REPLICATE on pods
		// without slots that should be replicas. Combined with minReadySeconds=45,
		// this ensures new pods are reassigned as replicas before the next pod
		// is terminated by the rolling update.
		if err := r.reconcileReplicaRoles(ctx, vc); err != nil {
			logger.Error(err, "Failed to reconcile replica roles")
			// Non-fatal.
		}

		// Guard: defer mutation-heavy cluster ops when a rolling update or scale
		// is in progress. Health + replica repair above are safe to run always.
		stable, err := r.isStatefulSetStable(ctx, vc)
		if err != nil {
			logger.Error(err, "Failed to check StatefulSet stability")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if !stable {
			logger.Info("StatefulSet not stable — deferring cluster mutations")
			if err := r.updateStatus(ctx, vc, statusPatch); err != nil {
				logger.Error(err, "Failed to update status")
			}
			return ctrl.Result{RequeueAfter: requeueInterval}, nil
		}

		// Step 10: Node drain detection — triggers CLUSTER FAILOVER when a primary
		// pod's node is cordoned (unschedulable) before Kubernetes drains it.
		// Rolling-update failovers are handled by the pod's PreStop hook, which
		// is atomically guaranteed to run before SIGTERM — no operator intervention
		// needed (and intervening would create a double-failover race condition).
		// Checked only when StatefulSet is stable to avoid racing with a rolling update.
		if requeued, err := r.reconcileNodeDrainFailover(ctx, vc); err != nil {
			logger.Error(err, "Failed to run node-drain failover")
		} else if requeued {
			logger.Info("Node-drain failover initiated — requeuing")
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// Step 11: Topology status — pod→zone mapping.
		if err := r.reconcileTopologyStatus(ctx, vc); err != nil {
			logger.Error(err, "Failed to reconcile topology status")
			// Non-fatal.
		}

		// Step 12: Orphan repair — re-integrate pods that are running standalone.
		if err := r.reconcileOrphanReplicas(ctx, vc); err != nil {
			logger.Error(err, "Failed to reconcile orphan replicas")
			// Non-fatal.
		}

		// Step 13: Shard stats — detect memory imbalance across shards.
		if err := r.reconcileShardStats(ctx, vc); err != nil {
			logger.Error(err, "Failed to reconcile shard stats")
			// Non-fatal.
		}

		// Step 14: Rebalance — create a valkey-cli rebalance Job when imbalanced.
		if vc.Spec.Rebalance != nil && vc.Spec.Rebalance.Enabled {
			if err := r.reconcileRebalance(ctx, vc); err != nil {
				logger.Error(err, "Failed to reconcile rebalance")
				// Non-fatal.
			}
		}
	}

	// Step 12: Update status.
	if err := r.updateStatus(ctx, vc, statusPatch); err != nil {
		logger.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// handleDeletion processes cleanup when a ValkeyCluster is being deleted.
func (r *ValkeyClusterReconciler) handleDeletion(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(vc, finalizerName) {
		return ctrl.Result{}, nil
	}

	// Owned resources are cleaned up via OwnerReferences GC.
	controllerutil.RemoveFinalizer(vc, finalizerName)
	if err := r.Update(ctx, vc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// updateStatus syncs the ValkeyCluster status from observed StatefulSet and cluster state.
// patch must be created from a DeepCopy taken before any in-memory status mutations
// so that all changes (including health conditions set earlier in the reconcile) are included.
// isStatefulSetStable returns true when the StatefulSet has no rolling update
// in progress and all expected replicas are ready. Any instability means cluster
// commands (CLUSTER MEET, slot ops, role sync) should be deferred to avoid
// operating on a partially available cluster.
func (r *ValkeyClusterReconciler) isStatefulSetStable(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKey{Name: statefulSetName(vc), Namespace: vc.Namespace}, sts); err != nil {
		return false, client.IgnoreNotFound(err)
	}

	expected := totalPods(vc)

	// Rolling update in progress.
	if sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		return false, nil
	}

	// Scale in progress — desired vs observed replicas mismatch.
	if sts.Status.Replicas != expected {
		return false, nil
	}

	// Not all pods ready.
	if sts.Status.ReadyReplicas != expected {
		return false, nil
	}

	// Pod with terminating state (eviction, manual delete).
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return false, err
	}
	for i := range podList.Items {
		if podList.Items[i].DeletionTimestamp != nil {
			return false, nil
		}
	}

	return true, nil
}

func (r *ValkeyClusterReconciler) updateStatus(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, patch client.Patch) error {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKey{Name: statefulSetName(vc), Namespace: vc.Namespace}, sts); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	vc.Status.ReadyReplicas = sts.Status.ReadyReplicas
	vc.Status.ObservedGeneration = vc.Generation

	total := totalPods(vc)
	readyReplicas := sts.Status.ReadyReplicas

	switch {
	case readyReplicas == 0:
		vc.Status.Phase = cachev1alpha1.PhaseInitializing
	case readyReplicas < total:
		if vc.Status.Phase != cachev1alpha1.PhaseInitializing {
			vc.Status.Phase = cachev1alpha1.PhaseDegraded
		}
		setCondition(vc, metav1.Condition{
			Type:               "Available",
			Status:             metav1.ConditionFalse,
			Reason:             "PartiallyReady",
			Message:            fmt.Sprintf("%d/%d pods ready", readyReplicas, total),
			ObservedGeneration: vc.Generation,
		})
	case readyReplicas == total:
		vc.Status.Phase = cachev1alpha1.PhaseRunning
		setCondition(vc, metav1.Condition{
			Type:               "Available",
			Status:             metav1.ConditionTrue,
			Reason:             "ClusterReady",
			Message:            fmt.Sprintf("All %d pods ready", total),
			ObservedGeneration: vc.Generation,
		})
	}

	if *sts.Spec.Replicas != total {
		vc.Status.Phase = cachev1alpha1.PhaseScaling
	}

	// Rolling update detection — overrides other phases.
	if sts.Status.CurrentRevision != sts.Status.UpdateRevision {
		vc.Status.Phase = cachev1alpha1.PhaseUpdating
		setCondition(vc, metav1.Condition{
			Type:   "RollingUpdate",
			Status: metav1.ConditionTrue,
			Reason: "UpdateInProgress",
			Message: fmt.Sprintf("rolling update in progress: %d/%d pods updated",
				sts.Status.UpdatedReplicas, total),
			ObservedGeneration: vc.Generation,
		})
	} else {
		setCondition(vc, metav1.Condition{
			Type:               "RollingUpdate",
			Status:             metav1.ConditionFalse,
			Reason:             "UpdateComplete",
			Message:            fmt.Sprintf("all %d pods on current revision", total),
			ObservedGeneration: vc.Generation,
		})
	}

	// Refresh cluster state from any ready pod.
	r.refreshClusterStatus(ctx, vc)

	return r.Status().Patch(ctx, vc, patch)
}

// refreshClusterStatus queries CLUSTER INFO and CLUSTER NODES to populate
// ClusterState, SlotsOk, NodesOk, and PodRoles in the status.
func (r *ValkeyClusterReconciler) refreshClusterStatus(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) {
	logger := log.FromContext(ctx)

	operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return
	}
	creds := aclCredentials{username: operatorUsername, password: operatorPassword}

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return
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

		tctx, cancel := context.WithTimeout(ctx, 3*time.Second)

		info, err := c.Do(tctx, c.B().ClusterInfo().Build()).ToString()
		if err != nil {
			cancel()
			c.Close()
			continue
		}

		nodesRaw, err := c.Do(tctx, c.B().ClusterNodes().Build()).ToString()
		cancel()
		c.Close()
		if err != nil {
			continue
		}

		vc.Status.ClusterState = parseClusterInfoField(info, "cluster_state")
		vc.Status.SlotsOk = int32(parseClusterInfoInt(info, "cluster_slots_ok"))
		vc.Status.NodesOk = int32(parseClusterInfoInt(info, "cluster_known_nodes"))

		// Sync PodRoles map — match by IP first, hostname as fallback.
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
		podRoles := make(map[string]string, len(podList.Items))
		for j := range podList.Items {
			p := &podList.Items[j]
			if role, ok := ipToRole[p.Status.PodIP]; ok {
				podRoles[p.Name] = role
				continue
			}
			stableDNS := fmt.Sprintf("%s.%s.%s.svc.cluster.local", p.Name, svcName, p.Namespace)
			if role, ok := hostnameToRole[stableDNS]; ok {
				podRoles[p.Name] = role
			}
		}
		vc.Status.PodRoles = podRoles

		logger.Info("Cluster status refreshed",
			"state", vc.Status.ClusterState,
			"slotsOk", vc.Status.SlotsOk,
			"nodesOk", vc.Status.NodesOk,
		)
		return
	}
}

// setCondition adds or updates a condition in the ValkeyCluster status.
func setCondition(vc *cachev1alpha1.ValkeyCluster, cond metav1.Condition) {
	cond.LastTransitionTime = metav1.Now()
	for i, existing := range vc.Status.Conditions {
		if existing.Type == cond.Type {
			if existing.Status == cond.Status {
				cond.LastTransitionTime = existing.LastTransitionTime
			}
			vc.Status.Conditions[i] = cond
			return
		}
	}
	vc.Status.Conditions = append(vc.Status.Conditions, cond)
}

// startupEnqueuer is a LeaderElectionRunnable that enqueues all existing
// ValkeyCluster objects into the controller work queue once the leader lease
// is acquired. This guarantees a reconcile fires on startup even when no
// Kubernetes events have occurred since the operator last ran.
type startupEnqueuer struct {
	mgr ctrl.Manager
	ch  chan event.GenericEvent
}

func (s *startupEnqueuer) NeedLeaderElection() bool { return true }

func (s *startupEnqueuer) Start(ctx context.Context) error {
	// Cache is guaranteed to be synced before leader-election runnables start.
	vcList := &cachev1alpha1.ValkeyClusterList{}
	if err := s.mgr.GetClient().List(ctx, vcList); err != nil {
		return err
	}
	for i := range vcList.Items {
		s.ch <- event.GenericEvent{Object: &vcList.Items[i]}
	}
	return nil
}

// SetupWithManager registers the controller with the manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// startupChannel enqueues all existing ValkeyCluster objects once the leader
	// is elected and workers are running. Without this, after a leader election
	// the first reconcile never fires if nothing has changed since the restart —
	// because the informer emits the initial "Added" events before the workers
	// start, and they are lost.
	startupChannel := make(chan event.GenericEvent, 1024)

	enqueueVC := handler.EnqueueRequestsFromMapFunc(func(_ context.Context, obj client.Object) []reconcile.Request {
		return []reconcile.Request{
			{NamespacedName: client.ObjectKeyFromObject(obj)},
		}
	})

	// startupEnqueuer implements LeaderElectionRunnable so it starts only after
	// the leader lease is acquired — at which point the controller workers are
	// already running and will drain the channel.
	if err := mgr.Add(&startupEnqueuer{mgr: mgr, ch: startupChannel}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.ValkeyCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&batchv1.Job{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToValkeyCluster)).
		WatchesRawSource(source.Channel(startupChannel, enqueueVC)).
		Complete(r)
}

// mapSecretToValkeyCluster retourne les reconcile.Request des ValkeyCluster
// qui référencent le Secret modifié dans operatorSecret, aclUsers[*].passwordSecret
// ou metrics.metricsSecret.
func (r *ValkeyClusterReconciler) mapSecretToValkeyCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	secretName := obj.GetName()

	vcList := &cachev1alpha1.ValkeyClusterList{}
	if err := r.Client.List(ctx, vcList, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for i := range vcList.Items {
		if referencesSecret(&vcList.Items[i], secretName) {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&vcList.Items[i]),
			})
		}
	}
	return requests
}

// referencesSecret returns true if the ValkeyCluster references the given secret
// name in operatorSecret, any aclUsers[*].passwordSecret, or metrics.metricsSecret.
func referencesSecret(vc *cachev1alpha1.ValkeyCluster, secretName string) bool {
	if vc.Spec.OperatorSecret.Name == secretName {
		return true
	}
	for _, user := range vc.Spec.ACLUsers {
		if user.PasswordSecret.Name == secretName {
			return true
		}
	}
	if vc.Spec.Metrics != nil && vc.Spec.Metrics.MetricsSecret.Name == secretName {
		return true
	}
	return false
}
