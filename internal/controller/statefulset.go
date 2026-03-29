package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

func statefulSetName(vc *cachev1alpha1.ValkeyCluster) string {
	return vc.Name
}

func (r *ValkeyClusterReconciler) reconcileStatefulSet(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	port := effectivePort(vc)
	busPort := clusterBusPort(vc)
	replicas := totalPods(vc)

	configHash := configMapHash(&vc.Spec)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName(vc),
			Namespace: vc.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		if err := controllerutil.SetControllerReference(vc, sts, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(vc)
		sts.Labels = labels

		sts.Spec.Replicas = &replicas
		sts.Spec.ServiceName = internalServiceName(vc)
		// Parallel: all pods start simultaneously without waiting for predecessors
		// to be Ready. Required for cluster mode where pods are interdependent —
		// OrderedReady would deadlock when restarting with existing PVCs since
		// pod-0 (replica) cannot reach its primary until pod-N is also running.
		sts.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		// maxUnavailable=1: at most one pod is restarted at a time during rolling updates.
		// Prevents a buggy reconcile loop from restarting all pods simultaneously,
		// which would cause a full cluster outage.
		maxUnavailable := intstr.FromInt32(1)
		sts.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
				MaxUnavailable: &maxUnavailable,
			},
		}

		// MinReadySeconds: a pod must be Ready for 10s before the rolling update
		// kills the next pod. This gives the operator time to reassign the new pod
		// as a replica via reconcileReplicaRoles before its master is terminated.
		sts.Spec.MinReadySeconds = 10

		sts.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"app.kubernetes.io/name":     "valkeycluster",
				"app.kubernetes.io/instance": vc.Name,
			},
		}

		podLabels := make(map[string]string)
		for k, v := range labels {
			podLabels[k] = v
		}

		image := vc.Spec.Image

		svcName := internalServiceName(vc)

		env := []corev1.EnvVar{
			{
				Name: "POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			},
			{
				Name: "POD_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				},
			},
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
				},
			},
			{
				Name: "VALKEY_OPERATOR_PASSWORD",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &vc.Spec.OperatorSecret,
				},
			},
		}

		// Liveness: simple PING — just check the process is alive.
		livenessCmd := []string{
			"sh", "-c",
			fmt.Sprintf(
				"valkey-cli -p %d --no-auth-warning --user operator --pass \"$VALKEY_OPERATOR_PASSWORD\" PING | grep -q PONG",
				port,
			),
		}

		// Readiness: OPERATOR.NODE.READY — single module call replacing the
		// multi-step shell script. Returns [1, "ready"] only when the node is
		// fully operational: role known, gossip converged, slots healthy,
		// replication link up. Kubernetes advances the rolling update to the
		// next pod only once this returns 1, making minReadySeconds redundant.
		// Capture output into a variable to avoid SIGPIPE from the head+grep pipe,
		// which caused spurious "context canceled" probe errors with FailureThreshold:1.
		readinessScript := fmt.Sprintf(
			`r=$(valkey-cli -p %d --no-auth-warning --user operator --pass "$VALKEY_OPERATOR_PASSWORD" `+
				`OPERATOR.NODE.READY 2>/dev/null); [ "$(echo "$r" | head -1)" = "1" ]`,
			port,
		)

		livenessProbe := &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: livenessCmd},
			},
			InitialDelaySeconds: 30,
			PeriodSeconds:       10,
			TimeoutSeconds:      5,
			FailureThreshold:    3,
		}
		readinessProbe := &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{"sh", "-c", readinessScript},
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
			TimeoutSeconds:      5,
			// Single failure immediately marks Not-Ready and removes the endpoint.
			// This minimises the window where a pod that lost its in-memory ACLs
			// (after a restart/failover) is still receiving client traffic.
			FailureThreshold: 1,
			// Single success is enough: the gate.c auth callback and
			// OPERATOR.NODE.READY already enforce all readiness invariants
			// (ACLs applied, gossip converged, slots healthy). A higher threshold
			// would leave the pod visible in cluster gossip but absent from
			// Kubernetes endpoints — exactly the race condition we want to avoid.
			SuccessThreshold: 1,
		}

		resources := vc.Spec.Resources
		if resources.Requests == nil {
			resources.Requests = corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("256Mi"),
				corev1.ResourceCPU:    resource.MustParse("100m"),
			}
		}

		gracePeriod := vc.Spec.TerminationGracePeriodSeconds
		if gracePeriod == 0 {
			gracePeriod = 60
		}

		// PreStop hook — graceful failover before pod termination.
		//
		// Budget allocation within the grace period (default 60s):
		//   - 5s   Kubernetes overhead (signal delivery, cgroup teardown)
		//   - 50s  OPERATOR.CLUSTER.SAFE polling (until replica is connected)
		//   - 5s   OPERATOR.FAILOVER.PREPARE timeout (CLIENT PAUSE + role swap wait)
		//
		// OPERATOR.FAILOVER.PREPARE uses ValkeyModule_BlockClient — the command
		// returns immediately so the event loop stays free during the role swap.
		// When ReplicationRoleChanged fires (~1-5ms), the blocked client is unblocked
		// and reply_fn issues WAIT 1 500 + CLIENT UNPAUSE before responding.
		clusterSafeMaxWait := int64(50)
		timeoutMs := (gracePeriod - 5 - clusterSafeMaxWait) * 1000
		if timeoutMs < 1000 {
			timeoutMs = 1000
		}
		preStopScript := fmt.Sprintf(`
PORT=%d
SAFE_MAX_WAIT=%d
TIMEOUT_MS=%d
CLI="valkey-cli -p $PORT --no-auth-warning --user operator --pass $VALKEY_OPERATOR_PASSWORD"

# Phase 1: Wait until the cluster can safely absorb the loss of this node.
# OPERATOR.CLUSTER.SAFE returns 1 when: cluster_state=ok AND connected_slaves>=1.
for i in $(seq 1 $SAFE_MAX_WAIT); do
  result=$($CLI OPERATOR.CLUSTER.SAFE 2>/dev/null)
  echo "$result" | grep -q "^1" && break
  echo "PreStop: not safe yet ($result) — waiting 1s"
  sleep 1
done

# Phase 2: Check local role — replicas have nothing to hand off.
ROLE=$($CLI INFO replication 2>/dev/null | grep "^role:" | cut -d: -f2 | tr -d '\r')
echo "PreStop: role=$ROLE"
if [ "$ROLE" != "master" ]; then
  echo "PreStop: replica — no failover needed"
  exit 0
fi

# Phase 3: Find a connected replica of this primary from CLUSTER NODES.
MY_ID=$($CLI CLUSTER MYID 2>/dev/null | tr -d '\r')
REPLICA_ADDR=$($CLI CLUSTER NODES 2>/dev/null \
  | grep "slave $MY_ID" \
  | grep -v "fail\|noaddr" \
  | head -1 \
  | awk '{print $2}' \
  | cut -d'@' -f1)

if [ -z "$REPLICA_ADDR" ]; then
  echo "PreStop: no replica found — cannot failover"
  exit 0
fi

# Extract IP and port. rev+cut splits on the LAST colon, supporting IPv6.
REPLICA_IP=$(echo "$REPLICA_ADDR" | rev | cut -d: -f2- | rev)
REPLICA_PORT=$(echo "$REPLICA_ADDR" | rev | cut -d: -f1 | rev)
echo "PreStop: issuing CLUSTER FAILOVER to replica $REPLICA_IP:$REPLICA_PORT"

# Phase 4: Send CLUSTER FAILOVER to the replica.
# Returns immediately — just ACKs that the replica received the request.
# The cooperative handshake takes ~1-5ms, so CLIENT PAUSE WRITE (phase 5)
# will be active well before the role swap completes. No race.
valkey-cli -h "$REPLICA_IP" -p "$REPLICA_PORT" \
  --no-auth-warning --user operator --pass "$VALKEY_OPERATOR_PASSWORD" \
  CLUSTER FAILOVER 2>/dev/null || true

# Phase 5: CLIENT PAUSE WRITE + WAIT 1 500 + UNPAUSE — atomic in the module.
$CLI OPERATOR.FAILOVER.PREPARE $TIMEOUT_MS

# Phase 6: Wait until this node has transitioned to role:slave.
# nodes.conf is saved with role=slave on exit — ensures a clean restart as replica.
# The new primary sends SLAVEOF to this node via gossip after the failover (~100-500ms).
for i in $(seq 1 50); do
  ROLE=$($CLI INFO replication 2>/dev/null | grep "^role:" | cut -d: -f2 | tr -d '\r')
  [ "$ROLE" = "slave" ] && echo "PreStop: now replica — exiting" && exit 0
  sleep 0.1
done
echo "PreStop: done (role swap not confirmed — nodes.conf may restart as master)"
`, port, clusterSafeMaxWait, timeoutMs)

		annotations := map[string]string{
			"valkey.io/config-hash":   configHash,
			"valkey.io/pod-spec-hash": podSpecHash(&vc.Spec),
		}

		// cluster-announce-hostname and cluster-announce-ip are passed as CLI
		// arguments so Valkey picks up the current pod identity at every start
		// without needing an init container to render the config file.
		// Kubernetes does NOT substitute $(VAR) in command/args — only in env.value.
		// We use a shell wrapper so that $POD_NAME, $POD_NAMESPACE and $POD_IP
		// (already resolved by the container runtime into the process environment)
		// are expanded by the shell before exec-ing valkey-server.
		valkeyCmd := []string{
			"sh", "-c",
			fmt.Sprintf(
				`exec valkey-server /etc/valkey/valkey.conf \
  --cluster-announce-hostname "$POD_NAME.%s.$POD_NAMESPACE.svc.cluster.local" \
  --cluster-announce-ip "$POD_IP"`,
				svcName,
			),
		}

		containers := []corev1.Container{
			{
				Name:            "valkey",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         valkeyCmd,
				Ports: []corev1.ContainerPort{
					{
						Name:          "valkey",
						ContainerPort: port,
						Protocol:      corev1.ProtocolTCP,
					},
					{
						Name:          "cluster-bus",
						ContainerPort: busPort,
						Protocol:      corev1.ProtocolTCP,
					},
				},
				Env:            env,
				Resources:      resources,
				LivenessProbe:  livenessProbe,
				ReadinessProbe: readinessProbe,
				Lifecycle: &corev1.Lifecycle{
					PreStop: &corev1.LifecycleHandler{
						Exec: &corev1.ExecAction{
							Command: []string{"sh", "-c", preStopScript},
						},
					},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "valkey-data", MountPath: "/data"},
					{Name: "config", MountPath: "/etc/valkey"},
				},
			},
		}

		// Inject the redis_exporter sidecar when metrics are enabled.
		if vc.Spec.Metrics != nil && vc.Spec.Metrics.Enabled {
			exporterImage := vc.Spec.Metrics.Image
			if exporterImage == "" {
				exporterImage = "oliver006/redis_exporter:latest"
			}
			sidecar := corev1.Container{
				Name:            "redis-exporter",
				Image:           exporterImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports: []corev1.ContainerPort{
					{
						Name:          "metrics",
						ContainerPort: 9121,
						Protocol:      corev1.ProtocolTCP,
					},
				},
				Env: []corev1.EnvVar{
					{
						Name:  "REDIS_ADDR",
						Value: fmt.Sprintf("redis://localhost:%d", port),
					},
					{
						Name:  "REDIS_USER",
						Value: metricsUsername,
					},
					{
						Name: "REDIS_PASSWORD",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &vc.Spec.Metrics.MetricsSecret,
						},
					},
				},
			}
			containers = append(containers, sidecar)

			// Prometheus autodiscovery annotations.
			annotations["prometheus.io/scrape"] = "true"
			annotations["prometheus.io/port"] = "9121"
			annotations["prometheus.io/path"] = "/metrics"
		}

		valkeyUID := int64(999)
		valkeyGID := int64(999)
		trueVal := true
		falseVal := false

		podSecurityContext := &corev1.PodSecurityContext{
			RunAsUser:    &valkeyUID,
			RunAsGroup:   &valkeyGID,
			FSGroup:      &valkeyGID,
			RunAsNonRoot: &trueVal,
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		}

		containerSecurityContext := &corev1.SecurityContext{
			RunAsUser:                &valkeyUID,
			RunAsGroup:               &valkeyGID,
			RunAsNonRoot:             &trueVal,
			AllowPrivilegeEscalation: &falseVal,
			ReadOnlyRootFilesystem:   &falseVal, // Valkey writes to /data and /tmp
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
		}
		containers[0].SecurityContext = containerSecurityContext

		sts.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: &gracePeriod,
				SecurityContext:               podSecurityContext,
				Affinity:                      buildPodAffinity(vc),
				TopologySpreadConstraints:     buildTopologySpreadConstraints(vc),
				Containers:                    containers,
				Volumes: []corev1.Volume{
					{
						Name: "config",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: configMapName(vc),
								},
							},
						},
					},
				},
			},
		}

		storageSize := vc.Spec.Storage.Size
		if storageSize.IsZero() {
			storageSize = resource.MustParse("10Gi")
		}

		pvcLabels := commonLabels(vc)
		pvc := corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "valkey-data",
				Labels: pvcLabels,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: storageSize,
					},
				},
				StorageClassName: vc.Spec.Storage.StorageClassName,
			},
		}

		// VolumeClaimTemplates are immutable after creation.
		if len(sts.Spec.VolumeClaimTemplates) == 0 {
			sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}
		}

		return nil
	})
	return err
}

// configMapHash computes a short hash of the Valkey config for change detection.
// Passwords are excluded — they are managed at runtime via reconcileACLs and a
// password change does not require a pod restart (the ConfigMap is updated with
// the new passwords for cold-start protection, but live pods are updated in-memory).
func configMapHash(spec *cachev1alpha1.ValkeyClusterSpec) string {
	config := buildValkeyConfig(spec, "", "", nil)
	h := sha256.Sum256([]byte(config))
	return fmt.Sprintf("%x", h[:16])
}

// podSpecHash computes a hash of the pod spec fields that are not covered by
// configMapHash: image, resources, probes, sidecar, gracePeriod, metrics.
// Stored as an annotation on the pod template so that any change triggers a
// StatefulSet rolling update automatically.
func podSpecHash(spec *cachev1alpha1.ValkeyClusterSpec) string {
	image := spec.Image
	gracePeriod := spec.TerminationGracePeriodSeconds
	if gracePeriod == 0 {
		gracePeriod = 60
	}
	port := spec.Port
	if port == 0 {
		port = 6379
	}

	var b strings.Builder
	fmt.Fprintf(&b, "image=%s\n", image)
	fmt.Fprintf(&b, "grace=%d\n", gracePeriod)
	fmt.Fprintf(&b, "port=%d\n", port)
	if spec.Resources.Limits != nil {
		fmt.Fprintf(&b, "limits=%v\n", spec.Resources.Limits)
	}
	if spec.Resources.Requests != nil {
		fmt.Fprintf(&b, "requests=%v\n", spec.Resources.Requests)
	}

	if spec.Metrics != nil {
		fmt.Fprintf(&b, "metricsEnabled=%v\n", spec.Metrics.Enabled)
		fmt.Fprintf(&b, "metricsImage=%s\n", spec.Metrics.Image)
	}

	h := sha256.Sum256([]byte(b.String()))
	return fmt.Sprintf("%x", h[:16])
}
