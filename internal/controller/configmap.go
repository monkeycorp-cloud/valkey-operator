package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

func configMapName(vc *cachev1alpha1.ValkeyCluster) string {
	return fmt.Sprintf("%s-config", vc.Name)
}

func (r *ValkeyClusterReconciler) reconcileConfigMap(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	// Resolve operator password so it can be baked into valkey.conf.
	// This ensures Valkey starts with the operator account already configured —
	// no unauthenticated window during bootstrap.
	operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return fmt.Errorf("resolving operator secret: %w", err)
	}
	if err := validatePassword(operatorPassword); err != nil {
		return fmt.Errorf("invalid operator password: %w", err)
	}

	// Resolve all ACL users so they are baked into valkey.conf at startup.
	// Without this, a restarted pod joins cluster gossip and peers send MOVED
	// redirections to it before reconcileACLs runs — clients receive
	// "invalid username-password pair" during that window.
	appUsers, err := r.resolveACLUsers(ctx, vc)
	if err != nil {
		return fmt.Errorf("resolving ACL users: %w", err)
	}

	var metricsPassword string
	if vc.Spec.Metrics != nil && vc.Spec.Metrics.Enabled {
		metricsPassword, err = r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.Metrics.MetricsSecret)
		if err != nil {
			return fmt.Errorf("resolving metrics secret: %w", err)
		}
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName(vc),
			Namespace: vc.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if err := controllerutil.SetControllerReference(vc, cm, r.Scheme); err != nil {
			return err
		}
		cm.Labels = commonLabels(vc)
		cm.Data = map[string]string{
			"valkey.conf": buildValkeyConfig(&vc.Spec, operatorPassword, metricsPassword, appUsers),
		}
		return nil
	})
	return err
}

// validatePassword rejects passwords that would break valkey.conf syntax or allow
// directive injection. Passwords containing control characters (newlines, carriage
// returns) or that are empty would corrupt the config file.
func validatePassword(password string) error {
	if strings.ContainsAny(password, "\n\r") {
		return fmt.Errorf("operator password contains control characters (newline/carriage return)")
	}
	return nil
}

func buildValkeyConfig(spec *cachev1alpha1.ValkeyClusterSpec, operatorPassword, metricsPassword string, appUsers []resolvedACLUser) string {
	port := spec.Port
	if port == 0 {
		port = 6379
	}

	clusterNodeTimeout := spec.ClusterNodeTimeout
	if clusterNodeTimeout == 0 {
		clusterNodeTimeout = 5000
	}

	var b strings.Builder

	// Operator module — always loaded from the custom Valkey image.
	// Built from server/Dockerfile which embeds the .so into the official Valkey image.
	fmt.Fprintf(&b, "loadmodule /usr/local/lib/valkey-operator-module.so\n\n")

	// Base configuration.
	fmt.Fprintf(&b, "bind 0.0.0.0\n")
	fmt.Fprintf(&b, "port %d\n", port)
	fmt.Fprintf(&b, "protected-mode no\n")
	fmt.Fprintf(&b, "loglevel notice\n")
	fmt.Fprintf(&b, "dir /data\n")

	// Cluster.
	fmt.Fprintf(&b, "\n# Cluster\n")
	fmt.Fprintf(&b, "cluster-enabled yes\n")
	fmt.Fprintf(&b, "cluster-config-file /data/nodes.conf\n")
	fmt.Fprintf(&b, "cluster-node-timeout %d\n", clusterNodeTimeout)
	// cluster-announce-hostname/ip are placeholders substituted at every pod startup
	// by the init container from the current downward API values.
	// This ensures valkey.conf on the PVC always reflects the current pod identity
	// even after pod recreation (new IP) or rolling update.
	//
	// cluster-preferred-endpoint-type ip: clients receive the current IP in MOVED/ASK
	// responses; cluster gossip uses the hostname for stable peer addressing.
	// cluster-announce-hostname and cluster-announce-ip are NOT set here —
	// they are passed as CLI arguments at container startup using downward API
	// env vars, which avoids the need for an init container to render the config.
	fmt.Fprintf(&b, "cluster-announce-port %d\n", port)
	fmt.Fprintf(&b, "cluster-announce-bus-port %d\n", port+10000)
	fmt.Fprintf(&b, "cluster-preferred-endpoint-type ip\n")
	// Allow read commands even when cluster_state=fail (e.g. during failover or
	// node loss). Serving potentially stale reads is preferable to returning
	// CLUSTERDOWN errors to PHP clients — the cluster recovers within seconds.
	fmt.Fprintf(&b, "cluster-allow-reads-when-down yes\n")

	// Persistence — AOF is disabled in cluster mode: replication between nodes
	// already provides durability. A crashed node rejoins and resyncs from its
	// primary. RDB snapshots are kept for fast node restarts.
	fmt.Fprintf(&b, "\n# Persistence\n")
	fmt.Fprintf(&b, "appendonly no\n")
	fmt.Fprintf(&b, "save 3600 1\n")
	fmt.Fprintf(&b, "save 300 100\n")
	fmt.Fprintf(&b, "save 60 10000\n")

	// ACL — operator account baked in so Valkey starts authenticated from the first second.
	// The default account is disabled to prevent unauthenticated access during bootstrap.
	// masterauth uses the operator password so replicas can authenticate to their primary
	// for RDB sync. reconcileACLs will keep these in sync on every reconcile cycle.
	fmt.Fprintf(&b, "\n# ACL\n")
	fmt.Fprintf(&b, "acllog-max-len 128\n")
	if operatorPassword != "" {
		fmt.Fprintf(&b, "masterauth %s\n", operatorPassword)
		fmt.Fprintf(&b, "masteruser operator\n")
		fmt.Fprintf(&b, "user default off\n")
		fmt.Fprintf(&b, "user operator on >%s ~* &* +@all\n", operatorPassword)
	}

	// Metrics user — baked in so redis_exporter can connect immediately.
	if metricsPassword != "" {
		fmt.Fprintf(&b, "user %s on >%s %s\n",
			metricsUsername, metricsPassword, strings.Join(metricsACLRules, " "))
	}

	// Application ACL users — baked in so clients can authenticate immediately
	// after a pod restart, before reconcileACLs runs. This eliminates the
	// "invalid username-password pair" window when peers send MOVED redirections
	// to a pod that has rejoined gossip but not yet received ACLs.
	for _, u := range appUsers {
		rules := buildACLRules(u)
		fmt.Fprintf(&b, "user %s on %s\n", u.name, strings.Join(rules, " "))
	}

	// Structured config parameters.
	if spec.Config != nil {
		cfg := spec.Config
		fmt.Fprintf(&b, "\n# Structured configuration\n")

		// maxmemory — calculated from pod memory limit * ratio.
		// Default ratio is 80% if not specified in spec, but the caller
		// (buildValkeyConfig) uses the ratio field which defaults to 80.
		maxmemoryBytes := computeMaxmemory(spec)
		if maxmemoryBytes > 0 {
			fmt.Fprintf(&b, "maxmemory %d\n", maxmemoryBytes)
		}

		policy := cfg.MaxmemoryPolicy
		if policy == "" {
			policy = "allkeys-lru"
		}
		fmt.Fprintf(&b, "maxmemory-policy %s\n", policy)

		hz := cfg.Hz
		if hz == 0 {
			hz = 10
		}
		fmt.Fprintf(&b, "hz %d\n", hz)

		if cfg.Lazyfree {
			fmt.Fprintf(&b, "lazyfree-lazy-eviction yes\n")
			fmt.Fprintf(&b, "lazyfree-lazy-expire yes\n")
			fmt.Fprintf(&b, "lazyfree-lazy-server-del yes\n")
			fmt.Fprintf(&b, "lazyfree-lazy-user-del yes\n")
		}

		tcpKeepalive := cfg.TCPKeepalive
		if tcpKeepalive == 0 {
			tcpKeepalive = 300
		}
		fmt.Fprintf(&b, "tcp-keepalive %d\n", tcpKeepalive)

		ioThreads := computeIOThreads(spec)
		if ioThreads > 1 {
			fmt.Fprintf(&b, "io-threads %d\n", ioThreads)
			fmt.Fprintf(&b, "io-threads-do-reads yes\n")
		}
	}

	// CustomConfig is appended last so the user can override anything.
	if spec.CustomConfig != "" {
		fmt.Fprintf(&b, "\n# Custom configuration\n")
		fmt.Fprintf(&b, "%s\n", spec.CustomConfig)
	}

	return b.String()
}

// computeIOThreads returns the io-threads value to use.
// If IOThreads is explicitly set in spec, that value is used directly.
// Otherwise it is derived from resources.limits.cpu:
//
//	< 4 CPUs → 1 (disabled), 4 → 2, 8 → 4, 16+ → 8 (cap).
func computeIOThreads(spec *cachev1alpha1.ValkeyClusterSpec) int32 {
	if spec.Config == nil {
		return 1
	}
	if spec.Config.IOThreads != nil {
		return *spec.Config.IOThreads
	}
	cpuLimit, ok := spec.Resources.Limits[corev1.ResourceCPU]
	if !ok || cpuLimit.IsZero() {
		return 1
	}
	// MilliValue() returns cores * 1000.
	cpus := int32(cpuLimit.MilliValue() / 1000)
	switch {
	case cpus >= 16:
		return 8
	case cpus >= 8:
		return 4
	case cpus >= 4:
		return 2
	default:
		return 1
	}
}

// computeMaxmemory returns the maxmemory value in bytes derived from the pod
// memory limit and the configured ratio. Returns 0 if no limit is set.
func computeMaxmemory(spec *cachev1alpha1.ValkeyClusterSpec) int64 {
	if spec.Config == nil {
		return 0
	}

	limitQty, ok := spec.Resources.Limits[corev1.ResourceMemory]
	if !ok || limitQty.IsZero() {
		return 0
	}

	ratio := spec.Config.MaxmemoryRatio
	if ratio <= 0 || ratio >= 100 {
		ratio = 80
	}

	limitBytes := limitQty.Value()
	return limitBytes * int64(ratio) / 100
}
