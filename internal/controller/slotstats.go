package controller

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

const (
	shardImbalancedAnnotation  = "valkey.io/shard-imbalanced-since"
	shardImbalancedGracePeriod = 60 * time.Second
	defaultImbalanceThreshold  = int32(20)
)

// shardStat holds the stats returned by OPERATOR.SLOT.STATS for one primary.
type shardStat struct {
	podName     string
	memoryBytes int64
	keys        int64
}

// reconcileShardStats calls OPERATOR.SLOT.STATS on each primary pod, computes
// the memory skew across shards, and raises or clears the ShardImbalanced condition.
func (r *ValkeyClusterReconciler) reconcileShardStats(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	creds, err := r.operatorCreds(ctx, vc)
	if err != nil {
		return fmt.Errorf("resolving operator creds: %w", err)
	}

	// Collect ready primary pods from PodRoles status.
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return err
	}

	var primaryPods []*corev1.Pod
	for i := range podList.Items {
		pod := &podList.Items[i]
		if !isPodReady(pod) || pod.Status.PodIP == "" {
			continue
		}
		role := vc.Status.PodRoles[pod.Name]
		if role == rolePrimary {
			primaryPods = append(primaryPods, pod)
		}
	}

	if len(primaryPods) < 2 {
		// Not enough primaries to compare — clear the condition if set.
		clearShardImbalanced(vc)
		return nil
	}

	// Query each primary.
	stats := make([]shardStat, 0, len(primaryPods))
	port := effectivePort(vc)
	for _, pod := range primaryPods {
		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, port)
		c, err := valkeyClient(addr, creds.username, creds.password)
		if err != nil {
			logger.Info("Cannot connect to primary for slot stats — skipping", "pod", pod.Name)
			continue
		}

		tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		result := c.Do(tctx, c.B().Arbitrary("operator.slot.stats").ReadOnly())
		cancel()
		c.Close()

		pairs, err := result.AsStrSlice()
		if err != nil {
			logger.Info("OPERATOR.SLOT.STATS failed", "pod", pod.Name, "err", err)
			continue
		}

		stat := shardStat{podName: pod.Name}
		for i := 0; i+1 < len(pairs); i += 2 {
			switch pairs[i] {
			case "memory_bytes":
				if v, err := strconv.ParseInt(pairs[i+1], 10, 64); err == nil {
					stat.memoryBytes = v
				}
			case "keys":
				if v, err := strconv.ParseInt(pairs[i+1], 10, 64); err == nil {
					stat.keys = v
				}
			}
		}
		stats = append(stats, stat)
	}

	if len(stats) < 2 {
		clearShardImbalanced(vc)
		return nil
	}

	// Compute memory skew: (max - min) * 100 / max.
	var maxMem, minMem int64
	maxMem = stats[0].memoryBytes
	minMem = stats[0].memoryBytes
	for _, s := range stats[1:] {
		if s.memoryBytes > maxMem {
			maxMem = s.memoryBytes
		}
		if s.memoryBytes < minMem {
			minMem = s.memoryBytes
		}
	}

	threshold := defaultImbalanceThreshold
	if vc.Spec.Rebalance != nil && vc.Spec.Rebalance.Threshold > 0 {
		threshold = vc.Spec.Rebalance.Threshold
	}

	var skewPct int64
	if maxMem > 0 {
		skewPct = (maxMem - minMem) * 100 / maxMem
	}

	logger.Info("Shard memory stats", "maxMem", maxMem, "minMem", minMem, "skewPct", skewPct, "threshold", threshold)

	if skewPct > int64(threshold) {
		// Apply grace period to avoid condition flapping during rebalance convergence.
		now := time.Now()
		if vc.Annotations == nil {
			vc.Annotations = make(map[string]string)
		}
		firstSeenStr, tracked := vc.Annotations[shardImbalancedAnnotation]
		if !tracked {
			vc.Annotations[shardImbalancedAnnotation] = now.UTC().Format(time.RFC3339)
			patch := client.MergeFrom(vc.DeepCopy())
			if err := r.Patch(ctx, vc, patch); err != nil {
				logger.Info("Failed to patch shard-imbalanced annotation", "err", err)
			}
			logger.Info("Shard imbalance first observed — waiting for grace period",
				"skewPct", skewPct, "threshold", threshold)
			return nil
		}
		firstSeen, err := time.Parse(time.RFC3339, firstSeenStr)
		if err != nil || now.Sub(firstSeen) < shardImbalancedGracePeriod {
			return nil
		}

		msg := fmt.Sprintf("memory skew %d%% exceeds threshold %d%% (max %d bytes, min %d bytes)",
			skewPct, threshold, maxMem, minMem)
		setCondition(vc, metav1.Condition{
			Type:               "ShardImbalanced",
			Status:             metav1.ConditionTrue,
			Reason:             "MemorySkew",
			Message:            msg,
			ObservedGeneration: vc.Generation,
		})
		r.Recorder.Eventf(vc, corev1.EventTypeWarning, "ShardImbalanced", msg)
	} else {
		// Skew within threshold — clear annotation and condition.
		if vc.Annotations != nil {
			if _, exists := vc.Annotations[shardImbalancedAnnotation]; exists {
				patch := client.MergeFrom(vc.DeepCopy())
				delete(vc.Annotations, shardImbalancedAnnotation)
				if err := r.Patch(ctx, vc, patch); err != nil {
					logger.Info("Failed to clear shard-imbalanced annotation", "err", err)
				}
			}
		}
		clearShardImbalanced(vc)
	}

	return nil
}

// clearShardImbalanced sets ShardImbalanced condition to False.
func clearShardImbalanced(vc *cachev1alpha1.ValkeyCluster) {
	setCondition(vc, metav1.Condition{
		Type:               "ShardImbalanced",
		Status:             metav1.ConditionFalse,
		Reason:             "Balanced",
		Message:            "Memory distribution across shards is within threshold",
		ObservedGeneration: vc.Generation,
	})
}
