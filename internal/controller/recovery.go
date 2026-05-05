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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

const (
	// crashLoopThreshold is how long a pod must be in CrashLoopBackOff before
	// the operator attempts automatic RDB recovery.
	crashLoopThreshold = 3 * time.Minute

	// crashLoopAnnotationPrefix tracks the first time a pod was observed crashing.
	// Full key: crashLoopAnnotationPrefix + podName
	crashLoopAnnotationPrefix = "valkey.io/crash-since."
)

// reconcileCrashLoopRecovery detects pods stuck in CrashLoopBackOff due to a
// corrupted RDB and triggers automatic recovery:
//  1. Delete dump.rdb and nodes.conf from the pod's PVC via a recovery Job.
//  2. Delete the pod once the Job is Active so the StatefulSet recreates it cleanly.
//
// The pod will resync its data from the primary (replica case) or trigger a
// cluster election first (primary case — handled by the PreStop / cluster timeout).
func (r *ValkeyClusterReconciler) reconcileCrashLoopRecovery(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return err
	}

	now := time.Now()
	patch := client.MergeFrom(vc.DeepCopy())
	annotationsChanged := false

	for i := range podList.Items {
		pod := &podList.Items[i]

		if !isCrashLoopBackOff(pod) {
			// Pod is healthy — clear any stale crash annotation.
			key := crashLoopAnnotationPrefix + pod.Name
			if vc.Annotations != nil {
				if _, exists := vc.Annotations[key]; exists {
					delete(vc.Annotations, key)
					annotationsChanged = true
				}
			}
			continue
		}

		key := crashLoopAnnotationPrefix + pod.Name
		if vc.Annotations == nil {
			vc.Annotations = make(map[string]string)
		}

		firstSeenStr, tracked := vc.Annotations[key]
		if !tracked {
			// First observation — record timestamp and wait.
			vc.Annotations[key] = now.UTC().Format(time.RFC3339)
			annotationsChanged = true
			logger.Info("Pod in CrashLoopBackOff — waiting before recovery",
				"pod", pod.Name, "threshold", crashLoopThreshold)
			continue
		}

		firstSeen, err := time.Parse(time.RFC3339, firstSeenStr)
		if err != nil || now.Sub(firstSeen) < crashLoopThreshold {
			continue
		}

		// Threshold exceeded — attempt RDB recovery.
		logger.Info("Pod in CrashLoopBackOff beyond threshold — attempting RDB recovery",
			"pod", pod.Name, "crashSince", firstSeenStr)

		r.Recorder.Eventf(vc, corev1.EventTypeWarning, "RDBRecovery",
			"Pod %s stuck in CrashLoopBackOff — deleting corrupted RDB and nodes.conf for recovery",
			pod.Name)

		podDeleted, err := r.recoverCorruptedPod(ctx, vc, pod)
		if err != nil {
			logger.Error(err, "RDB recovery failed", "pod", pod.Name)
			continue
		}

		if podDeleted {
			// Pod deleted — clear annotation to avoid immediate retry.
			delete(vc.Annotations, key)
			annotationsChanged = true
			logger.Info("RDB recovery complete — pod deleted, will resync from primary", "pod", pod.Name)
		} else {
			logger.Info("RDB recovery Job created or in progress — waiting for next cycle", "pod", pod.Name)
		}
	}

	if annotationsChanged {
		if err := r.Client.Patch(ctx, vc, patch); err != nil {
			logger.Info("Failed to patch crash-loop annotations", "err", err)
		}
	}

	return nil
}

// recoverCorruptedPod removes dump.rdb and nodes.conf via a one-shot Job,
// then deletes the pod only once the Job is Active (Status.Active > 0).
//
// This two-cycle approach prevents a race where the pod restarts and rewrites
// a corrupted RDB before the Job has mounted the PVC and deleted the files.
//
// Cycle N  — Job absent  : creates the Job, returns podDeleted=false.
// Cycle N+1 — Job Active : deletes the pod, returns podDeleted=true.
// Cycle N+1 — Job Pending: waits another cycle, returns podDeleted=false.
// Cycle N+1 — Job Failed : returns an error without deleting the pod.
func (r *ValkeyClusterReconciler) recoverCorruptedPod(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, pod *corev1.Pod) (podDeleted bool, err error) {
	pvcName := fmt.Sprintf("valkey-data-%s", pod.Name)
	script := `rm -f /data/dump.rdb /data/nodes.conf && echo "RDB recovery: deleted dump.rdb and nodes.conf"`
	jobName := fmt.Sprintf("%s-rdb-recovery", pod.Name)

	// Check if the Job already exists (cycle N+1+).
	existingJob := &batchv1.Job{}
	err = r.Client.Get(ctx, client.ObjectKey{Name: jobName, Namespace: vc.Namespace}, existingJob)
	if err != nil && !errors.IsNotFound(err) {
		return false, fmt.Errorf("getting recovery job: %w", err)
	}

	if errors.IsNotFound(err) {
		// Cycle N: Job absent — create it and wait for the next cycle.
		// The Job must mount the PVC exclusively before the pod is deleted.
		job := buildOneOffJob(vc, jobName, pvcName, script)
		if err := r.Client.Create(ctx, job); err != nil {
			return false, fmt.Errorf("creating recovery job: %w", err)
		}
		return false, nil
	}

	// Cycle N+1+: Job exists — act on its state.
	switch {
	case existingJob.Status.Failed > 0:
		// Job exhausted retries — do not delete the pod, require manual intervention.
		return false, fmt.Errorf("recovery job %s failed (%d failures)", jobName, existingJob.Status.Failed)
	case existingJob.Status.Active > 0 || existingJob.Status.Succeeded > 0:
		// Job is running or finished — PVC is exclusively held, safe to delete the pod.
		if err := r.Client.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			return false, fmt.Errorf("deleting crashed pod: %w", err)
		}
		return true, nil
	default:
		// Job is Pending (not yet scheduled) — wait another cycle.
		return false, nil
	}
}

// isCrashLoopBackOff returns true if any container in the pod is in CrashLoopBackOff
// and the restart count suggests a persistent failure (not a transient start hiccup).
func isCrashLoopBackOff(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != "valkey" {
			continue
		}
		if cs.State.Waiting != nil &&
			cs.State.Waiting.Reason == "CrashLoopBackOff" &&
			cs.RestartCount >= 3 {
			return true
		}
	}
	return false
}
