// Copyright 2026 Gorilla-Ops contributors
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
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// isPodReady returns true if the pod is running and its Ready condition is true.
func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podOrdinal extracts the ordinal from a pod name like "mycluster-2".
func podOrdinal(podName, clusterName string) int {
	prefix := clusterName + "-"
	if !strings.HasPrefix(podName, prefix) {
		return -1
	}
	suffix := strings.TrimPrefix(podName, prefix)
	var ordinal int
	if _, err := fmt.Sscanf(suffix, "%d", &ordinal); err != nil {
		return -1
	}
	return ordinal
}

// buildOneOffJob creates a one-shot Job that mounts a PVC and runs a shell script.
// Used for recovery operations (RDB cleanup) that need direct PVC access.
func buildOneOffJob(vc *cachev1alpha1.ValkeyCluster, jobName, pvcName, script string) *batchv1.Job {
	backoffLimit := int32(0) // no retry — if it fails, we want to know
	ttl := int32(300)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: vc.Namespace,
			Labels:    commonLabels(vc),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: commonLabels(vc),
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "recovery",
							Image:           "busybox:1.36",
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"sh", "-c", script},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
					},
				},
			},
		},
	}
}
