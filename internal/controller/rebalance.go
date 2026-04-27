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
	"context"
	"fmt"
	"strings"

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
	rebalanceJobSuffix = "-rebalance"
)

// reconcileRebalance creates a rebalance Job when the ShardImbalanced condition
// is True and spec.rebalance.enabled is set. Skips if a Job is already running.
func (r *ValkeyClusterReconciler) reconcileRebalance(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	// Only act when ShardImbalanced condition is True.
	imbalanced := false
	for _, cond := range vc.Status.Conditions {
		if cond.Type == "ShardImbalanced" && cond.Status == metav1.ConditionTrue {
			imbalanced = true
			break
		}
	}
	if !imbalanced {
		return nil
	}

	jobName := vc.Name + rebalanceJobSuffix

	// Check if a rebalance Job already exists.
	existing := &batchv1.Job{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: vc.Namespace}, existing)
	if err == nil {
		// Job exists — check if it's still running.
		if existing.Status.CompletionTime == nil &&
			(existing.Status.Succeeded == 0 && existing.Status.Failed == 0) {
			logger.Info("Rebalance Job already running — skipping creation")
			return nil
		}
		// Completed or failed — delete it so we can create a fresh one next cycle.
		if existing.Status.CompletionTime != nil {
			logger.Info("Previous rebalance Job completed — cleaning up")
			if err := r.Client.Delete(ctx, existing); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("deleting completed rebalance job: %w", err)
			}
			return nil
		}
		// Failed — log and return; operator will retry on the next reconcile cycle.
		if existing.Status.Failed > 0 {
			logger.Info("Previous rebalance Job failed — will retry next cycle")
			if err := r.Client.Delete(ctx, existing); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("deleting failed rebalance job: %w", err)
			}
			return nil
		}
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("checking rebalance job: %w", err)
	}

	// Find a ready primary pod to use as the cluster seed address.
	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, podSelector(vc), client.InNamespace(vc.Namespace)); err != nil {
		return err
	}

	var seedAddr string
	for i := range podList.Items {
		pod := &podList.Items[i]
		if isPodReady(pod) && pod.Status.PodIP != "" && vc.Status.PodRoles[pod.Name] == rolePrimary {
			seedAddr = fmt.Sprintf("%s:%d", pod.Status.PodIP, effectivePort(vc))
			break
		}
	}
	if seedAddr == "" {
		logger.Info("No ready primary pod found — cannot create rebalance Job")
		return nil
	}

	job := r.buildRebalanceJob(vc, seedAddr)
	logger.Info("Creating rebalance Job", "job", job.Name, "seed", seedAddr)
	if err := r.Client.Create(ctx, job); err != nil {
		if errors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("creating rebalance job: %w", err)
	}

	r.Recorder.Eventf(vc, corev1.EventTypeNormal, "RebalanceStarted",
		"Rebalance Job %s created (seed: %s)", job.Name, seedAddr)
	return nil
}

// buildRebalanceJob constructs the Job spec that runs valkey-cli --cluster rebalance.
func (r *ValkeyClusterReconciler) buildRebalanceJob(vc *cachev1alpha1.ValkeyCluster, seedAddr string) *batchv1.Job {
	maxSlotsPerRound := int32(100)
	if vc.Spec.Rebalance != nil && vc.Spec.Rebalance.MaxSlotsPerRound > 0 {
		maxSlotsPerRound = vc.Spec.Rebalance.MaxSlotsPerRound
	}

	// Split seed addr into host and port for valkey-cli flags.
	parts := strings.SplitN(seedAddr, ":", 2)
	host := parts[0]
	port := "6379"
	if len(parts) == 2 {
		port = parts[1]
	}

	script := fmt.Sprintf(`#!/bin/sh
set -e

CLI="valkey-cli --no-auth-warning --user operator --pass $VALKEY_OPERATOR_PASSWORD"

echo "==> Running valkey-cli --cluster rebalance..."
$CLI -h %s -p %s \
  --cluster rebalance %s \
  --cluster-use-empty-masters \
  --cluster-pipeline 100 \
  --cluster-timeout 5000 \
  --cluster-slots %d \
  --cluster-yes

echo "==> Rebalance complete."
`, host, port, seedAddr, maxSlotsPerRound)

	backoffLimit := int32(0) // one-shot; operator retries on next reconcile cycle
	ttl := int32(300)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vc.Name + rebalanceJobSuffix,
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
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           vc.Name + bootstrapSASuffix,
					AutomountServiceAccountToken: func() *bool { f := false; return &f }(),
					Containers: []corev1.Container{
						{
							Name:            "rebalance",
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

	_ = controllerutil.SetControllerReference(vc, job, r.Scheme)
	return job
}
