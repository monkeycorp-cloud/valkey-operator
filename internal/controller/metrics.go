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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

const metricsPort = int32(9121)

func metricsServiceName(vc *cachev1alpha1.ValkeyCluster) string {
	return fmt.Sprintf("%s-metrics", vc.Name)
}

func serviceMonitorName(vc *cachev1alpha1.ValkeyCluster) string {
	return fmt.Sprintf("%s-metrics", vc.Name)
}

// reconcileMetrics manages the metrics Service and ServiceMonitor lifecycle.
// Both are created only when metrics.enabled=true and deleted when disabled.
func (r *ValkeyClusterReconciler) reconcileMetrics(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	metricsEnabled := vc.Spec.Metrics != nil && vc.Spec.Metrics.Enabled

	// Reconcile metrics Service.
	if err := r.reconcileMetricsService(ctx, vc, metricsEnabled); err != nil {
		logger.Error(err, "Failed to reconcile metrics Service")
		return err
	}

	// Reconcile ServiceMonitor.
	smEnabled := metricsEnabled &&
		vc.Spec.Metrics.ServiceMonitor != nil &&
		vc.Spec.Metrics.ServiceMonitor.Enabled

	if err := r.reconcileServiceMonitor(ctx, vc, smEnabled); err != nil {
		// Non-fatal: Prometheus Operator may not be installed.
		logger.Error(err, "Failed to reconcile ServiceMonitor (Prometheus Operator may not be installed)")
	}

	return nil
}

// reconcileMetricsService creates or updates a ClusterIP Service exposing port 9121
// on all pods, used by Prometheus to scrape redis_exporter metrics.
func (r *ValkeyClusterReconciler) reconcileMetricsService(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, enabled bool) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      metricsServiceName(vc),
			Namespace: vc.Namespace,
		},
	}

	if !enabled {
		existing := &corev1.Service{}
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(svc), existing)
		if err == nil {
			return r.Client.Delete(ctx, existing)
		}
		return client.IgnoreNotFound(err)
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(vc, svc, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(vc)
		svc.Labels = labels

		svc.Spec.Selector = podSelectorMap(vc)
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "metrics",
				Port:       metricsPort,
				TargetPort: intstr.FromInt32(metricsPort),
				Protocol:   corev1.ProtocolTCP,
			},
		}
		return nil
	})
	return err
}

// serviceMonitorGVR is the GroupVersionResource for Prometheus Operator ServiceMonitor.
var serviceMonitorGVK = schema.GroupVersionKind{
	Group:   "monitoring.coreos.com",
	Version: "v1",
	Kind:    "ServiceMonitor",
}

// reconcileServiceMonitor creates or updates a ServiceMonitor for Prometheus Operator.
// Written as unstructured to avoid a hard dependency on the prometheus-operator Go client.
func (r *ValkeyClusterReconciler) reconcileServiceMonitor(ctx context.Context, vc *cachev1alpha1.ValkeyCluster, enabled bool) error {
	sm := &unstructured.Unstructured{}
	sm.SetGroupVersionKind(serviceMonitorGVK)
	sm.SetName(serviceMonitorName(vc))
	sm.SetNamespace(vc.Namespace)

	if !enabled {
		err := r.Client.Get(ctx, client.ObjectKeyFromObject(sm), sm)
		if err == nil {
			return r.Client.Delete(ctx, sm)
		}
		if meta.IsNoMatchError(err) {
			return nil
		}
		return client.IgnoreNotFound(err)
	}

	smSpec := vc.Spec.Metrics.ServiceMonitor

	// Build the endpoint spec — "metrics" matches the port name on the Service.
	endpoint := map[string]interface{}{
		"port":   "metrics", // matches metricsPort (9121) on the metrics Service
		"scheme": "http",
	}
	if smSpec.Interval != "" {
		endpoint["interval"] = smSpec.Interval
	}
	if smSpec.ScrapeTimeout != "" {
		endpoint["scrapeTimeout"] = smSpec.ScrapeTimeout
	}

	// Build labels for the ServiceMonitor metadata — merge common labels + user labels.
	smLabels := commonLabels(vc)
	for k, v := range smSpec.Labels {
		smLabels[k] = v
	}

	desired := buildServiceMonitor(vc, smLabels, endpoint)
	if err := controllerutil.SetControllerReference(vc, desired, r.Scheme); err != nil {
		return fmt.Errorf("setting owner reference on ServiceMonitor: %w", err)
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(serviceMonitorGVK)
	err := r.Client.Get(ctx, client.ObjectKey{Name: serviceMonitorName(vc), Namespace: vc.Namespace}, existing)
	if errors.IsNotFound(err) {
		if err := r.Client.Create(ctx, desired); err != nil {
			if meta.IsNoMatchError(err) {
				return nil
			}
			return err
		}
		return nil
	}
	if err != nil {
		// If the CRD is not installed (Prometheus Operator absent), treat as non-fatal.
		if meta.IsNoMatchError(err) {
			return nil
		}
		return fmt.Errorf("getting ServiceMonitor: %w", err)
	}

	// Preserve resourceVersion for the update.
	desired.SetResourceVersion(existing.GetResourceVersion())
	return r.Client.Update(ctx, desired)
}

// buildServiceMonitor constructs the unstructured ServiceMonitor object.
func buildServiceMonitor(vc *cachev1alpha1.ValkeyCluster, labels map[string]string, endpoint map[string]interface{}) *unstructured.Unstructured {
	sm := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "ServiceMonitor",
			"metadata": map[string]interface{}{
				"name":      serviceMonitorName(vc),
				"namespace": vc.Namespace,
				"labels":    toStringInterface(labels),
			},
			"spec": map[string]interface{}{
				// namespaceSelector scoped to the same namespace as the cluster.
				"namespaceSelector": map[string]interface{}{
					"matchNames": []interface{}{vc.Namespace},
				},
				// selector matches the metrics Service by common labels.
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						"app.kubernetes.io/name":     "valkeycluster",
						"app.kubernetes.io/instance": vc.Name,
					},
				},
				"endpoints": []interface{}{endpoint},
			},
		},
	}
	return sm
}

// toStringInterface converts map[string]string to map[string]interface{} for unstructured.
func toStringInterface(in map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
