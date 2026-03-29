package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// internalServiceName is the headless service used for StatefulSet pod identity
// and inter-pod cluster bus communication. publishNotReadyAddresses: true so
// that pods are DNS-reachable before passing their readiness probe (needed for
// CLUSTER MEET during bootstrap).
func internalServiceName(vc *cachev1alpha1.ValkeyCluster) string {
	return fmt.Sprintf("%s-internal", vc.Name)
}

// clientServiceName is the headless service exposed to application clients.
// publishNotReadyAddresses: false — only ready pods appear in DNS.
func clientServiceName(vc *cachev1alpha1.ValkeyCluster) string {
	return fmt.Sprintf("%s-headless", vc.Name)
}

// effectivePort returns the configured Valkey port or 6379 as default.
func effectivePort(vc *cachev1alpha1.ValkeyCluster) int32 {
	if vc.Spec.Port != 0 {
		return vc.Spec.Port
	}
	return 6379
}

// clusterBusPort returns the Valkey Cluster bus port (data port + 10000).
func clusterBusPort(vc *cachev1alpha1.ValkeyCluster) int32 {
	return effectivePort(vc) + 10000
}

// totalPods returns the total number of pods for this cluster.
func totalPods(vc *cachev1alpha1.ValkeyCluster) int32 {
	return vc.Spec.Shards * (1 + vc.Spec.ReplicasPerShard)
}

// reconcileInternalService manages the headless service used for StatefulSet
// pod DNS and cluster bus communication between nodes.
// publishNotReadyAddresses is true so that pods can meet each other before
// passing their readiness probe during bootstrap.
func (r *ValkeyClusterReconciler) reconcileInternalService(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	port := effectivePort(vc)
	busPort := clusterBusPort(vc)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      internalServiceName(vc),
			Namespace: vc.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(vc, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = commonLabels(vc)
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Selector = podSelectorMap(vc)
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "valkey",
				Port:       port,
				TargetPort: intstr.FromInt32(port),
				Protocol:   corev1.ProtocolTCP,
			},
			{
				Name:       "cluster-bus",
				Port:       busPort,
				TargetPort: intstr.FromInt32(busPort),
				Protocol:   corev1.ProtocolTCP,
			},
		}
		return nil
	})
	return err
}

// reconcileClientService manages the headless service for application clients.
// Only ready pods appear in DNS (publishNotReadyAddresses: false).
func (r *ValkeyClusterReconciler) reconcileClientService(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	port := effectivePort(vc)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clientServiceName(vc),
			Namespace: vc.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(vc, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = commonLabels(vc)
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = false
		svc.Spec.Selector = podSelectorMap(vc)
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "valkey",
				Port:       port,
				TargetPort: intstr.FromInt32(port),
				Protocol:   corev1.ProtocolTCP,
			},
		}
		return nil
	})
	return err
}
