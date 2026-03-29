package controller

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// commonLabels returns the standard labels applied to all resources of a ValkeyCluster.
func commonLabels(vc *cachev1alpha1.ValkeyCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "valkeycluster",
		"app.kubernetes.io/instance":   vc.Name,
		"app.kubernetes.io/managed-by": "valkey-operator",
	}
}

// podSelector returns labels used to select pods belonging to a ValkeyCluster.
func podSelector(vc *cachev1alpha1.ValkeyCluster) client.MatchingLabels {
	return client.MatchingLabels{
		"app.kubernetes.io/name":     "valkeycluster",
		"app.kubernetes.io/instance": vc.Name,
	}
}

// podSelectorMap returns the labels used to select pods for this cluster.
func podSelectorMap(vc *cachev1alpha1.ValkeyCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "valkeycluster",
		"app.kubernetes.io/instance": vc.Name,
	}
}

// bootstrapJobLabels returns labels for the bootstrap Job pod template.
// Uses a distinct app.kubernetes.io/name so the pod does NOT match the
// StatefulSet anti-affinity selector (which targets "valkeycluster").
func bootstrapJobLabels(vc *cachev1alpha1.ValkeyCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "valkeycluster-bootstrap",
		"app.kubernetes.io/instance":   vc.Name,
		"app.kubernetes.io/managed-by": "valkey-operator",
	}
}

// metav1LabelSelector returns a LabelSelector matching all pods of this cluster.
func metav1LabelSelector(vc *cachev1alpha1.ValkeyCluster) metav1.LabelSelector {
	return metav1.LabelSelector{
		MatchLabels: map[string]string{
			"app.kubernetes.io/name":     "valkeycluster",
			"app.kubernetes.io/instance": vc.Name,
		},
	}
}

// metav1LabelSelectorPtr returns a pointer to a LabelSelector for TopologySpreadConstraint.
func metav1LabelSelectorPtr(vc *cachev1alpha1.ValkeyCluster) *metav1.LabelSelector {
	s := metav1LabelSelector(vc)
	return &s
}
