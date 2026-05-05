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
	"math"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// podTopologyInfo holds resolved topology and lag data for a replica candidate.
type podTopologyInfo struct {
	pod           *corev1.Pod
	domainValue   string // e.g. "zone-a"
	replicaOffset int64  // higher = more up-to-date
}

// buildPodAffinity returns the affinity rules for the StatefulSet pod template.
//
// Two layers:
//  1. Node-level (kubernetes.io/hostname):
//     Hard — RequiredDuringScheduling, blocks placement if unsatisfiable.
//     Soft — PreferredDuringScheduling, best-effort spread.
//     None — no rule generated.
//  2. Zone-level (nodeTopologyKey):
//     Hard — RequiredDuringScheduling, blocks placement if unsatisfiable.
//     Soft — PreferredDuringScheduling, best-effort spread.
//     None — no rule generated.
func buildPodAffinity(vc *cachev1alpha1.ValkeyCluster) *corev1.Affinity {
	topo := vc.Spec.Topology

	nodePolicy := cachev1alpha1.SpreadPolicyHard
	if topo != nil && topo.NodeSpreadPolicy != "" {
		nodePolicy = topo.NodeSpreadPolicy
	}

	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{},
	}

	switch nodePolicy {
	case cachev1alpha1.SpreadPolicyHard:
		sel := metav1LabelSelector(vc)
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
			{LabelSelector: &sel, TopologyKey: "kubernetes.io/hostname"},
		}
	case cachev1alpha1.SpreadPolicySoft:
		sel := metav1LabelSelector(vc)
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1.WeightedPodAffinityTerm{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &sel,
					TopologyKey:   "kubernetes.io/hostname",
				},
			},
		)
	}

	if topo == nil {
		return affinity
	}

	zonePolicy := topo.ZoneSpreadPolicy
	topologyKey := topo.NodeTopologyKey
	if topologyKey == "" {
		topologyKey = "topology.kubernetes.io/zone"
	}
	// Avoid duplicating the node-level rule if the zone key is the same.
	if topologyKey == "kubernetes.io/hostname" {
		return affinity
	}

	switch zonePolicy {
	case cachev1alpha1.SpreadPolicyHard:
		sel := metav1LabelSelector(vc)
		affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(
			affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
			corev1.PodAffinityTerm{LabelSelector: &sel, TopologyKey: topologyKey},
		)
	case cachev1alpha1.SpreadPolicySoft:
		sel := metav1LabelSelector(vc)
		affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
			affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1.WeightedPodAffinityTerm{
				Weight: 100,
				PodAffinityTerm: corev1.PodAffinityTerm{
					LabelSelector: &sel,
					TopologyKey:   topologyKey,
				},
			},
		)
	}

	return affinity
}

// buildTopologySpreadConstraints returns spread constraints for the StatefulSet.
//
// Two constraints (each controlled by SpreadPolicy):
//  1. Node-level (kubernetes.io/hostname): DoNotSchedule — at most 1 pod per node.
//  2. Zone-level (nodeTopologyKey): DoNotSchedule — at most 1 pod per zone.
func buildTopologySpreadConstraints(vc *cachev1alpha1.ValkeyCluster) []corev1.TopologySpreadConstraint {
	sel := metav1LabelSelectorPtr(vc)
	topo := vc.Spec.Topology

	nodePolicy := cachev1alpha1.SpreadPolicyHard
	if topo != nil && topo.NodeSpreadPolicy != "" {
		nodePolicy = topo.NodeSpreadPolicy
	}

	var constraints []corev1.TopologySpreadConstraint

	switch nodePolicy {
	case cachev1alpha1.SpreadPolicyHard:
		constraints = append(constraints, corev1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       "kubernetes.io/hostname",
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     sel,
		})
	case cachev1alpha1.SpreadPolicySoft:
		constraints = append(constraints, corev1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       "kubernetes.io/hostname",
			WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector:     sel,
		})
	}

	if topo == nil {
		return constraints
	}

	topologyKey := topo.NodeTopologyKey
	if topologyKey == "" {
		topologyKey = "topology.kubernetes.io/zone"
	}
	// Avoid duplicating the node-level rule if the zone key is the same.
	if topologyKey == "kubernetes.io/hostname" {
		return constraints
	}

	switch topo.ZoneSpreadPolicy {
	case cachev1alpha1.SpreadPolicyHard:
		constraints = append(constraints, corev1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       topologyKey,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     sel,
		})
	case cachev1alpha1.SpreadPolicySoft:
		constraints = append(constraints, corev1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       topologyKey,
			WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector:     sel,
		})
	}

	return constraints
}

// reconcileTopologyStatus updates status.podTopology and status.primaryTopologyValue.
func (r *ValkeyClusterReconciler) reconcileTopologyStatus(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	if vc.Spec.Topology == nil {
		return nil
	}

	topologyKey := vc.Spec.Topology.NodeTopologyKey
	if topologyKey == "" {
		topologyKey = "topology.kubernetes.io/zone"
	}

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList,
		podSelector(vc),
		client.InNamespace(vc.Namespace),
	); err != nil {
		return err
	}

	podTopology := make(map[string]string, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Spec.NodeName == "" {
			continue
		}
		node := &corev1.Node{}
		if err := r.Client.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
			continue
		}
		domainValue := node.Labels[topologyKey]
		podTopology[pod.Name] = domainValue

		if pod.Labels != nil && pod.Labels[roleLabelKey] == rolePrimary {
			vc.Status.PrimaryTopologyValue = domainValue
		}
	}
	vc.Status.PodTopology = podTopology
	return nil
}

// pickBestReplicaTopologyAware selects the best replica from a given candidate list
// using a combined score of replication lag and topology preference.
//
// Score formula (higher = better):
//
//	score = (1 - w) * lagScore + w * topoScore
//
// where w = ElectionTopologyWeight / 100.
func (r *ValkeyClusterReconciler) pickBestReplicaTopologyAware(
	ctx context.Context,
	vc *cachev1alpha1.ValkeyCluster,
	failedPrimaryName string,
	creds aclCredentials,
	candidatePods []*corev1.Pod,
) (*corev1.Pod, error) {
	logger := log.FromContext(ctx)

	topo := vc.Spec.Topology
	if topo == nil {
		return r.pickBestReplicaByLag(ctx, vc, creds, candidatePods)
	}

	topologyKey := topo.NodeTopologyKey
	if topologyKey == "" {
		topologyKey = "topology.kubernetes.io/zone"
	}

	failedZone := r.podTopologyDomain(ctx, vc.Namespace, failedPrimaryName, topologyKey)

	candidates, err := r.collectCandidates(ctx, vc, creds, topologyKey, candidatePods)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no ready replica candidates found")
	}

	var maxOffset int64
	for _, c := range candidates {
		if c.replicaOffset > maxOffset {
			maxOffset = c.replicaOffset
		}
	}

	weight := float64(topo.ElectionTopologyWeight) / 100.0
	// Use the actual shard ordinal of the failing primary so that podAssignments
	// preferences are applied correctly for every shard, not just shard 0.
	primaryOrdinal := int32(podOrdinal(failedPrimaryName, vc.Name))
	preferredZones := preferredZonesForIndex(topo, primaryOrdinal)

	var bestCandidate *podTopologyInfo
	var bestScore float64 = -1

	for i := range candidates {
		c := &candidates[i]

		lagScore := float64(1)
		if maxOffset > 0 {
			lagScore = float64(c.replicaOffset) / float64(maxOffset)
		}

		topoScore := topologyScore(c.domainValue, failedZone, preferredZones, topo.AvoidSameZoneAsFailed)
		score := (1-weight)*lagScore + weight*topoScore

		logger.Info("Candidate scored",
			"pod", c.pod.Name,
			"zone", c.domainValue,
			"replicaOffset", c.replicaOffset,
			"lagScore", math.Round(lagScore*100)/100,
			"topoScore", topoScore,
			"totalScore", math.Round(score*100)/100,
		)

		if bestCandidate == nil || score > bestScore {
			bestScore = score
			bestCandidate = c
		}
	}

	if bestCandidate == nil {
		return nil, fmt.Errorf("no suitable replica found after scoring")
	}

	logger.Info("Elected replica",
		"pod", bestCandidate.pod.Name,
		"zone", bestCandidate.domainValue,
		"score", math.Round(bestScore*100)/100,
	)
	return bestCandidate.pod, nil
}

// topologyScore returns a score [0, 1] for a candidate's topology domain.
//   - 1.0 : candidate is in a preferred zone
//   - 0.5 : neutral zone
//   - 0.0 : same zone as the failed primary (when avoidSameZone=true)
func topologyScore(candidateZone, failedZone string, preferredZones []string, avoidSameZone bool) float64 {
	if avoidSameZone && candidateZone != "" && candidateZone == failedZone {
		return 0.0
	}
	for _, pz := range preferredZones {
		if pz == candidateZone {
			return 1.0
		}
	}
	return 0.5
}

// preferredZonesForIndex returns preferred topology domains for a given pod ordinal.
func preferredZonesForIndex(topo *cachev1alpha1.ValkeyTopologySpec, podIndex int32) []string {
	for _, a := range topo.PodAssignments {
		if a.PodIndex == podIndex {
			return a.PreferredValues
		}
	}
	return nil
}

// collectCandidates resolves topology domain and replication offset for a list of pods.
func (r *ValkeyClusterReconciler) collectCandidates(
	ctx context.Context,
	vc *cachev1alpha1.ValkeyCluster,
	creds aclCredentials,
	topologyKey string,
	candidatePods []*corev1.Pod,
) ([]podTopologyInfo, error) {
	type result struct {
		info podTopologyInfo
		err  error
	}
	ch := make(chan result, len(candidatePods))

	for _, pod := range candidatePods {
		go func(p *corev1.Pod) {
			// Each goroutine gets its own timeout to avoid blocking indefinitely.
			gctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			info := podTopologyInfo{pod: p}

			if p.Spec.NodeName != "" {
				node := &corev1.Node{}
				if err := r.Client.Get(gctx, client.ObjectKey{Name: p.Spec.NodeName}, node); err == nil {
					if node.Labels != nil {
						info.domainValue = node.Labels[topologyKey]
					}
				}
			}

			addr := fmt.Sprintf("%s:%d", p.Status.PodIP, effectivePort(vc))
			offset, err := getReplicaOffset(gctx, addr, creds)
			if err == nil {
				info.replicaOffset = offset
			}

			ch <- result{info: info}
		}(pod)
	}

	candidates := make([]podTopologyInfo, 0, len(candidatePods))
	for i := 0; i < len(candidatePods); i++ {
		res := <-ch
		if res.err == nil {
			candidates = append(candidates, res.info)
		}
	}
	return candidates, nil
}

// podTopologyDomain returns the topology domain value for a named pod.
func (r *ValkeyClusterReconciler) podTopologyDomain(ctx context.Context, namespace, podName, topologyKey string) string {
	pod := &corev1.Pod{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podName}, pod); err != nil {
		return ""
	}
	if pod.Spec.NodeName == "" {
		return ""
	}
	node := &corev1.Node{}
	if err := r.Client.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
		return ""
	}
	return node.Labels[topologyKey]
}
