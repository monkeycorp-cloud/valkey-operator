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

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// reconcilePDB ensures a PodDisruptionBudget exists for the ValkeyCluster.
//
// maxUnavailable=1 guarantees that voluntary disruptions (node drain, nodepool
// upgrade, GKE node recycling) evict at most one pod at a time. Combined with
// the node-drain pro-active failover, this ensures the cluster always has a
// quorum of primaries available during infrastructure maintenance.
func (r *ValkeyClusterReconciler) reconcilePDB(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	maxUnavailable := intstr.FromInt32(1)

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vc.Name,
			Namespace: vc.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := controllerutil.SetControllerReference(vc, pdb, r.Scheme); err != nil {
			return err
		}
		pdb.Labels = commonLabels(vc)
		pdb.Spec.MaxUnavailable = &maxUnavailable
		pdb.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: podSelectorMap(vc),
		}
		return nil
	})
	return err
}
