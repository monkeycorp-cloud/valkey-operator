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
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "valkey-operator/api/v1alpha1"

	"github.com/valkey-io/valkey-go"
)

const (
	operatorUsername  = "operator"
	metricsUsername   = "metrics"
	aclHashAnnotation = "valkey.io/acl-hash"

	// aclSkipAge is the minimum pod age before a pod is eligible to be skipped
	// during ACL reconciliation when the ACL hash has not changed.
	// Covers the startup window: gate poll (500ms) + reconcile cycle (~3s).
	aclSkipAge = 60 * time.Second
)

// metricsACLRules are the exact permissions required by redis_exporter.
// Source: https://github.com/oliver006/redis_exporter
// -@all denies everything by default, then we allow only what is needed.
var metricsACLRules = []string{
	"-@all",
	"+@connection",
	"+memory",
	"+config|get",
	"+info",
	"+scan",
	"+strlen",
	"+type",
	"+xlen",
	"+xinfo",
	"+zcard",
	"+scard",
	"+llen",
	"+hlen",
	"+get",
	"+eval",
	"+slowlog",
	"+latency",
	"+cluster|info",
	"+cluster|slots",
	"+cluster|nodes",
	"+client",
	"+pfcount",
}

// shouldSkipACLPod reports whether a pod should be skipped during ACL
// reconciliation. A pod is skipped only when the hash has not changed AND
// the pod has been running for longer than aclSkipAge.
// startTime==nil means the pod has no start time recorded — never skip.
func shouldSkipACLPod(hashChanged bool, startTime *metav1.Time) bool {
	if startTime == nil {
		return false
	}
	return !hashChanged && time.Since(startTime.Time) > aclSkipAge
}

// aclHash returns an 8-byte hex digest of the resolved ACL configuration.
// Same pattern as configMapHash in statefulset.go.
func aclHash(operatorPassword, metricsPassword string, metricsEnabled bool, users []resolvedACLUser) string {
	var b strings.Builder
	fmt.Fprintf(&b, "operator=%s\n", operatorPassword)
	fmt.Fprintf(&b, "metricsEnabled=%t\n", metricsEnabled)
	fmt.Fprintf(&b, "metrics=%s\n", metricsPassword)
	for _, u := range users {
		fmt.Fprintf(&b, "user=%s pass=%s keys=%v cmds=%s\n",
			u.name, u.password, u.keyPatterns, u.commands)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return fmt.Sprintf("%x", sum[:8])
}

// reconcileACLs applies the ACL configuration on every Running pod.
//
//  1. Disables the default account.
//  2. Creates/updates the internal operator account (full access).
//  3. Creates/updates the metrics account (redis_exporter permissions).
//  4. Creates/updates each application ACL user defined in spec.aclUsers.
//  5. Removes stale users no longer in the spec.
//
// Skip logic: pods that have been running for more than aclSkipAge and whose
// ACL hash (valkey.io/acl-hash annotation) has not changed are skipped.
// This avoids N connections per reconcile cycle when the cluster is stable.
// Newly started pods (age < aclSkipAge) are always reconciled so the gate
// can clear NO_REDIRECTION as soon as ACLs are applied.
func (r *ValkeyClusterReconciler) reconcileACLs(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) error {
	logger := log.FromContext(ctx)

	operatorPassword, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return fmt.Errorf("resolving operator secret: %w", err)
	}

	appUsers, err := r.resolveACLUsers(ctx, vc)
	if err != nil {
		return err
	}

	// Resolve metrics password if metrics are enabled.
	var metricsPassword string
	if vc.Spec.Metrics != nil && vc.Spec.Metrics.Enabled {
		metricsPassword, err = r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.Metrics.MetricsSecret)
		if err != nil {
			return fmt.Errorf("resolving metrics secret: %w", err)
		}
	}

	metricsEnabled := vc.Spec.Metrics != nil && vc.Spec.Metrics.Enabled
	hash := aclHash(operatorPassword, metricsPassword, metricsEnabled, appUsers)
	hashChanged := vc.Annotations[aclHashAnnotation] != hash

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList,
		podSelector(vc),
		client.InNamespace(vc.Namespace),
	); err != nil {
		return err
	}

	applied, skipped := 0, 0
	for i := range podList.Items {
		pod := &podList.Items[i]
		// Apply ACLs to all Running pods, not just Ready ones.
		// A pod cannot become Ready until ACLs are applied (OPERATOR.NODE.READY
		// checks for an active application user), so skipping non-Ready pods
		// creates a deadlock: pod never Ready → ACLs never applied → never Ready.
		if pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			skipped++
			continue
		}

		// Skip pods that are stable (age > aclSkipAge) and whose ACL config
		// has not changed since last successful apply.
		if shouldSkipACLPod(hashChanged, pod.Status.StartTime) {
			skipped++
			continue
		}

		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, effectivePort(vc))
		if err := r.applyACLsOnPod(ctx, addr, operatorPassword, metricsPassword, appUsers); err != nil {
			logger.Info("Failed to apply ACLs on pod", "pod", pod.Name, "error", err)
		} else {
			applied++
		}
	}

	// Persist the hash once all pods have been reconciled.
	if hashChanged && applied > 0 {
		patch := client.MergeFrom(vc.DeepCopy())
		if vc.Annotations == nil {
			vc.Annotations = make(map[string]string)
		}
		vc.Annotations[aclHashAnnotation] = hash
		if err := r.Client.Patch(ctx, vc, patch); err != nil {
			logger.Info("Failed to persist ACL hash annotation", "error", err)
		}
	}

	logger.Info("ACLs reconciled", "applied", applied, "skipped", skipped)
	return nil
}

// applyACLsOnPod connects to a single pod and applies the full ACL ruleset.
func (r *ValkeyClusterReconciler) applyACLsOnPod(ctx context.Context, addr, operatorPassword, metricsPassword string, appUsers []resolvedACLUser) error {
	// Try operator account first; fall back to unauthenticated for bootstrap.
	c, err := valkeyClient(addr, operatorUsername, operatorPassword)
	if err != nil {
		c, err = valkeyClient(addr, "", "")
		if err != nil {
			return fmt.Errorf("connecting to %s: %w", addr, err)
		}
	}
	defer c.Close()

	tctx, cancel := context.WithTimeout(ctx, clusterTimeout)
	defer cancel()

	// 1. Disable the default account entirely.
	if err := c.Do(tctx, c.B().AclSetuser().Username("default").
		Rule("off").Rule("nopass").Rule("resetkeys").Rule("nocommands").
		Build()).Error(); err != nil {
		return fmt.Errorf("disabling default account on %s: %w", addr, err)
	}

	// 2. Upsert the operator account: full access, password-protected.
	if err := c.Do(tctx, c.B().AclSetuser().Username(operatorUsername).
		Rule("on").
		Rule(">"+operatorPassword).
		Rule("~*").
		Rule("&*").
		Rule("+@all").
		Build()).Error(); err != nil {
		return fmt.Errorf("setting operator account on %s: %w", addr, err)
	}

	// 3. Upsert the metrics account with redis_exporter minimal permissions.
	if metricsPassword != "" {
		cmd := c.B().AclSetuser().Username(metricsUsername).Rule("on").Rule(">" + metricsPassword)
		for _, rule := range metricsACLRules {
			cmd = cmd.Rule(rule)
		}
		if err := c.Do(tctx, cmd.Build()).Error(); err != nil {
			return fmt.Errorf("setting metrics account on %s: %w", addr, err)
		}
	}

	// 4. Upsert application accounts.
	for _, u := range appUsers {
		rules := buildACLRules(u)
		cmd := c.B().AclSetuser().Username(u.name).Rule("on")
		for _, rule := range rules {
			cmd = cmd.Rule(rule)
		}
		if err := c.Do(tctx, cmd.Build()).Error(); err != nil {
			return fmt.Errorf("setting ACL user %q on %s: %w", u.name, addr, err)
		}
	}

	// 5. Remove stale users no longer in the spec.
	if err := r.removeStaleACLUsers(tctx, c, appUsers, metricsPassword != ""); err != nil {
		return fmt.Errorf("removing stale ACL users on %s: %w", addr, err)
	}

	return nil
}

// buildACLRules converts a resolvedACLUser into a list of ACL SETUSER rule strings.
func buildACLRules(u resolvedACLUser) []string {
	rules := []string{
		">" + u.password,
		"resetchannels",
		"&*",
	}

	keyPatterns := u.keyPatterns
	if len(keyPatterns) == 0 {
		keyPatterns = []string{"~*"}
	}
	for _, kp := range keyPatterns {
		rules = append(rules, kp)
	}

	commands := u.commands
	if commands == "" {
		commands = "+@all"
	}
	for _, cmd := range strings.Fields(commands) {
		rules = append(rules, cmd)
	}

	return rules
}

// removeStaleACLUsers deletes ACL users that exist in Valkey but are no longer
// defined in the spec. Reserved accounts (default, operator, metrics) are preserved.
func (r *ValkeyClusterReconciler) removeStaleACLUsers(ctx context.Context, c valkey.Client, current []resolvedACLUser, metricsEnabled bool) error {
	result, err := c.Do(ctx, c.B().AclList().Build()).AsStrSlice()
	if err != nil {
		return fmt.Errorf("ACL LIST: %w", err)
	}

	desired := map[string]struct{}{
		"default":        {},
		operatorUsername: {},
	}
	if metricsEnabled {
		desired[metricsUsername] = struct{}{}
	}
	for _, u := range current {
		desired[u.name] = struct{}{}
	}

	for _, entry := range result {
		parts := strings.Fields(entry)
		if len(parts) < 2 || parts[0] != "user" {
			continue
		}
		username := parts[1]
		if _, keep := desired[username]; keep {
			continue
		}
		if err := c.Do(ctx, c.B().AclDeluser().Username(username).Build()).Error(); err != nil {
			return fmt.Errorf("ACL DELUSER %q: %w", username, err)
		}
	}

	return nil
}

// resolvedACLUser holds a fully resolved ACL user (password fetched from Secret).
type resolvedACLUser struct {
	name        string
	password    string
	keyPatterns []string
	commands    string
}

// validACLKeyPattern checks that a key pattern starts with "~" or "%"
// (read/write channel prefixes like "%R~" are also valid).
func validACLKeyPattern(p string) bool {
	return strings.HasPrefix(p, "~") || strings.HasPrefix(p, "%")
}

// resolveACLUsers fetches all application user passwords from their Secrets.
func (r *ValkeyClusterReconciler) resolveACLUsers(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) ([]resolvedACLUser, error) {
	users := make([]resolvedACLUser, 0, len(vc.Spec.ACLUsers))
	for _, u := range vc.Spec.ACLUsers {
		for _, kp := range u.KeyPatterns {
			if !validACLKeyPattern(kp) {
				return nil, fmt.Errorf("ACL user %q: invalid key pattern %q (must start with ~ or %%)", u.Name, kp)
			}
		}
		password, err := r.resolveSecretKey(ctx, vc.Namespace, u.PasswordSecret)
		if err != nil {
			return nil, fmt.Errorf("resolving password for ACL user %q: %w", u.Name, err)
		}
		users = append(users, resolvedACLUser{
			name:        u.Name,
			password:    password,
			keyPatterns: u.KeyPatterns,
			commands:    u.Commands,
		})
	}
	return users, nil
}
