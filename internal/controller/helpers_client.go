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
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/valkey-io/valkey-go"
	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// aclCredentials holds resolved credentials for a Valkey connection.
type aclCredentials struct {
	username string
	password string
}

// valkeyClient creates a single-node Valkey client, optionally authenticated.
// DialTimeout is intentionally short (2s) so transient connection failures on
// pods that are not yet fully ready do not block the reconcile loop.
func valkeyClient(addr, username, password string) (valkey.Client, error) {
	opts := valkey.ClientOption{
		InitAddress: []string{addr},
		// ForceSingleClient: stay connected to this specific pod IP.
		// Without this, NewClient issues CLUSTER SLOTS and becomes a cluster
		// client that may route commands (including CLUSTER INFO) to a
		// different node than the one we targeted.
		ForceSingleClient: true,
		DisableCache:      true,
		Dialer:            net.Dialer{Timeout: 2 * time.Second},
	}
	if username != "" {
		opts.Username = username
	}
	if password != "" {
		opts.Password = password
	}
	return valkey.NewClient(opts)
}

// resolveSecretKey fetches a single key from a Kubernetes Secret.
func (r *ValkeyClusterReconciler) resolveSecretKey(ctx context.Context, namespace string, ref corev1.SecretKeySelector) (string, error) {
	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ref.Name}, secret); err != nil {
		return "", fmt.Errorf("getting secret %s/%s: %w", namespace, ref.Name, err)
	}
	val, ok := secret.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s/%s", ref.Key, namespace, ref.Name)
	}
	return string(val), nil
}

// operatorCreds resolves the operator credentials from the ValkeyCluster spec.
func (r *ValkeyClusterReconciler) operatorCreds(ctx context.Context, vc *cachev1alpha1.ValkeyCluster) (aclCredentials, error) {
	password, err := r.resolveSecretKey(ctx, vc.Namespace, vc.Spec.OperatorSecret)
	if err != nil {
		return aclCredentials{}, fmt.Errorf("resolving operator secret: %w", err)
	}
	return aclCredentials{username: operatorUsername, password: password}, nil
}
