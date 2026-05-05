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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// --- podOrdinal ---

func TestPodOrdinal(t *testing.T) {
	cases := []struct {
		podName, clusterName string
		want                 int
	}{
		{"cluster-0", "cluster", 0},
		{"cluster-5", "cluster", 5},
		{"cluster-10", "cluster", 10},
		{"other-3", "cluster", -1},     // wrong prefix
		{"cluster-abc", "cluster", -1}, // non-numeric suffix
		{"cluster-", "cluster", -1},    // empty suffix
		{"cluster", "cluster", -1},     // no ordinal at all
	}

	for _, tc := range cases {
		t.Run(tc.podName, func(t *testing.T) {
			got := podOrdinal(tc.podName, tc.clusterName)
			if got != tc.want {
				t.Fatalf("podOrdinal(%q, %q) = %d, want %d", tc.podName, tc.clusterName, got, tc.want)
			}
		})
	}
}

// --- totalPods / effectivePort / clusterBusPort ---

func mkVC(shards, replicasPerShard int32, port int32) *cachev1alpha1.ValkeyCluster {
	return &cachev1alpha1.ValkeyCluster{
		Spec: cachev1alpha1.ValkeyClusterSpec{
			Shards:           shards,
			ReplicasPerShard: replicasPerShard,
			Port:             port,
		},
	}
}

func TestTotalPods(t *testing.T) {
	cases := []struct {
		shards, replicas int32
		want             int32
	}{
		{3, 1, 6},
		{3, 0, 3},
		{1, 2, 3},
		{6, 1, 12},
	}
	for _, tc := range cases {
		got := totalPods(mkVC(tc.shards, tc.replicas, 0))
		if got != tc.want {
			t.Errorf("totalPods(shards=%d, replicas=%d) = %d, want %d", tc.shards, tc.replicas, got, tc.want)
		}
	}
}

func TestEffectivePort(t *testing.T) {
	if got := effectivePort(mkVC(1, 1, 0)); got != 6379 {
		t.Errorf("port=0 should default to 6379, got %d", got)
	}
	if got := effectivePort(mkVC(1, 1, 6380)); got != 6380 {
		t.Errorf("port=6380 should return 6380, got %d", got)
	}
}

func TestClusterBusPort(t *testing.T) {
	if got := clusterBusPort(mkVC(1, 1, 6379)); got != 16379 {
		t.Errorf("bus port for 6379 should be 16379, got %d", got)
	}
	if got := clusterBusPort(mkVC(1, 1, 6380)); got != 16380 {
		t.Errorf("bus port for 6380 should be 16380, got %d", got)
	}
}

// --- stripVerbatimPrefix ---

func TestStripVerbatimPrefix(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"ValidPrefix", "txt:cluster_state:ok", "cluster_state:ok"},
		{"NoColonAtPos3", "abcXcluster_state:ok", "abcXcluster_state:ok"},
		{"ShortString3Chars", "abc", "abc"},
		// len(s) > 4 is required — exactly 4 chars is NOT stripped
		{"ShortString4Chars", "abc:", "abc:"},
		{"PrefixContainsNewline", "ab\n:cluster_state:ok", "ab\n:cluster_state:ok"},
		{"EmptyString", "", ""},
		// "txt:" is exactly 4 chars — not stripped (needs len > 4)
		{"ExactlyFourCharsNotStripped", "txt:", "txt:"},
		// 5+ chars with valid prefix — stripped
		{"FiveCharsValidPrefix", "txt:x", "x"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := stripVerbatimPrefix(tc.input)
			if got != tc.want {
				t.Fatalf("stripVerbatimPrefix(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// --- parseClusterInfoField ---

const clusterInfoBlob = `cluster_enabled:1
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:6
cluster_size:3
`

func TestParseClusterInfoField(t *testing.T) {
	cases := []struct {
		name  string
		info  string
		field string
		want  string
	}{
		{"PresentField", clusterInfoBlob, "cluster_state", "ok"},
		{"AnotherField", clusterInfoBlob, "cluster_slots_assigned", "16384"},
		{"AbsentField", clusterInfoBlob, "nonexistent_field", ""},
		{"WhitespaceTrimmed", "cluster_state:  ok  \r\n", "cluster_state", "ok"},
		{"MultilineParsesCorrect", clusterInfoBlob, "cluster_known_nodes", "6"},
		{"VerbatimPrefix", "txt:" + clusterInfoBlob, "cluster_state", "ok"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseClusterInfoField(tc.info, tc.field)
			if got != tc.want {
				t.Fatalf("parseClusterInfoField(info, %q) = %q, want %q", tc.field, got, tc.want)
			}
		})
	}
}

// --- parseClusterInfoInt ---

func TestParseClusterInfoInt(t *testing.T) {
	cases := []struct {
		name  string
		info  string
		field string
		want  int64
	}{
		{"PresentField", clusterInfoBlob, "cluster_slots_assigned", 16384},
		{"FieldIsZero", "cluster_slots_pfail:0\n", "cluster_slots_pfail", 0},
		{"AbsentField", clusterInfoBlob, "nonexistent_field", 0},
		{"NonNumericValue", "cluster_state:ok\n", "cluster_state", 0},
		{"EmptyInfo", "", "cluster_slots_assigned", 0},
		{"VerbatimPrefix", "txt:" + clusterInfoBlob, "cluster_known_nodes", 6},
		{"WhitespaceTrimmed", "cluster_slots_ok: 16384 \r\n", "cluster_slots_ok", 16384},
		// Guard against prefix collision: "cluster_slots_ok" must not match "cluster_slots_ok_extra"
		{"NoPartialPrefixMatch", "cluster_slots_ok_extra:999\ncluster_slots_ok:42\n", "cluster_slots_ok", 42},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseClusterInfoInt(tc.info, tc.field)
			if got != tc.want {
				t.Fatalf("parseClusterInfoInt(info, %q) = %d, want %d", tc.field, got, tc.want)
			}
		})
	}
}

// --- buildGossipPeersJSON ---

func makePod(name, ip string) *corev1.Pod {
	return &corev1.Pod{
		Status: corev1.PodStatus{PodIP: ip},
	}
}

func TestBuildGossipPeersJSON(t *testing.T) {
	t.Run("EmptyPods", func(t *testing.T) {
		got := buildGossipPeersJSON(nil, 6379)
		if got != "[]" {
			t.Fatalf("expected [], got %q", got)
		}
	})

	t.Run("SinglePod", func(t *testing.T) {
		pods := []*corev1.Pod{makePod("pod-0", "10.0.0.1")}
		got := buildGossipPeersJSON(pods, 6379)
		if got != `["10.0.0.1:6379"]` {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("MultiplePods", func(t *testing.T) {
		pods := []*corev1.Pod{
			makePod("pod-0", "10.0.0.1"),
			makePod("pod-1", "10.0.0.2"),
			makePod("pod-2", "10.0.0.3"),
		}
		got := buildGossipPeersJSON(pods, 6379)
		if !strings.HasPrefix(got, "[") || !strings.HasSuffix(got, "]") {
			t.Fatalf("not a JSON array: %q", got)
		}
		if !strings.Contains(got, `"10.0.0.1:6379"`) {
			t.Errorf("missing pod-0: %q", got)
		}
		if !strings.Contains(got, `"10.0.0.2:6379"`) {
			t.Errorf("missing pod-1: %q", got)
		}
		if !strings.Contains(got, `"10.0.0.3:6379"`) {
			t.Errorf("missing pod-2: %q", got)
		}
		// Exactly 2 commas for 3 elements.
		if strings.Count(got, ",") != 2 {
			t.Errorf("expected 2 commas in %q", got)
		}
	})

	t.Run("CustomPort", func(t *testing.T) {
		pods := []*corev1.Pod{makePod("pod-0", "192.168.1.5")}
		got := buildGossipPeersJSON(pods, 6380)
		if got != `["192.168.1.5:6380"]` {
			t.Fatalf("got %q", got)
		}
	})

	t.Run("PayloadShape", func(t *testing.T) {
		// Verify the full payload produced by reconcileGossipRecovery has correct shape.
		pods := []*corev1.Pod{
			makePod("pod-0", "10.0.0.1"),
			makePod("pod-1", "10.0.0.2"),
		}
		peersJSON := buildGossipPeersJSON(pods, 6379)
		payload := `{"peers":` + peersJSON + `,"role":"primary"}`
		if !strings.Contains(payload, `"role":"primary"`) {
			t.Errorf("payload missing role: %q", payload)
		}
		if !strings.Contains(payload, `"peers":[`) {
			t.Errorf("payload missing peers: %q", payload)
		}
	})
}

// --- convergenceTimeoutFor ---

func TestConvergenceTimeoutFor(t *testing.T) {
	mkVC := func(nodeTimeoutMs int32) *cachev1alpha1.ValkeyCluster {
		return &cachev1alpha1.ValkeyCluster{
			Spec: cachev1alpha1.ValkeyClusterSpec{
				ClusterNodeTimeout: nodeTimeoutMs,
			},
		}
	}

	cases := []struct {
		name          string
		nodeTimeoutMs int32
		want          time.Duration
	}{
		{"ZeroUsesDefault2000ms_ClampsTo30s", 0, 30 * time.Second},
		{"2000ms_10x20s_ClampsTo30s", 2000, 30 * time.Second},
		{"3000ms_10x30s_ExactMin", 3000, 30 * time.Second},
		{"6000ms_10x60s", 6000, 60 * time.Second},
		{"12000ms_10x120s_ExactMax", 12000, 120 * time.Second},
		{"15000ms_10x150s_ClampsTo120s", 15000, 120 * time.Second},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := convergenceTimeoutFor(mkVC(tc.nodeTimeoutMs))
			if got != tc.want {
				t.Fatalf("convergenceTimeoutFor(nodeTimeout=%dms) = %v, want %v", tc.nodeTimeoutMs, got, tc.want)
			}
		})
	}
}
