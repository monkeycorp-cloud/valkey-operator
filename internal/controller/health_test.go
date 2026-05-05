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
)

// clusterNodesFixture is a realistic CLUSTER NODES output used across multiple tests.
// primary-2 and replica-2 have an announce-hostname to exercise the hostname path.
const clusterNodesFixture = `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10922
cccccccccccccccccccccccccccccccccccccccc 10.0.0.3:6379@16379,hostname-3 master - 0 0 3 connected 10923-16383
dddddddddddddddddddddddddddddddddddddddd 10.0.0.4:6379@16379 slave aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0 0 4 connected
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee 10.0.0.5:6379@16379 slave bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 0 0 5 connected
ffffffffffffffffffffffffffffffffffffffff 10.0.0.6:6379@16379,hostname-6 slave cccccccccccccccccccccccccccccccccccccccc 0 0 6 connected
`

// --- parseClusterNodes ---

func TestParseClusterNodes(t *testing.T) {
	t.Run("CountNodes", func(t *testing.T) {
		nodes := parseClusterNodes(clusterNodesFixture)
		if len(nodes) != 6 {
			t.Fatalf("expected 6 nodes, got %d", len(nodes))
		}
	})

	t.Run("PrimaryFieldsExtracted", func(t *testing.T) {
		nodes := parseClusterNodes(clusterNodesFixture)
		p0 := nodes[0]
		if p0.id != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
			t.Errorf("id: got %q", p0.id)
		}
		if p0.ip != "10.0.0.1" {
			t.Errorf("ip: got %q", p0.ip)
		}
		if p0.flags != "master" {
			t.Errorf("flags: got %q", p0.flags)
		}
		if p0.masterID != "-" {
			t.Errorf("masterID: got %q", p0.masterID)
		}
		if p0.slots != "0-5460" {
			t.Errorf("slots: got %q", p0.slots)
		}
	})

	t.Run("ReplicaMasterIDSet", func(t *testing.T) {
		nodes := parseClusterNodes(clusterNodesFixture)
		r0 := nodes[3]
		if r0.masterID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
			t.Errorf("replica masterID: got %q", r0.masterID)
		}
		if r0.slots != "" {
			t.Errorf("replica slots should be empty, got %q", r0.slots)
		}
	})

	t.Run("HostnameExtractedWhenPresent", func(t *testing.T) {
		nodes := parseClusterNodes(clusterNodesFixture)
		p2 := nodes[2] // primary-2 has hostname-3
		if p2.hostname != "hostname-3" {
			t.Errorf("hostname: got %q, want hostname-3", p2.hostname)
		}
		r2 := nodes[5] // replica-2 has hostname-6
		if r2.hostname != "hostname-6" {
			t.Errorf("hostname: got %q, want hostname-6", r2.hostname)
		}
	})

	t.Run("HostnameEmptyWhenAbsent", func(t *testing.T) {
		nodes := parseClusterNodes(clusterNodesFixture)
		p0 := nodes[0]
		if p0.hostname != "" {
			t.Errorf("expected empty hostname, got %q", p0.hostname)
		}
	})

	t.Run("MultipleSlotsConcatenated", func(t *testing.T) {
		raw := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460 5461-10922\n"
		nodes := parseClusterNodes(raw)
		if len(nodes) != 1 {
			t.Fatalf("expected 1 node")
		}
		if nodes[0].slots != "0-5460 5461-10922" {
			t.Errorf("slots: got %q", nodes[0].slots)
		}
	})

	t.Run("ShortLineIgnored", func(t *testing.T) {
		raw := "too short line\n" + clusterNodesFixture
		nodes := parseClusterNodes(raw)
		if len(nodes) != 6 {
			t.Fatalf("short line should be ignored, got %d nodes", len(nodes))
		}
	})

	t.Run("EmptyLineIgnored", func(t *testing.T) {
		raw := "\n\n" + clusterNodesFixture
		nodes := parseClusterNodes(raw)
		if len(nodes) != 6 {
			t.Fatalf("empty lines should be ignored, got %d nodes", len(nodes))
		}
	})

	t.Run("VerbatimPrefixStripped", func(t *testing.T) {
		nodes := parseClusterNodes("txt:" + clusterNodesFixture)
		if len(nodes) != 6 {
			t.Fatalf("verbatim prefix should be stripped, got %d nodes", len(nodes))
		}
	})
}

// --- detectUnderReplicatedShards ---

func TestDetectUnderReplicatedShards(t *testing.T) {
	nodes := parseClusterNodes(clusterNodesFixture)

	t.Run("BalancedCluster", func(t *testing.T) {
		result := detectUnderReplicatedShards(nodes, 1)
		if len(result) != 0 {
			t.Fatalf("expected no under-replicated shards, got: %v", result)
		}
	})

	t.Run("PrimaryWithNoReplica", func(t *testing.T) {
		// Only primary-0 with no replicas.
		onlyPrimary := parseClusterNodes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460\n")
		result := detectUnderReplicatedShards(onlyPrimary, 1)
		if len(result) != 1 {
			t.Fatalf("expected 1 under-replicated shard, got: %v", result)
		}
		if !strings.Contains(result[0], "aaaaaaaa") {
			t.Errorf("result should contain truncated node ID, got: %v", result)
		}
	})

	t.Run("OverReplicatedNotFlagged", func(t *testing.T) {
		// Two replicas for one primary, expected 1 → not flagged.
		raw := `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460
dddddddddddddddddddddddddddddddddddddddd 10.0.0.4:6379@16379 slave aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0 0 4 connected
eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee 10.0.0.5:6379@16379 slave aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0 0 5 connected
`
		twoReplicas := parseClusterNodes(raw)
		result := detectUnderReplicatedShards(twoReplicas, 1)
		if len(result) != 0 {
			t.Fatalf("over-replicated primary should not be flagged, got: %v", result)
		}
	})

	t.Run("FailedPrimaryIgnored", func(t *testing.T) {
		raw := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master,fail - 0 0 1 connected 0-5460\n"
		failedNodes := parseClusterNodes(raw)
		result := detectUnderReplicatedShards(failedNodes, 1)
		if len(result) != 0 {
			t.Fatalf("failed primary should be ignored, got: %v", result)
		}
	})

	t.Run("NoaddrPrimaryIgnored", func(t *testing.T) {
		raw := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master,noaddr - 0 0 1 connected 0-5460\n"
		noaddrNodes := parseClusterNodes(raw)
		result := detectUnderReplicatedShards(noaddrNodes, 1)
		if len(result) != 0 {
			t.Fatalf("noaddr primary should be ignored, got: %v", result)
		}
	})

	t.Run("MessageFormat", func(t *testing.T) {
		onlyPrimary := parseClusterNodes("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460\n")
		result := detectUnderReplicatedShards(onlyPrimary, 1)
		if len(result) != 1 {
			t.Fatalf("expected 1 result")
		}
		// Format: "aaaaaaaa(0/1 replicas)"
		if !strings.Contains(result[0], "(0/1 replicas)") {
			t.Errorf("unexpected format: %q", result[0])
		}
	})
}
