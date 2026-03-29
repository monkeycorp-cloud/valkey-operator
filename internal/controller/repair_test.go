package controller

import (
	"testing"
)

// --- pickMasterForReplica ---

func TestPickMasterForReplica(t *testing.T) {
	t.Run("BalancedCluster_TiebreakerByID", func(t *testing.T) {
		// All primaries have 1 replica — tiebreaker: lowest node ID wins.
		id, err := pickMasterForReplica(clusterNodesFixture, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// "aaa..." < "bbb..." < "ccc..." — expect aaaa...
		if id != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
			t.Errorf("expected aaaa... (lowest ID), got %q", id)
		}
	})

	t.Run("UnderReplicatedPrimaryChosen", func(t *testing.T) {
		// primary-b has 0 replicas, primary-a has 1 → pick primary-b.
		raw := `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10922
dddddddddddddddddddddddddddddddddddddddd 10.0.0.4:6379@16379 slave aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0 0 4 connected
`
		id, err := pickMasterForReplica(raw, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
			t.Errorf("expected bbbb... (0 replicas), got %q", id)
		}
	})

	t.Run("FailedPrimarySkipped", func(t *testing.T) {
		raw := `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master,fail - 0 0 1 connected 0-5460
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10922
`
		id, err := pickMasterForReplica(raw, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
			t.Errorf("expected bbbb... (only healthy primary), got %q", id)
		}
	})

	t.Run("MasterWithoutSlotsSkipped", func(t *testing.T) {
		// After rolling update: restarted node gossips as master but owns no slots.
		// It must not be selected as a CLUSTER REPLICATE target.
		raw := `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10922
`
		id, err := pickMasterForReplica(raw, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
			t.Errorf("expected bbbb... (master with slots), got %q", id)
		}
	})

	t.Run("AllMastersWithoutSlotsReturnsError", func(t *testing.T) {
		raw := `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master - 0 0 1 connected
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 10.0.0.2:6379@16379 master - 0 0 2 connected
`
		_, err := pickMasterForReplica(raw, 1)
		if err == nil {
			t.Fatal("expected error when all masters have no slots")
		}
	})

	t.Run("NoaddrPrimarySkipped", func(t *testing.T) {
		raw := `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 10.0.0.1:6379@16379 master,noaddr - 0 0 1 connected 0-5460
bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb 10.0.0.2:6379@16379 master - 0 0 2 connected 5461-10922
`
		id, err := pickMasterForReplica(raw, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
			t.Errorf("expected bbbb..., got %q", id)
		}
	})

	t.Run("NoPrimaryReturnsError", func(t *testing.T) {
		raw := `dddddddddddddddddddddddddddddddddddddddd 10.0.0.4:6379@16379 slave aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa 0 0 4 connected
`
		_, err := pickMasterForReplica(raw, 1)
		if err == nil {
			t.Fatal("expected error when no healthy primary exists")
		}
	})

	t.Run("EmptyInputReturnsError", func(t *testing.T) {
		_, err := pickMasterForReplica("", 1)
		if err == nil {
			t.Fatal("expected error on empty input")
		}
	})
}

// --- buildKnownAddrSets ---

func TestBuildKnownAddrSets(t *testing.T) {
	t.Run("AllIPsExtracted", func(t *testing.T) {
		ips, _ := buildKnownAddrSets(clusterNodesFixture)
		for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5", "10.0.0.6"} {
			if _, ok := ips[ip]; !ok {
				t.Errorf("expected IP %q in set, got: %v", ip, ips)
			}
		}
	})

	t.Run("HostnamesExtractedWhenPresent", func(t *testing.T) {
		_, hostnames := buildKnownAddrSets(clusterNodesFixture)
		for _, h := range []string{"hostname-3", "hostname-6"} {
			if _, ok := hostnames[h]; !ok {
				t.Errorf("expected hostname %q in set, got: %v", h, hostnames)
			}
		}
	})

	t.Run("NodeWithoutHostnameNotInHostnames", func(t *testing.T) {
		_, hostnames := buildKnownAddrSets(clusterNodesFixture)
		// Nodes 0,1,3,4 have no hostname — their IPs should not appear in hostnames.
		for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.4", "10.0.0.5"} {
			if _, ok := hostnames[ip]; ok {
				t.Errorf("IP %q should not be in hostnames set", ip)
			}
		}
	})

	t.Run("ShortLineIgnored", func(t *testing.T) {
		raw := "only-one-field\n" + clusterNodesFixture
		ips, _ := buildKnownAddrSets(raw)
		if len(ips) != 6 {
			t.Fatalf("expected 6 IPs, got %d", len(ips))
		}
	})

	t.Run("VerbatimPrefixStripped", func(t *testing.T) {
		ips, _ := buildKnownAddrSets("txt:" + clusterNodesFixture)
		if len(ips) != 6 {
			t.Fatalf("verbatim prefix should be stripped, got %d IPs", len(ips))
		}
	})
}
