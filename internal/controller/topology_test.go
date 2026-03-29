package controller

import (
	"testing"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// --- topologyScore ---

func TestTopologyScore(t *testing.T) {
	cases := []struct {
		name           string
		candidateZone  string
		failedZone     string
		preferredZones []string
		avoidSameZone  bool
		want           float64
	}{
		{
			name:          "SameAsFailedAvoidTrue",
			candidateZone: "zone-a", failedZone: "zone-a",
			preferredZones: []string{"zone-a"}, avoidSameZone: true,
			want: 0.0,
		},
		{
			name:          "SameAsFailedAvoidFalse_InPreferred",
			candidateZone: "zone-a", failedZone: "zone-a",
			preferredZones: []string{"zone-a"}, avoidSameZone: false,
			want: 1.0, // avoidSameZone disabled → preferred check runs
		},
		{
			name:          "InPreferredZones",
			candidateZone: "zone-b", failedZone: "zone-a",
			preferredZones: []string{"zone-b", "zone-c"}, avoidSameZone: true,
			want: 1.0,
		},
		{
			name:          "NeutralZone",
			candidateZone: "zone-c", failedZone: "zone-a",
			preferredZones: []string{"zone-b"}, avoidSameZone: true,
			want: 0.5,
		},
		{
			name:          "EmptyPreferredZones",
			candidateZone: "zone-b", failedZone: "zone-a",
			preferredZones: nil, avoidSameZone: true,
			want: 0.5,
		},
		{
			name:          "EmptyCandidateZoneAvoidTrue",
			candidateZone: "", failedZone: "zone-a",
			preferredZones: nil, avoidSameZone: true,
			want: 0.5, // guard: candidateZone != "" prevents 0.0
		},
		{
			name:          "DifferentZoneNotInPreferred",
			candidateZone: "zone-d", failedZone: "zone-a",
			preferredZones: []string{"zone-b", "zone-c"}, avoidSameZone: true,
			want: 0.5,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := topologyScore(tc.candidateZone, tc.failedZone, tc.preferredZones, tc.avoidSameZone)
			if got != tc.want {
				t.Fatalf("topologyScore(%q, %q, %v, %v) = %v, want %v",
					tc.candidateZone, tc.failedZone, tc.preferredZones, tc.avoidSameZone, got, tc.want)
			}
		})
	}
}

// --- preferredZonesForIndex ---

func TestPreferredZonesForIndex(t *testing.T) {
	topo := &cachev1alpha1.ValkeyTopologySpec{
		PodAssignments: []cachev1alpha1.TopologyPodAssignment{
			{PodIndex: 0, PreferredValues: []string{"zone-a"}},
			{PodIndex: 1, PreferredValues: []string{"zone-b"}},
			{PodIndex: 2, PreferredValues: []string{"zone-c", "zone-d"}},
		},
	}

	t.Run("IndexPresent", func(t *testing.T) {
		zones := preferredZonesForIndex(topo, 0)
		if len(zones) != 1 || zones[0] != "zone-a" {
			t.Errorf("expected [zone-a], got %v", zones)
		}
	})

	t.Run("IndexPresentMultipleZones", func(t *testing.T) {
		zones := preferredZonesForIndex(topo, 2)
		if len(zones) != 2 || zones[0] != "zone-c" || zones[1] != "zone-d" {
			t.Errorf("expected [zone-c zone-d], got %v", zones)
		}
	})

	t.Run("IndexAbsentReturnsNil", func(t *testing.T) {
		zones := preferredZonesForIndex(topo, 99)
		if zones != nil {
			t.Errorf("expected nil for absent index, got %v", zones)
		}
	})

	t.Run("EmptyAssignmentsReturnsNil", func(t *testing.T) {
		empty := &cachev1alpha1.ValkeyTopologySpec{}
		zones := preferredZonesForIndex(empty, 0)
		if zones != nil {
			t.Errorf("expected nil for empty assignments, got %v", zones)
		}
	})
}
