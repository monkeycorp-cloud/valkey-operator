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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "valkey-operator/api/v1alpha1"
)

// --- validatePassword ---

func TestValidatePassword(t *testing.T) {
	t.Run("ValidPassword", func(t *testing.T) {
		if err := validatePassword("s3cr3t!@#$%^&*()"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("EmptyPasswordValid", func(t *testing.T) {
		if err := validatePassword(""); err != nil {
			t.Fatalf("empty password should be allowed: %v", err)
		}
	})

	t.Run("NewlineRejected", func(t *testing.T) {
		if err := validatePassword("pass\nword"); err == nil {
			t.Fatal("expected error for password with newline")
		}
	})

	t.Run("CarriageReturnRejected", func(t *testing.T) {
		if err := validatePassword("pass\rword"); err == nil {
			t.Fatal("expected error for password with carriage return")
		}
	})
}

// --- computeIOThreads ---

func mkSpecWithCPU(cpu string, ioThreads *int32) *cachev1alpha1.ValkeyClusterSpec {
	spec := &cachev1alpha1.ValkeyClusterSpec{
		Config: &cachev1alpha1.ValkeyConfigSpec{
			IOThreads: ioThreads,
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{},
		},
	}
	if cpu != "" {
		spec.Resources.Limits[corev1.ResourceCPU] = resource.MustParse(cpu)
	}
	return spec
}

func int32ptr(v int32) *int32 { return &v }

func TestComputeIOThreads(t *testing.T) {
	cases := []struct {
		name      string
		cpu       string
		ioThreads *int32
		want      int32
	}{
		{"NilConfig", "", nil, 1},
		{"NoCPULimit", "", nil, 1},
		{"Under4CPUs_2000m", "2000m", nil, 1},
		{"Exactly4CPUs", "4000m", nil, 2},
		{"Between4and8_6CPUs", "6", nil, 2},
		{"Exactly8CPUs", "8", nil, 4},
		{"Between8and16_12CPUs", "12", nil, 4},
		{"Exactly16CPUs", "16", nil, 8},
		{"Over16CPUs_32CPUs", "32", nil, 8},
		{"ExplicitOverride_1", "16", int32ptr(1), 1},
		{"ExplicitOverride_4", "2", int32ptr(4), 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var spec *cachev1alpha1.ValkeyClusterSpec
			if tc.name == "NilConfig" {
				spec = &cachev1alpha1.ValkeyClusterSpec{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{},
					},
				}
			} else {
				spec = mkSpecWithCPU(tc.cpu, tc.ioThreads)
			}
			got := computeIOThreads(spec)
			if got != tc.want {
				t.Fatalf("computeIOThreads(cpu=%q, ioThreads=%v) = %d, want %d", tc.cpu, tc.ioThreads, got, tc.want)
			}
		})
	}
}

// --- computeMaxmemory ---

func mkSpecWithMemory(memory string, ratio int32) *cachev1alpha1.ValkeyClusterSpec {
	spec := &cachev1alpha1.ValkeyClusterSpec{
		Config: &cachev1alpha1.ValkeyConfigSpec{
			MaxmemoryRatio: ratio,
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{},
		},
	}
	if memory != "" {
		spec.Resources.Limits[corev1.ResourceMemory] = resource.MustParse(memory)
	}
	return spec
}

func TestComputeMaxmemory(t *testing.T) {
	cases := []struct {
		name   string
		memory string
		ratio  int32
		want   int64
	}{
		{"NoMemoryLimit", "", 80, 0},
		{"Ratio80_256Mi", "256Mi", 80, 214748364}, // 268435456 * 80 / 100
		{"Ratio50_1Gi", "1Gi", 50, 536870912},     // 1073741824 * 50 / 100
		{"RatioZeroDefaultTo80_256Mi", "256Mi", 0, 214748364},
		{"RatioOver100DefaultTo80_256Mi", "256Mi", 101, 214748364},
		{"Ratio99_256Mi", "256Mi", 99, 265751101}, // 268435456 * 99 / 100
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := mkSpecWithMemory(tc.memory, tc.ratio)
			got := computeMaxmemory(spec)
			if got != tc.want {
				t.Fatalf("computeMaxmemory(memory=%q, ratio=%d) = %d, want %d", tc.memory, tc.ratio, got, tc.want)
			}
		})
	}

	t.Run("NilConfig", func(t *testing.T) {
		spec := &cachev1alpha1.ValkeyClusterSpec{}
		if got := computeMaxmemory(spec); got != 0 {
			t.Fatalf("nil config should return 0, got %d", got)
		}
	})
}
