package controller

import (
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- aclHash ---

func TestAclHash(t *testing.T) {
	user := func(name, password string, keys []string, cmds string) resolvedACLUser {
		return resolvedACLUser{name: name, password: password, keyPatterns: keys, commands: cmds}
	}
	base := []resolvedACLUser{user("app", "pass1", []string{"~*"}, "+@read")}

	t.Run("SameInputsSameHash", func(t *testing.T) {
		h1 := aclHash("op", "met", base)
		h2 := aclHash("op", "met", base)
		if h1 != h2 {
			t.Fatalf("expected identical hashes, got %q and %q", h1, h2)
		}
	})

	t.Run("DifferentOperatorPassword", func(t *testing.T) {
		if aclHash("op1", "met", base) == aclHash("op2", "met", base) {
			t.Fatal("expected different hashes for different operator passwords")
		}
	})

	t.Run("DifferentMetricsPassword", func(t *testing.T) {
		if aclHash("op", "met1", base) == aclHash("op", "met2", base) {
			t.Fatal("expected different hashes for different metrics passwords")
		}
	})

	t.Run("DifferentUserName", func(t *testing.T) {
		u1 := []resolvedACLUser{user("alice", "p", []string{"~*"}, "+@read")}
		u2 := []resolvedACLUser{user("bob", "p", []string{"~*"}, "+@read")}
		if aclHash("op", "met", u1) == aclHash("op", "met", u2) {
			t.Fatal("expected different hashes for different user names")
		}
	})

	t.Run("DifferentUserPassword", func(t *testing.T) {
		u1 := []resolvedACLUser{user("app", "pass1", []string{"~*"}, "+@read")}
		u2 := []resolvedACLUser{user("app", "pass2", []string{"~*"}, "+@read")}
		if aclHash("op", "met", u1) == aclHash("op", "met", u2) {
			t.Fatal("expected different hashes for different user passwords")
		}
	})

	t.Run("DifferentKeyPatterns", func(t *testing.T) {
		u1 := []resolvedACLUser{user("app", "p", []string{"~*"}, "+@read")}
		u2 := []resolvedACLUser{user("app", "p", []string{"~app:*"}, "+@read")}
		if aclHash("op", "met", u1) == aclHash("op", "met", u2) {
			t.Fatal("expected different hashes for different key patterns")
		}
	})

	t.Run("DifferentCommands", func(t *testing.T) {
		u1 := []resolvedACLUser{user("app", "p", []string{"~*"}, "+@read")}
		u2 := []resolvedACLUser{user("app", "p", []string{"~*"}, "+@write")}
		if aclHash("op", "met", u1) == aclHash("op", "met", u2) {
			t.Fatal("expected different hashes for different commands")
		}
	})

	t.Run("EmptyUsersStable", func(t *testing.T) {
		h1 := aclHash("op", "met", nil)
		h2 := aclHash("op", "met", nil)
		if h1 != h2 {
			t.Fatalf("expected stable hash for empty users, got %q and %q", h1, h2)
		}
		if len(h1) == 0 {
			t.Fatal("expected non-empty hash")
		}
	})

	t.Run("UserOrderMatters", func(t *testing.T) {
		u1 := []resolvedACLUser{user("alice", "pa", []string{"~*"}, "+@read"), user("bob", "pb", []string{"~*"}, "+@write")}
		u2 := []resolvedACLUser{user("bob", "pb", []string{"~*"}, "+@write"), user("alice", "pa", []string{"~*"}, "+@read")}
		if aclHash("op", "met", u1) == aclHash("op", "met", u2) {
			t.Fatal("expected different hashes when user order differs")
		}
	})

	t.Run("HashLengthIs16Chars", func(t *testing.T) {
		h := aclHash("op", "met", base)
		if len(h) != 16 {
			t.Fatalf("expected 16-char hash, got %d chars: %q", len(h), h)
		}
	})

	t.Run("EmptyPasswordsDoNotPanic", func(t *testing.T) {
		h := aclHash("", "", nil)
		if len(h) != 16 {
			t.Fatalf("expected 16-char hash, got %d chars: %q", len(h), h)
		}
	})
}

// --- buildACLRules ---

func TestBuildACLRules(t *testing.T) {
	contains := func(rules []string, s string) bool {
		return slices.Contains(rules, s)
	}

	t.Run("PasswordRuleIsFirst", func(t *testing.T) {
		rules := buildACLRules(resolvedACLUser{name: "u", password: "secret", keyPatterns: []string{"~*"}, commands: "+@read"})
		if len(rules) == 0 || rules[0] != ">secret" {
			t.Fatalf("expected first rule to be >secret, got %v", rules)
		}
	})

	t.Run("ResetchannelsPresent", func(t *testing.T) {
		rules := buildACLRules(resolvedACLUser{name: "u", password: "p", keyPatterns: []string{"~*"}, commands: "+@read"})
		if !contains(rules, "resetchannels") {
			t.Fatalf("expected resetchannels in rules: %v", rules)
		}
	})

	t.Run("ChannelWildcardPresent", func(t *testing.T) {
		rules := buildACLRules(resolvedACLUser{name: "u", password: "p", keyPatterns: []string{"~*"}, commands: "+@read"})
		if !contains(rules, "&*") {
			t.Fatalf("expected &* in rules: %v", rules)
		}
	})

	t.Run("DefaultKeyPatternWhenEmpty", func(t *testing.T) {
		rules := buildACLRules(resolvedACLUser{name: "u", password: "p", keyPatterns: nil, commands: "+@read"})
		if !contains(rules, "~*") {
			t.Fatalf("expected ~* default key pattern, got: %v", rules)
		}
	})

	t.Run("ExplicitKeyPatternsNoDefault", func(t *testing.T) {
		rules := buildACLRules(resolvedACLUser{name: "u", password: "p", keyPatterns: []string{"~app:*", "~session:*"}, commands: "+@read"})
		if !contains(rules, "~app:*") || !contains(rules, "~session:*") {
			t.Fatalf("expected explicit key patterns in rules: %v", rules)
		}
		if contains(rules, "~*") {
			t.Fatalf("expected no ~* default when explicit patterns provided: %v", rules)
		}
	})

	t.Run("DefaultCommandsWhenEmpty", func(t *testing.T) {
		rules := buildACLRules(resolvedACLUser{name: "u", password: "p", keyPatterns: []string{"~*"}, commands: ""})
		if !contains(rules, "+@all") {
			t.Fatalf("expected +@all default commands, got: %v", rules)
		}
	})

	t.Run("MultipleCommandsSplitBySpace", func(t *testing.T) {
		rules := buildACLRules(resolvedACLUser{name: "u", password: "p", keyPatterns: []string{"~*"}, commands: "+get +set"})
		if !contains(rules, "+get") || !contains(rules, "+set") {
			t.Fatalf("expected +get and +set as separate rules: %v", rules)
		}
		if contains(rules, "+@all") {
			t.Fatalf("expected no +@all when commands explicitly set: %v", rules)
		}
	})
}

// --- shouldSkipACLPod ---

func TestShouldSkipACLPod(t *testing.T) {
	now := time.Now()
	old := metav1.NewTime(now.Add(-2 * time.Minute))
	young := metav1.NewTime(now.Add(-1 * time.Second))
	exactBoundary := metav1.NewTime(now.Add(-aclSkipAge + 1*time.Millisecond))
	justOver := metav1.NewTime(now.Add(-aclSkipAge - 1*time.Millisecond))

	cases := []struct {
		name        string
		hashChanged bool
		startTime   *metav1.Time
		want        bool
	}{
		{"NilStartTimeHashUnchanged", false, nil, false},
		{"NilStartTimeHashChanged", true, nil, false},
		{"YoungPodHashUnchanged", false, &young, false},
		{"YoungPodHashChanged", true, &young, false},
		{"OldPodHashUnchanged", false, &old, true},
		{"OldPodHashChanged", true, &old, false},
		{"ExactBoundaryNotSkipped", false, &exactBoundary, false},
		{"JustOverBoundarySkipped", false, &justOver, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldSkipACLPod(tc.hashChanged, tc.startTime)
			if got != tc.want {
				t.Fatalf("shouldSkipACLPod(%v, %v) = %v, want %v", tc.hashChanged, tc.startTime, got, tc.want)
			}
		})
	}
}
