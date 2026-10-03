package index

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestExactLookup(t *testing.T) {
	ix := New()
	ix.Add("u1", map[string]interface{}{"city": "Lisbon", "age": 30})
	ix.Add("u2", map[string]interface{}{"city": "Berlin", "age": 30})
	ix.Add("u3", map[string]interface{}{"city": "Lisbon", "age": 25})

	got := ix.Lookup("city", "Lisbon")
	if len(got) != 2 || got[0] != "u1" || got[1] != "u3" {
		t.Fatalf("expected [u1 u3], got %v", got)
	}
	if got := ix.Lookup("city", "Nowhere"); len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestRemovePurgesEntries(t *testing.T) {
	ix := New()
	ix.Add("a", map[string]interface{}{"k": "v"})
	ix.Add("b", map[string]interface{}{"k": "v"})
	ix.Remove("a")
	got := ix.Lookup("k", "v")
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected [b], got %v", got)
	}
}

func TestReAddReplacesOldValue(t *testing.T) {
	ix := New()
	ix.Add("a", map[string]interface{}{"city": "Lisbon"})
	// Simulate update: remove then add with new value.
	ix.Remove("a")
	ix.Add("a", map[string]interface{}{"city": "Berlin"})
	if got := ix.Lookup("city", "Lisbon"); len(got) != 0 {
		t.Fatalf("stale index entry: %v", got)
	}
	if got := ix.Lookup("city", "Berlin"); len(got) != 1 {
		t.Fatalf("expected Berlin hit, got %v", got)
	}
}

func TestRangeLookup(t *testing.T) {
	ix := New()
	ix.Add("a", map[string]interface{}{"age": 20})
	ix.Add("b", map[string]interface{}{"age": 40})
	ix.Add("c", map[string]interface{}{"age": 60})
	ix.Add("d", map[string]interface{}{"age": "not-a-number"})

	f := func(x float64) *float64 { return &x }
	got := ix.LookupRange("age", f(30), f(60))
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("expected [b c], got %v", got)
	}
	// Open-ended low bound.
	got = ix.LookupRange("age", nil, f(25))
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}
}

func TestIndexedFieldRestriction(t *testing.T) {
	ix := New()
	ix.SetIndexedFields([]string{"city"})
	ix.Add("a", map[string]interface{}{"city": "Lisbon", "secret": "x"})
	if got := ix.Lookup("city", "Lisbon"); len(got) != 1 {
		t.Fatalf("city should be indexed, got %v", got)
	}
	if got := ix.Lookup("secret", "x"); len(got) != 0 {
		t.Fatalf("secret should NOT be indexed, got %v", got)
	}
}

func TestStats(t *testing.T) {
	ix := New()
	ix.Add("a", map[string]interface{}{"k": "1"})
	ix.Add("b", map[string]interface{}{"k": "2"})
	st := ix.Stats()
	if st["k"] != 2 {
		t.Fatalf("expected 2 distinct values for k, got %v", st)
	}
}

// The reverse map (byID) is what keeps Remove O(fields on the doc)
// instead of O(collection). These tests pin that behaviour so it cannot
// silently regress into a full scan.

func TestRemoveDoesNotDisturbNeighbours(t *testing.T) {
	ix := New()
	const n = 5000
	for i := 0; i < n; i++ {
		ix.Add(docID(i), map[string]interface{}{"n": i, "tag": "common"})
	}
	// Removing one doc must leave every other membership intact, and
	// must leave its OWN entries gone (no stale reverse-map leftovers).
	ix.Remove(docID(n / 2))
	if got := ix.Lookup("n", n/2); len(got) != 0 {
		t.Fatalf("removed doc still indexed: %v", got)
	}
	// "common" shared by everyone else: n-1 members remain.
	if got := ix.Lookup("tag", "common"); len(got) != n-1 {
		t.Fatalf("expected %d neighbours, got %d", n-1, len(got))
	}
	// Removing an unknown id is a no-op, not a scan.
	ix.Remove("never-indexed")
	if got := ix.Lookup("tag", "common"); len(got) != n-1 {
		t.Fatalf("unknown remove corrupted the index: %d", len(got))
	}
}

func TestAddSelfCorrectsStaleValue(t *testing.T) {
	ix := New()
	ix.Add("a", map[string]interface{}{"city": "Lisbon", "age": 30})
	// Direct overwrite with no Remove in between: the old bucket must
	// still be dropped, otherwise the index reports two homes.
	ix.Add("a", map[string]interface{}{"city": "Berlin", "age": 31})
	if got := ix.Lookup("city", "Lisbon"); len(got) != 0 {
		t.Fatalf("stale city entry: %v", got)
	}
	if got := ix.Lookup("city", "Berlin"); len(got) != 1 {
		t.Fatalf("expected Berlin hit, got %v", got)
	}
	if got := ix.Lookup("age", 30); len(got) != 0 {
		t.Fatalf("stale age entry: %v", got)
	}
	if got := ix.Lookup("age", 31); len(got) != 1 {
		t.Fatalf("expected age=31 hit, got %v", got)
	}
}

func TestSetIndexedFieldsPurgesDroppedField(t *testing.T) {
	ix := New()
	ix.Add("a", map[string]interface{}{"city": "Lisbon", "secret": "x"})
	ix.SetIndexedFields([]string{"city"})
	if got := ix.Lookup("secret", "x"); len(got) != 0 {
		t.Fatalf("dropped field still indexed: %v", got)
	}
	// The reverse map must have forgotten it too, so a later Remove
	// does not try to clean a bucket that no longer exists.
	ix.Remove("a")
	if got := ix.Lookup("city", "Lisbon"); len(got) != 0 {
		t.Fatalf("city survived remove: %v", got)
	}
}

func docID(i int) string { return fmt.Sprintf("d%06d", i) }

// TestEncodeValueMatchesMarshal is the contract the fast path must
// never break: the index stores one encoding and queries encode with
// the other, so any divergence is a silently missed match.
func TestEncodeValueMatchesMarshal(t *testing.T) {
	long := strings.Repeat("x", 10000)
	cases := []interface{}{
		nil, true, false,
		0, -1, 42, 1 << 40, -9223372036854775808,
		"", "plain", "with spaces", "user-000123",
		`has "quotes"`, `back\slash`, "tab\there", "new\nline",
		"<html>&", "a+b=c", "ünïcode", "emoji 🚀", " line",
		1.5, 0.0, 1e21, -2.5e-9, json.Number("123.45"),
		long, "trailing space ",
	}
	for i, c := range cases {
		want, err := json.Marshal(c)
		if err != nil {
			t.Fatalf("case %d: marshal: %v", i, err)
		}
		got := encodeValue(c)
		if got != string(want) {
			t.Errorf("case %d (%#v): encodeValue = %s, json.Marshal = %s", i, c, got, want)
		}
	}
	// Randomised: mostly-plain strings with an occasional hostile byte.
	rnd := rand.New(rand.NewSource(1))
	alphabet := "abcXYZ019 \t\"\\<>&/\n" + "\u00e9\u2028"
	for i := 0; i < 500; i++ {
		n := rnd.Intn(24)
		var b strings.Builder
		for j := 0; j < n; j++ {
			b.WriteByte(alphabet[rnd.Intn(len(alphabet))])
		}
		s := b.String()
		want, _ := json.Marshal(s)
		if got := encodeValue(s); got != string(want) {
			t.Fatalf("random case %d: %q -> %s, want %s", i, s, got, want)
		}
	}
}

func BenchmarkEncodeValueString(b *testing.B) {
	v := strings.Repeat("user-profile-field-value/", 40)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if encodeValue(v) == "" {
			b.Fatal("empty")
		}
	}
}

func BenchmarkEncodeValueInt(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if encodeValue(i) == "" {
			b.Fatal("empty")
		}
	}
}
