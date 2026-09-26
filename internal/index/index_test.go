package index

import (
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
