package bloom

import (
	"fmt"
	"testing"
)

func TestNoFalseNegatives(t *testing.T) {
	f := New(1000, 0.01)
	for i := 0; i < 1000; i++ {
		f.Add(token(i))
	}
	for i := 0; i < 1000; i++ {
		if !f.Has(token(i)) {
			t.Fatalf("false negative on %q — Has must never lose an added item", token(i))
		}
	}
}

// TestRemoveLeavesNoFalseNegative is the property the counting variant
// exists for: after a value is deleted, it must read as absent (a
// plain filter would keep answering "maybe" forever).
func TestRemoveLeavesNoFalseNegative(t *testing.T) {
	// Deterministic case: one token, removed — every bucket it touched
	// must go back to zero.
	f := New(10, 0.01)
	f.Add("only")
	f.Remove("only")
	if f.Has("only") {
		t.Fatal("removed token still present")
	}

	// Statistical case: removing half of a populated filter must not
	// resurrect the removed set beyond the filter's false-positive
	// budget (a handful of survivors is expected and harmless).
	g := New(500, 0.01)
	for i := 0; i < 500; i++ {
		g.Add(token(i))
	}
	live := map[string]bool{}
	for i := 0; i < 500; i++ {
		if i%2 == 1 {
			live[token(i)] = true
			continue
		}
		g.Remove(token(i))
	}
	survivors := 0
	for i := 0; i < 500; i++ {
		if i%2 == 0 && g.Has(token(i)) {
			survivors++
		}
	}
	if rate := float64(survivors) / 250; rate > 0.03 {
		t.Errorf("%.2f%% of removed tokens survived removal (budget 3%%)", rate*100)
	}
	for k := range live {
		if !g.Has(k) {
			t.Fatalf("live item %q lost after removals", k)
		}
	}
}

func TestFalsePositiveRateIsBounded(t *testing.T) {
	const n = 10000
	f := New(n, 0.01)
	for i := 0; i < n; i++ {
		f.Add(token(i))
	}
	// Probe tokens that were never added.
	fp, probes := 0, 20000
	for i := 0; i < probes; i++ {
		if f.Has(fmt.Sprintf("never-seen-%d", i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	t.Logf("false-positive rate: %.4f%% (target 1%%), filter=%d bytes for %d items", rate*100, f.Bytes(), n)
	if rate > 0.03 {
		t.Errorf("false-positive rate %.4f%% is too far above the 1%% target", rate*100)
	}
}

func TestResetAndNilSemantics(t *testing.T) {
	f := New(10, 0.01)
	f.Add("x")
	if !f.Has("x") {
		t.Fatal("lost x")
	}
	f.Reset()
	if f.Has("x") {
		t.Fatal("reset did not clear the filter")
	}
	if f.Len() != 0 {
		t.Fatalf("Len after reset = %d", f.Len())
	}

	// A nil filter must never rule anything out: it is the "filter not
	// attached" case, and a false negative there would silently drop
	// query results.
	var nilF *Filter
	nilF.Add("x")
	if !nilF.Has("x") {
		t.Fatal("nil filter must answer 'maybe' for everything")
	}
	if nilF.Bytes() != 0 || nilF.Len() != 0 {
		t.Fatal("nil filter stats must be zero")
	}
}

func TestSaturationDoesNotLoseItems(t *testing.T) {
	// Repeatedly add and remove the SAME tokens: 4-bit counters
	// saturate at 15, and a naive implementation would wrap and start
	// producing false negatives.
	f := New(100, 0.01)
	for round := 0; round < 40; round++ {
		for i := 0; i < 100; i++ {
			f.Add(token(i))
		}
		for i := 0; i < 100; i++ {
			f.Remove(token(i))
		}
	}
	for i := 0; i < 100; i++ {
		f.Add(token(i))
	}
	for i := 0; i < 100; i++ {
		if !f.Has(token(i)) {
			t.Fatalf("false negative after churn on %q", token(i))
		}
	}
}

func token(i int) string { return fmt.Sprintf("city\x00Lisbon-%d", i) }
