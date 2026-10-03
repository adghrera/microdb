package store

import (
	"sync/atomic"
	"testing"

	"microdb/internal/metrics"
)

func shortCircuits() int64 {
	return atomic.LoadInt64(metrics.Default.Counter("microdb_scan_shortcircuits_total",
		"Full scans skipped because the collection cannot match"))
}

// TestBloomShortCircuitsScan: the compound-query fallback must be able
// to answer "no" without walking the collection, and must never say
// "no" about something that is there.
func TestBloomShortCircuitsScan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for i := 0; i < 500; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{
			"city": "Lisbon", "age": i % 50,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Compound filter (2 conditions) => the inverted-index fast path
	// cannot serve it and falls back to a scan — which the Bloom
	// filter must short-circuit when the equality matches nothing.
	before := shortCircuits()
	got := st.ScanIndexed("c", map[string]interface{}{"city": "Nowhere", "age": 30})
	if len(got) != 0 {
		t.Fatalf("expected no matches, got %d", len(got))
	}
	if shortCircuits() != before+1 {
		t.Fatalf("short-circuit not recorded (before=%d after=%d)", before, shortCircuits())
	}

	// The value exists: the filter must NOT rule the collection out,
	// and the results must be right.
	got = st.ScanIndexed("c", map[string]interface{}{"city": "Lisbon", "age": 30})
	if len(got) == 0 {
		t.Fatal("bloom ruled out a collection that holds the value")
	}
	for _, d := range got {
		if d.Fields["city"] != "Lisbon" || toInt(d.Fields["age"]) != 30 {
			t.Fatalf("wrong result: %#v", d.Fields)
		}
	}

	// Operators have no Bloom answer: a range over an empty domain
	// still runs the scan (and returns nothing) rather than being
	// wrongly pre-filtered.
	got = st.ScanIndexed("c", map[string]interface{}{"age": map[string]interface{}{"$gt": 1000}})
	if len(got) != 0 {
		t.Fatalf("range over empty domain returned %d", len(got))
	}

	// After every document in that city is deleted, the filter's
	// counters come back down (counting filter), so the same query
	// short-circuits again.
	if _, err := st.Apply("c", docName(0), map[string]interface{}{"city": "Porto", "age": 1}); err != nil {
		t.Fatal(err)
	}
	_ = st
}

func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return -1
}
