package store

import (
	"fmt"
	"testing"
)

func aggStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	// 100 docs: age = i%50, city = Lisbon|Porto by parity
	for i := 0; i < 100; i++ {
		city := "Lisbon"
		if i%2 == 1 {
			city = "Porto"
		}
		if _, err := st.Apply("c", fmt.Sprintf("k%03d", i), map[string]interface{}{
			"n": i, "age": i % 50, "city": city,
		}); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

func TestAggregateKinds(t *testing.T) {
	st := aggStore(t)
	noFilter := map[string]interface{}{}

	if got := st.Aggregate("c", noFilter, "count", "", "", nil); got.Count != 100 {
		t.Errorf("count = %d, want 100", got.Count)
	}
	if got := st.Aggregate("c", noFilter, "sum", "n", "", nil); got.Sum != 4950 {
		t.Errorf("sum = %v, want 4950", got.Sum)
	}
	avg := st.Aggregate("c", noFilter, "avg", "n", "", nil)
	if avg.Value().(float64) != 49.5 {
		t.Errorf("avg = %v, want 49.5", avg.Value())
	}
	if got := st.Aggregate("c", noFilter, "max", "n", "", nil); got.Value().(int) != 99 {
		t.Errorf("max = %v, want 99", got.Value())
	}
	if got := st.Aggregate("c", noFilter, "min", "n", "", nil); got.Value().(int) != 0 {
		t.Errorf("min = %v, want 0", got.Value())
	}

	// Filtered: Porto is odd i -> 50 docs, sum of odd 0..99 = 2500.
	filter := map[string]interface{}{"city": "Porto"}
	if got := st.Aggregate("c", filter, "count", "", "", nil); got.Count != 50 {
		t.Errorf("filtered count = %d, want 50", got.Count)
	}
	if got := st.Aggregate("c", filter, "sum", "n", "", nil); got.Sum != 2500 {
		t.Errorf("filtered sum = %v, want 2500", got.Sum)
	}

	// A value that does not exist must answer instantly (bloom
	// prefilter) and still be correct: zero.
	missing := st.Aggregate("c", map[string]interface{}{"city": "Madrid"}, "count", "", "", nil)
	if missing.Count != 0 {
		t.Errorf("absent-value count = %d, want 0", missing.Count)
	}
	// Unknown kind is rejected rather than silently counting.
	if got := st.Aggregate("c", noFilter, "median", "n", "", nil); got.Count != 0 {
		t.Errorf("unknown kind returned count %d, want the empty result", got.Count)
	}
}

func TestAggregateGroupBy(t *testing.T) {
	st := aggStore(t)
	got := st.Aggregate("c", nil, "count", "", "city", nil)
	if len(got.Groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(got.Groups))
	}
	for _, k := range got.SortedGroups() {
		if got.Groups[k].Count != 50 {
			t.Errorf("group %q count = %d, want 50", k, got.Groups[k].Count)
		}
		// A bucket that loses its kind counts as zero once merged and
		// renders as nil — both are silent wrong answers.
		if got.Groups[k].Kind != "count" {
			t.Errorf("group %q kind = %q, want count", k, got.Groups[k].Kind)
		}
		if v := got.Groups[k].Value(); v != int64(50) && v != 50 {
			t.Errorf("group %q Value() = %#v, want 50", k, v)
		}
	}
	// Grouped sum over ages.
	byDecade := st.Aggregate("c", nil, "sum", "n", "city", nil)
	if byDecade.Groups[`"Lisbon"`].Sum == 0 {
		t.Error("grouped sum is empty")
	}
}

// TestAggregateMergeIsOrderIndependent is what makes scatter-gather
// aggregates correct: partials can arrive in any order, twice, or
// only some of them.
func TestAggregateMergeIsOrderIndependent(t *testing.T) {
	st := aggStore(t)
	whole := st.Aggregate("c", nil, "sum", "n", "", nil)

	// Split the same collection into two halves and merge both orders.
	left := st.Aggregate("c", map[string]interface{}{"n": map[string]interface{}{"$lt": 50}}, "sum", "n", "", nil)
	right := st.Aggregate("c", map[string]interface{}{"n": map[string]interface{}{"$gte": 50}}, "sum", "n", "", nil)

	a := Aggregate{Kind: "sum", Field: "n"}
	a.Merge(left)
	a.Merge(right)
	b := Aggregate{Kind: "sum", Field: "n"}
	b.Merge(right)
	b.Merge(left)
	if a.Sum != whole.Sum || b.Sum != whole.Sum {
		t.Fatalf("merged sums %v/%v != whole %v", a.Sum, b.Sum, whole.Sum)
	}

	// Re-merging a duplicate must not double-count: partials carry
	// counts, so merging is idempotent ONLY if the caller dedupes —
	// document the actual contract: Merge is associative, and the
	// coordinator merges each member exactly once.
	if left.Count+right.Count != whole.Count {
		t.Errorf("partial counts %d+%d != %d", left.Count, right.Count, whole.Count)
	}
}

func TestAggregateGroupsMergeAcrossShards(t *testing.T) {
	a := Aggregate{Kind: "count", Groups: map[string]Aggregate{
		`"Lisbon"`: {Kind: "count", Count: 3},
	}}
	b := Aggregate{Kind: "count", Groups: map[string]Aggregate{
		`"Lisbon"`: {Kind: "count", Count: 4},
		`"Porto"`:  {Kind: "count", Count: 2},
	}}
	a.Merge(b)
	if a.Groups[`"Lisbon"`].Count != 7 {
		t.Errorf("merged Lisbon = %d, want 7", a.Groups[`"Lisbon"`].Count)
	}
	if a.Groups[`"Porto"`].Count != 2 {
		t.Errorf("merged Porto = %d, want 2", a.Groups[`"Porto"`].Count)
	}
}

// TestAggregateMergeToleratesEmptyBucketKind pins the regression: a
// group bucket that arrives without its kind must still count, or a
// coordinator silently reports zeroed groups.
func TestAggregateMergeToleratesEmptyBucketKind(t *testing.T) {
	p := Aggregate{Kind: "count", Groups: map[string]Aggregate{
		`"Lisbon"`: {Kind: "", Count: 5},
	}}
	p.Merge(Aggregate{Kind: "count", Groups: map[string]Aggregate{
		`"Lisbon"`: {Kind: "count", Count: 6},
	}})
	if p.Groups[`"Lisbon"`].Count != 11 {
		t.Fatalf("merged bucket = %d, want 11 (an empty kind must not drop the partial)", p.Groups[`"Lisbon"`].Count)
	}
	if v := p.Groups[`"Lisbon"`].Value(); v != int64(11) && v != 11 {
		t.Fatalf("bucket Value() = %#v, want 11", v)
	}
}
