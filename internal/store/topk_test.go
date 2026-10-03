package store

import (
	"fmt"
	"sort"
	"testing"
)

// refTopK is the old shape: walk every match, stable-sort, cut. Used as
// the oracle for ScanTopK.
func refTopK(st *Store, col string, filter map[string]interface{}, sortField string, desc bool, n int) ([]*Doc, bool) {
	docs := st.ScanIndexed(col, filter)
	sort.SliceStable(docs, func(i, j int) bool {
		c := CompareValues(docs[i].Fields[sortField], docs[j].Fields[sortField])
		if c == 0 {
			return docs[i].ID < docs[j].ID
		}
		if desc {
			return c > 0
		}
		return c < 0
	})
	trunc := len(docs) > n
	if trunc {
		docs = docs[:n]
	}
	return docs, trunc
}

// TestScanTopKMatchesSortThenCut pins the pushdown to the exact
// results the previous implementation produced, including ties (which
// are where a heap usually diverges from a stable sort).
func TestScanTopKMatchesSortThenCut(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Duplicate sort values (ages repeat), mixed types, ids in
	// non-alphabetical write order.
	ages := []int{30, 25, 30, 40, 25, 40, 30, 99, 25, 1}
	for i := 0; i < 200; i++ {
		fields := map[string]interface{}{
			"age":  ages[i%len(ages)],
			"name": fmt.Sprintf("user-%03d", (i*7)%50),
		}
		if _, err := st.Apply("c", fmt.Sprintf("id-%02d", (i*13)%200), fields); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		sortField string
		desc      bool
		n         int
	}{
		{"age", false, 5},
		{"age", true, 5},
		{"age", false, 1},
		{"name", false, 7},
		{"name", true, 13},
		{"age", false, 200},
		{"age", false, 500}, // window bigger than the collection
	}
	filters := []map[string]interface{}{
		{},
		{"age": 30},
		{"age": map[string]interface{}{"$gte": 25}, "name": map[string]interface{}{"$regex": "^user"}},
	}
	for _, f := range filters {
		for _, c := range cases {
			want, wantTrunc := refTopK(st, "c", f, c.sortField, c.desc, c.n)
			got, gotTrunc := st.ScanTopK("c", f, nil, c.sortField, c.desc, c.n)
			if len(got) != len(want) {
				t.Fatalf("filter=%v sort=%s desc=%v n=%d: got %d docs, want %d",
					f, c.sortField, c.desc, c.n, len(got), len(want))
			}
			for i := range want {
				if got[i].ID != want[i].ID {
					t.Fatalf("filter=%v sort=%s desc=%v n=%d at %d: got %s, want %s",
						f, c.sortField, c.desc, c.n, i, got[i].ID, want[i].ID)
				}
			}
			if gotTrunc != wantTrunc {
				t.Errorf("filter=%v sort=%s n=%d: truncated=%v, want %v",
					f, c.sortField, c.n, gotTrunc, wantTrunc)
			}
		}
	}
}

// TestScanTopKTransformSeesMigratedFields: ordering must happen on the
// document the client will receive, not the one on disk.
func TestScanTopKTransformSeesMigratedFields(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for i := 0; i < 10; i++ {
		if _, err := st.Apply("c", fmt.Sprintf("k%d", i), map[string]interface{}{"v": 10 - i}); err != nil {
			t.Fatal(err)
		}
	}
	// The "migration" renames v -> rank and doubles it; ordering by
	// rank must use the transformed value.
	transform := func(d *Doc) *Doc {
		if v, ok := d.Fields["v"].(int); ok {
			cp := *d
			cp.Fields = map[string]interface{}{"rank": v * 2}
			return &cp
		}
		return d
	}
	got, _ := st.ScanTopK("c", nil, transform, "rank", false, 3)
	if len(got) != 3 {
		t.Fatalf("got %d docs", len(got))
	}
	// v ascending after transform: rank 0,2,4 -> ids k9 (v=1*2=2)?
	// v = 10-i, so smallest v is i=9 -> rank 18? No: v*2, asc => smallest v first => i=9 (v=1) => rank 2.
	if got[0].ID != "k9" || got[0].Fields["rank"] != 2 {
		t.Fatalf("first doc = %s/%#v, want k9/rank=2", got[0].ID, got[0].Fields)
	}
	if _, hasV := got[0].Fields["v"]; hasV {
		t.Error("sort saw the untransformed document")
	}
}
