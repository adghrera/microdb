package ranges

import (
	"fmt"
	"reflect"
	"testing"
)

// fakeBase mimics the vnode ring: deterministic owners by string key.
type fakeBase struct{ nodes []string }

func (f *fakeBase) Owners(key string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < len(f.nodes) && len(out) < n; i++ {
		out = append(out, f.nodes[(len(key)+i)%len(f.nodes)])
	}
	return out
}

func load(hot map[int]float64, base float64) []float64 {
	b := make([]float64, Buckets)
	for i := range b {
		b[i] = base
	}
	for k, v := range hot {
		b[k] = v
	}
	return b
}

func TestUniformLoadBalancedPlan(t *testing.T) {
	// Uniform load produces a balanced full partition: every range
	// carries at most a node's fair share, and the ranges tile the
	// whole token space.
	p := PlanFor([]string{"a", "b", "c"}, 3, load(nil, 1.0), 1, &fakeBase{[]string{"a", "b", "c"}})
	if p == nil {
		t.Fatal("expected a balanced plan under uniform load")
	}
	total := 0.0
	for _, v := range b256(1.0) {
		total += v
	}
	fair := total / 3.0
	for _, r := range p.Ranges {
		if r.Load > fair+1e-9 {
			t.Fatalf("range %+v exceeds fair share %.2f", r, fair)
		}
	}
	if err := assertTiled(p); err != nil {
		t.Fatalf("plan does not tile the token space: %v", err)
	}
}

func b256(v float64) []float64 {
	b := make([]float64, Buckets)
	for i := range b {
		b[i] = v
	}
	return b
}

func TestZeroLoadNoPlan(t *testing.T) {
	p := PlanFor([]string{"a"}, 1, load(nil, 0), 1, &fakeBase{[]string{"a"}})
	if p != nil {
		t.Fatal("zero load must produce no overlay")
	}
}

func TestHotSpotSplits(t *testing.T) {
	// One bucket carries most of the cluster's writes with 3 nodes:
	// fair share = total/3, so the hot bucket is carved out of the
	// base-ring territory as its own owned range.
	base := 0.05
	b := load(map[int]float64{200: 100.0}, base)
	p := PlanFor([]string{"a", "b", "c"}, 3, b, 7, &fakeBase{[]string{"a", "b", "c"}})
	if p == nil {
		t.Fatal("expected a plan for a hot bucket")
	}
	if p.Epoch != 7 {
		t.Fatalf("epoch = %d, want 7", p.Epoch)
	}
	total := 0.0
	for _, v := range b {
		total += v
	}
	fair := total / 3.0
	hotCarved := false
	for _, r := range p.Ranges {
		if r.Start == 200<<24 && r.End == 201<<24 && r.Load > fair {
			hotCarved = true
		}
	}
	if !hotCarved {
		t.Fatalf("hot bucket 200 not carved out as its own hot range: %+v", p.Ranges)
	}
	// Once split, the overlay tiles the whole space (no gaps): a
	// cold leaf must not fall back to the ring and re-concentrate.
	if err := assertTiled(p); err != nil {
		t.Fatalf("plan does not tile the token space: %v", err)
	}
	if len(p.Ranges) != 3 {
		t.Fatalf("expected 3 ranges (cold-left, hot atom, cold-right), got %d: %+v", len(p.Ranges), p.Ranges)
	}
}

// assertTiled checks the plan's ranges cover [0, 2^32) contiguously.
func assertTiled(p *Plan) error {
	var end uint32
	for _, r := range p.Ranges {
		if r.Start != end {
			return fmt.Errorf("gap: range starts at %d, previous ended at %d", r.Start, end)
		}
		end = r.End
	}
	if end != 0 { // 256<<24 wraps to 0 = 2^32
		return fmt.Errorf("tiling ends at %d, want 2^32 (0 wrapped)", end)
	}
	return nil
}

func TestHotPlateauSplitsIntoMany(t *testing.T) {
	// A wide hot plateau must split into multiple ranges so the hot
	// spot is served by several nodes, not one.
	hot := map[int]float64{}
	for i := 100; i < 108; i++ {
		hot[i] = 20
	}
	b := load(hot, 0.01)
	p := PlanFor([]string{"a", "b", "c"}, 3, b, 1, &fakeBase{[]string{"a", "b", "c"}})
	if p == nil || len(p.Ranges) < 3 {
		t.Fatalf("hot plateau should split into >=3 ranges, got %+v", p)
	}
	// Every hot bucket is covered by some range.
	for i := 100; i < 108; i++ {
		if p.Owners(uint32(i)<<24, 1) == nil {
			t.Fatalf("hot bucket %d uncovered", i)
		}
	}
}

func TestPlanInvariants(t *testing.T) {
	b := load(map[int]float64{10: 50, 11: 40, 200: 80}, 0.1)
	p := PlanFor([]string{"a", "b", "c", "d"}, 3, b, 2, &fakeBase{[]string{"a", "b", "c", "d"}})
	if p == nil {
		t.Fatal("expected plan")
	}
	for i, r := range p.Ranges {
		end := r.End
		if end == 0 && i == len(p.Ranges)-1 {
			end = ^uint32(0) // last range ends at the top of the 32-bit space
		}
		if r.Start >= end {
			t.Fatalf("range %d empty/inverted: %+v", i, r)
		}
		if len(r.Owners) == 0 {
			t.Fatalf("range %d has no owners", i)
		}
		seen := map[string]bool{}
		for _, o := range r.Owners {
			if seen[o] {
				t.Fatalf("range %d duplicate owner %s", i, o)
			}
			seen[o] = true
		}
		if i > 0 {
			prev := p.Ranges[i-1]
			if prev.End > r.Start {
				t.Fatalf("ranges %d and %d overlap: %+v %+v", i-1, i, prev, r)
			}
		}
	}
}

func TestPlanDeterminism(t *testing.T) {
	b := load(map[int]float64{3: 30, 77: 60, 200: 12, 201: 9}, 0.2)
	nodes := []string{"n3", "n1", "n2"} // unsorted input
	base := &fakeBase{nodes}
	p1 := PlanFor(nodes, 3, b, 5, base)
	p2 := PlanFor(nodes, 3, b, 5, base)
	if !reflect.DeepEqual(p1, p2) {
		t.Fatalf("plans diverged for identical inputs:\n%+v\n%+v", p1, p2)
	}
}

func TestOwnersLookup(t *testing.T) {
	p := &Plan{Epoch: 1, Ranges: []Range{
		{Start: 10 << 24, End: 20 << 24, Owners: []string{"x", "y", "z"}},
	}}
	if got := p.Owners(15<<24, 3); !reflect.DeepEqual(got, []string{"x", "y", "z"}) {
		t.Fatalf("inside range: got %v", got)
	}
	if got := p.Owners(10<<24, 1); !reflect.DeepEqual(got, []string{"x"}) {
		t.Fatalf("at start boundary: got %v", got)
	}
	if got := p.Owners(20<<24, 3); got != nil {
		t.Fatalf("end is exclusive: got %v", got)
	}
	if got := p.Owners(5<<24, 2); got != nil {
		t.Fatalf("outside overlay: got %v", got)
	}
	if got := p.Owners(12<<24, 2); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("n=2: got %v", got)
	}
	var nilPlan *Plan
	if nilPlan.Owners(1, 1) != nil {
		t.Fatal("nil plan must return nil")
	}
}

func TestSplitMergeRoundTrip(t *testing.T) {
	r := Range{Start: 100, End: 200, Owners: []string{"a", "b"}, Load: 10}
	l, rr, ok := Split(r, 150)
	if !ok || l.Start != 100 || l.End != 150 || rr.Start != 150 || rr.End != 200 {
		t.Fatalf("split wrong: %+v %+v", l, rr)
	}
	if !reflect.DeepEqual(l.Owners, []string{"a", "b"}) || !reflect.DeepEqual(rr.Owners, []string{"a", "b"}) {
		t.Fatal("split must copy owners")
	}
	if _, _, ok := Split(r, 99); ok {
		t.Fatal("split outside range must fail")
	}
	if _, _, ok := Split(r, 100); ok {
		t.Fatal("split at boundary must fail")
	}
	m, ok := MergeAdjacent(l, rr)
	if !ok || m.Start != 100 || m.End != 200 || m.Load != 10 {
		t.Fatalf("merge wrong: %+v", m)
	}
	if _, ok := MergeAdjacent(Range{Start: 0, End: 5}, Range{Start: 6, End: 9}); ok {
		t.Fatal("non-touching ranges must not merge")
	}
}

func TestGreedySpreadsPrimaries(t *testing.T) {
	// Two equally hot, far-apart regions with 3 nodes: they should
	// land on DIFFERENT primaries (greedy least-assigned-load).
	b := load(map[int]float64{10: 150, 200: 150}, 0.01)
	p := PlanFor([]string{"a", "b", "c"}, 2, b, 1, &fakeBase{[]string{"a", "b", "c"}})
	if p == nil {
		t.Fatal("expected plan")
	}
	primaries := map[string]bool{}
	hotRanges := 0
	for _, r := range p.Ranges {
		if r.Load > 50 {
			hotRanges++
			primaries[r.Owners[0]] = true
		}
	}
	if hotRanges < 2 {
		t.Fatalf("expected >=2 hot ranges, got %d", hotRanges)
	}
	if len(primaries) < 2 {
		t.Fatalf("hot ranges piled on one primary: %v", primaries)
	}
}

func TestCooledPlanHasNoHotRanges(t *testing.T) {
	// A hot cluster has at least one range above fair share (the hot
	// atom). When the same cluster cools to uniform, no range exceeds
	// fair share — the hot ranges merged back into balanced territory.
	hot := map[int]float64{50: 100, 51: 100}
	b := load(hot, 0.01)
	p := PlanFor([]string{"a", "b", "c"}, 2, b, 1, &fakeBase{[]string{"a", "b", "c"}})
	if p == nil || len(p.Ranges) == 0 {
		t.Fatal("expected hot plan")
	}
	total := 0.0
	for _, v := range b {
		total += v
	}
	fair := total / 3.0
	hotFound := false
	for _, r := range p.Ranges {
		if r.Load > fair {
			hotFound = true
		}
	}
	if !hotFound {
		t.Fatal("hot cluster should have a range above fair share")
	}
	cold := load(nil, 1.0)
	p2 := PlanFor([]string{"a", "b", "c"}, 2, cold, 2, &fakeBase{[]string{"a", "b", "c"}})
	if p2 == nil {
		t.Fatal("expected cooled plan")
	}
	for _, r := range p2.Ranges {
		if r.Load > 256.0/3.0+1e-9 {
			t.Fatalf("cooled plan still has a hot range: %+v", r)
		}
	}
}

// --- Minimal-movement rebalancer (A5) ---

func TestRebalanceStickyIdempotent(t *testing.T) {
	// Rebalancing a plan against itself must move NOTHING: every
	// range keeps its previous primary because the previous primary
	// is live and already inside the band.
	nodes := []string{"a", "b", "c"}
	b := load(map[int]float64{10: 200, 200: 150}, 0.5)
	p1 := PlanFor(nodes, 2, b, 1, &fakeBase{nodes})
	if p1 == nil || len(p1.Ranges) == 0 {
		t.Fatal("expected a non-trivial plan")
	}
	p2 := Rebalance(nodes, 2, b, 2, &fakeBase{nodes}, p1)
	if p2 == nil {
		t.Fatal("expected a rebalanced plan")
	}
	if m := MovementCount(p1, p2); m != 0 {
		t.Fatalf("sticky rebalance against self moved %d ranges, want 0", m)
	}
}

func TestRebalanceHysteresisSmallDrift(t *testing.T) {
	// A small load drift (inside ImbalanceThreshold) must cause ZERO
	// range moves — that is the hysteresis that stops churn.
	nodes := []string{"a", "b", "c"}
	base := load(map[int]float64{40: 120, 41: 90, 42: 60}, 1.0)
	p1 := PlanFor(nodes, 2, base, 1, &fakeBase{nodes})
	if p1 == nil {
		t.Fatal("expected plan")
	}
	// Nudge every bucket by ~3% — well inside the 10% band.
	drift := make([]float64, Buckets)
	for i, v := range base {
		drift[i] = v * 1.03
	}
	p2 := Rebalance(nodes, 2, drift, 2, &fakeBase{nodes}, p1)
	if p2 == nil {
		t.Fatal("expected plan after drift")
	}
	if m := MovementCount(p1, p2); m != 0 {
		t.Fatalf("small drift moved %d ranges, want 0 (hysteresis)", m)
	}
}

func TestRebalanceNodeJoinFewerMovesThanFresh(t *testing.T) {
	// Adding a node: the sticky rebalancer must move no MORE ranges
	// than a cold-start fresh plan would. This is the core minimal-
	// movement guarantee — we fill the new node with the minimum set
	// of ranges needed to restore balance.
	nodes2 := []string{"a", "b"}
	nodes3 := []string{"a", "b", "c"}
	b := load(map[int]float64{10: 300, 150: 250}, 1.0)
	p2 := PlanFor(nodes2, 2, b, 1, &fakeBase{nodes2})
	if p2 == nil {
		t.Fatal("expected 2-node plan")
	}
	fresh := PlanFor(nodes3, 2, b, 2, &fakeBase{nodes3})
	sticky := Rebalance(nodes3, 2, b, 2, &fakeBase{nodes3}, p2)
	if fresh == nil || sticky == nil {
		t.Fatal("expected 3-node plans")
	}
	mf := MovementCount(p2, fresh)
	ms := MovementCount(p2, sticky)
	if ms > mf {
		t.Fatalf("sticky moved %d ranges but fresh moved %d — sticky must be <= fresh", ms, mf)
	}
	// And the new node must actually receive some load (we are not
	// just keeping the old plan and ignoring the new capacity).
	gotNew := false
	for _, r := range sticky.Ranges {
		if len(r.Owners) > 0 && r.Owners[0] == "c" {
			gotNew = true
		}
	}
	if !gotNew {
		t.Fatal("new node 'c' received no primary ranges — rebalance did nothing")
	}
}

func TestRebalanceNodeLeaveOnlyDepartedMoves(t *testing.T) {
	// Removing a node: ranges that were NOT owned by the departed
	// node keep their primary; only the departed node's ranges move.
	nodes := []string{"a", "b", "c"}
	b := load(map[int]float64{5: 180, 100: 160, 200: 140}, 1.0)
	p1 := PlanFor(nodes, 2, b, 1, &fakeBase{nodes})
	if p1 == nil {
		t.Fatal("expected plan")
	}
	// Count how many ranges in p1 were owned by 'c'.
	cOwned := 0
	for _, r := range p1.Ranges {
		if len(r.Owners) > 0 && r.Owners[0] == "c" {
			cOwned++
		}
	}
	if cOwned == 0 {
		t.Skip("load did not place any primary on 'c'; nothing to test")
	}
	survivors := []string{"a", "b"}
	p2 := Rebalance(survivors, 2, b, 2, &fakeBase{survivors}, p1)
	if p2 == nil {
		t.Fatal("expected plan after leave")
	}
	// No surviving range should have moved: every move counted must
	// correspond to a range that 'c' previously owned. MovementCount
	// counts ranges in p2 whose primary differs from prev coverage.
	moves := MovementCount(p1, p2)
	if moves > cOwned {
		t.Fatalf("node leave moved %d ranges but only %d were owned by 'c'", moves, cOwned)
	}
	// And no range in the new plan may still name the departed node.
	for _, r := range p2.Ranges {
		for _, o := range r.Owners {
			if o == "c" {
				t.Fatalf("departed node 'c' still owns a range: %+v", r)
			}
		}
	}
}

func TestRebalanceDeterministic(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	b := load(map[int]float64{30: 220, 90: 180, 210: 160}, 0.7)
	p1 := PlanFor(nodes, 2, b, 1, &fakeBase{nodes})
	a := Rebalance(nodes, 2, b, 2, &fakeBase{nodes}, p1)
	bb := Rebalance(nodes, 2, b, 2, &fakeBase{nodes}, p1)
	if !reflect.DeepEqual(a, bb) {
		t.Fatal("Rebalance is not deterministic")
	}
}

// TestRebalanceOrderIndependent is the cross-node property: every node
// builds its input as peers-plus-self in ITS OWN order, so two nodes
// looking at identical state must produce identical plans even though
// their node slices are different permutations. Without this, "no
// agreement protocol needed" would be false.
func TestRebalanceOrderIndependent(t *testing.T) {
	load_ := load(map[int]float64{30: 220, 90: 180, 210: 160}, 0.7)
	nodes := []string{"a", "b", "c", "d"}
	perms := [][]string{
		{"a", "b", "c", "d"},
		{"d", "c", "b", "a"},
		{"c", "a", "d", "b"},
	}
	var first *Plan
	for _, perm := range perms {
		got := Rebalance(perm, 2, load_, 7, &fakeBase{nodes}, nil)
		if first == nil {
			first = got
			continue
		}
		if !reflect.DeepEqual(first, got) {
			t.Fatalf("node order %v changed the plan:\n%+v\nvs\n%+v", perm, first, got)
		}
	}
	if first == nil {
		t.Fatal("hot load should produce a plan")
	}
	// And with a seeded previous plan (minimal movement), the same must
	// hold: sticky plans are still a function of the inputs, not of the
	// order they arrived in.
	for i, perm := range perms {
		got := Rebalance(perm, 2, load_, 8, &fakeBase{nodes}, first)
		if i == 0 {
			first = got
			continue
		}
		if !reflect.DeepEqual(first, got) {
			t.Fatalf("with prev plan, node order %v changed the result", perm)
		}
	}
}
