package ring

import "testing"

func TestWeightedVnodeProportionality(t *testing.T) {
	nodes := []string{"hot", "cold"}
	weights := map[string]float64{"hot": 4.0, "cold": 1.0}
	r := BuildWeighted(nodes, weights, 1)
	counts := map[string]int{}
	for _, v := range r.vnodes {
		counts[v.node]++
	}
	// hot should get ~4x the vnodes of cold (128*4/2.5=204 vs 51).
	if counts["hot"] < counts["cold"]*3 {
		t.Fatalf("hot not weighted up: %v", counts)
	}
	if counts["cold"] < 8 {
		t.Fatalf("cold starved: %v", counts)
	}
}

func TestWeightedDeterministic(t *testing.T) {
	nodes := []string{"a", "b", "c"}
	w := map[string]float64{"a": 2, "b": 1, "c": 3}
	r1 := BuildWeighted(nodes, w, 7)
	r2 := BuildWeighted([]string{"c", "b", "a"}, w, 7) // different input order
	if len(r1.vnodes) != len(r2.vnodes) {
		t.Fatal("different vnode counts")
	}
	for i := range r1.vnodes {
		if r1.vnodes[i] != r2.vnodes[i] {
			t.Fatalf("ring diverges at %d: %v vs %v", i, r1.vnodes[i], r2.vnodes[i])
		}
	}
}

func TestWeightedFallbackUniform(t *testing.T) {
	// No weights -> identical to BuildWithEpoch.
	nodes := []string{"x", "y"}
	r1 := BuildWithEpoch(nodes, 3)
	r2 := BuildWeighted(nodes, nil, 3)
	if len(r1.vnodes) != len(r2.vnodes) {
		t.Fatal("nil weights changed ring size")
	}
	for i := range r1.vnodes {
		if r1.vnodes[i] != r2.vnodes[i] {
			t.Fatal("nil weights changed ring")
		}
	}
	// Zero/negative weights fall back to average, never starve.
	r3 := BuildWeighted(nodes, map[string]float64{"x": 0, "y": -5}, 3)
	if len(r3.vnodes) != len(r1.vnodes) {
		t.Fatalf("zero weights should equal uniform: %d vs %d", len(r3.vnodes), len(r1.vnodes))
	}
}

func TestWeightedOwnershipSkew(t *testing.T) {
	// With hot=3x weight, hot should own noticeably more of a key
	// sample than cold.
	r := BuildWeighted([]string{"hot", "cold"}, map[string]float64{"hot": 3, "cold": 1}, 1)
	own := map[string]int{}
	for i := 0; i < 3000; i++ {
		owners := r.Owners("k"+itoa(i), 1)
		own[owners[0]]++
	}
	if own["hot"] <= own["cold"]*2 {
		t.Fatalf("ownership not skewed to hot: %v", own)
	}
}
