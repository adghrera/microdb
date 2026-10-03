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

// TestOwnersSpreadAcrossZones is the rack-awareness claim: replicas of
// one key must not all land in one failure domain when the topology
// has enough of them.
func TestOwnersSpreadAcrossZones(t *testing.T) {
	nodes := []string{"n0", "n1", "n2", "n3", "n4", "n5"}
	zones := map[string]string{
		"n0": "az-a", "n1": "az-a",
		"n2": "az-b", "n3": "az-b",
		"n4": "az-c", "n5": "az-c",
	}
	r := BuildWeightedIn(nodes, nil, zones, 1)

	for i := 0; i < 500; i++ {
		owners := r.Owners("key-"+itoa(i), 3)
		if len(owners) != 3 {
			t.Fatalf("owners = %d, want 3", len(owners))
		}
		seen := map[string]bool{}
		for _, o := range owners {
			if seen[o] {
				t.Fatalf("duplicate owner %s in %v", o, owners)
			}
			seen[o] = true
		}
		used := map[string]bool{}
		for _, o := range owners {
			used[zones[o]] = true
		}
		if len(used) != 3 {
			t.Fatalf("key %d replicas span %d zones (%v), want 3", i, len(used), owners)
		}
	}
}

// TestOwnersFillWhenZonesRunOut: two AZs with RF=3 cannot give three
// distinct domains — the ring must still return three DISTINCT NODES
// rather than three copies on one.
func TestOwnersFillWhenZonesRunOut(t *testing.T) {
	nodes := []string{"a0", "a1", "b0", "b1"}
	zones := map[string]string{"a0": "az-a", "a1": "az-a", "b0": "az-b", "b1": "az-b"}
	r := BuildWeightedIn(nodes, nil, zones, 1)
	for i := 0; i < 300; i++ {
		owners := r.Owners("k"+itoa(i), 3)
		if len(owners) != 3 {
			t.Fatalf("owners = %v, want 3 distinct nodes", owners)
		}
		seen := map[string]bool{}
		for _, o := range owners {
			if seen[o] {
				t.Fatalf("duplicate owner %s in %v", o, owners)
			}
			seen[o] = true
		}
	}
}

// TestNoZonesMeansNoBehaviourChange: without topology data the ring
// must pick exactly what it picked before zones existed.
func TestNoZonesMeansNoBehaviourChange(t *testing.T) {
	nodes := []string{"n0", "n1", "n2", "n3"}
	before := BuildWeighted(nodes, nil, 7)
	after := BuildWeightedIn(nodes, nil, nil, 7)
	for i := 0; i < 200; i++ {
		k := "k" + itoa(i)
		a, b := before.Owners(k, 3), after.Owners(k, 3)
		if len(a) != len(b) {
			t.Fatalf("length changed for %s: %v vs %v", k, a, b)
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("placement changed for %s: %v vs %v", k, a, b)
			}
		}
	}
}

// TestOwnersSpreadAcrossRegions: losing a region must not take more
// than one replica of a key — region diversity outranks zone diversity
// because a region outage costs a region.
func TestOwnersSpreadAcrossRegions(t *testing.T) {
	nodes := []string{"a1", "a2", "b1", "b2", "c1", "c2"}
	topo := Topology{
		Regions: map[string]string{
			"a1": "eu", "a2": "eu",
			"b1": "us", "b2": "us",
			"c1": "ap", "c2": "ap",
		},
		Zones: map[string]string{
			"a1": "eu-a", "a2": "eu-b",
			"b1": "us-a", "b2": "us-b",
			"c1": "ap-a", "c2": "ap-b",
		},
	}
	r := BuildWeightedTopo(nodes, nil, topo, 1)
	for i := 0; i < 500; i++ {
		owners := r.Owners("key-"+itoa(i), 3)
		if len(owners) != 3 {
			t.Fatalf("owners = %v", owners)
		}
		seenNodes := map[string]bool{}
		regions := map[string]bool{}
		zones := map[string]bool{}
		for _, o := range owners {
			if seenNodes[o] {
				t.Fatalf("duplicate owner %s in %v", o, owners)
			}
			seenNodes[o] = true
			regions[topo.Regions[o]] = true
			zones[topo.Zones[o]] = true
		}
		if len(regions) != 3 {
			t.Fatalf("key %d replicas span %d regions (%v), want 3", i, len(regions), owners)
		}
		// Region diversity plus zone diversity falls out of it here,
		// but assert it: two nodes in different regions can still share
		// a zone name in a badly labelled topology.
		if len(zones) != 3 {
			t.Fatalf("key %d replicas span %d zones (%v), want 3", i, len(zones), owners)
		}
	}
}

// TestOwnersMeetRFWhenRegionsRunOut: one region with RF=3 must still
// return three DISTINCT nodes — diversity is a preference, RF is a
// requirement.
func TestOwnersMeetRFWhenRegionsRunOut(t *testing.T) {
	nodes := []string{"e1", "e2", "e3", "e4"}
	topo := Topology{
		Regions: map[string]string{"e1": "eu", "e2": "eu", "e3": "eu", "e4": "eu"},
		Zones:   map[string]string{"e1": "eu-a", "e2": "eu-b", "e3": "eu-c", "e4": "eu-d"},
	}
	r := BuildWeightedTopo(nodes, nil, topo, 1)
	for i := 0; i < 300; i++ {
		owners := r.Owners("k"+itoa(i), 3)
		if len(owners) != 3 {
			t.Fatalf("owners = %v, want 3 distinct nodes", owners)
		}
		seen := map[string]bool{}
		for _, o := range owners {
			if seen[o] {
				t.Fatalf("duplicate owner %s in %v", o, owners)
			}
			seen[o] = true
		}
	}
}
