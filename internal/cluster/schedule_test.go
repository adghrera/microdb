package cluster

import (
	"sort"
	"testing"
)

func TestAELapVisitsEveryPeerExactlyOnce(t *testing.T) {
	peers := []string{"a", "b", "c", "d", "e"}

	// Fanout == cluster size: each call is exactly one lap, so every
	// call must return every peer precisely once.
	s := &aeSchedule{}
	for lap := 0; lap < 5; lap++ {
		got := s.next(peers, len(peers))
		sort.Strings(got)
		if len(got) != len(peers) {
			t.Fatalf("lap %d returned %v, want all peers", lap, got)
		}
		for i := 1; i < len(got); i++ {
			if got[i] == got[i-1] {
				t.Fatalf("lap %d repeated %q: %v", lap, got[i], got)
			}
		}
	}

	// Smaller fanout: over two full laps (2 * 5 slots, 2 per round)
	// every peer is contacted exactly twice — no peer is starved while
	// another is hammered.
	s2 := &aeSchedule{}
	counts := map[string]int{}
	for round := 0; round < 5; round++ {
		got := s2.next(peers, 2)
		if len(got) == 0 || len(got) > 2 {
			t.Fatalf("round %d returned %d peers, want 1..2", round, len(got))
		}
		for _, p := range got {
			counts[p]++
		}
	}
	// Laps and rounds do not align (a lap of five served two at a time
	// leaves one straggler), so the property to assert is coverage and
	// balance: nobody is starved, and nobody is contacted more often
	// than anybody else by more than one.
	lo, hi := 1<<30, -1
	for _, p := range peers {
		if counts[p] == 0 {
			t.Fatalf("peer %q starved over two laps: %v", p, counts)
		}
		if counts[p] < lo {
			lo = counts[p]
		}
		if counts[p] > hi {
			hi = counts[p]
		}
	}
	if hi-lo > 1 {
		t.Errorf("coverage is unbalanced: %v (spread %d)", counts, hi-lo)
	}
}

func TestAEFanoutBoundsRoundCost(t *testing.T) {
	s := &aeSchedule{}
	peers := []string{"a", "b", "c", "d", "e", "f", "g"}
	for round := 0; round < 20; round++ {
		got := s.next(peers, 3)
		if len(got) == 0 || len(got) > 3 {
			t.Fatalf("round %d contacted %d peers, want 1..3", round, len(got))
		}
	}
	// Fanout larger than the cluster contacts everyone (small-cluster
	// behaviour, which is what keeps the existing tests honest).
	all := (&aeSchedule{}).next(peers, 100)
	if len(all) != len(peers) {
		t.Fatalf("fanout beyond the cluster size returned %d of %d", len(all), len(peers))
	}
}

func TestAEReadsMembershipChanges(t *testing.T) {
	s := &aeSchedule{}
	first := s.next([]string{"a", "b", "c"}, 3)
	sort.Strings(first)
	if len(first) != 3 {
		t.Fatalf("first lap = %v", first)
	}
	// One peer leaves, one joins: the lap must be rebuilt around the
	// new membership rather than retrying a dead address.
	second := s.next([]string{"a", "b", "d"}, 3)
	sort.Strings(second)
	if len(second) != 3 {
		t.Fatalf("second lap = %v", second)
	}
	for _, p := range second {
		if p == "c" {
			t.Fatalf("departed peer still scheduled: %v", second)
		}
	}
}

func TestAEDegenerateInputs(t *testing.T) {
	s := &aeSchedule{}
	if got := s.next(nil, 3); got != nil {
		t.Errorf("no peers returned %v", got)
	}
	if got := s.next([]string{"only"}, 0); len(got) != 1 {
		t.Errorf("want<=0 must still make progress, got %v", got)
	}
}

func zonePref(local string) func(a, b string) bool {
	zones := map[string]string{
		"z1-local": "z1", "z2-a": "z2", "z2-b": "z2", "z2-c": "z2", "z3-x": "z3",
	}
	return func(a, b string) bool {
		if local == "" {
			return false
		}
		za, zb := zones[a] == local, zones[b] == local
		return za && !zb
	}
}

// TestAEPrefersLocalFailureDomain: repair should meet a same-zone peer
// first (cross-AZ traffic costs money and buys no correctness), while
// the lap still visits everyone — locals first, remote after.
func TestAEPrefersLocalFailureDomain(t *testing.T) {
	peers := []string{"z2-a", "z2-b", "z1-local", "z3-x", "z2-c"}
	prefer := zonePref("z1")

	s := &aeSchedule{}
	first := s.nextPref(peers, 1, prefer)
	if len(first) != 1 || first[0] != "z1-local" {
		t.Fatalf("first contact = %v, want the same-zone peer", first)
	}
	// The rest of the lap still covers every other peer exactly once:
	// the first call took slot 1, so two more calls of two cover slots
	// 2..5 and the lap ends exactly there.
	seen := map[string]bool{"z1-local": true}
	for i := 0; i < 2; i++ {
		for _, p := range s.nextPref(peers, 2, prefer) {
			if seen[p] {
				t.Fatalf("peer %q visited twice in one lap", p)
			}
			seen[p] = true
		}
	}
	if len(seen) != len(peers) {
		t.Fatalf("lap covered %d of %d peers (locals-first must not cost coverage)", len(seen), len(peers))
	}

	// Without a zone configured, behaviour is the uniform shuffle the
	// existing tests pin (no preference, full coverage).
	n := &aeSchedule{}
	got := n.nextPref(peers, 5, zonePref(""))
	seen2 := map[string]bool{}
	for _, p := range got {
		seen2[p] = true
	}
	if len(seen2) != len(peers) {
		t.Fatalf("unzoned lap covered %d of %d", len(seen2), len(peers))
	}

	// Several same-zone peers stay shuffled within their run, so one
	// peer is not always the repair partner.
	counts := map[string]int{}
	for lap := 0; lap < 6; lap++ {
		l := &aeSchedule{}
		c := l.nextPref(peers, 1, prefer) // only the local run is first
		counts[c[0]]++
	}
	// Only the local-zone peer can be first — it is alone in its run.
	if counts["z1-local"] != 6 {
		t.Fatalf("first contact distribution: %v, want only z1-local", counts)
	}
}
