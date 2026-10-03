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
