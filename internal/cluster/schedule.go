package cluster

import (
	"math/rand"
	"sync"
)

// aeSchedule hands out anti-entropy contacts as a shuffled lap over
// the live peer set.
//
// The properties that matter at a few hundred nodes:
//
//   - Bounded: one round contacts at most `fanout` peers, so per-tick
//     traffic is O(fanout) per node instead of O(peers) — the cluster
//     total stops being O(N^2) per interval.
//   - Complete: a lap visits every peer exactly once before starting
//     again, so nothing goes unrepaired for longer than
//     ceil(peers/fanout) rounds. Purely random selection would leave a
//     long tail of peers unvisited with the same expected traffic.
//   - Membership-aware: a peer joining or leaving reshuffles the lap,
//     and departed peers are dropped rather than retried forever.
type aeSchedule struct {
	mu    sync.Mutex
	order []string
	pos   int
}

// next returns up to `want` peers for this round.
func (s *aeSchedule) next(peers []string, want int) []string {
	if want <= 0 {
		want = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.needsRebuild(peers) {
		s.order = append([]string(nil), peers...)
		rand.Shuffle(len(s.order), func(i, j int) {
			s.order[i], s.order[j] = s.order[j], s.order[i]
		})
		s.pos = 0
	}
	if len(s.order) == 0 {
		return nil
	}
	if s.pos >= len(s.order) {
		s.pos = 0 // lap complete: start the next one
	}
	end := s.pos + want
	if end > len(s.order) {
		end = len(s.order)
	}
	out := append([]string(nil), s.order[s.pos:end]...)
	s.pos = end
	return out
}

// needsRebuild reports whether the current lap no longer describes the
// membership (order-insensitive: the set matters, not the sequence).
func (s *aeSchedule) needsRebuild(peers []string) bool {
	if len(s.order) != len(peers) {
		return true
	}
	if len(s.order) == 0 {
		return false
	}
	have := make(map[string]bool, len(s.order))
	for _, p := range s.order {
		have[p] = true
	}
	for _, p := range peers {
		if !have[p] {
			return true
		}
	}
	return false
}
