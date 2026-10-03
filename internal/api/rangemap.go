// Range-map integration: contiguous token-range ownership with
// load-driven split/merge on top of the vnode ring.
//
// Every mutation observed in the change feed (local writes AND
// replicated applies) bumps a per-bucket counter for the high byte
// of the routing-key token. On a fixed interval the loop turns the
// counters into an EWMA writes/sec per bucket, publishes it to a
// reserved _config doc (which replicates like any other write),
// sums every node's published buckets, and recomputes the
// deterministic range plan (internal/ranges). Because the planner
// is a pure function of gossiped inputs, every node converges on
// the identical plan without any agreement protocol.
//
// Routing consults the plan first: a token inside a planned (split)
// range is owned by that range's owner list; everything else falls
// through to the base vnode ring. A plan computed under an older
// membership epoch is ignored until the next replan, so range
// ownership never overrides a fresher membership view.
package api

import (
	"encoding/json"
	"log"
	"net/http"

	"sync"
	"time"

	"microdb/internal/changelog"
	"microdb/internal/ranges"
	"microdb/internal/ring"
)

// EnableRangeMap turns on load-adaptive contiguous range
// ownership. The interval controls how often load is published and
// the plan recomputed (0 = default 2s).
func (s *Server) EnableRangeMap(interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	s.rangeMu = new(sync.RWMutex)
	s.rangeBuckets = make([]int64, ranges.Buckets)
	s.rangeEWMA = make([]float64, ranges.Buckets)
	// The change-feed callback runs under the store lock, so it
	// must not call back into the store (routingKey reads collection
	// config). Hand events to a worker goroutine instead; the load
	// signal is best-effort, so a full channel just drops.
	events := make(chan changelog.Event, 4096)
	s.st.ChangeLog().OnAppend(func(ev changelog.Event) {
		select {
		case events <- ev:
		default:
		}
	})
	go func() {
		for ev := range events {
			key := s.routingKey(ev.Collection, ev.ID)
			b := ranges.Bucket(ring.Hash(key))
			s.rangeMu.Lock()
			s.rangeBuckets[b]++
			s.rangeMu.Unlock()
		}
	}()
	go s.rangeLoop(interval)
}

func (s *Server) rangeLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		if err := s.replanOnce(interval); err != nil {
			log.Printf("range replan: %v", err)
		}
	}
}

// replanOnce publishes this node's bucket EWMA, gathers every
// node's published buckets, and recomputes the range plan.
func (s *Server) replanOnce(interval time.Duration) error {
	// 1. Fold raw counters into an EWMA rate (writes/sec).
	s.rangeMu.Lock()
	total := int64(0)
	for _, v := range s.rangeBuckets {
		total += v
	}
	for i := range s.rangeEWMA {
		rate := float64(s.rangeBuckets[i]) / interval.Seconds()
		s.rangeEWMA[i] = 0.7*s.rangeEWMA[i] + 0.3*rate
		s.rangeBuckets[i] = 0
	}
	ewma := append([]float64(nil), s.rangeEWMA...)
	s.rangeMu.Unlock()

	// 2. Gather every peer's bucket EWMA directly over the internal
	// endpoint. We deliberately do NOT route the load signal through
	// replicated _config docs: at RF=1 a peer's buckets would never
	// reach us. A direct all-to-all fetch is honest about what the
	// planner needs and tolerates RF < node count.
	cluster := append([]float64(nil), ewma...)
	for _, peer := range s.cl.Peers() {
		if b, ok := s.fetchBuckets(peer); ok {
			for i, v := range b {
				cluster[i] += v
			}
		}
	}

	// 3. Recompute the plan from (nodes, rf, cluster load, epoch),
	// seeded with the current plan for MINIMAL MOVEMENT: ranges keep
	// their existing primary unless the band is exceeded, so node
	// joins/leaves and load drift move the fewest ranges possible.
	nodes := append(s.cl.Peers(), s.self)
	rf := s.rf
	if rf > len(nodes) {
		rf = len(nodes)
	}
	s.rangeMu.RLock()
	prev := s.rangePlan
	s.rangeMu.RUnlock()
	plan := ranges.Rebalance(nodes, rf, cluster, s.cl.Epoch(), s.currentRing(), prev)
	s.rangeMu.Lock()
	s.rangePlan = plan
	s.rangeMu.Unlock()
	return nil
}

// fetchBuckets pulls a peer's current bucket EWMA over the internal
// endpoint. Returns ok=false on any error: a missing/stale signal is
// simply not added, and the planner tolerates partial input.
func (s *Server) fetchBuckets(peer string) ([]float64, bool) {
	resp, err := s.fwdClient.Get(peer + "/internal/loadbuckets")
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, false
	}
	var out struct {
		Buckets []float64 `json:"buckets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, false
	}
	if len(out.Buckets) != ranges.Buckets {
		return nil, false
	}
	return out.Buckets, true
}

// handleLoadBuckets serves this node's current bucket EWMA to peers.
func (s *Server) handleLoadBuckets(w http.ResponseWriter, r *http.Request) {
	if s.rangeMu == nil {
		writeJSON(w, 200, map[string]interface{}{"buckets": []float64{}})
		return
	}
	s.rangeMu.RLock()
	b := append([]float64(nil), s.rangeEWMA...)
	s.rangeMu.RUnlock()
	writeJSON(w, 200, map[string]interface{}{"buckets": b})
}

// placementOwners resolves the ordered owner set for a routing key:
// the range plan wins for tokens inside a planned (split) range;
// everything else falls through to the base vnode ring. A plan
// stamped with an older membership epoch is ignored.
func (s *Server) placementOwners(key string, n int) []string {
	if s.rangeMu != nil {
		s.rangeMu.RLock()
		plan := s.rangePlan
		s.rangeMu.RUnlock()
		if plan != nil && plan.Epoch >= s.cl.Epoch() {
			if owners := plan.Owners(ring.Hash(key), n); owners != nil {
				return owners
			}
		}
	}
	return s.currentRing().Owners(key, n)
}

// RangePlan returns the current range plan (nil when the range map
// is disabled or the cluster is cold).
func (s *Server) RangePlan() *ranges.Plan {
	if s.rangeMu == nil {
		return nil
	}
	s.rangeMu.RLock()
	defer s.rangeMu.RUnlock()
	return s.rangePlan
}

// handleRangePlan serves the current plan as JSON for ops/tests.
func (s *Server) handleRangePlan(w http.ResponseWriter, r *http.Request) {
	plan := s.RangePlan()
	if plan == nil {
		writeJSON(w, 200, map[string]interface{}{"enabled": s.rangeMu != nil, "plan": nil})
		return
	}
	writeJSON(w, 200, map[string]interface{}{"enabled": true, "plan": plan})
}

// handleRangeReplan forces an immediate replan (ops/tests).
func (s *Server) handleRangeReplan(w http.ResponseWriter, r *http.Request) {
	if s.rangeMu == nil {
		writeJSON(w, 400, map[string]string{"error": "range map disabled"})
		return
	}
	if err := s.replanOnce(2 * time.Second); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	// Echo the fresh plan.
	plan := s.RangePlan()
	b, _ := json.Marshal(map[string]interface{}{"ok": true, "ranges": plan})
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}
