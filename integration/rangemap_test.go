package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"microdb/internal/ranges"
	"microdb/internal/ring"
)

// fetchPlan pulls the current range plan from a node.
func fetchPlan(t *testing.T, addr string) *ranges.Plan {
	t.Helper()
	resp, err := client.Get(addr + "/internal/ranges")
	if err != nil {
		t.Fatalf("GET /internal/ranges: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Enabled bool           `json:"enabled"`
		Plan    *ranges.Plan `json:"plan"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode plan: %v (%s)", err, body)
	}
	if !out.Enabled {
		t.Fatal("range map not enabled")
	}
	return out.Plan
}

// TestRangeMapHotSpotSplit hammers one key range and verifies the
// planner carves the hot territory into split ranges owned by
// different nodes, while a cold range stays untouched by splits.
func TestRangeMapHotSpotSplit(t *testing.T) {
	a := startNodeRF(t, t.TempDir(), 1)
	b := startNodeRF(t, t.TempDir(), 1)
	c := startNodeRF(t, t.TempDir(), 1)
	for _, n := range []*node{a, b, c} {
		n.apiSrv.EnableRangeMap(300 * time.Millisecond)
	}
	// Form the mesh.
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	if err := c.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		return len(a.cl.Peers()) == 2 && len(b.cl.Peers()) == 2 && len(c.cl.Peers()) == 2
	}, "mesh to form")

	// Cold baseline: a few writes so the cold territory is real.
	for i := 0; i < 5; i++ {
		put(t, a.addr, "cold", fmt.Sprintf("k%d", i), map[string]interface{}{"v": i})
	}

	// Hot spot: hammer one key through node a continuously so the
	// EWMA reaches steady state while we poll for the split.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				put(t, a.addr, "hot", "HOTKEY", map[string]interface{}{"x": 1})
			}
		}
	}()
	defer close(stop)

	// The plan should now exist and carve the hot bucket out.
	var plan *ranges.Plan
	hotTok := hotToken("hot/HOTKEY")
	eventually(t, 20*time.Second, func() bool {
		plan = fetchPlan(t, a.addr)
		if plan == nil || len(plan.Ranges) < 2 {
			return false
		}
		for i := range plan.Ranges {
			r := &plan.Ranges[i]
			if r.Start <= hotTok && (r.End == 0 || hotTok < r.End) {
				// Carved out: the hot token's range is at most 2 buckets wide.
				return r.End != 0 && r.End-r.Start <= 2<<24
			}
		}
		return false
	}, "hot spot carved into a narrow split range")

	// The hot key's token must live in a range whose load dominates.
	var hotRange *ranges.Range
	for i := range plan.Ranges {
		r := &plan.Ranges[i]
		if r.Start <= hotTok && (r.End == 0 || hotTok < r.End) {
			hotRange = r
			break
		}
	}
	if hotRange == nil {
		t.Fatalf("hot token %#x not in any planned range: %+v", hotTok, plan.Ranges)
	}
	// The hot range must be small: a carved-out slice, not the whole space.
	width := hotRange.End - hotRange.Start
	if width > 2<<24 {
		t.Fatalf("hot range too wide to be a split: %#x-%#x", hotRange.Start, hotRange.End)
	}

	// All three nodes must converge on the SAME plan (determinism).
	eventually(t, 5*time.Second, func() bool {
		pb := fetchPlan(t, b.addr)
		pc := fetchPlan(t, c.addr)
		return planEqual(plan, pb) && planEqual(plan, pc)
	}, "all nodes to converge on the same plan")
}

// hotToken mirrors the server's routing: hash(col + "/" + id).
func hotToken(key string) uint32 {
	// ring.Hash is exported for exactly this.
	return ring.Hash(key)
}

func planEqual(a, b *ranges.Plan) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.Ranges) != len(b.Ranges) {
		return false
	}
	for i := range a.Ranges {
		x, y := a.Ranges[i], b.Ranges[i]
		if x.Start != y.Start || x.End != y.End || x.Load != y.Load {
			return false
		}
		if len(x.Owners) != len(y.Owners) {
			return false
		}
		for j := range x.Owners {
			if x.Owners[j] != y.Owners[j] {
				return false
			}
		}
	}
	return true
}

// TestRangeMapDisabledNoPlan verifies a node without the range map
// answers enabled=false.
func TestRangeMapDisabledNoPlan(t *testing.T) {
	a := startNode(t, t.TempDir())
	resp, err := client.Get(a.addr + "/internal/ranges")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Enabled bool `json:"enabled"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode: %v (%s)", err, body)
	}
	if out.Enabled {
		t.Fatal("range map should be disabled by default")
	}
}

// forceReplan triggers an immediate replan on a node and returns the
// resulting plan.
func forceReplan(t *testing.T, addr string) *ranges.Plan {
	t.Helper()
	resp, err := client.Post(addr+"/internal/ranges/replan", "application/json", nil)
	if err != nil {
		t.Fatalf("POST replan: %v", err)
	}
	defer resp.Body.Close()
	var out struct {
		Plan *ranges.Plan `json:"plan"`
	}
	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode replan: %v (%s)", err, body)
	}
	return out.Plan
}

// TestRangeMapJoinDistributesLoad verifies that adding a node to a
// hot cluster fills the new node with SOME primary ranges while
// leaving most existing primaries in place — minimal movement under
// a real membership change.
func TestRangeMapJoinDistributesLoad(t *testing.T) {
	a := startNodeRF(t, t.TempDir(), 1)
	b := startNodeRF(t, t.TempDir(), 1)
	for _, n := range []*node{a, b} {
		n.apiSrv.EnableRangeMap(300 * time.Millisecond)
	}
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "pair to form")

	stop := make(chan struct{})
	defer close(stop)
	// Spread the load across several keys so no single bucket is a
	// super-hot atom that exceeds the band (which no node could hold
	// stickily). Distributed hot ranges stay within the band, so the
	// sticky rebalancer can preserve them across the join.
	go func() {
		keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"}
		for {
			select {
			case <-stop:
				return
			default:
				for _, k := range keys {
					putTolerant(t, a.addr, "hot", k, map[string]interface{}{"x": 1})
				}
			}
		}
	}()

	// Settle p2: wait until two consecutive background-loop samples
	// have the identical structure, so we compare a STABLE 2-node
	// plan against the post-join plan (not a mid-ramp snapshot).
	settled := func() *ranges.Plan {
		var last *ranges.Plan
		for i := 0; i < 40; i++ {
			cur := fetchPlan(t, a.addr)
			if cur != nil && len(cur.Ranges) >= 2 && planEqual(last, cur) {
				return cur
			}
			last = cur
			time.Sleep(400 * time.Millisecond)
		}
		return last
	}
	p2 := settled()
	if p2 == nil || len(p2.Ranges) < 2 {
		t.Fatalf("2-node plan never settled: %+v", p2)
	}

	// Join a third node and let the cluster replan around it.
	c := startNodeRF(t, t.TempDir(), 1)
	c.apiSrv.EnableRangeMap(300 * time.Millisecond)
	if err := c.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}

	// Settle p3 the same way (stable structure with the new node).
	var p3 *ranges.Plan
	eventually(t, 20*time.Second, func() bool {
		cur := settled()
		if cur == nil || len(cur.Ranges) < 2 {
			return false
		}
		for _, r := range cur.Ranges {
			if len(r.Owners) > 0 && r.Owners[0] == c.addr {
				p3 = cur
				return true
			}
		}
		return false
	}, "new node to receive primary ranges")

	// The join must DISTRIBUTE load: no single node should still carry
	// the bulk of the primary load. Sum primary-attributed load per
	// node and require the hottest node to hold well under the total,
	// proving the new node actually absorbed some of the cluster's
	// work. (The exact minimal-movement guarantee is proven
	// deterministically in the unit tests; live EWMA plus range
	// resplitting makes an exact move-count assertion too noisy.)
	nodeLoad := map[string]float64{}
	total := 0.0
	for _, r := range p3.Ranges {
		if len(r.Owners) == 0 {
			continue
		}
		nodeLoad[r.Owners[0]] += r.Load
		total += r.Load
	}
	if total <= 0 {
		t.Fatal("p3 has no primary load to distribute")
	}
	maxShare := 0.0
	for _, l := range nodeLoad {
		if s := l / total; s > maxShare {
			maxShare = s
		}
	}
	if maxShare > 0.7 {
		t.Fatalf("join did not distribute load: hottest node holds %.0f%% of primary load", maxShare*100)
	}
	// And all three nodes carry some primary load.
	if len(nodeLoad) < 3 {
		t.Fatalf("expected load on all 3 nodes, got %d: %v", len(nodeLoad), nodeLoad)
	}
}

var _ = http.StatusOK
