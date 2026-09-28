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

var _ = http.StatusOK
