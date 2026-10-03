package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestScatterGatherQuery writes docs to three different nodes, then
// queries the collection from ONE node and expects the union of all
// matching docs across the cluster — not just the local shard.
func TestScatterGatherQuery(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	c := startNode(t, t.TempDir()+"/c")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	if err := c.cl.Join(b.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 2 && len(b.cl.Peers()) == 2 && len(c.cl.Peers()) == 2
	}, "full mesh")

	// One matching doc per node, plus one non-matching doc.
	put(t, a.addr, "sg", "a1", map[string]interface{}{"tier": 5})
	put(t, b.addr, "sg", "b1", map[string]interface{}{"tier": 7})
	put(t, c.addr, "sg", "c1", map[string]interface{}{"tier": 9})
	put(t, a.addr, "sg", "low", map[string]interface{}{"tier": 1})

	// Query from C: must see a1, b1, c1 (tier > 4) even though only
	// c1 was written here.
	query := a.addr + "/api/collections/sg/docs?filter=" + urlQuery(`{"tier":{"$gt":4}}`)
	resp, err := client.Get(query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Count      int                      `json:"count"`
		Docs       []map[string]interface{} `json:"docs"`
		NodesQuery int                      `json:"nodes_queried"`
		Partial    bool                     `json:"partial"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Partial {
		t.Fatalf("query reported partial results: %v", out)
	}
	got := map[string]bool{}
	for _, d := range out.Docs {
		got[d["id"].(string)] = true
	}
	for _, want := range []string{"a1", "b1", "c1"} {
		if !got[want] {
			t.Fatalf("scatter-gather missed %q; got %v", want, got)
		}
	}
	if got["low"] {
		t.Fatal("non-matching doc leaked into results")
	}
	if out.NodesQuery != 3 {
		t.Fatalf("expected 3 nodes queried, got %d", out.NodesQuery)
	}
	_ = fmt.Sprint
	_ = http.StatusOK
}

// TestLoadAwarePlacement: hammering one node with writes raises its
// gossiped load, and the weighted ring shifts key ownership toward it.
func TestLoadAwarePlacement(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// Baseline: count how many of a key sample a owns (primary).
	ownA := func() int {
		n := 0
		for i := 0; i < 2000; i++ {
			if a.apiSrv.RingSnapshot().Owners("loadkey"+itoa2(i), 1)[0] == a.addr {
				n++
			}
		}
		return n
	}
	base := ownA()

	// Hammer node A with writes for ~4 gossip ticks so its EWMA
	// write-rate climbs well above B's. Only keys A is primary for
	// count toward A's load — writes to B-primary keys would be
	// forwarded and load B instead (which is correct behavior, but
	// would muddy this test's signal).
	deadline := time.Now().Add(4500 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		key := "h" + itoa2(i)
		if !a.apiSrv.Owns("hotcol", key) {
			continue
		}
		put(t, a.addr, "hotcol", key, map[string]interface{}{"i": i})
	}

	// A's self load must be positive and gossiped to B.
	eventually(t, 8*time.Second, func() bool {
		return a.cl.SelfLoad() > 1 && b.cl.Loads()[a.addr] > 1
	}, "load gossiped")

	// The ring must have shifted: A now owns more of the sample than
	// baseline (it carries all the load, B ~0, so A gets the max
	// weight share). The rebuild happens on gossip ticks with the
	// loads from the PREVIOUS round, so poll until it lands.
	eventually(t, 15*time.Second, func() bool {
		return ownA() > base
	}, "ownership shifted to hot node")
}

func itoa2(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
