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
		Count        int                      `json:"count"`
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
