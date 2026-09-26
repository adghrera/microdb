package integration

import (
	"testing"
	"time"
)

// TestAntiEntropyRepairsDroppedWrites simulates the known failure mode
// of fire-and-forget replication: a write lands on one node but its
// fanout never reaches the others (e.g. a peer was briefly unknown).
// The Merkle anti-entropy pass must converge all replicas.
func TestAntiEntropyRepairsDroppedWrites(t *testing.T) {
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

	// Bypass the API: write straight into A's store as if fanout dropped.
	// (store.Apply persists + merges locally; no replication triggered.)
	if _, err := a.st.Apply("ghosts", "g1", map[string]interface{}{"haunted": true}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.st.Apply("ghosts", "g2", map[string]interface{}{"haunted": false, "era": "victorian"}); err != nil {
		t.Fatal(err)
	}

	// Immediately after, B and C must NOT have the collection.
	if _, ok := b.st.Get("ghosts", "g1"); ok {
		t.Fatal("g1 should not be on B yet")
	}

	// Anti-entropy runs every 10s; wait up to 30s for convergence.
	for _, id := range []string{"g1", "g2"} {
		for _, n := range []*node{b, c} {
			eventually(t, 30*time.Second, func() bool {
				_, ok := n.st.Get("ghosts", id)
				return ok
			}, "ghost doc "+id+" synced to "+n.addr)
		}
	}

	// Field fidelity after anti-entropy.
	d, ok := c.st.Get("ghosts", "g2")
	if !ok || d.Fields["era"] != "victorian" {
		t.Fatalf("g2 fields wrong after sync: %v", d)
	}
}

// TestAntiEntropyResolvesConcurrentDivergence writes conflicting
// versions of the same doc to two nodes directly (no fanout), then
// verifies LWW by (ver, ts) converges all three replicas to the same
// value.
func TestAntiEntropyResolvesConcurrentDivergence(t *testing.T) {
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

	// Same doc, two divergent versions: A has ver=1, B has ver=2 (winner).
	if _, err := a.st.Apply("kv", "k", map[string]interface{}{"v": "old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.st.Apply("kv", "k", map[string]interface{}{"v": "old"}); err != nil {
		t.Fatal(err)
	}
	// Second apply on B bumps ver to 2.
	if _, err := b.st.Apply("kv", "k", map[string]interface{}{"v": "new"}); err != nil {
		t.Fatal(err)
	}

	for _, n := range []*node{a, b, c} {
		eventually(t, 30*time.Second, func() bool {
			d, ok := n.st.Get("kv", "k")
			return ok && d.Fields["v"] == "new" && d.Ver == 2
		}, "kv/k converged to ver=2 'new' on "+n.addr)
	}
}
