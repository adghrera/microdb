package integration

import (
	"testing"
	"time"
)

// TestReplicationFactor verifies --rf controls fanout breadth:
// RF=1 -> doc stays only on nodes that own it (no extra copies pushed);
// RF=2 -> doc reaches both owners.
func TestReplicationFactor(t *testing.T) {
	// RF=1 cluster: a + b.
	a1 := startNodeRF(t, t.TempDir()+"/a", 1)
	b1 := startNodeRF(t, t.TempDir()+"/b", 1)
	if err := b1.cl.Join(a1.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a1.cl.Peers()) == 1 && len(b1.cl.Peers()) == 1
	}, "mesh")

	// Write to the owner of rf1/k. With RF=1 only the owner holds it.
	owner := a1
	if !owner.apiSrv.Owns("rf1", "k") {
		owner = b1
	}
	put(t, owner.addr, "rf1", "k", map[string]interface{}{"v": 1})
	time.Sleep(1500 * time.Millisecond)
	nonOwner := b1
	if owner == b1 {
		nonOwner = a1
	}
	if _, ok := nonOwner.st.Get("rf1", "k"); ok {
		t.Fatal("RF=1 should not replicate to the non-owner")
	}
	if _, ok := owner.st.Get("rf1", "k"); !ok {
		t.Fatal("owner lost the doc")
	}

	// RF=2 cluster: a + b. Both nodes are owners of every key (2-node
	// ring, RF=2), so the doc must reach both.
	a2 := startNodeRF(t, t.TempDir()+"/a", 2)
	b2 := startNodeRF(t, t.TempDir()+"/b", 2)
	if err := b2.cl.Join(a2.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a2.cl.Peers()) == 1 && len(b2.cl.Peers()) == 1
	}, "mesh")
	put(t, a2.addr, "rf2", "k", map[string]interface{}{"v": 2})
	eventually(t, 10*time.Second, func() bool {
		_, ok := b2.st.Get("rf2", "k")
		return ok
	}, "RF=2: doc should reach the second owner")
}
