package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

// TestDecommissionHandsOffData: a 3-node cluster; decommission one
// node and verify (a) its docs land on the remaining peers, (b) the
// remaining nodes evict it from the ring immediately (no 15s TTL wait),
// (c) the drained node refuses new writes with 503.
func TestDecommissionHandsOffData(t *testing.T) {
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
	}, "3-node mesh")

	// Seed data across collections.
	for i := 0; i < 12; i++ {
		id := string(rune('a' + i))
		put(t, a.addr, "dc", id, map[string]interface{}{"i": i})
	}
	eventually(t, 10*time.Second, func() bool {
		return b.st.DocCount() >= 12 && c.st.DocCount() >= 12
	}, "initial replication")

	// Decommission C.
	resp, err := http.Post(c.addr+"/internal/decommission", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("decommission status %d: %s", resp.StatusCode, body)
	}
	var out map[string]interface{}
	json.Unmarshal(body, &out)
	if out["handed_off"].(float64) < 12 {
		t.Fatalf("handed_off=%v want >=12", out["handed_off"])
	}

	// A and B must evict C from membership WITHOUT waiting for TTL.
	eventually(t, 5*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1 &&
			!containsAddr(a.cl.Peers(), c.addr) && !containsAddr(b.cl.Peers(), c.addr)
	}, "peers dropped C immediately")

	// Drained node refuses writes.
	if code := put(t, c.addr, "dc", "late", map[string]interface{}{"x": 1}); code != 503 {
		t.Fatalf("write to draining node: got %d want 503", code)
	}

	// All 12 docs readable from A and B after the drain (stop C first
	// so reads can't silently depend on it).
	c.srv.Close()
	for i := 0; i < 12; i++ {
		id := string(rune('a' + i))
		if code, _ := get(a.addr, "dc", id); code != 200 {
			t.Fatalf("doc %s missing on A after drain: %d", id, code)
		}
		if code, _ := get(b.addr, "dc", id); code != 200 {
			t.Fatalf("doc %s missing on B after drain: %d", id, code)
		}
	}
}

// TestLeaveTombstoneBlocksGossipResurrection: after C leaves, a stale
// gossip from a node that still lists C must not re-add C — but an
// explicit join from C (a restart) clears the tombstone.
func TestLeaveTombstoneBlocksGossipResurrection(t *testing.T) {
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
		return len(a.cl.Peers()) == 2 && len(b.cl.Peers()) == 2
	}, "mesh")

	// B learns C departed (same call C's leave broadcast makes).
	resp, err := http.Post(b.addr+"/internal/leave", "application/json",
		jsonBody(map[string]interface{}{"addr": c.addr, "epoch": b.cl.Epoch()}))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	eventually(t, 3*time.Second, func() bool {
		return !containsAddr(b.cl.Peers(), c.addr)
	}, "B dropped C")

	// A still thinks C is alive. A gossips its stale member list to B.
	// B must NOT resurrect C — the tombstone blocks it.
	staleMembers := []map[string]interface{}{
		{"addr": a.addr, "ttl": time.Now().UnixMilli() + 15000},
		{"addr": b.addr, "ttl": time.Now().UnixMilli() + 15000},
		{"addr": c.addr, "ttl": time.Now().UnixMilli() + 15000},
	}
	body, _ := json.Marshal(map[string]interface{}{
		"addr":    a.addr,
		"members": staleMembers,
		"epoch":   a.cl.Epoch(),
	})
	resp2, err := http.Post(b.addr+"/internal/gossip", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	time.Sleep(300 * time.Millisecond)
	if containsAddr(b.cl.Peers(), c.addr) {
		t.Fatal("stale gossip resurrected a gracefully-departed node")
	}

	// But an explicit JOIN from C clears the tombstone (restart case).
	resp3, err := http.Post(b.addr+"/internal/join", "application/json",
		jsonBody(map[string]interface{}{"addr": c.addr, "epoch": b.cl.Epoch()}))
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	eventually(t, 3*time.Second, func() bool {
		return containsAddr(b.cl.Peers(), c.addr)
	}, "explicit join overrides tombstone")
}

func jsonBody(v interface{}) io.Reader {
	b, _ := json.Marshal(v)
	return bytes.NewReader(b)
}

func containsAddr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
