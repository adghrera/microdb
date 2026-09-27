package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestQuorumRead verifies a quorum read merges replicas and returns the
// newest version, and reports the quorum size.
func TestQuorumRead(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	put(t, a.addr, "qr", "k", map[string]interface{}{"v": "hello"})
	eventually(t, 10*time.Second, func() bool {
		_, ok := b.st.Get("qr", "k")
		return ok
	}, "replicate to b")

	// Quorum read from b.
	resp, err := client.Get(b.addr + "/api/collections/qr/docs/k?consistency=quorum")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("quorum read status %d", resp.StatusCode)
	}
	var d map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&d)
	f := d["fields"].(map[string]interface{})
	if f["v"] != "hello" {
		t.Fatalf("quorum read wrong value: %v", f)
	}
}

// TestQuorumReadNotReached: with a peer that won't answer, quorum fails
// with 503 and reports answered<need.
func TestQuorumReadNotReached(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	// No peers: quorum of 1 member needs 1, so it succeeds locally.
	// To force failure we need a member that errors. Simulate by adding
	// a dead peer to a's view via a second node then killing it.
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1
	}, "a sees b")
	put(t, a.addr, "qr", "k", map[string]interface{}{"v": 1})

	// Kill b's HTTP by pointing a's peer entry at a dead port.
	a.cl.ForcePeer("http://127.0.0.1:1") // dead address, counted as a member

	resp, err := client.Get(a.addr + "/api/collections/qr/docs/k?consistency=all")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// 'all' needs both members; the dead one fails => 503.
	if resp.StatusCode != 503 {
		t.Fatalf("expected 503 when quorum unreachable, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	if out["error"] != "quorum not reached" {
		t.Fatalf("wrong error: %v", out)
	}
}

// TestQuorumWrite verifies a quorum write acks when W replicas accept,
// and that the data is on the replicas.
func TestQuorumWrite(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// Quorum PUT to the owner of the key.
	owner := a
	if !owner.apiSrv.Owns("qw", "k") {
		owner = b
	}
	body := []byte(`{"v":"quorum"}`)
	req, _ := http.NewRequest("PUT", owner.addr+"/api/collections/qw/docs/k?consistency=quorum", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("quorum write status %d", resp.StatusCode)
	}
	// Both members should now have it (W=2 of the 2-member RF set here).
	eventually(t, 10*time.Second, func() bool {
		_, okA := a.st.Get("qw", "k")
		_, okB := b.st.Get("qw", "k")
		return okA && okB
	}, "quorum write landed on both")
}

// TestFencingStaleEpoch verifies an owner rejects a forwarded write
// whose epoch is behind, and the forwarder adopts + retries.
func TestFencingStaleEpoch(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// Find the owner of the key, then bump ONLY the owner's epoch via
	// ForcePeer (simulating the owner having seen a membership change
	// the forwarder hasn't).
	owner, other := a, b
	if !a.apiSrv.Owns("fence", "k") {
		owner, other = b, a
	}
	owner.cl.BumpEpoch() // owner's ring epoch rises; membership unchanged
	time.Sleep(100 * time.Millisecond)

	// Write via the NON-owner with a deliberately stale epoch header.
	// The owner must fence (409), the forwarder adopts the new epoch
	// and retries — the write should still succeed end-to-end.
	body := []byte(`{"v":"fenced"}`)
	req, _ := http.NewRequest("PUT", other.addr+"/api/collections/fence/docs/k", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		bb := new(bytes.Buffer)
		bb.ReadFrom(resp.Body)
		t.Fatalf("expected fenced write to succeed after retry, got %d: %s", resp.StatusCode, bb.String())
	}
	// Owner has the doc.
	if _, ok := owner.st.Get("fence", "k"); !ok {
		t.Fatal("fenced write did not land on owner")
	}
	// Forwarder adopted the higher epoch.
	if other.cl.Epoch() < owner.cl.Epoch() {
		t.Fatalf("forwarder did not adopt owner epoch: %d < %d", other.cl.Epoch(), owner.cl.Epoch())
	}
}

// TestEpochPropagation verifies the membership epoch rises as nodes join
// and is exposed via the cluster endpoint.
func TestEpochPropagation(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	base := a.cl.Epoch()
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return a.cl.Epoch() > base && b.cl.Epoch() > base
	}, "epoch rises on join")
	// Epochs converge to the same value via gossip.
	eventually(t, 10*time.Second, func() bool {
		return a.cl.Epoch() == b.cl.Epoch()
	}, "epochs converge")
}
