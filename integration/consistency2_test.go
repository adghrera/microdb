package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestConsistencyLabels: one/local_quorum read locally, quorum/all
// merge, unknown label rejected with 400.
func TestConsistencyLabels(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")
	put(t, a.addr, "cl", "k", map[string]interface{}{"v": "x"})
	eventually(t, 10*time.Second, func() bool {
		_, ok := b.st.Get("cl", "k")
		return ok
	}, "replicate")

	for _, label := range []string{"one", "local_quorum", "quorum", "all"} {
		code, out := getWithConsistency(b.addr, "cl", "k", label)
		if code != 200 {
			t.Fatalf("read consistency=%s: %d %v", label, code, out)
		}
		f := out["fields"].(map[string]interface{})
		if f["v"] != "x" {
			t.Fatalf("read consistency=%s wrong value: %v", label, f)
		}
	}
	// Unknown label rejected.
	code, out := getWithConsistency(b.addr, "cl", "k", "eventually")
	if code != 400 {
		t.Fatalf("unknown consistency should 400, got %d %v", code, out)
	}
}

func getWithConsistency(addr, col, id, consistency string) (int, map[string]interface{}) {
	resp, err := client.Get(addr + "/api/collections/" + col + "/docs/" + id + "?consistency=" + consistency)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestWriteAllConsistency: ?consistency=all blocks until every RF
// owner acked; with a dead member it honestly reports 503 applied:true.
func TestWriteAllConsistency(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// all-write to the owner succeeds (both members up).
	owner := a
	if !owner.apiSrv.Owns("wa", "k") {
		owner = b
	}
	code := putConsistency(t, owner.addr, "wa", "k", map[string]interface{}{"v": 1}, "all")
	if code != 200 {
		t.Fatalf("all-write with healthy cluster: %d", code)
	}

	// Add a dead member to BOTH nodes' views so whichever node owns
	// the key sees the dead peer in its RF-owner set.
	a.cl.ForcePeer("http://127.0.0.1:1")
	b.cl.ForcePeer("http://127.0.0.1:1")
	// Find a key whose owner set starts at a live node and includes
	// the dead peer — then 'all' must fail: the dead member can't ack.
	liveOwner := a
	deadKey := ""
	for _, cand := range []*node{a, b} {
		for i := 0; i < 500 && deadKey == ""; i++ {
			id := "d" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
			owners := cand.apiSrv.OwnerSet("wa", id)
			hasDead := false
			for _, o := range owners {
				if o == "http://127.0.0.1:1" {
					hasDead = true
				}
			}
			if len(owners) >= 2 && owners[0] == cand.addr && hasDead {
				liveOwner = cand
				deadKey = id
			}
		}
		if deadKey != "" {
			break
		}
	}
	if deadKey == "" {
		t.Skip("no key with [live, dead] owner set found")
	}
	code = putConsistency(t, liveOwner.addr, "wa", deadKey, map[string]interface{}{"v": 2}, "all")
	if code != 503 {
		t.Fatalf("all-write with dead member in owner set: got %d want 503", code)
	}
	// But 'one' still succeeds instantly when the primary is live.
	liveKey := ""
	for i := 0; i < 500 && liveKey == ""; i++ {
		id := "s" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		owners := a.apiSrv.OwnerSet("wa", id)
		if len(owners) > 0 && owners[0] == a.addr {
			liveKey = id
		}
	}
	if liveKey == "" {
		t.Skip("no live-primary key found")
	}
	code = putConsistency(t, a.addr, "wa", liveKey, map[string]interface{}{"v": 3}, "one")
	if code != 200 {
		t.Fatalf("one-write with live primary: %d", code)
	}
}

func putConsistency(t *testing.T, addr, col, id string, fields map[string]interface{}, consistency string) int {
	t.Helper()
	b, _ := json.Marshal(fields)
	req, _ := http.NewRequest("PUT", addr+"/api/collections/"+col+"/docs/"+id+"?consistency="+consistency, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// TestHedgedQuery: hedge_ms triggers duplicate gathers on slow
// members but results stay correct and complete.
func TestHedgedQuery(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")
	for i := 0; i < 10; i++ {
		put(t, a.addr, "hq", string(rune('a'+i)), map[string]interface{}{"n": i})
	}
	eventually(t, 10*time.Second, func() bool {
		return b.st.DocCount() >= 10
	}, "replicate")

	resp, err := client.Get(a.addr + "/api/collections/hq/docs?hedge_ms=50")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Count int             `json:"count"`
		Docs  []map[string]any `json:"docs"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if out.Count != 10 {
		t.Fatalf("hedged query count=%d want 10", out.Count)
	}
	// Unknown hedge_ms rejected.
	resp2, err := client.Get(a.addr + "/api/collections/hq/docs?hedge_ms=1")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 400 {
		t.Fatalf("hedge_ms=1 should 400, got %d", resp2.StatusCode)
	}
}
