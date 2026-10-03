package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestPerCollectionRF: a collection configured with rf=1 replicates
// to only its primary; a collection with no override uses the node
// default. Config is set over the API and replicates like normal data.
func TestPerCollectionRF(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// Set rf=1 on collection "solo" via the API.
	body, _ := json.Marshal(map[string]interface{}{"rf": 1})
	req, _ := http.NewRequest("PUT", a.addr+"/api/collections/solo/config", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != 200 || out["effective"].(float64) != 1 {
		t.Fatalf("config set failed: %d %v", resp.StatusCode, out)
	}

	// Owner set for a solo key must have exactly 1 member.
	for i := 0; i < 50; i++ {
		id := string(rune('a' + i))
		if owners := a.apiSrv.OwnerSet("solo", id); len(owners) != 1 {
			t.Fatalf("solo owner set len=%d want 1", len(owners))
		}
	}
	// Default collection still uses node RF (3 from startNode), but
	// the ring can only return as many distinct owners as there are
	// members (2 here).
	if owners := a.apiSrv.OwnerSet("normal", "x"); len(owners) != 2 {
		t.Fatalf("normal owner set len=%d want 2 (2 members, rf=3)", len(owners))
	}

	// Config replicates: b sees the same effective RF. (Must happen
	// BEFORE the data write, or a forwarded write would fan out with
	// b's still-default RF and "leak" to the non-primary.)
	eventually(t, 10*time.Second, func() bool {
		return b.st.EffectiveRF("solo", 3) == 1
	}, "config replicated to b")

	// Write to the rf=1 collection: only the primary should get it.
	primary := a.apiSrv.OwnerSet("solo", "onlykey")[0]
	put(t, a.addr, "solo", "onlykey", map[string]interface{}{"v": 1})
	time.Sleep(500 * time.Millisecond)
	// The non-primary must NOT have the doc (no fanout to it).
	nonPrimary := b
	if primary == b.addr {
		nonPrimary = a
	}
	if _, ok := nonPrimary.st.Get("solo", "onlykey"); ok {
		t.Fatal("rf=1 doc leaked to non-primary")
	}
	// The primary has it.
	primaryNode := a
	if primary == b.addr {
		primaryNode = b
	}
	if _, ok := primaryNode.st.Get("solo", "onlykey"); !ok {
		t.Fatal("rf=1 doc missing on primary")
	}

	// Clearing the override (rf=0) restores the node default.
	body0, _ := json.Marshal(map[string]interface{}{"rf": 0})
	req2, _ := http.NewRequest("PUT", a.addr+"/api/collections/solo/config", bytes.NewReader(body0))
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	eventually(t, 10*time.Second, func() bool {
		return a.st.EffectiveRF("solo", 3) == 3
	}, "override cleared")
}

// TestConfigValidation: bad rf values rejected.
func TestConfigValidation(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	for _, bad := range []int{-1, 10, 99} {
		body, _ := json.Marshal(map[string]interface{}{"rf": bad})
		req, _ := http.NewRequest("PUT", a.addr+"/api/collections/bad/config", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("rf=%d should be rejected, got %d", bad, resp.StatusCode)
		}
	}
}
