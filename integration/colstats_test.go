package integration

import (
	"encoding/json"
	"net/http"
	"testing"

	"microdb/internal/store"
)

func colStats(t *testing.T, addr string) map[string]map[string]float64 {
	t.Helper()
	resp, err := client.Get(addr + "/api/stats/collections")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Collections map[string]map[string]float64 `json:"collections"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	return out.Collections
}

// TestCollectionIsolationStats: per-collection ops counters and live
// doc counts are tracked independently — a hot collection is visible
// without disturbing others.
func TestCollectionIsolationStats(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()

	for i := 0; i < 10; i++ {
		put(t, a.addr, "hot", "h"+string(rune('a'+i)), map[string]interface{}{"i": i})
	}
	put(t, a.addr, "cold", "c1", map[string]interface{}{"i": 1})
	get(a.addr, "hot", "ha")
	get(a.addr, "cold", "c1")
	if code := put(t, a.addr, "hot", "ha", map[string]interface{}{"i": 99}); code != 200 {
		t.Fatal("rewrite")
	}
	// Delete one hot doc.
	req, _ := http.NewRequest("DELETE", a.addr+"/api/collections/hot/docs/hb", nil)
	resp, _ := client.Do(req)
	resp.Body.Close()

	stats := colStats(t, a.addr)
	hot, cold := stats["hot"], stats["cold"]
	if hot == nil || cold == nil {
		t.Fatalf("missing collection stats: %v", stats)
	}
	// hot: 10 new + 1 rewrite = 11 writes; 1 delete; live = 9.
	if hot["writes"] != 11 {
		t.Fatalf("hot writes: %v want 11", hot["writes"])
	}
	if hot["deletes"] != 1 {
		t.Fatalf("hot deletes: %v want 1", hot["deletes"])
	}
	if hot["live_docs"] != 9 {
		t.Fatalf("hot live: %v want 9", hot["live_docs"])
	}
	// cold untouched by hot's traffic.
	if cold["writes"] != 1 || cold["live_docs"] != 1 {
		t.Fatalf("cold stats polluted by hot: %v", cold)
	}
}

// TestCollectionQuota: max_docs caps a collection's growth without
// touching neighbors; overwrites are free; deletes free quota.
func TestCollectionQuota(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	code, _ := gsiDo(t, "PUT", a.addr+"/api/collections/capped/config", `{"max_docs": 2}`)
	if code != 200 {
		t.Fatalf("config: %d", code)
	}
	// Fill quota.
	if code := put(t, a.addr, "capped", "x1", map[string]interface{}{"v": 1}); code != 200 {
		t.Fatal("x1")
	}
	if code := put(t, a.addr, "capped", "x2", map[string]interface{}{"v": 1}); code != 200 {
		t.Fatal("x2")
	}
	// 3rd new doc refused.
	code, out := gsiDo(t, "PUT", a.addr+"/api/collections/capped/docs/x3", `{"v":1}`)
	if code != 403 || out["error"] != "collection quota exceeded" {
		t.Fatalf("quota must refuse new doc: %d %v", code, out)
	}
	// Overwrite free.
	if code := put(t, a.addr, "capped", "x1", map[string]interface{}{"v": 2}); code != 200 {
		t.Fatalf("overwrite at quota: %d", code)
	}
	// Neighbors unaffected.
	if code := put(t, a.addr, "other", "y1", map[string]interface{}{"v": 1}); code != 200 {
		t.Fatalf("neighbor blocked: %d", code)
	}
	// Delete frees quota.
	req, _ := http.NewRequest("DELETE", a.addr+"/api/collections/capped/docs/x2", nil)
	resp, _ := client.Do(req)
	resp.Body.Close()
	if code := put(t, a.addr, "capped", "x3", map[string]interface{}{"v": 1}); code != 200 {
		t.Fatalf("quota not freed by delete: %d", code)
	}
}

// TestCollectionStatsSurviveRestart: live counts reseed from replay.
func TestCollectionStatsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	a := startNode(t, dir)
	a.cl.MarkBootstrapped()
	for i := 0; i < 5; i++ {
		put(t, a.addr, "persist", "p"+string(rune('a'+i)), map[string]interface{}{"v": i})
	}
	req, _ := http.NewRequest("DELETE", a.addr+"/api/collections/persist/docs/pa", nil)
	resp, _ := client.Do(req)
	resp.Body.Close()
	if got := colStats(t, a.addr)["persist"]["live_docs"]; got != 4 {
		t.Fatalf("pre-restart live: %v want 4", got)
	}
	// Restart the store in place (close + reopen through a fresh node
	// on the same dir).
	a.st.Close()
	st2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if got := st2.StatsFor("persist").Live.Load(); got != 4 {
		t.Fatalf("post-restart live count: %v want 4", got)
	}
}
