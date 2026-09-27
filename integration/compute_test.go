package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"microdb/internal/compute"
)

// TestComputeEngineRoutesThroughStorage: a stateless engine fronts a
// 2-node storage cluster; writes route to the primary, reads come
// back, queries merge across nodes. The engine holds no data itself.
func TestComputeEngineRoutesThroughStorage(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	eng := compute.New([]string{a.addr, b.addr})
	if err := eng.RefreshPlacement(); err != nil {
		t.Fatal(err)
	}
	if len(eng.Nodes()) != 2 {
		t.Fatalf("placement should see 2 nodes: %v", eng.Nodes())
	}

	// Writes through the engine.
	for i := 1; i <= 6; i++ {
		if err := eng.Put("ceng", "d"+string(rune('0'+i)), map[string]interface{}{"i": i}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Read back through the engine.
	f, ok := eng.Get("ceng", "d3")
	if !ok || f["i"].(float64) != 3 {
		t.Fatalf("engine get wrong: %v ok=%v", f, ok)
	}
	// The data lives in the storage tier, not the engine: verify it
	// exists on at least one storage node directly.
	found := 0
	for _, n := range []string{a.addr, b.addr} {
		code, _ := get(n, "ceng", "d3")
		if code == 200 {
			found++
		}
	}
	if found == 0 {
		t.Fatal("engine write never reached the storage tier")
	}
	// Query merges across storage nodes.
	docs := eng.Query("ceng", nil, "i", 10)
	if len(docs) != 6 {
		t.Fatalf("engine query merged %d docs, want 6", len(docs))
	}
	// Filtered query.
	docs = eng.Query("ceng", map[string]interface{}{"i": 4}, "", 0)
	if len(docs) != 1 {
		t.Fatalf("filtered query: %d docs", len(docs))
	}
}

// TestComputeEngineHTTP: the engine's HTTP surface end-to-end.
func TestComputeEngineHTTP(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	eng := compute.New([]string{a.addr})
	if err := eng.RefreshPlacement(); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(eng.Handler())
	defer srv.Close()

	// PUT through compute.
	req, _ := http.NewRequest("PUT", srv.URL+"/api/collections/web/docs/k",
		jsonBody(map[string]interface{}{"hello": "world"}))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("compute PUT: %d", resp.StatusCode)
	}
	// GET through compute.
	resp, err = http.Get(srv.URL + "/api/collections/web/docs/k")
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Fields map[string]interface{} `json:"fields"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if out.Fields["hello"] != "world" {
		t.Fatalf("compute GET: %v", out)
	}
	// Data is on the storage node, readable directly.
	code, sout := get(a.addr, "web", "k")
	if code != 200 || sout["fields"].(map[string]interface{})["hello"] != "world" {
		t.Fatalf("storage node missing compute write: %d %v", code, sout)
	}
	// Health reports the compute role.
	resp, _ = http.Get(srv.URL + "/health")
	var h map[string]string
	json.NewDecoder(resp.Body).Decode(&h)
	resp.Body.Close()
	if h["role"] != "compute" {
		t.Fatalf("health role: %v", h)
	}
}

// TestComputeEngineOverwrite: re-putting a doc through the engine
// increments the version and the new value wins (no LWW rejection).
func TestComputeEngineOverwrite(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	eng := compute.New([]string{a.addr})
	if err := eng.RefreshPlacement(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put("ow", "k", map[string]interface{}{"v": "first"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put("ow", "k", map[string]interface{}{"v": "second"}); err != nil {
		t.Fatal(err)
	}
	f, ok := eng.Get("ow", "k")
	if !ok || f["v"] != "second" {
		t.Fatalf("overwrite lost: %v", f)
	}
}
