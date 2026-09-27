package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"microdb/internal/api"
)

// TestClusterNameGuard: a node configured for cluster "prod" rejects
// internal traffic from a node that says "staging" — and vice versa.
func TestClusterNameGuard(t *testing.T) {
	prod := startNode(t, t.TempDir()+"/prod")
	prod.cl.SetClusterName("prod-cluster")
	// A raw internal call without the header must be refused.
	resp, err := client.Get(prod.addr + "/internal/collections")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("unguarded internal call: got %d want 403", resp.StatusCode)
	}
	// Wrong cluster name refused.
	req, _ := http.NewRequest("GET", prod.addr+"/internal/collections", nil)
	req.Header.Set("X-Microdb-Cluster", "staging-cluster")
	resp2, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 403 {
		t.Fatalf("wrong-cluster call: got %d want 403", resp2.StatusCode)
	}
	// Right name passes.
	req3, _ := http.NewRequest("GET", prod.addr+"/internal/collections", nil)
	req3.Header.Set("X-Microdb-Cluster", "prod-cluster")
	resp3, err := client.Do(req3)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatalf("right-cluster call: got %d want 200", resp3.StatusCode)
	}
	// Public API unaffected by the guard.
	if code, _ := get(prod.addr, "x", "y"); code != 404 {
		t.Fatalf("public API should pass the guard (404 expected, not 403): %d", code)
	}
}

// TestJoinRefusedAcrossClusters: a node with a different cluster name
// cannot join — Join surfaces the refusal instead of merging.
func TestJoinRefusedAcrossClusters(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.SetClusterName("alpha")
	b := startNode(t, t.TempDir()+"/b")
	b.cl.SetClusterName("beta")
	if err := b.cl.Join(a.addr); err == nil {
		t.Fatal("cross-cluster join must fail")
	}
	if len(a.cl.Peers()) != 0 || len(b.cl.Peers()) != 0 {
		t.Fatalf("clusters merged anyway: a=%v b=%v", a.cl.Peers(), b.cl.Peers())
	}
	// Same name joins fine.
	c := startNode(t, t.TempDir()+"/c")
	c.cl.SetClusterName("alpha")
	if err := c.cl.Join(a.addr); err != nil {
		t.Fatalf("same-cluster join failed: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(c.cl.Peers()) == 1
	}, "same-cluster mesh")
}

// TestReadyEndpoint: /ready gates on bootstrap + drain state.
func TestReadyEndpoint(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	// Fresh node: streaming -> not ready.
	resp, err := client.Get(a.addr + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if resp.StatusCode != 503 || out["reason"] != "bootstrapping" {
		t.Fatalf("streaming node should be not-ready: %d %v", resp.StatusCode, out)
	}
	a.cl.MarkBootstrapped()
	resp2, _ := client.Get(a.addr + "/ready")
	var out2 map[string]interface{}
	json.NewDecoder(resp2.Body).Decode(&out2)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || out2["ready"] != true {
		t.Fatalf("bootstrapped node should be ready: %d %v", resp2.StatusCode, out2)
	}
	// /health still 200 throughout (process liveness).
	resp3, _ := client.Get(a.addr + "/health")
	resp3.Body.Close()
	if resp3.StatusCode != 200 {
		t.Fatal("/health should always be 200")
	}
}

// TestVersionEndpoint reports build identity.
func TestVersionEndpoint(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	resp, err := client.Get(a.addr + "/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	if out["version"] != api.Version {
		t.Fatalf("version mismatch: %v want %s", out["version"], api.Version)
	}
	if out["go"] == nil || out["uptime_s"] == nil {
		t.Fatalf("version payload incomplete: %v", out)
	}
}

// TestIdempotencyKey: a retried PUT with the same Idempotency-Key is
// executed once; the replay returns the cached response and does NOT
// bump the document version.
func TestIdempotencyKey(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	body := []byte(`{"v":"once"}`)

	doPut := func(key string) (*http.Response, map[string]interface{}) {
		req, _ := http.NewRequest("PUT", a.addr+"/api/collections/idem/docs/k", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&out)
		return resp, out
	}
	_, first := doPut("key-1")
	if first["ver"].(float64) != 1 {
		t.Fatalf("first write ver=%v want 1", first["ver"])
	}
	// Retry with same key: replay, same version, flagged.
	resp2, replay := doPut("key-1")
	if replay["ver"].(float64) != 1 {
		t.Fatalf("replay bumped version: %v", replay)
	}
	if resp2.Header.Get("X-Idempotent-Replay") != "true" {
		t.Fatal("replay not flagged")
	}
	// Different key: real second write.
	_, second := doPut("key-2")
	if second["ver"].(float64) != 2 {
		t.Fatalf("different key should create v2: %v", second)
	}
	// No key at all: every call is a real write.
	_, third := doPut("")
	if third["ver"].(float64) != 3 {
		t.Fatalf("keyless writes should not dedupe: %v", third)
	}
}

// TestIdempotencyCacheExpiry: after the TTL passes, the same
// Idempotency-Key executes again instead of replaying the cached
// response.
func TestIdempotencyCacheExpiry(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	a.apiSrv.SetIdempotencyTTL(80 * time.Millisecond)

	doPut := func() map[string]interface{} {
		req, _ := http.NewRequest("PUT", a.addr+"/api/collections/exp/docs/k", bytes.NewReader([]byte(`{"v":"x"}`)))
		req.Header.Set("Idempotency-Key", "ttl-key")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&out)
		return out
	}
	first := doPut()
	if first["ver"].(float64) != 1 {
		t.Fatalf("first write ver=%v want 1", first["ver"])
	}
	// Within TTL: replay.
	replay := doPut()
	if replay["ver"].(float64) != 1 {
		t.Fatalf("within-TTL call should replay v1: %v", replay)
	}
	// Past TTL: real second write.
	time.Sleep(120 * time.Millisecond)
	after := doPut()
	if after["ver"].(float64) != 2 {
		t.Fatalf("post-TTL call should re-execute (v2): %v", after)
	}
}
