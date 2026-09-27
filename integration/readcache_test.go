package integration

import (
	"testing"
	"time"

	"microdb/internal/readcache"
)

func TestReadCacheHitMissInvalidate(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	a.apiSrv.EnableReadCache(1000, time.Minute)

	put(t, a.addr, "rc", "d1", map[string]interface{}{"v": 1})

	// First read: miss (populates).
	get(a.addr, "rc", "d1")
	_, _, inv := a.apiSrv.CacheStats()
	h, m, _ := a.apiSrv.CacheStats()
	if h != 0 || m != 1 {
		t.Fatalf("first read should be a miss: hits=%d misses=%d", h, m)
	}
	// Second read: hit.
	get(a.addr, "rc", "d1")
	h, m, _ = a.apiSrv.CacheStats()
	if h != 1 || m != 1 {
		t.Fatalf("second read should be a hit: hits=%d misses=%d", h, m)
	}
	// A write to the doc must invalidate the cached entry.
	put(t, a.addr, "rc", "d1", map[string]interface{}{"v": 2})
	h, m, inv = a.apiSrv.CacheStats()
	if inv < 1 {
		t.Fatalf("write must invalidate: inv=%d", inv)
	}
	// Read after write: miss again, and returns the NEW value.
	code, out := get(a.addr, "rc", "d1")
	if code != 200 || out["fields"].(map[string]interface{})["v"].(float64) != 2 {
		t.Fatalf("stale value served after invalidation: %v", out)
	}
	// Delete invalidates too: subsequent read is 404.
	// (delete -> invalidation -> miss -> store says gone)
	if code, _ = get(a.addr, "rc", "d1"); code != 200 {
		t.Fatalf("doc should still exist: %d", code)
	}
	a.st.Delete("rc", "d1")
	if code, _ = get(a.addr, "rc", "d1"); code != 404 {
		t.Fatalf("deleted doc must 404 through cache: %d", code)
	}
}

func TestReadCacheReplicatedApplyInvalidates(t *testing.T) {
	// Two-node mesh: a write to A replicates to B; B's cache must be
	// invalidated by the replicated apply's change-feed event.
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	a.cl.MarkBootstrapped()
	b.cl.MarkBootstrapped()
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")
	b.apiSrv.EnableReadCache(1000, time.Minute)

	put(t, a.addr, "repc", "x", map[string]interface{}{"v": 1})
	// Wait for replication to B, then warm B's cache.
	eventually(t, 10*time.Second, func() bool {
		code, _ := get(b.addr, "repc", "x")
		return code == 200
	}, "replicated to B")
	get(b.addr, "repc", "x") // ensure cached (hit)

	// New write on A -> replicates to B -> B's cache invalidated.
	put(t, a.addr, "repc", "x", map[string]interface{}{"v": 2})
	eventually(t, 10*time.Second, func() bool {
		_, _, inv := b.apiSrv.CacheStats()
		return inv >= 2 // upsert v1 apply + v2 apply both invalidate
	}, "B cache invalidated by replicated apply")
	code, out := get(b.addr, "repc", "x")
	if code != 200 || out["fields"].(map[string]interface{})["v"].(float64) != 2 {
		t.Fatalf("B served stale through cache: %v", out)
	}
}

func TestReadCacheLRUBound(t *testing.T) {
	c := readcache.New(32, 0) // 2 per shard x 16
	for i := 0; i < 1000; i++ {
		c.Put(itoa2(i), i)
	}
	if c.Len() > 32 {
		t.Fatalf("cache exceeded capacity: %d > 32", c.Len())
	}
	// Recently inserted keys must be present.
	if _, ok := c.Get("999"); !ok {
		t.Fatal("recent key missing")
	}
}
