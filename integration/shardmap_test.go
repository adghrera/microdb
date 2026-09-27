package integration

import (
	"context"
	"testing"
	"time"

	mdbclient "microdb/client"
)

// TestClientShardMapRouting: a client attached to a NON-owner node
// uses PutRouted to write directly to the key's primary owner, and
// the data lands correctly. Cache invalidation forces re-lookup.
func TestClientShardMapRouting(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// Find a key owned by B, then attach the client to A (non-owner).
	key := ""
	for i := 0; i < 200 && key == ""; i++ {
		id := "r" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
		if b.apiSrv.Owns("sm", id) {
			key = id
		}
	}
	if key == "" {
		t.Skip("no B-owned key found")
	}

	c := mdbclient.New(a.addr)
	ctx := context.Background()

	// Learn the shard map: primary should be B.
	primary, err := c.Owners(ctx, "sm", key)
	if err != nil {
		t.Fatal(err)
	}
	if primary != b.addr {
		t.Fatalf("expected primary %s got %s", b.addr, primary)
	}

	// Routed write goes straight to B.
	d, err := c.PutRouted(ctx, "sm", key, map[string]interface{}{"routed": true})
	if err != nil {
		t.Fatal(err)
	}
	if d.Fields["routed"] != true {
		t.Fatalf("routed write wrong: %v", d.Fields)
	}
	if _, ok := b.st.Get("sm", key); !ok {
		t.Fatal("routed write did not land on primary B")
	}

	// Second write uses the cached primary (no error even though we
	// never re-queried).
	if _, err := c.PutRouted(ctx, "sm", key, map[string]interface{}{"routed": 2}); err != nil {
		t.Fatal(err)
	}

	// Invalidate and route again — re-lookup path.
	c.InvalidateShardMap()
	if _, err := c.PutRouted(ctx, "sm", key, map[string]interface{}{"routed": 3}); err != nil {
		t.Fatal(err)
	}
	d, _ = c.Get(ctx, "sm", key)
	if d.Fields["routed"].(float64) != 3 {
		t.Fatalf("final value wrong after invalidation cycle: %v", d.Fields)
	}
}
