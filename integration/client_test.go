package integration

import (
	"context"
	"testing"
	"time"

	mdb "microdb/client"
)

// TestGoClient exercises the typed client against a live node:
// put/get/delete, batch, query with filter+sort+limit, cluster info.
func TestGoClient(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	c := mdb.New(a.addr)
	ctx := context.Background()

	d, err := c.Put(ctx, "users", "u1", map[string]interface{}{"name": "Zoe", "age": 28})
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != "u1" || d.Fields["name"] != "Zoe" {
		t.Fatalf("put returned wrong doc: %+v", d)
	}

	got, err := c.Get(ctx, "users", "u1")
	if err != nil || got.Ver != 1 {
		t.Fatalf("get: %v %+v", err, got)
	}

	docs, err := c.Batch(ctx, "users", map[string]map[string]interface{}{
		"u2": {"name": "Yan", "age": 35},
		"u3": {"name": "Xan", "age": 41},
	})
	if err != nil || len(docs) != 2 {
		t.Fatalf("batch: %v %d", err, len(docs))
	}

	res, err := c.Query(ctx, "users", mdb.QueryOpts{
		Filter: map[string]interface{}{"age": map[string]interface{}{"$gte": float64(28)}},
		Sort:   "age",
		Desc:   true,
		Limit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	// limit=2 triggers pushdown: the window is exact (2 docs, desc)
	// but the pre-pagination total is a lower bound.
	if res.Count != 2 || res.Total < 2 {
		t.Fatalf("query counts wrong: %+v", res)
	}
	if res.TotalExact {
		t.Fatalf("pushed-down window should report inexact total: %+v", res)
	}
	if res.Docs[0].ID != "u3" || res.Docs[1].ID != "u2" {
		t.Fatalf("sort desc wrong: %s %s", res.Docs[0].ID, res.Docs[1].ID)
	}

	if err := c.Delete(ctx, "users", "u1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx, "users", "u1"); err == nil {
		t.Fatal("deleted doc still readable")
	}

	info, err := c.Cluster(ctx)
	if err != nil || info.Self != a.addr {
		t.Fatalf("cluster: %v %+v", err, info)
	}
	if err := c.Health(ctx); err != nil {
		t.Fatalf("health: %v", err)
	}
}

// TestGoClientWatch verifies the typed watch stream receives events.
func TestGoClientWatch(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	c := mdb.New(a.addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	w, err := c.Watch(ctx, "wc", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	go func() {
		time.Sleep(200 * time.Millisecond)
		c.Put(ctx, "wc", "x", map[string]interface{}{"v": 1})
	}()

	ev, err := w.Next()
	if err != nil {
		t.Fatalf("watch next: %v", err)
	}
	if ev.Kind != "upsert" || ev.ID != "x" || ev.Collection != "wc" {
		t.Fatalf("wrong event: %+v", ev)
	}
	if w.Since() != ev.Seq {
		t.Fatalf("cursor not advanced: %d vs %d", w.Since(), ev.Seq)
	}
}
