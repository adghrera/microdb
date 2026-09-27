package integration

import (
	"encoding/json"
	"testing"
	"time"
)

// TestPaginationSort verifies sort=, desc=, limit=, offset= on the
// scatter-gather query, with docs spread across nodes.
func TestPaginationSort(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// 5 docs, scores 1..5, alternating nodes.
	for i := 1; i <= 5; i++ {
		node := a
		if i%2 == 0 {
			node = b
		}
		put(t, node.addr, "pg", "d"+string(rune('0'+i)), map[string]interface{}{"score": float64(i)})
	}

	get := func(q string) (int, int, []string) {
		resp, err := client.Get(a.addr + "/api/collections/pg/docs?" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Count      int                      `json:"count"`
			Total      int                      `json:"total"`
			TotalExact *bool                    `json:"total_exact"`
			Docs       []map[string]interface{} `json:"docs"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		ids := []string{}
		for _, d := range out.Docs {
			ids = append(ids, d["id"].(string))
		}
		return out.Count, out.Total, ids
	}
	getExact := func(q string) (int, int, []string, bool) {
		resp, err := client.Get(a.addr + "/api/collections/pg/docs?" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Count      int                      `json:"count"`
			Total      int                      `json:"total"`
			TotalExact *bool                    `json:"total_exact"`
			Docs       []map[string]interface{} `json:"docs"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		ids := []string{}
		for _, d := range out.Docs {
			ids = append(ids, d["id"].(string))
		}
		exact := out.TotalExact != nil && *out.TotalExact
		return out.Count, out.Total, ids, exact
	}

	// Sort ascending by score across the merged set.
	_, total, ids := get("sort=score")
	if total != 5 {
		t.Fatalf("total should be 5, got %d", total)
	}
	want := []string{"d1", "d2", "d3", "d4", "d5"}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("asc sort wrong: %v", ids)
		}
	}

	// Descending.
	_, _, ids = get("sort=score&desc=true")
	if ids[0] != "d5" || ids[4] != "d1" {
		t.Fatalf("desc sort wrong: %v", ids)
	}

	// Limit + offset page 2 of size 2 => d3, d4. With sort+limit
	// pushdown each shard returns only its top (offset+limit) docs,
	// so the pre-pagination total is a lower bound (total_exact=false)
	// while the window itself is exact.
	count, total, ids, exact := getExact("sort=score&limit=2&offset=2")
	if count != 2 || ids[0] != "d3" || ids[1] != "d4" {
		t.Fatalf("pagination wrong: count=%d ids=%v", count, ids)
	}
	// With RF=3 on 2 nodes, each node holds a full replica (5 docs).
	// cap=4 truncates both shards -> merged total is a lower bound
	// (4), but the returned window is still exactly d3, d4.
	if exact || total != 4 {
		t.Fatalf("truncated replicas should give inexact lower-bound total: total=%d exact=%v", total, exact)
	}
	// cap=5 covers every replica fully -> no truncation -> exact.
	count, total, ids, exact = getExact("sort=score&limit=5&offset=0")
	if count != 5 || !exact || total != 5 {
		t.Fatalf("full-window query must be exact: count=%d total=%d exact=%v", count, total, exact)
	}
	// cap=1 forces every shard (2+ docs each) to truncate: the
	// merged total becomes a lower bound, but the window is exact.
	count, total, ids, exact = getExact("sort=score&limit=1&offset=0")
	if count != 1 || ids[0] != "d1" {
		t.Fatalf("top-1 wrong: %v", ids)
	}
	if exact {
		t.Fatalf("truncated shards must flag total inexact: total=%d", total)
	}
	// Both replicas' top-1 is the same doc (d1) -> merged set is 1.
	if total < 1 {
		t.Fatalf("lower-bound total below merged count: %d", total)
	}
	// No limit => no pushdown => exact total.
	_, total, _, exact = getExact("sort=score")
	if !exact || total != 5 {
		t.Fatalf("unlimited query must report exact total: %d exact=%v", total, exact)
	}

	// Offset past end => empty.
	count, _, ids = get("limit=2&offset=99")
	if count != 0 || len(ids) != 0 {
		t.Fatalf("offset past end should be empty: %d %v", count, ids)
	}
}
