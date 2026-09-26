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
			Count int                      `json:"count"`
			Total int                      `json:"total"`
			Docs  []map[string]interface{} `json:"docs"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		ids := []string{}
		for _, d := range out.Docs {
			ids = append(ids, d["id"].(string))
		}
		return out.Count, out.Total, ids
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

	// Limit + offset page 2 of size 2 => d3, d4.
	count, total, ids := get("sort=score&limit=2&offset=2")
	if count != 2 || total != 5 || ids[0] != "d3" || ids[1] != "d4" {
		t.Fatalf("pagination wrong: count=%d total=%d ids=%v", count, total, ids)
	}

	// Offset past end => empty.
	count, _, ids = get("limit=2&offset=99")
	if count != 0 || len(ids) != 0 {
		t.Fatalf("offset past end should be empty: %d %v", count, ids)
	}
}
