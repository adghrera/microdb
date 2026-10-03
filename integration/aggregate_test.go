package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"testing"
	"time"
)

func aggQuery(t *testing.T, addr, query string) map[string]interface{} {
	t.Helper()
	// query is built by the caller with url.Values so filters are
	// properly escaped.
	resp, err := client.Get(addr + "/api/collections/aggcol/docs?" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("aggregate query %q -> %d: %s", query, resp.StatusCode, raw)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("bad response: %v (%s)", err, raw)
	}
	return out
}

func num(t *testing.T, v interface{}, what string) float64 {
	t.Helper()
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("%s is %T (%v), want a number", what, v, v)
	}
	return f
}

// TestAggregatePushdownAcrossNodes is the correctness test that makes
// aggregates usable at all: documents are REPLICATED to every owner, so
// a shard may only count what it is the primary copy of — otherwise a
// two-node cluster reports twice the documents.
func TestAggregatePushdownAcrossNodes(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) >= 1 && len(b.cl.Peers()) >= 1
	}, "cluster formed")

	const n = 20
	for i := 0; i < n; i++ {
		city := "Lisbon"
		if i%2 == 1 {
			city = "Porto"
		}
		if code := put(t, a.addr, "aggcol", fmt.Sprintf("k%02d", i),
			map[string]interface{}{"n": i, "city": city}); code != 200 {
			t.Fatalf("write %d: %d", i, code)
		}
	}
	// Both nodes must hold every document (RF=3 over two nodes) before
	// the count means anything.
	eventually(t, 10*time.Second, func() bool {
		return a.st.DocCount() >= n && b.st.DocCount() >= n
	}, "documents replicated to both nodes")

	for _, node := range []string{a.addr, b.addr} {
		count := num(t, aggQuery(t, node, "agg=count")["aggregate"], "count")
		if count != n {
			t.Fatalf("%s: count = %v, want %d (replicas double-counted?)", node, count, n)
		}
		sum := num(t, aggQuery(t, node, "agg=sum&field=n")["aggregate"], "sum")
		if sum != 190 { // 0..19
			t.Errorf("%s: sum = %v, want 190", node, sum)
		}
		avg := num(t, aggQuery(t, node, "agg=avg&field=n")["aggregate"], "avg")
		if avg != 9.5 {
			t.Errorf("%s: avg = %v, want 9.5", node, avg)
		}
		max := num(t, aggQuery(t, node, "agg=max&field=n")["aggregate"], "max")
		if max != 19 {
			t.Errorf("%s: max = %v, want 19", node, max)
		}
		// Filtered count: Porto = odd i = 10 docs.
		filtered := num(t, aggQuery(t, node,
			"agg=count&filter="+url.QueryEscape(`{"city":"Porto"}`))["aggregate"], "filtered count")
		if filtered != 10 {
			t.Errorf("%s: filtered count = %v, want 10", node, filtered)
		}
		// Grouped: two buckets of 10.
		grouped := aggQuery(t, node, "agg=count&group_by=city")
		groups, ok := grouped["groups"].(map[string]interface{})
		if !ok || len(groups) != 2 {
			t.Fatalf("%s: groups = %#v, want 2 buckets", node, grouped["groups"])
		}
		for key, v := range groups {
			if num(t, v, "group "+key) != 10 {
				t.Errorf("%s: group %s = %v, want 10", node, key, v)
			}
		}
	}
}

// TestAggregateShipsNumbersNotDocuments: the point of pushdown is that
// the response carries a scalar, not a result set.
func TestAggregateShipsNumbersNotDocuments(t *testing.T) {
	a := startNode(t, t.TempDir())
	for i := 0; i < 5; i++ {
		put(t, a.addr, "aggcol", fmt.Sprintf("k%d", i), map[string]interface{}{"n": i})
	}
	resp, err := client.Get(a.addr + "/api/collections/aggcol/docs?agg=count")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) > 2000 {
		t.Errorf("aggregate response is %d bytes — it should be numbers, not documents", len(raw))
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if _, hasDocs := out["docs"]; hasDocs {
		t.Error("aggregate response still carries documents")
	}
	if out["nodes_queried"] == nil {
		t.Error("response should say how many shards were asked")
	}

	// Validation: unknown kind and a missing field must be 400s, not
	// silently wrong numbers.
	for _, q := range []string{"agg=median", "agg=sum"} {
		r2, err := client.Get(a.addr + "/api/collections/aggcol/docs?" + q)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, r2.Body)
		r2.Body.Close()
		if r2.StatusCode != 400 {
			t.Errorf("%q -> %d, want 400", q, r2.StatusCode)
		}
	}
}
