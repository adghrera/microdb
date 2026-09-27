package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func gsiDo(t *testing.T, method, url, body string) (int, map[string]interface{}) {
	t.Helper()
	var r *http.Request
	if body != "" {
		r, _ = http.NewRequest(method, url, bytes.NewReader([]byte(body)))
		r.Header.Set("Content-Type", "application/json")
	} else {
		r, _ = http.NewRequest(method, url, nil)
	}
	resp, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// TestGSIAsyncIndexing: declare an index, writes get indexed
// asynchronously, lookups return matching ids with lag metadata.
func TestGSIAsyncIndexing(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	a.apiSrv.EnableGSI()
	defer a.apiSrv.GSI().Stop()

	// Declare index by_city over field "city".
	code, out := gsiDo(t, "PUT", a.addr+"/api/collections/places/gsi", `{"name":"by_city","field":"city"}`)
	if code != 200 {
		t.Fatalf("declare: %d %v", code, out)
	}

	// Write docs with cities.
	put(t, a.addr, "places", "p1", map[string]interface{}{"city": "Lisbon", "n": 1})
	put(t, a.addr, "places", "p2", map[string]interface{}{"city": "Porto", "n": 2})
	put(t, a.addr, "places", "p3", map[string]interface{}{"city": "Lisbon", "n": 3})

	// Eventually the async worker indexes them.
	eventually(t, 10*time.Second, func() bool {
		code, out := gsiDo(t, "GET", a.addr+"/api/collections/places/gsi/by_city?value=Lisbon", "")
		if code != 200 {
			return false
		}
		ids, _ := out["ids"].([]interface{})
		return len(ids) == 2
	}, "async gsi indexing")

	code, out = gsiDo(t, "GET", a.addr+"/api/collections/places/gsi/by_city?value=Lisbon", "")
	if code != 200 {
		t.Fatalf("lookup: %d", code)
	}
	ids := out["ids"].([]interface{})
	if ids[0] != "p1" || ids[1] != "p3" {
		t.Fatalf("wrong ids: %v", ids)
	}
	if _, ok := out["lag_events"]; !ok {
		t.Fatal("lag metadata missing")
	}

	// Delete removes from the index (async).
	if code := put(t, a.addr, "places", "p1", map[string]interface{}{"city": "Lisbon"}); code != 200 {
		t.Fatal("rewrite")
	}
	// Actually delete p3.
	req, _ := http.NewRequest("DELETE", a.addr+"/api/collections/places/docs/p3", nil)
	resp, _ := client.Do(req)
	resp.Body.Close()
	eventually(t, 10*time.Second, func() bool {
		_, out := gsiDo(t, "GET", a.addr+"/api/collections/places/gsi/by_city?value=Lisbon", "")
		ids, _ := out["ids"].([]interface{})
		return len(ids) == 1 && ids[0] == "p1"
	}, "delete removes from gsi")
}

// TestGSIBackfill: declaring an index AFTER docs exist backfills them.
func TestGSIBackfill(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	a.apiSrv.EnableGSI()
	defer a.apiSrv.GSI().Stop()

	put(t, a.addr, "old", "a", map[string]interface{}{"tag": "x"})
	put(t, a.addr, "old", "b", map[string]interface{}{"tag": "y"})
	put(t, a.addr, "old", "c", map[string]interface{}{"tag": "x"})

	code, out := gsiDo(t, "PUT", a.addr+"/api/collections/old/gsi", `{"name":"by_tag","field":"tag"}`)
	if code != 200 {
		t.Fatalf("declare: %d %v", code, out)
	}
	if out["backfilled"].(float64) != 3 {
		t.Fatalf("backfill count: %v", out["backfilled"])
	}
	_, out = gsiDo(t, "GET", a.addr+"/api/collections/old/gsi/by_tag?value=x", "")
	ids := out["ids"].([]interface{})
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "c" {
		t.Fatalf("backfilled lookup wrong: %v", ids)
	}
}

// TestGSIListAndUnknown: list defs; unknown index returns empty.
func TestGSIListAndUnknown(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	a.apiSrv.EnableGSI()
	defer a.apiSrv.GSI().Stop()

	gsiDo(t, "PUT", a.addr+"/api/collections/z/gsi", `{"name":"by_a","field":"a"}`)
	_, out := gsiDo(t, "GET", a.addr+"/api/collections/z/gsi", "")
	idx := out["indexes"].([]interface{})
	if len(idx) != 1 {
		t.Fatalf("list: %v", out)
	}
	code, _ := gsiDo(t, "GET", a.addr+"/api/collections/z/gsi/nope?value=1", "")
	if code != 200 {
		t.Fatalf("unknown index should 200-empty: %d", code)
	}
}
