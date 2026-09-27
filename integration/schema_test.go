package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func putSchema(t *testing.T, addr, col, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("PUT", addr+"/api/collections/"+col+"/schema", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestLazySchemaMigration: docs written before a rename are served
// with the new field name after the schema is installed — without any
// rewrite. New writes in the new shape pass through untouched.
func TestLazySchemaMigration(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()

	// Old-shape doc: field "city".
	put(t, a.addr, "users", "u1", map[string]interface{}{"city": "Lisbon", "age": "42"})

	// Install schema: rename city->town, retype age->int.
	resp := putSchema(t, a.addr, "users",
		`{"transforms":[{"op":"rename","from":"city","to":"town"},{"op":"retype","from":"age","to":"int"}]}`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var e map[string]string
		json.NewDecoder(resp.Body).Decode(&e)
		t.Fatalf("schema install failed: %d %v", resp.StatusCode, e)
	}

	// Read: old doc now shows town + numeric age.
	code, out := get(a.addr, "users", "u1")
	if code != 200 {
		t.Fatalf("get: %d", code)
	}
	f := out["fields"].(map[string]interface{})
	if f["town"] != "Lisbon" {
		t.Fatalf("rename not applied on read: %v", f)
	}
	if _, ok := f["city"]; ok {
		t.Fatalf("old field still visible: %v", f)
	}
	if f["age"] != float64(42) {
		t.Fatalf("retype not applied: %#v", f["age"])
	}
	if f["_schema_ver"] != float64(2) {
		t.Fatalf("schema version not stamped in response: %v", f["_schema_ver"])
	}

	// New-shape write (already migrated client): passes through,
	// stored as-is with current version.
	put(t, a.addr, "users", "u2", map[string]interface{}{"town": "Porto", "age": 30})
	_, out2 := get(a.addr, "users", "u2")
	f2 := out2["fields"].(map[string]interface{})
	if f2["town"] != "Porto" || f2["age"] != float64(30) {
		t.Fatalf("new-shape write altered: %v", f2)
	}

	// Query with sort on the NEW field name works across migrated docs.
	qresp, err := client.Get(a.addr + "/api/collections/users/docs?sort=town")
	if err != nil {
		t.Fatal(err)
	}
	var qout struct {
		Docs []struct {
			ID     string                 `json:"id"`
			Fields map[string]interface{} `json:"fields"`
		} `json:"docs"`
	}
	json.NewDecoder(qresp.Body).Decode(&qout)
	qresp.Body.Close()
	// Ascending by town: Lisbon (u1) < Porto (u2).
	if len(qout.Docs) != 2 || qout.Docs[0].ID != "u1" || qout.Docs[1].ID != "u2" {
		t.Fatalf("query on migrated field wrong order: %v", qout.Docs)
	}
	for _, d := range qout.Docs {
		if _, ok := d.Fields["city"]; ok {
			t.Fatalf("query leaked old field: %v", d.Fields)
		}
	}
}

// TestSchemaReplicatesAndAppendOnly: the schema itself is data — it
// fans out to peers; rewriting applied transforms is refused.
func TestSchemaReplicatesAndAppendOnly(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	resp := putSchema(t, a.addr, "ev", `{"transforms":[{"op":"rename","from":"a","to":"b"}]}`)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("schema install: %d", resp.StatusCode)
	}
	// Schema replicates: peer B sees it and migrates its reads too.
	put(t, a.addr, "ev", "x", map[string]interface{}{"a": 1})
	eventually(t, 10*time.Second, func() bool {
		_, out := get(b.addr, "ev", "x")
		f, _ := out["fields"].(map[string]interface{})
		return f != nil && f["b"] != nil
	}, "schema applied on peer B")

	// Append-only: rewriting the first transform is refused.
	bad := putSchema(t, a.addr, "ev", `{"transforms":[{"op":"rename","from":"a","to":"z"}]}`)
	bad.Body.Close()
	if bad.StatusCode != 400 {
		t.Fatalf("history rewrite must be refused: %d", bad.StatusCode)
	}
	// Appending a second transform is allowed.
	ok := putSchema(t, a.addr, "ev", `{"transforms":[{"op":"rename","from":"a","to":"b"},{"op":"drop","from":"c"}]}`)
	ok.Body.Close()
	if ok.StatusCode != 200 {
		t.Fatalf("append transform refused: %d", ok.StatusCode)
	}
	// Downgrade refused.
	down := putSchema(t, a.addr, "ev", `{"transforms":[{"op":"rename","from":"a","to":"b"}]}`)
	down.Body.Close()
	if down.StatusCode != 400 {
		t.Fatalf("downgrade must be refused: %d", down.StatusCode)
	}
}
