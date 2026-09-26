package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"microdb/internal/api"
	"microdb/internal/cluster"
	"microdb/internal/store"
)

type node struct {
	addr string
	st   *store.Store
	cl   *cluster.Cluster
	srv  *http.Server
}

func startNode(t *testing.T, dir string) *node {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := "http://" + ln.Addr().String()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cl := cluster.New(addr, st)
	srv := api.New(addr, st, cl)
	cl.Start()
	httpSrv := &http.Server{Handler: srv}
	go httpSrv.Serve(ln)
	n := &node{addr: addr, st: st, cl: cl, srv: httpSrv}
	t.Cleanup(func() { cl.Stop(); httpSrv.Close(); st.Close() })
	return n
}

var client = &http.Client{Timeout: 5 * time.Second}

func put(t *testing.T, addr, col, id string, fields map[string]interface{}) int {
	t.Helper()
	b, _ := json.Marshal(fields)
	req, _ := http.NewRequest("PUT", addr+"/api/collections/"+col+"/docs/"+id, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("PUT %s %s/%s: %v", addr, col, id, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Logf("PUT %s %s/%s -> %d: %s", addr, col, id, resp.StatusCode, body)
	}
	return resp.StatusCode
}

func get(addr, col, id string) (int, map[string]interface{}) {
	resp, err := client.Get(addr + "/api/collections/" + col + "/docs/" + id)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// eventually polls f until true or timeout.
func eventually(t *testing.T, timeout time.Duration, f func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

func TestClusterReplication(t *testing.T) {
	// 1. Start node A, then B joins A, then C joins B.
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	c := startNode(t, t.TempDir()+"/c")

	if err := b.cl.Join(a.addr); err != nil {
		t.Fatalf("B join A: %v", err)
	}
	if err := c.cl.Join(b.addr); err != nil {
		t.Fatalf("C join B: %v", err)
	}

	// 2. All three converge on full membership via gossip.
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 2 && len(b.cl.Peers()) == 2 && len(c.cl.Peers()) == 2
	}, "all nodes see 2 peers each")

	// 3. Write three docs with DIFFERENT schemas, each to a different node.
	//    Any node must accept any write (forwarding to the ring owner).
	if code := put(t, a.addr, "users", "alice", map[string]interface{}{"name": "Alice", "age": 31, "tags": []string{"admin"}}); code != 200 {
		t.Fatalf("alice write to A: %d", code)
	}
	if code := put(t, b.addr, "users", "bob", map[string]interface{}{"name": "Bob", "city": "Lisbon", "score": 9.5}); code != 200 {
		t.Fatalf("bob write to B: %d", code)
	}
	if code := put(t, c.addr, "users", "carol", map[string]interface{}{"nickname": "Caz", "active": true}); code != 200 {
		t.Fatalf("carol write to C: %d", code)
	}

	// 4. Every doc readable from every node (RF=3 replication).
	for _, id := range []string{"alice", "bob", "carol"} {
		for _, n := range []*node{a, b, c} {
			eventually(t, 10*time.Second, func() bool {
				code, _ := get(n.addr, "users", id)
				return code == 200
			}, fmt.Sprintf("doc %s readable from %s", id, n.addr))
		}
	}

	// 5. Field-level fidelity: carol's schema-free fields survive replication.
	_, doc := get(a.addr, "users", "carol")
	fields, _ := doc["fields"].(map[string]interface{})
	if fields["nickname"] != "Caz" || fields["active"] != true {
		t.Fatalf("carol fields lost in replication: %v", fields)
	}

	// 6. Update propagates with version bump.
	put(t, b.addr, "users", "alice", map[string]interface{}{"name": "Alice", "age": 32})
	eventually(t, 10*time.Second, func() bool {
		_, d := get(c.addr, "users", "alice")
		f, _ := d["fields"].(map[string]interface{})
		return f["age"] == float64(32)
	}, "alice age=32 replicated to C")

	// 7. Delete propagates as a tombstone.
	req, _ := http.NewRequest("DELETE", b.addr+"/api/collections/users/docs/bob", nil)
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("delete bob: %v %v", err, resp)
	}
	resp.Body.Close()
	eventually(t, 10*time.Second, func() bool {
		code, _ := get(a.addr, "users", "bob")
		return code == 404
	}, "bob deleted everywhere")
}

func TestQueryFilter(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	put(t, a.addr, "users", "u1", map[string]interface{}{"age": 20})
	put(t, a.addr, "users", "u2", map[string]interface{}{"age": 40})
	put(t, a.addr, "users", "u3", map[string]interface{}{"age": 60})

	resp, err := client.Get(a.addr + "/api/collections/users/docs?filter=" + urlQuery(`{"age":{"$gt":30}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Count int                      `json:"count"`
		Docs  []map[string]interface{} `json:"docs"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if out.Count != 2 {
		t.Fatalf("expected 2 docs with age>30, got %d", out.Count)
	}
}

func urlQuery(s string) string {
	// minimal percent-encoding for the filter param
	var b bytes.Buffer
	for _, ch := range s {
		switch ch {
		case '{', '}', '"', ':', ',', ' ':
			fmt.Fprintf(&b, "%%%02X", ch)
		default:
			b.WriteRune(ch)
		}
	}
	return b.String()
}
