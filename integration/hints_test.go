package integration

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"microdb/internal/api"
	"microdb/internal/cluster"
	"microdb/internal/store"
)

// startNodeAt starts a node bound to a specific address ("" = random
// port) with hinted handoff enabled from dir.
func startNodeAt(t *testing.T, addr, dir string, rf int, withHints bool) *node {
	t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	url := "http://" + ln.Addr().String()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	cl, err := cluster.New(url, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	if withHints {
		if err := cl.EnableHints(dir); err != nil {
			t.Fatal(err)
		}
	}
	apiSrv := api.NewWithRF(url, st, cl, rf)
	cl.Start()
	httpSrv := &http.Server{Handler: apiSrv}
	go httpSrv.Serve(ln)
	n := &node{addr: url, st: st, cl: cl, srv: httpSrv, apiSrv: apiSrv}
	t.Cleanup(func() { cl.Stop(); httpSrv.Close(); st.Close() })
	return n
}

// TestHintedHandoffDeliversOnReturn: B dies, A keeps writing, the
// replication debt is stashed as hints, and when B comes back on the
// same address the hints are delivered automatically.
func TestHintedHandoffDeliversOnReturn(t *testing.T) {
	dirA, dirB := t.TempDir()+"/a", t.TempDir()+"/b"
	a := startNodeAt(t, "", dirA, 2, true)
	b := startNodeAt(t, "", dirB, 2, true)
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// Find a key that A owns (so the write stays on A and fans out to B).
	key := ""
	for i := 0; i < 100 && key == ""; i++ {
		id := "k" + string(rune('a'+i))
		if a.apiSrv.Owns("hh", id) {
			key = id
		}
	}
	if key == "" {
		t.Fatal("no key owned by A found")
	}
	// Baseline write reaches B.
	put(t, a.addr, "hh", key, map[string]interface{}{"gen": 1})
	eventually(t, 10*time.Second, func() bool {
		_, ok := b.st.Get("hh", key)
		return ok
	}, "baseline replication")

	// Kill B's HTTP server (membership may still show it briefly).
	b.srv.Close()
	time.Sleep(200 * time.Millisecond)

	// Write again — replication to B fails, so a hint must be recorded.
	put(t, a.addr, "hh", key, map[string]interface{}{"gen": 2})
	eventually(t, 5*time.Second, func() bool {
		return a.cl.Hints() != nil && a.cl.Hints().Pending() >= 1
	}, "hint recorded for dead peer")

	// Bring B back on the SAME address with a fresh process state
	// (new store dir is reused; the old server is gone).
	b2 := startNodeAt(t, b.addr[len("http://"):], t.TempDir()+"/b2", 2, true)
	// The replay loop (1s tick) should deliver the owed doc without any
	// anti-entropy round being needed.
	eventually(t, 15*time.Second, func() bool {
		d, ok := b2.st.Get("hh", key)
		if !ok {
			return false
		}
		f := d.Fields
		return f["gen"] == float64(2)
	}, "hint delivered after peer returned")

	// Hint debt cleared on the sender.
	eventually(t, 5*time.Second, func() bool {
		return a.cl.Hints().Pending() == 0
	}, "hint removed after delivery")

	// Status endpoint reflects the counters.
	resp, err := client.Get(a.addr + "/internal/hints")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out map[string]interface{}
	json.Unmarshal(body, &out)
	if out["enabled"] != true || out["delivered"].(float64) < 1 {
		t.Fatalf("hints status wrong: %s", body)
	}
}

// TestHintsDisabledNoop: without EnableHints, failed replication does
// not create hint records and the status endpoint says disabled.
func TestHintsDisabledNoop(t *testing.T) {
	a := startNodeAt(t, "", t.TempDir()+"/a", 2, false)
	a.cl.ForcePeer("http://127.0.0.1:1") // dead peer, replication will fail
	// Write something owned by a so fanout targets the dead peer too.
	for i := 0; i < 100; i++ {
		id := "n" + string(rune('a'+i))
		if a.apiSrv.Owns("nd", id) {
			put(t, a.addr, "nd", id, map[string]interface{}{"x": 1})
			break
		}
	}
	time.Sleep(500 * time.Millisecond)
	if a.cl.Hints() != nil {
		t.Fatal("hints should be nil when disabled")
	}
	resp, err := client.Get(a.addr + "/internal/hints")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	if out["enabled"] != false {
		t.Fatalf("expected enabled=false, got %v", out)
	}
}

// TestHintsSurviveSenderRestart: hints are durable — a sender crash
// between failure and delivery must not lose the handoff debt.
func TestHintsSurviveSenderRestart(t *testing.T) {
	dirA := t.TempDir() + "/a"
	a := startNodeAt(t, "", dirA, 2, true)
	a.cl.ForcePeer("http://127.0.0.1:1")
	for i := 0; i < 100; i++ {
		id := "p" + string(rune('a'+i))
		if a.apiSrv.Owns("ps", id) {
			put(t, a.addr, "ps", id, map[string]interface{}{"x": 1})
			break
		}
	}
	eventually(t, 5*time.Second, func() bool { return a.cl.Hints().Pending() >= 1 }, "hint recorded")

	// Restart the SENDER on a new address, same data dir.
	a.srv.Close()
	a2 := startNodeAt(t, "", dirA, 2, true)
	if a2.cl.Hints().Pending() < 1 {
		t.Fatalf("hint debt lost across sender restart: %d", a2.cl.Hints().Pending())
	}
}
