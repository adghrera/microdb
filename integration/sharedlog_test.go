package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"microdb/internal/sharedlog"
	"microdb/internal/store"
)

// logCluster runs N shared-log nodes on reserved loopback ports.
type logCluster struct {
	urls []string
	http []*http.Server
	srv  []*sharedlog.Server
}

func startLogCluster(t *testing.T, n int) *logCluster {
	t.Helper()
	listeners := make([]net.Listener, n)
	urls := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		urls[i] = fmt.Sprintf("http://%s", ln.Addr().String())
	}
	c := &logCluster{urls: urls}
	for i := 0; i < n; i++ {
		s, err := sharedlog.NewServer(urls, urls[i], t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		hs := &http.Server{Handler: s.Handler()}
		go hs.Serve(listeners[i])
		c.srv = append(c.srv, s)
		c.http = append(c.http, hs)
		// Deterministic leader election requires earlier members to
		// be reachable before later members probe them at startup.
		deadline := time.Now().Add(10 * time.Second)
		probe := &http.Client{Timeout: 1 * time.Second}
		for time.Now().Before(deadline) {
			resp, err := probe.Get(urls[i] + "/log/health")
			if err == nil {
				resp.Body.Close()
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Cleanup(func() {
		for _, hs := range c.http {
			hs.Close()
		}
		for _, s := range c.srv {
			s.Close()
		}
	})
	return c
}

func (c *logCluster) stop(i int) { c.http[i].Close() }

func TestSharedLogCrashRecovery(t *testing.T) {
	lc := startLogCluster(t, 3)
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	client := sharedlog.NewClient(lc.urls)
	if n, err := store.RecoverSharedLog(st, client); err != nil || n != 0 {
		t.Fatalf("fresh recovery: n=%d err=%v", n, err)
	}
	st.AttachSharedLog(client)

	for i := 1; i <= 5; i++ {
		if _, err := st.Apply("docs", fmt.Sprintf("d%d", i), map[string]interface{}{"i": i}); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if st.SharedLSN() != 5 {
		t.Fatalf("shared lsn = %d, want 5", st.SharedLSN())
	}
	st.Close()

	// Simulate total local disk loss: wipe the node's log AND its
	// checkpoint. The shared log holds everything.
	os.Remove(filepath.Join(dir, "data.jsonl"))
	os.Remove(filepath.Join(dir, "sharedlog.state"))

	st2, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := store.RecoverSharedLog(st2, client)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if applied != 5 {
		t.Fatalf("recovered %d docs, want 5", applied)
	}
	for i := 1; i <= 5; i++ {
		d, ok := st2.Get("docs", fmt.Sprintf("d%d", i))
		if !ok {
			t.Fatalf("doc d%d not recovered", i)
		}
		if d.Fields["i"] != float64(i) {
			t.Fatalf("doc d%d fields wrong: %v", i, d.Fields)
		}
	}
	st2.Close()
}

func TestSharedLogNoQuorumRejectsWrite(t *testing.T) {
	lc := startLogCluster(t, 3)
	client := sharedlog.NewClient(lc.urls)
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st.AttachSharedLog(client)
	if _, err := st.Apply("c", "ok", map[string]interface{}{"a": 1}); err != nil {
		t.Fatalf("baseline write: %v", err)
	}
	// Lose quorum: 2 of 3 log nodes down.
	lc.stop(1)
	lc.stop(2)
	_, err = st.Apply("c", "doomed", map[string]interface{}{"a": 2})
	if err == nil {
		t.Fatal("expected write to fail without shared-log quorum")
	}
	if _, ok := st.Get("c", "doomed"); ok {
		t.Fatal("failed write must NOT be applied locally")
	}
	// Deletes obey the same contract.
	if err := st.Delete("c", "doomed2"); err == nil {
		t.Fatal("expected delete to fail without quorum")
	}
	if _, ok := st.Get("c", "ok"); !ok {
		t.Fatal("earlier durable write must still be readable")
	}
	st.Close()
}

func TestSharedLogCheckpointResumes(t *testing.T) {
	lc := startLogCluster(t, 3)
	dir := t.TempDir()
	st, _ := store.Open(dir)
	client := sharedlog.NewClient(lc.urls)
	st.AttachSharedLog(client)
	for i := 1; i <= 3; i++ {
		st.Apply("c", fmt.Sprintf("k%d", i), map[string]interface{}{"i": i})
	}
	st.Close()

	// Lose only the local data log; the checkpoint (lsn 3) survives.
	os.Remove(filepath.Join(dir, "data.jsonl"))
	st2, _ := store.Open(dir)
	applied, err := store.RecoverSharedLog(st2, client)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("checkpoint honored: expected 0 new entries applied, got %d", applied)
	}
	if st2.DocCount() != 0 {
		t.Fatalf("expected empty store, got %d docs", st2.DocCount())
	}
	// Another coordinator writes 2 more entries straight to the log.
	for i := 10; i < 12; i++ {
		b, _ := json.Marshal(store.Doc{
			ID: fmt.Sprintf("k%d", i), Ver: 1, TS: time.Now().UnixMilli(),
			Collection: "c", Fields: map[string]interface{}{"i": i},
		})
		if _, err := client.Append(b); err != nil {
			t.Fatal(err)
		}
	}
	applied, err = store.RecoverSharedLog(st2, client)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("resume from checkpoint: applied %d, want 2", applied)
	}
	if _, ok := st2.Get("c", "k10"); !ok {
		t.Fatal("k10 not recovered")
	}
	st2.Close()
}

func TestSharedLogBatchAtomic(t *testing.T) {
	lc := startLogCluster(t, 3)
	client := sharedlog.NewClient(lc.urls)
	dir := t.TempDir()
	st, _ := store.Open(dir)
	st.AttachSharedLog(client)
	docs := map[string]map[string]interface{}{
		"a": {"v": 1},
		"b": {"v": 2},
		"c": {"v": 3},
	}
	if _, err := st.ApplyBatch("bb", docs); err != nil {
		t.Fatal(err)
	}
	st.Close()
	os.Remove(filepath.Join(dir, "data.jsonl"))
	os.Remove(filepath.Join(dir, "sharedlog.state"))
	st2, _ := store.Open(dir)
	applied, err := store.RecoverSharedLog(st2, client)
	if err != nil {
		t.Fatal(err)
	}
	// The whole batch is one log entry; all three docs come back.
	if applied != 3 {
		t.Fatalf("applied %d, want 3", applied)
	}
	for id := range docs {
		if _, ok := st2.Get("bb", id); !ok {
			t.Fatalf("batch doc %s missing after recovery", id)
		}
	}
	st2.Close()
}
