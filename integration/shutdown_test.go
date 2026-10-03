package integration

import (
	"io"
	"testing"
	"time"
)

func statusOf(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestGracefulShutdownSequence covers the observable steps of a
// SIGTERM: readiness goes dark first (so a load balancer stops sending
// work) while liveness stays up (so the supervisor does not kill a
// process that is mid-flush), and an explicit leave evicts this node
// from peers immediately rather than after the membership TTL.
func TestGracefulShutdownSequence(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) >= 1 && len(b.cl.Peers()) >= 1
	}, "two-node cluster formed")

	// Write something so B is a member holding data.
	if code := put(t, b.addr, "shutdown", "doc", map[string]interface{}{"v": 1}); code != 200 {
		t.Fatalf("write: %d", code)
	}

	// 1. Draining: readiness refuses traffic...
	b.cl.SetDraining(true)
	code, body := statusOf(t, b.addr+"/ready")
	if code != 503 {
		t.Fatalf("ready while draining = %d, want 503 (%s)", code, body)
	}
	// ...but liveness stays 200: a deliberate stop must not look like
	// a crash to the supervisor.
	if code, _ = statusOf(t, b.addr+"/health"); code != 200 {
		t.Fatalf("health while draining = %d, want 200", code)
	}
	// Reads keep working for clients already talking to us...
	if code, body := statusOf(t, b.addr+"/api/collections/shutdown/docs/doc"); code != 200 {
		t.Fatalf("read during drain = %d, want 200 (%s)", code, body)
	}
	// ...but new WRITES are refused: we are about to exit, and a write
	// accepted here would be served by a process that is closing its
	// log file. The client retries against a replica.
	if code := put(t, b.addr, "shutdown", "doc2", map[string]interface{}{"v": 2}); code != 503 {
		t.Fatalf("write during drain = %d, want 503 (draining refuses new writes)", code)
	}

	// 2. Leave: peers drop us immediately instead of waiting 15s.
	if n := b.cl.Leave(); n < 1 {
		t.Fatalf("leave reached %d peers, want at least 1", n)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 0
	}, "peer evicted the leaver immediately")
}
