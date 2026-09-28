package sharedlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

// cluster starts N log nodes on reserved loopback ports. All nodes
// share the same ordered member list, so leadership is deterministic.
type cluster struct {
	urls    []string
	servers []*Server
	http    []*http.Server
}

func newCluster(t *testing.T, n int) *cluster {
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
	c := &cluster{urls: urls}
	for i := 0; i < n; i++ {
		s, err := NewServer(urls, urls[i], t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		hs := &http.Server{Handler: s.Handler()}
		go hs.Serve(listeners[i])
		c.servers = append(c.servers, s)
		c.http = append(c.http, hs)
		// Wait until this node answers before creating the next one:
		// leader election probes earlier members, so startup order
		// must be deterministic or two nodes could both self-elect.
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
		for _, s := range c.servers {
			s.Close()
		}
	})
	return c
}

// stop shuts down node i (both HTTP and the server object's handles).
func (c *cluster) stop(i int) {
	c.http[i].Close()
}

func payload(s string) json.RawMessage { return json.RawMessage(`{"msg":"` + s + `"}`) }

func TestQuorumAppendAndTail(t *testing.T) {
	c := newCluster(t, 3)
	client := NewClient(c.urls)

	for i := 1; i <= 3; i++ {
		lsn, err := client.Append(payload(fmt.Sprintf("e%d", i)))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if lsn != int64(i) {
			t.Fatalf("expected lsn %d, got %d", i, lsn)
		}
	}
	// Every member persisted every entry.
	for i, s := range c.servers {
		if got := s.LastLSN(); got != 3 {
			t.Fatalf("member %d last lsn = %d, want 3", i, got)
		}
	}
	entries := c.servers[0].Tail(0, 100)
	if len(entries) != 3 {
		t.Fatalf("tail returned %d entries, want 3", len(entries))
	}
	if entries[0].LSN != 1 || entries[2].LSN != 3 {
		t.Fatalf("entries out of order: %d..%d", entries[0].LSN, entries[2].LSN)
	}
	var p map[string]string
	json.Unmarshal(entries[1].Payload, &p)
	if p["msg"] != "e2" {
		t.Fatalf("payload mismatch: %v", p)
	}
	// Tail resumes after a cursor.
	rest := c.servers[0].Tail(2, 100)
	if len(rest) != 1 || rest[0].LSN != 3 {
		t.Fatalf("tail after 2: %+v", rest)
	}
}

func TestNoQuorumRejectsAppend(t *testing.T) {
	c := newCluster(t, 3)
	client := NewClient(c.urls)
	if _, err := client.Append(payload("ok")); err != nil {
		t.Fatalf("baseline append: %v", err)
	}
	// Kill two of three: quorum (2) unreachable.
	c.stop(1)
	c.stop(2)
	_, err := client.Append(payload("doomed"))
	if !errors.Is(err, ErrNoQuorum) {
		t.Fatalf("expected ErrNoQuorum, got %v", err)
	}
}

func TestFailoverPromotion(t *testing.T) {
	c := newCluster(t, 3)
	client := NewClient(c.urls)
	if _, err := client.Append(payload("one")); err != nil {
		t.Fatal(err)
	}
	// Leader (member 0) dies. Appends must still succeed via the
	// next member in order, which promotes itself.
	c.stop(0)
	lsn, err := client.Append(payload("two"))
	if err != nil {
		t.Fatalf("append after leader death: %v", err)
	}
	if lsn != 2 {
		t.Fatalf("expected lsn 2 after failover, got %d", lsn)
	}
	// The new leader replicated to the surviving member.
	if got := c.servers[2].LastLSN(); got != 2 {
		t.Fatalf("survivor lsn = %d, want 2", got)
	}
}

func TestLSNSeedOnPromotion(t *testing.T) {
	// Build a 3-node cluster, write 3 entries, then restart the
	// would-be leader with an EMPTY log while a follower still holds
	// lsn 3. The restarted leader must seed above 3, not restart at 1.
	c := newCluster(t, 3)
	client := NewClient(c.urls)
	for i := 1; i <= 3; i++ {
		if _, err := client.Append(payload("x")); err != nil {
			t.Fatal(err)
		}
	}
	c.stop(0)
	c.stop(1)
	// Fresh empty-dir replacement for member 0 on a new port, with
	// the follower still alive at lsn 3.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	self := "http://" + ln.Addr().String()
	members := []string{self, c.urls[2]}
	s, err := NewServer(members, self, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hs := &http.Server{Handler: s.Handler()}
	go hs.Serve(ln)
	t.Cleanup(func() { hs.Close(); s.Close() })
	if got := s.LastLSN(); got != 3 {
		t.Fatalf("fresh leader should seed lsn from peer: got %d, want 3", got)
	}
	lsn, err := s.Append(payload("after-seed"))
	if err != nil {
		t.Fatalf("append after seed: %v", err)
	}
	if lsn != 4 {
		t.Fatalf("expected lsn 4 after seeding, got %d", lsn)
	}
}

func TestReplicateIdempotent(t *testing.T) {
	c := newCluster(t, 1)
	e := Entry{LSN: 7, TS: time.Now().UnixMilli(), Payload: payload("dup")}
	b, _ := json.Marshal(e)
	for i := 0; i < 2; i++ {
		resp, err := http.Post(c.urls[0]+"/log/replicate", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("replicate %d: status %d", i, resp.StatusCode)
		}
	}
	if entries := c.servers[0].Tail(0, 10); len(entries) != 1 {
		t.Fatalf("duplicate replicate created %d entries, want 1", len(entries))
	}
}

func TestHealthReportsLeader(t *testing.T) {
	c := newCluster(t, 2)
	resp, err := http.Get(c.urls[1] + "/log/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		OK     bool   `json:"ok"`
		Leader string `json:"leader"`
		LSN    int64  `json:"lsn"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if !out.OK || out.Leader != c.urls[0] {
		t.Fatalf("member 1 should believe member 0 leads: %+v", out)
	}
}
