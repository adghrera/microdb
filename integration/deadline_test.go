package integration

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func itoaMs(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// startBlackHole returns a listener that accepts connections and then
// never answers — the shape of a partitioned peer, a wedged node, or a
// middlebox dropping packets.
func startBlackHole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		var held []net.Conn
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, c) // accept, read, say nothing, ever
		}
	}()
	return "http://" + ln.Addr().String()
}

// TestQuerySharedDeadline: gathers already run in parallel, but the
// merge used to wait for ALL of them — so one peer that never answers
// held the entire query hostage until the HTTP client gave up. With a
// shared deadline the query returns what it has, says what it is
// missing, and does so inside the budget.
func TestQuerySharedDeadline(t *testing.T) {
	a := startNode(t, t.TempDir())
	bh := startBlackHole(t)

	// Our own shard must have something to return.
	if code := put(t, a.addr, "deadline", "local", map[string]interface{}{"v": 1}); code != 200 {
		t.Fatalf("seed write: %d", code)
	}

	// Teach the node about the black hole through gossip — the same
	// way it would learn about a peer that then becomes unreachable.
	// Members are full records with a TTL in unix millis — a string
	// array would fail to decode and be silently ignored.
	ttl := time.Now().Add(time.Minute).UnixMilli()
	gossip := `{"addr":"` + a.addr + `","members":[` +
		`{"addr":"` + a.addr + `","ttl":` + itoaMs(ttl) + `},` +
		`{"addr":"` + bh + `","ttl":` + itoaMs(ttl) + `}` +
		`],"epoch":1,"left":{}}`
	req, _ := http.NewRequest(http.MethodPost, a.addr+"/internal/gossip", strings.NewReader(gossip))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	eventually(t, 5*time.Second, func() bool { return len(a.cl.Peers()) >= 1 }, "peer learned via gossip")

	const budget = 250 * time.Millisecond
	start := time.Now()
	q, err := client.Get(a.addr + "/api/collections/deadline/docs?limit=10&timeout_ms=250")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Body.Close()
	raw, _ := io.ReadAll(q.Body)
	elapsed := time.Since(start)

	if q.StatusCode != 200 {
		t.Fatalf("query -> %d: %s", q.StatusCode, raw)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("query ignored its deadline: took %v for a %v budget", elapsed, budget)
	}
	var out struct {
		Count     int             `json:"count"`
		Total     int             `json:"total"`
		Partial   bool            `json:"partial"`
		TimedOut  bool            `json:"timed_out"`
		TimeoutMs int             `json:"timeout_ms"`
		Errors    []string        `json:"errors"`
		Docs      json.RawMessage `json:"docs"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("bad response: %v (%s)", err, raw)
	}
	if !out.TimedOut {
		t.Error("expected timed_out=true when a member never answers")
	}
	if !out.Partial {
		t.Error("expected partial=true — the result is missing a shard")
	}
	if out.TimeoutMs != 250 {
		t.Errorf("timeout_ms echoed %d, want 250", out.TimeoutMs)
	}
	// The error must name the member that did not answer, not just
	// say "something went wrong".
	found := false
	for _, e := range out.Errors {
		if strings.Contains(e, "timeout after") && strings.Contains(e, bh) {
			found = true
		}
	}
	if !found {
		t.Errorf("errors should name the missing member, got %v", out.Errors)
	}
	// And our own shard's document must still be in the answer.
	if !strings.Contains(string(out.Docs), `"local"`) {
		t.Errorf("own shard's data missing from a partial answer: %s", out.Docs)
	}
}

// TestQueryRejectsBadTimeout keeps the knob honest.
func TestQueryRejectsBadTimeout(t *testing.T) {
	a := startNode(t, t.TempDir())
	for _, bad := range []string{"0", "-5", "abc", "60001"} {
		resp, err := client.Get(a.addr + "/api/collections/c/docs?timeout_ms=" + bad)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Errorf("timeout_ms=%q -> %d, want 400", bad, resp.StatusCode)
		}
	}
}
