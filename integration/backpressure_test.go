package integration

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"microdb/internal/api"
	"microdb/internal/metrics"
)

// serveHandler starts an ad-hoc HTTP server for a bare handler and
// returns its base URL. Cleaned up with the test.
func serveHandler(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return "http://" + ln.Addr().String()
}

// TestBackpressureSheds: with a tiny inflight cap and a slow handler,
// concurrent requests beyond the cap get 429 + Retry-After while
// /health is never shed.
func TestBackpressureSheds(t *testing.T) {
	release := make(chan struct{})
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/slow") {
			<-release
		}
		w.WriteHeader(200)
	})
	base := serveHandler(t, api.Backpressure(2, slow))

	// Occupy both slots with parked requests.
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); client.Get(base + "/slow") }()
	}
	time.Sleep(150 * time.Millisecond) // let them park

	// Third concurrent request must be shed.
	resp, err := client.Get(base + "/slow")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("expected 429 over cap, got %d: %s", resp.StatusCode, body)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("shed response missing Retry-After header")
	}
	// /health passes through untouched even when saturated.
	resp2, err := client.Get(base + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("/health should never shed, got %d", resp2.StatusCode)
	}
	close(release)
	wg.Wait()
}

// TestTracingEchoesTraceID: every response carries X-Trace-Id; an
// incoming client trace id is honored (cross-service correlation).
func TestTracingEchoesTraceID(t *testing.T) {
	base := serveHandler(t, api.Tracing(500, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})))

	// No incoming id -> generated.
	resp, err := client.Get(base + "/anything")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("X-Trace-Id") == "" {
		t.Fatal("missing generated X-Trace-Id")
	}
	// Incoming id -> echoed unchanged.
	req, _ := http.NewRequest("GET", base+"/anything", nil)
	req.Header.Set("X-Trace-Id", "abc-123")
	resp2, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if got := resp2.Header.Get("X-Trace-Id"); got != "abc-123" {
		t.Fatalf("client trace id not honored: %s", got)
	}
}

// TestTracingLatencyBuckets: a ~60ms request lands in the 50-500ms
// slow bucket and increments the request counter.
func TestTracingLatencyBuckets(t *testing.T) {
	base := serveHandler(t, api.Tracing(10_000, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
		w.WriteHeader(200)
	})))
	// Warm-up request so both metrics exist before we snapshot.
	if resp, err := client.Get(base + "/warmup"); err == nil {
		resp.Body.Close()
	}
	before := metrics.Default.Render()
	resp, err := client.Get(base + "/slowish")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	after := metrics.Default.Render()
	if !bucketIncreased(before, after, "microdb_latency_slow_total") {
		t.Fatalf("slow latency bucket not incremented.\nbefore:\n%s\nafter:\n%s", before, after)
	}
	if !bucketIncreased(before, after, "microdb_requests_total") {
		t.Fatal("request counter not incremented")
	}
}

// bucketIncreased parses "name N" lines from two Prometheus renders
// and reports whether name's value grew.
func bucketIncreased(before, after, name string) bool {
	get := func(render string) int64 {
		for _, line := range strings.Split(render, "\n") {
			if strings.HasPrefix(line, name+" ") {
				var v int64
				fmt.Sscanf(strings.TrimPrefix(line, name+" "), "%d", &v)
				return v
			}
		}
		return -1
	}
	b, a := get(before), get(after)
	// Metric may not exist in `before` (first use creates it).
	return a > 0 && (b < 0 || a > b)
}
