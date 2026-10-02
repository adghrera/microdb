package loadgen

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	// 1..100 ms
	sorted := make([]time.Duration, 100)
	for i := range sorted {
		sorted[i] = time.Duration(i+1) * time.Millisecond
	}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{0, 1 * time.Millisecond},   // clamped low, still a real sample
		{50, 50 * time.Millisecond}, // nearest-rank
		{95, 95 * time.Millisecond},
		{99, 99 * time.Millisecond},
		{100, 100 * time.Millisecond},
		{-10, 1 * time.Millisecond},
		{150, 100 * time.Millisecond},
	}
	for _, c := range cases {
		if got := Percentile(sorted, c.p); got != c.want {
			t.Errorf("Percentile(%v) = %v, want %v", c.p, got, c.want)
		}
	}
	if got := Percentile(nil, 50); got != 0 {
		t.Errorf("empty slice: got %v, want 0", got)
	}
}

func TestSummary(t *testing.T) {
	// Deliberately unsorted input: Summary must sort in place.
	in := []time.Duration{
		10 * time.Millisecond, 1 * time.Millisecond, 5 * time.Millisecond,
		100 * time.Millisecond, 3 * time.Millisecond,
	}
	s := Summary(in)
	if s.Max != 100 {
		t.Errorf("max = %v, want 100ms", s.Max)
	}
	if s.P50 != 5 {
		t.Errorf("p50 = %v, want 5ms", s.P50)
	}
	if s.Mean <= 0 {
		t.Errorf("mean = %v, want > 0", s.Mean)
	}
}

func TestNormalizeRejectsBadInput(t *testing.T) {
	if err := (&Config{}).normalize(); err == nil {
		t.Error("empty config should be rejected")
	}
	if err := (&Config{URL: "http://x", Workload: "nonsense"}).normalize(); err == nil {
		t.Error("unknown workload should be rejected")
	}
}

// testTarget is a minimal document server: any PUT/GET under
// /api/collections/ succeeds, so the harness measures the client loop
// rather than server behaviour we do not control.
func testTarget(t *testing.T) *httptest.Server {
	t.Helper()
	var hits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/api/collections/", func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestRunMixedReportsOps(t *testing.T) {
	srv := testTarget(t)
	rep, err := Run(Config{
		URL:        srv.URL,
		Collection: "bench",
		Duration:   150 * time.Millisecond,
		Workers:    4,
		Workload:   WorkloadMixed,
		ReadPct:    50,
		Keyspace:   50,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Ops == 0 {
		t.Fatal("expected some ops")
	}
	if rep.Latency.P99 <= 0 {
		t.Errorf("p99 = %v, want > 0", rep.Latency.P99)
	}
	if rep.OpsPerSec <= 0 {
		t.Errorf("ops/sec = %v, want > 0", rep.OpsPerSec)
	}
	if rep.Reads == 0 || rep.Writes == 0 {
		t.Errorf("mixed workload: reads=%d writes=%d, want both > 0", rep.Reads, rep.Writes)
	}
	// The report must be valid JSON with the fields dashboards read.
	var round Report
	if err := json.Unmarshal(rep.Marshal(), &round); err != nil {
		t.Fatalf("report is not valid JSON: %v", err)
	}
	if round.Ops != rep.Ops {
		t.Errorf("JSON round-trip changed ops: %d != %d", round.Ops, rep.Ops)
	}
}

func TestRunCountsHTTPErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	rep, err := Run(Config{
		URL:      srv.URL,
		Duration: 80 * time.Millisecond,
		Workers:  2,
		Workload: WorkloadWrite,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if rep.Errors == 0 {
		t.Fatal("expected errors to be counted against a 503 target")
	}
	if len(rep.ErrorSamps) == 0 {
		t.Error("expected error samples for diagnosis")
	}
	if rep.Errors != rep.Ops {
		t.Errorf("errors=%d ops=%d, want equal when every op fails", rep.Errors, rep.Ops)
	}
}

func TestRunBurstAddsConcurrency(t *testing.T) {
	srv := testTarget(t)
	rep, err := Run(Config{
		URL:      srv.URL,
		Duration: 300 * time.Millisecond,
		Workers:  2,
		Burst:    true,
		Workload: WorkloadWrite,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !rep.Burst || rep.Ops == 0 {
		t.Errorf("burst run: burst=%v ops=%d", rep.Burst, rep.Ops)
	}
}
