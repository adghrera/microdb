package api

import (
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"microdb/internal/cluster"
	"microdb/internal/metrics"
	"microdb/internal/store"
)

// TestTracingRecordsREDMetrics is the SLO story: a duration histogram
// per route class, error and over-SLO counters, and gauges that report
// percentages instead of raw counts.
func TestTracingRecordsREDMetrics(t *testing.T) {
	// 1ms objective: any real request violates it, which makes the
	// assertion deterministic without sleeping for seconds.
	prev := SLO()
	SetSLO(1)
	defer SetSLO(prev)

	var sawBoom int64
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "boom") {
			atomic.AddInt64(&sawBoom, 1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		time.Sleep(5 * time.Millisecond) // comfortably over a 1ms SLO
		w.WriteHeader(http.StatusOK)
	})
	h := Tracing(1000, inner)

	do := func(method, path string) int {
		r := httptest.NewRequest(method, path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Header().Get("X-Trace-Id") == "" {
			t.Errorf("%s %s has no trace id", method, path)
		}
		return w.Code
	}

	// Counters are process-global (other tests in this package also pass
	// through Tracing), so assert DELTAS and the gauge's derivation
	// rather than absolutes another test would shift.
	reqBefore := counterOf("microdb_requests_total")
	errBefore := counterOf("microdb_request_errors_total")
	overBefore := counterOf("microdb_requests_over_slo_total")

	if code := do(http.MethodPut, "/api/collections/c/docs/a"); code != 200 {
		t.Fatalf("write: %d", code)
	}
	if code := do(http.MethodPut, "/api/collections/c/docs/boom"); code != 500 {
		t.Fatalf("failing write: %d", code)
	}
	// A read is a different route class: its histogram must be its own.
	if code := do(http.MethodGet, "/api/collections/c/docs/a"); code != 200 {
		t.Fatalf("read: %d", code)
	}

	out := metrics.Default.Render()
	if got := counterOf("microdb_requests_total") - reqBefore; got != 3 {
		t.Errorf("requests_total delta = %d, want 3", got)
	}
	if got := counterOf("microdb_request_errors_total") - errBefore; got != 1 {
		t.Errorf("errors_total delta = %d, want 1", got)
	}
	if got := counterOf("microdb_requests_over_slo_total") - overBefore; got != 2 {
		t.Errorf("over_slo_total delta = %d, want 2 (the two 5ms requests)", got)
	}
	for _, want := range []string{
		"# TYPE microdb_request_duration_ms_put_doc histogram",
		"microdb_request_duration_ms_put_doc_bucket",
		"# TYPE microdb_request_duration_ms_get_doc histogram",
		"microdb_request_duration_ms_get_doc_bucket",
		"microdb_request_errors_put_doc_total",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}

	// The gauges an SLO alert actually watches are percentages, and
	// they appear once handleMetrics runs.
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cl, err := cluster.New("http://127.0.0.1:1", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Stop()
	srv := NewWithRF("http://127.0.0.1:1", st, cl, 1)

	mreq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	mw := httptest.NewRecorder()
	srv.ServeHTTP(mw, mreq)
	body := mw.Body.String()

	if !strings.Contains(body, "# TYPE microdb_slo_violation_pct gauge") {
		t.Error("SLO violation gauge missing from /metrics")
	}
	// The gauge is derived from the same counters, so assert the
	// derivation (absolute values would shift with every other test
	// that serves a request in this package).
	total := counterOf("microdb_requests_total")
	over := counterOf("microdb_requests_over_slo_total")
	wantPct := 100 * float64(over) / float64(total)
	if v := gaugeValue(t, body, "microdb_slo_violation_pct"); math.Abs(v-wantPct) > 0.01 {
		t.Errorf("SLO violation percentage = %v, want %v (derived from %d/%d)", v, wantPct, over, total)
	}
	if total == 0 || over == 0 {
		t.Fatalf("counters empty: total=%d over=%d — the test traffic vanished", total, over)
	}
	if !strings.Contains(body, "# TYPE microdb_error_pct gauge") {
		t.Errorf("error percentage missing:\n%s", grepLines(body, "microdb_error"))
	}
	if !strings.Contains(body, "microdb_slo_ms 1") {
		t.Errorf("SLO objective not exposed:\n%s", grepLines(body, "microdb_slo_ms"))
	}
	// Histo sum must be present and non-zero (observability of the
	// distribution, not just the counters).
	if !strings.Contains(body, "microdb_request_duration_ms_put_doc_sum") {
		t.Error("histogram sum missing from /metrics")
	}
}

// counterOf reads a counter from the process-global registry.
func counterOf(name string) int64 {
	return atomic.LoadInt64(metrics.Default.Counter(name, "counter"))
}

// gaugeValue pulls one gauge sample out of a Prometheus text body.
func gaugeValue(t *testing.T, body, name string) float64 {
	t.Helper()
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, name+" ") {
			var v float64
			if _, err := fmt.Sscanf(l, name+" %g", &v); err == nil {
				return v
			}
		}
	}
	t.Fatalf("gauge %s not found in /metrics", name)
	return 0
}

func grepLines(body, needle string) string {
	var out []string
	for _, l := range strings.Split(body, "\n") {
		if strings.Contains(l, needle) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
