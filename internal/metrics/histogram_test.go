package metrics

import (
	"strings"
	"testing"
)

func TestHistogramCumulativeBuckets(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("test_request_duration_ms", "test", []float64{10, 100, 1000})
	for _, v := range []float64{5, 50, 500, 5000, 100} {
		h.Observe(v)
	}
	out := r.Render()

	// Buckets are cumulative and every sample lands somewhere (5000 is
	// beyond the last bound and belongs in +Inf only).
	want := []string{
		`test_request_duration_ms_bucket{le="10"} 1`,
		`test_request_duration_ms_bucket{le="100"} 3`,
		`test_request_duration_ms_bucket{le="1000"} 4`,
		`test_request_duration_ms_bucket{le="+Inf"} 5`,
		`test_request_duration_ms_count 5`,
		`# TYPE test_request_duration_ms histogram`,
	}
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("render is missing %q:\n%s", w, out)
		}
	}
	if !strings.Contains(out, "test_request_duration_ms_sum 5655") {
		t.Errorf("sum should be 5655, got:\n%s", out)
	}
}

func TestFloatGaugeRendersFraction(t *testing.T) {
	r := NewRegistry()
	r.SetFloat("test_slo_pct", "percent of requests over the SLO", 12.5)
	r.SetFloat("test_ratio", "ratio", 0.4)
	out := r.Render()
	if !strings.Contains(out, "test_slo_pct 12.5") {
		t.Errorf("float gauge truncated: %s", out)
	}
	if !strings.Contains(out, "test_ratio 0.4") {
		t.Errorf("fractional gauge truncated: %s", out)
	}
	if !strings.Contains(out, "# TYPE test_slo_pct gauge") {
		t.Errorf("float gauge not typed: %s", out)
	}
}

func TestRenderIsStableAcrossScrapes(t *testing.T) {
	r := NewRegistry()
	r.Histogram("h_b", "b", nil)
	r.Histogram("h_a", "a", nil)
	r.Counter("c_x", "x")
	r.SetFloat("g_z", "z", 1)
	first := r.Render()
	for i := 0; i < 5; i++ {
		if r.Render() != first {
			t.Fatal("/metrics output changed between scrapes with no new observations")
		}
	}
}

func TestHistogramRegistrationIsIdempotent(t *testing.T) {
	r := NewRegistry()
	h1 := r.Histogram("same", "doc", []float64{1})
	h1.Observe(0.5)
	h2 := r.Histogram("same", "doc", []float64{99}) // second registration must not reset
	if h1 != h2 {
		t.Fatal("registration returned a different histogram")
	}
	_, _, _, count := h1.Snapshot()
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}
