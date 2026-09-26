package metrics

import (
	"strings"
	"sync/atomic"
	"testing"
)

func TestRender(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("test_ops_total", "Operations performed")
	atomic.AddInt64(c, 3)
	g := r.Gauge("test_size", "Current size")
	atomic.StoreInt64(g, 42)

	out := r.Render()
	for _, want := range []string{
		"# HELP test_ops_total Operations performed",
		"# TYPE test_ops_total counter",
		"test_ops_total 3",
		"# TYPE test_size gauge",
		"test_size 42",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in:\n%s", want, out)
		}
	}
}

func TestCounterIdempotentRegistration(t *testing.T) {
	r := NewRegistry()
	c1 := r.Counter("x_total", "doc")
	c2 := r.Counter("x_total", "doc")
	atomic.AddInt64(c1, 1)
	if atomic.LoadInt64(c2) != 1 {
		t.Fatal("re-registering a counter must return the same cell")
	}
}
