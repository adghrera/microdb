package metrics

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultLatencyBounds are the histogram buckets in milliseconds —
// chosen around the latency SLOs operators actually set (a 500ms SLO
// needs resolution below and around it, not at 10s granularity).
var DefaultLatencyBounds = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// Histogram is a Prometheus histogram: cumulative buckets, a sum and a
// count. Buckets are updated atomically (observed on every request);
// sum and count share a small mutex because they must not drift apart.
type Histogram struct {
	mu     sync.Mutex
	name   string
	doc    string
	bounds []float64
	counts []int64
	sum    float64
	count  int64
}

// Observe records one sample.
func (h *Histogram) Observe(v float64) {
	for i, b := range h.bounds {
		if v <= b {
			atomic.AddInt64(&h.counts[i], 1)
			break
		}
	}
	h.mu.Lock()
	h.sum += v
	h.count++
	h.mu.Unlock()
}

// Snapshot returns (bounds, cumulative counts, sum, count).
func (h *Histogram) Snapshot() ([]float64, []int64, float64, int64) {
	out := make([]int64, len(h.bounds))
	var total int64
	for i := range h.bounds {
		total += atomic.LoadInt64(&h.counts[i])
		out[i] = total // Prometheus buckets are cumulative
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.bounds, out, h.sum, h.count
}

// Histogram registers (or returns) a histogram by name.
func (r *Registry) Histogram(name, doc string, bounds []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.hist[name]; ok {
		return h
	}
	if len(bounds) == 0 {
		bounds = DefaultLatencyBounds
	}
	h := &Histogram{
		name:   name,
		doc:    doc,
		bounds: append([]float64(nil), bounds...),
		counts: make([]int64, len(bounds)),
	}
	r.hist[name] = h
	r.docs[name] = doc
	return h
}

// SetFloat records a floating-point gauge (percentages, ratios) so
// 0.4 does not render as 0.
func (r *Registry) SetFloat(name, doc string, v float64) {
	r.mu.Lock()
	g, ok := r.fgauge[name]
	if !ok {
		g = &fgBits{doc: doc}
		r.fgauge[name] = g
		r.docs[name] = doc
	}
	r.mu.Unlock()
	g.bits.Store(math.Float64bits(v))
}

type fgBits struct {
	doc  string
	bits atomic.Uint64
}

// render writes the histogram block in Prometheus text exposition.
func (h *Histogram) render(sb *strings.Builder) {
	bounds, counts, sum, count := h.Snapshot()
	sb.WriteString("# HELP " + h.name + " " + h.doc + "\n")
	sb.WriteString("# TYPE " + h.name + " histogram\n")
	for i, b := range bounds {
		sb.WriteString(fmt.Sprintf("%s_bucket{le=\"%s\"} %d\n", h.name, formatLE(b), counts[i]))
	}
	sb.WriteString(fmt.Sprintf("%s_bucket{le=\"+Inf\"} %d\n", h.name, count))
	sb.WriteString(fmt.Sprintf("%s_sum %s\n", h.name, strconv.FormatFloat(sum, 'g', -1, 64)))
	sb.WriteString(fmt.Sprintf("%s_count %d\n", h.name, count))
}

// formatLE renders a bound the way Prometheus expects: integral bounds
// without a decimal point.
func formatLE(b float64) string {
	if b == math.Trunc(b) && math.Abs(b) < 1e15 {
		return strconv.FormatInt(int64(b), 10)
	}
	return strconv.FormatFloat(b, 'g', -1, 64)
}

// renderFloatGauges emits every registered float gauge, sorted.
func (r *Registry) renderFloatGauges(sb *strings.Builder) {
	names := make([]string, 0, len(r.fgauge))
	for n := range r.fgauge {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		g := r.fgauge[n]
		sb.WriteString("# HELP " + n + " " + g.doc + "\n")
		sb.WriteString("# TYPE " + n + " gauge\n")
		sb.WriteString(fmt.Sprintf("%s %s\n", n,
			strconv.FormatFloat(math.Float64frombits(g.bits.Load()), 'g', -1, 64)))
	}
}

// renderHistograms emits every registered histogram, sorted.
func (r *Registry) renderHistograms(sb *strings.Builder) {
	names := make([]string, 0, len(r.hist))
	for n := range r.hist {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		r.hist[n].render(sb)
	}
}
