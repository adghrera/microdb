// Package metrics exposes a tiny Prometheus-text-format endpoint:
// counters and gauges registered by name, rendered on demand.
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu     sync.Mutex
	docs   map[string]string
	count  map[string]*int64
	gauge  map[string]*int64
	hist   map[string]*Histogram
	fgauge map[string]*fgBits
}

// Default is the process-wide registry used by store/cluster/api.
var Default = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{
		docs:   map[string]string{},
		count:  map[string]*int64{},
		gauge:  map[string]*int64{},
		hist:   map[string]*Histogram{},
		fgauge: map[string]*fgBits{},
	}
}

func (r *Registry) Counter(name, doc string) *int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.count[name]; ok {
		return c
	}
	c := new(int64)
	r.count[name] = c
	r.docs[name] = doc
	return c
}

func (r *Registry) Gauge(name, doc string) *int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauge[name]; ok {
		return g
	}
	g := new(int64)
	r.gauge[name] = g
	r.docs[name] = doc
	return g
}

// Render returns the Prometheus text exposition format.
func (r *Registry) Render() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	names := make([]string, 0, len(r.count)+len(r.gauge))
	for n := range r.count {
		names = append(names, n)
	}
	for n := range r.gauge {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		b.WriteString("# HELP " + n + " " + r.docs[n] + "\n")
		if _, ok := r.count[n]; ok {
			b.WriteString("# TYPE " + n + " counter\n")
			b.WriteString(fmt.Sprintf("%s %d\n", n, atomic.LoadInt64(r.count[n])))
		} else {
			b.WriteString("# TYPE " + n + " gauge\n")
			b.WriteString(fmt.Sprintf("%s %d\n", n, atomic.LoadInt64(r.gauge[n])))
		}
	}
	// Histograms and float gauges render after the integer metrics;
	// both sorted so /metrics output is stable across scrapes.
	r.renderHistograms(&b)
	r.renderFloatGauges(&b)
	return b.String()
}
