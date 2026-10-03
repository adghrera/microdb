// Package profile serves and records Go runtime profiles.
//
// Two things operators need when p99 regresses at 3am: a live pprof
// endpoint to attach a debugger to, and a trail of profiles from
// BEFORE the incident — a profile taken after the fact explains the
// wrong moment. Both are off by default: a pprof endpoint can read
// process memory, so it gets its own listener (never the public port)
// and an operator must ask for it.
package profile

import (
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	runtimepprof "runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"
)

// Start serves net/http/pprof on its own listener and returns the
// server plus the address it actually bound (addr may ask for port 0).
// An empty addr means disabled: (nil, "", nil).
func Start(addr string) (*http.Server, string, error) {
	if addr == "" {
		return nil, "", nil
	}
	mux := http.NewServeMux()
	// The standard pprof routes, on a mux we control: this listener is
	// deliberately separate from the API so it can be bound to
	// localhost or a private interface without exposing the database.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", fmt.Errorf("pprof listen %s: %w", addr, err)
	}
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln) //nolint:errcheck // shutdown is the caller's business
	return srv, ln.Addr().String(), nil
}

// Sampler writes periodic CPU and heap profiles so a regression can be
// explained with the code that was running at the time, not with a
// profile taken after the restart that "fixed" it.
type Sampler struct {
	Dir      string
	Interval time.Duration
	// CPULimit bounds one CPU profile: profiling for longer than the
	// interval would make the sampler fall behind.
	CPULimit time.Duration
	// Keep is how many profiles of each kind to retain (bounded disk).
	Keep int

	mu      sync.Mutex
	stop    chan struct{}
	done    chan struct{}
	stopOne sync.Once
}

// NewSampler builds a sampler; interval <= 0 disables it.
func NewSampler(dir string, interval time.Duration, keep int) *Sampler {
	if keep <= 0 {
		keep = 10
	}
	if interval <= 0 {
		interval = time.Minute
	}
	return &Sampler{Dir: dir, Interval: interval, CPULimit: 30 * time.Second, Keep: keep}
}

// Run blocks until Stop. Safe to call once.
func (s *Sampler) Run() {
	s.mu.Lock()
	if s.stop != nil {
		s.mu.Unlock()
		return
	}
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	stop := s.stop
	s.mu.Unlock()
	defer close(s.done)

	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			s.Heap()
			cpu := s.Interval
			if s.CPULimit > 0 && cpu > s.CPULimit {
				cpu = s.CPULimit
			}
			s.CPU(cpu)
			s.Prune()
		}
	}
}

// Stop asks a running sampler to finish and waits for it.
func (s *Sampler) Stop() {
	s.mu.Lock()
	if s.stop == nil {
		s.mu.Unlock()
		return
	}
	stop, done := s.stop, s.done
	s.stop = nil
	s.mu.Unlock()
	s.stopOne.Do(func() { close(stop) })
	<-done
}

// Heap writes an instant heap profile.
func (s *Sampler) Heap() error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.Dir, fmt.Sprintf("heap-%d.pprof", time.Now().UnixMilli()))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := runtimepprof.WriteHeapProfile(f); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	runtime.GC() // keep the next heap profile comparable
	return nil
}

// CPU profiles for d and writes it out.
func (s *Sampler) CPU(d time.Duration) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.Dir, fmt.Sprintf("cpu-%d.pprof", time.Now().UnixMilli()))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := runtimepprof.StartCPUProfile(f); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	time.Sleep(d)
	runtimepprof.StopCPUProfile()
	return f.Close()
}

// Prune keeps at most Keep profiles of each kind, oldest dropped first
// — profiles are for forensics, not for filling a disk.
func (s *Sampler) Prune() error {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, kind := range []string{"cpu", "heap"} {
		var mine []string
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".pprof") {
				continue
			}
			if strings.HasPrefix(name, kind+"-") {
				mine = append(mine, name)
			}
		}
		// Names embed unix millis, so lexicographic order is time order
		// (same digit width until year 2286).
		sort.Strings(mine)
		for len(mine) > s.Keep {
			os.Remove(filepath.Join(s.Dir, mine[0]))
			mine = mine[1:]
		}
	}
	return nil
}
