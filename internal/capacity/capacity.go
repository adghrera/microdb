// Package capacity decides whether this node can afford to accept more
// work — the admission control that stands between a growing dataset
// and the two quiet ways a database dies: the OOM killer and a full
// disk.
//
// Both checks are deliberately cheap and cached: a statfs per write
// would double the write path, and a runtime.ReadMemStats per write is
// stop-the-world. Refresh on an interval instead; a watermark that is
// two seconds stale is still years earlier than the crash it prevents.
package capacity

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// Usage is a filesystem snapshot.
type Usage struct {
	TotalBytes uint64  `json:"total_bytes"`
	FreeBytes  uint64  `json:"free_bytes"`
	UsedPct    float64 `json:"used_pct"`
	Err        string  `json:"error,omitempty"` // why the numbers are unknown
}

// Snapshot is what /api/capacity and `microctl capacity` report.
type Snapshot struct {
	Disk           Usage   `json:"disk"`
	MinFreePct     float64 `json:"min_free_pct"`
	HeapBytes      uint64  `json:"heap_bytes"`
	HeapLimitBytes uint64  `json:"heap_limit_bytes"`
	Docs           int     `json:"docs"`
	ShedReason     string  `json:"shed_reason,omitempty"`
	CheckedUnixMs  int64   `json:"checked_unix_ms"`
}

// Checker evaluates watermarks on a cached schedule.
type Checker struct {
	// Path is probed for disk usage (the data directory).
	Path string
	// MinFreePct refuses new client writes when free space drops below
	// this percentage (0 disables the disk watermark).
	MinFreePct float64
	// HeapLimitBytes refuses new client writes once the live heap
	// passes this size (0 disables the heap watermark).
	HeapLimitBytes uint64
	// Every is the refresh interval (default 2s).
	Every time.Duration

	mu      sync.Mutex
	usage   Usage
	checked time.Time
	heap    uint64
	err     error
}

// Disk probes path's filesystem.
func Disk(path string) (Usage, error) {
	total, free, err := diskUsage(path)
	if err != nil {
		return Usage{Err: err.Error()}, err
	}
	u := Usage{TotalBytes: total, FreeBytes: free}
	if total > 0 {
		u.UsedPct = 100 * float64(total-free) / float64(total)
	}
	return u, nil
}

// refreshIfNeeded re-reads disk and heap at most once per Every.
func (c *Checker) refreshIfNeeded() {
	every := c.Every
	if every <= 0 {
		every = 2 * time.Second
	}
	c.mu.Lock()
	if !c.checked.IsZero() && time.Since(c.checked) < every {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	u, err := Disk(c.Path)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	c.mu.Lock()
	c.usage = u
	c.err = err
	c.heap = ms.HeapAlloc
	c.checked = time.Now()
	c.mu.Unlock()
}

// ShedReason returns "" when this node should accept new client
// writes, or the reason it should not. DELETE is never shed by the
// caller: deleting frees the very space the watermark is about.
func (c *Checker) ShedReason() string {
	if c == nil || (c.MinFreePct <= 0 && c.HeapLimitBytes == 0) {
		return ""
	}
	c.refreshIfNeeded()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.shedReasonLocked()
}

// shedReasonLocked evaluates the cached numbers. It never probes, so
// both ShedReason (after a refresh) and Snap (reporting cached state)
// give the same answer — an endpoint that reported no reason while the
// node was shedding would be worse than no endpoint.
func (c *Checker) shedReasonLocked() string {
	if c.MinFreePct > 0 && c.usage.TotalBytes > 0 {
		freePct := 100 * float64(c.usage.FreeBytes) / float64(c.usage.TotalBytes)
		if freePct < c.MinFreePct {
			return fmt.Sprintf("disk free %.1f%% below the %.1f%% watermark", freePct, c.MinFreePct)
		}
	}
	if c.HeapLimitBytes > 0 && c.heap > c.HeapLimitBytes {
		return fmt.Sprintf("heap %d bytes above the %d byte limit", c.heap, c.HeapLimitBytes)
	}
	return ""
}

// Snap returns the current view for reporting. It refreshes on the
// same schedule as ShedReason — an endpoint that showed zeros (or
// "not shedding") until the first write had happened would be worse
// than no endpoint at all, because it answers the question at the
// moment an operator asks it.
func (c *Checker) Snap() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.refreshIfNeeded()
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Snapshot{
		Disk:           c.usage,
		MinFreePct:     c.MinFreePct,
		HeapBytes:      c.heap,
		HeapLimitBytes: c.HeapLimitBytes,
		CheckedUnixMs:  c.checked.UnixMilli(),
		ShedReason:     c.shedReasonLocked(),
	}
	if c.err != nil && s.Disk.Err == "" {
		s.Disk.Err = c.err.Error()
	}
	return s
}
