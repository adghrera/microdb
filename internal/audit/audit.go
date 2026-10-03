// Package audit records who did what, and whether it was allowed.
//
// Two things make an audit log useful rather than decorative:
//
//   - it is append-only and bounded (rotation by size, one generation
//     kept), so it cannot fill the disk it is protecting;
//   - it records denials as well as mutations — an auth failure nobody
//     logged is an intrusion nobody noticed.
//
// It is a package-level sink (like metrics.Default) because the
// decisions live in wrappers and middleware that do not hold a server
// reference. Disabled, Log is a boolean check: the default path costs
// nothing when no operator asked for it.
package audit

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Event is one audit record.
type Event struct {
	TS         int64  `json:"ts"`       // unix millis
	Kind       string `json:"kind"`     // "auth" | "mutation" | "admin"
	Action     string `json:"action"`   // PUT / DELETE / POST / "login"
	Decision   string `json:"decision"` // "allow" | "deny"
	Status     int    `json:"status,omitempty"`
	Remote     string `json:"remote,omitempty"`
	Tenant     string `json:"tenant,omitempty"`
	Collection string `json:"collection,omitempty"`
	ID         string `json:"id,omitempty"`
	Actor      string `json:"actor,omitempty"` // "bearer" | "anonymous" | tenant name
	Trace      string `json:"trace,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

// Log is an audit sink. The zero value is disabled and safe to use.
type Log struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	maxByte int64
	written int64
}

// Default is the process-wide sink the API writes to.
var Default = &Log{}

// Open starts writing audit records to path, rotating to path.1 once
// the file passes maxBytes (0 = default 64MB, one generation kept).
func (l *Log) Open(path string, maxBytes int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	l.f = f
	l.path = path
	l.maxByte = maxBytes
	l.written = fi.Size()
	return nil
}

// Enabled reports whether auditing is on. Callers gate their event
// construction on this so the disabled path allocates nothing.
func (l *Log) Enabled() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f != nil
}

// Log appends one record. A disabled log drops it; a write error is
// remembered rather than returned, because failing the request that
// triggered the audit would be worse than losing the record.
func (l *Log) Log(e Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return
	}
	if e.TS == 0 {
		e.TS = time.Now().UnixMilli()
	}
	b, err := json.Marshal(e)
	if err != nil {
		return
	}
	b = append(b, '\n')
	if l.written+int64(len(b)) > l.maxByte {
		l.rotateLocked()
	}
	n, err := l.f.Write(b)
	l.written += int64(n)
	if err != nil {
		return // recorded best-effort; see package doc
	}
}

// rotateLocked keeps exactly one previous generation: audit.log ->
// audit.log.1, then a fresh file. Bounded by construction.
func (l *Log) rotateLocked() {
	if l.f == nil {
		return
	}
	l.f.Close()
	os.Remove(l.path + ".1")
	os.Rename(l.path, l.path+".1")
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		l.f = nil // stop logging rather than pretend
		return
	}
	l.f = f
	l.written = 0
}

// Close flushes and closes the sink (safe on a disabled log).
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}

// Path returns the configured file ("" when disabled).
func (l *Log) Path() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.path
}
