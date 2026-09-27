// Package changelog keeps a bounded, per-collection event log so
// clients can watch mutations via long-poll. Events are local to the
// node that observed them (direct writes and replicated applies).
//
// By default the feed is in-memory and expires after a retention
// window — a lightweight feed, not event sourcing. With Open() the
// feed is durable: every event is appended to a JSONL file and
// replayed on restart, so watchers can resume from a stored cursor
// across node restarts. The on-disk window is trimmed to the same
// retention bounds as memory (size + age), keeping the file bounded.
package changelog

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	Seq        int64       `json:"seq"`
	TS         int64       `json:"ts"`
	Collection string      `json:"collection"`
	ID         string      `json:"id"`
	Kind       string      `json:"kind"` // "upsert" | "delete"
	Doc        interface{} `json:"doc,omitempty"`
}

// DurableWriteErrors counts events that could not be written to the
// durable feed file (disk full, etc.). The event still lands in the
// live in-memory window — watchers see it — but it will not survive
// restart. Surfaced so operators can notice silent degradation.
var DurableWriteErrors int64

type Log struct {
	mu       sync.Mutex
	seq      int64
	events   []Event
	maxAge   time.Duration
	maxSize  int
	signal   chan struct{} // closed+replaced on every append
	f        *os.File      // durable feed file (nil = in-memory only)
	fbuf     *bufio.Writer
	// onAppend, if set, is invoked synchronously on every Append
	// (under the log lock). Used by the read cache to invalidate on
	// every local mutation — writes AND replicated applies — without
	// polling. Must be fast and must not call back into the log.
	onAppend func(Event)
}

// OnAppend registers an invalidation callback fired on every event.
func (l *Log) OnAppend(fn func(Event)) {
	l.mu.Lock()
	l.onAppend = fn
	l.mu.Unlock()
}

func New(maxSize int, maxAge time.Duration) *Log {
	l := &Log{
		maxAge:  maxAge,
		maxSize: maxSize,
		signal:  make(chan struct{}),
	}
	return l
}

// Open creates a DURABLE feed backed by path: existing events are
// replayed (restoring the sequence counter and the retained window),
// and every future Append is written through to disk. The file is
// rewritten to the retained window on open, so restarts also perform
// retention GC. A corrupt trailing line (crash mid-append) is
// tolerated by dropping it; corruption mid-file fails loudly.
func Open(path string, maxSize int, maxAge time.Duration) (*Log, error) {
	l := New(maxSize, maxAge)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	cutoff := time.Now().Add(-maxAge).UnixMilli()
	var kept []string
	for i, line := range lines {
		var ev Event
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			if i == len(lines)-1 {
				break // torn append at EOF: drop it
			}
			f.Close()
			return nil, err
		}
		if ev.Seq > l.seq {
			l.seq = ev.Seq
		}
		if ev.TS >= cutoff {
			l.events = append(l.events, ev)
			kept = append(kept, line)
		}
	}
	// Enforce the size cap on the retained window too.
	if len(kept) > maxSize {
		drop := len(kept) - maxSize
		kept = kept[drop:]
		l.events = l.events[drop:]
	}
	// Rewrite the file to the retained window (bounded growth).
	if err := f.Truncate(0); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(0, 0); err != nil {
		f.Close()
		return nil, err
	}
	for _, line := range kept {
		if _, err := f.WriteString(line + "\n"); err != nil {
			f.Close()
			return nil, err
		}
	}
	l.f = f
	l.fbuf = bufio.NewWriter(f)
	return l, nil
}

// Durable reports whether this feed writes through to disk.
func (l *Log) Durable() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f != nil
}

// Append records an event and wakes any waiting pollers. In durable
// mode the event is flushed to the file before returning, so a
// cursor handed to a client is backed by disk. A failed durable write
// is counted in DurableWriteErrors but does not block the live feed.
func (l *Log) Append(collection, id, kind string, doc interface{}) Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	ev := Event{Seq: l.seq, TS: time.Now().UnixMilli(), Collection: collection, ID: id, Kind: kind, Doc: doc}
	if l.f != nil {
		b, err := json.Marshal(ev)
		if err == nil {
			l.fbuf.Write(b)
			l.fbuf.WriteByte('\n')
			err = l.fbuf.Flush()
		}
		if err != nil {
			atomic.AddInt64(&DurableWriteErrors, 1)
		}
	}
	l.events = append(l.events, ev)
	l.trimLocked()
	// Wake all waiters.
	close(l.signal)
	l.signal = make(chan struct{})
	if l.onAppend != nil {
		l.onAppend(ev)
	}
	return ev
}

// Since returns all retained events with Seq > since.
func (l *Log) Since(since int64) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Event, 0, 8)
	for _, e := range l.events {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	return out
}

// Head returns the current sequence number (client's starting cursor).
func (l *Log) Head() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// Wait blocks until there are events after `since` or timeout fires.
// Returns the events (possibly empty on timeout) and a channel that
// closes on the next append (for callers managing their own select).
func (l *Log) Wait(since int64, timeout time.Duration) []Event {
	deadline := time.After(timeout)
	for {
		l.mu.Lock()
		evs := l.SinceLocked(since)
		sig := l.signal
		l.mu.Unlock()
		if len(evs) > 0 {
			return evs
		}
		select {
		case <-sig:
			// new events; loop re-checks
		case <-deadline:
			return nil
		}
	}
}

func (l *Log) SinceLocked(since int64) []Event {
	out := make([]Event, 0, 8)
	for _, e := range l.events {
		if e.Seq > since {
			out = append(out, e)
		}
	}
	return out
}

// WaitFiltered blocks until there are events for `collection` with
// Seq > since, or timeout. Events from other collections do not
// release the wait (but do not lose data — the cursor is global).
func (l *Log) WaitFiltered(collection string, since int64, timeout time.Duration) []Event {
	deadline := time.After(timeout)
	for {
		l.mu.Lock()
		var evs []Event
		for _, e := range l.events {
			if e.Seq > since && (collection == "" || e.Collection == collection) {
				evs = append(evs, e)
			}
		}
		sig := l.signal
		l.mu.Unlock()
		if len(evs) > 0 {
			return evs
		}
		select {
		case <-sig:
		case <-deadline:
			return nil
		}
	}
}

func (l *Log) trimLocked() {
	// Drop oldest beyond size cap.
	if len(l.events) > l.maxSize {
		l.events = append([]Event(nil), l.events[len(l.events)-l.maxSize:]...)
	}
	// Drop events older than retention.
	cutoff := time.Now().Add(-l.maxAge).UnixMilli()
	idx := 0
	for idx < len(l.events) && l.events[idx].TS < cutoff {
		idx++
	}
	if idx > 0 {
		l.events = append([]Event(nil), l.events[idx:]...)
	}
}

// Close flushes and closes the durable file (no-op for memory feeds).
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	if l.fbuf != nil {
		l.fbuf.Flush()
	}
	err := l.f.Close()
	l.f = nil
	return err
}
