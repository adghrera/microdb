// Package changelog keeps a bounded, in-memory, per-collection event
// log so clients can watch mutations via long-poll. Events are local
// to the node that observed them (direct writes and replicated
// applies) and expire after a retention window — this is a lightweight
// feed, not durable event sourcing.
package changelog

import (
	"sync"
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

type Log struct {
	mu       sync.Mutex
	seq      int64
	events   []Event
	maxAge   time.Duration
	maxSize  int
	signal   chan struct{} // closed+replaced on every append
}

func New(maxSize int, maxAge time.Duration) *Log {
	l := &Log{
		maxAge:  maxAge,
		maxSize: maxSize,
		signal:  make(chan struct{}),
	}
	return l
}

// Append records an event and wakes any waiting pollers.
func (l *Log) Append(collection, id, kind string, doc interface{}) Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	ev := Event{Seq: l.seq, TS: time.Now().UnixMilli(), Collection: collection, ID: id, Kind: kind, Doc: doc}
	l.events = append(l.events, ev)
	l.trimLocked()
	// Wake all waiters.
	close(l.signal)
	l.signal = make(chan struct{})
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
