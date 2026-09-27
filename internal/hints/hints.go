// Package hints stores hinted-handoff records: when a replication
// target is unreachable, the sender keeps the doc ("the hint") and a
// retry schedule, and delivers it when the target comes back. This keeps
// writes at full replication factor without blocking on a failed node.
//
// Hints are persisted as JSONL so a sender crash doesn't lose pending
// handoffs. The set is capped: if the cap is hit the oldest hints are
// dropped (anti-entropy is the backstop), and drops are counted.
package hints

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"microdb/internal/store"
)

// Hint is one pending handoff: doc `Doc` owed to node `Target`.
type Hint struct {
	Target    string     `json:"target"`
	Doc       *store.Doc `json:"doc"`
	Attempts  int        `json:"attempts"`
	NextRetry int64      `json:"next_retry"` // unix millis
}

// Store is a capped, persisted set of pending hints.
type Store struct {
	mu      sync.Mutex
	path    string
	pending []Hint
	max     int
	// counters (exposed for metrics)
	added     int64
	delivered int64
	dropped   int64
}

// DefaultMax caps the pending set; beyond it the oldest hints are
// dropped. 10k docs of handoff debt is already a lot for a tiny db.
const DefaultMax = 10000

// Load opens (or creates) the hint file at path and loads any
// persisted hints. A missing file is fine (empty set).
func Load(path string) (*Store, error) {
	s := &Store{path: path, max: DefaultMax}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var h Hint
		if err := json.Unmarshal(line, &h); err != nil {
			// Corrupt tail (crash mid-write): skip, keep what we have.
			continue
		}
		if h.Target != "" && h.Doc != nil {
			s.pending = append(s.pending, h)
		}
	}
	return s, nil
}

// Add records a hint owed to target for doc d. If the same (target,
// collection, id) is already pending, the newer doc replaces it — the
// newest version is all the target needs (LWW merge on arrival).
func (s *Store) Add(target string, d *store.Doc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.pending {
		if s.pending[i].Target == target && s.pending[i].Doc.Collection == d.Collection && s.pending[i].Doc.ID == d.ID {
			s.pending[i].Doc = d
			s.pending[i].NextRetry = time.Now().UnixMilli()
			return
		}
	}
	// Cap: drop oldest to make room.
	for len(s.pending) >= s.max {
		s.pending = s.pending[1:]
		s.dropped++
	}
	s.pending = append(s.pending, Hint{Target: target, Doc: d, NextRetry: time.Now().UnixMilli()})
	s.added++
	_ = s.saveLocked()
}

// Due returns hints whose retry time has passed (copy; caller decides
// delivery outcome and calls Remove or Reschedule).
func (s *Store) Due(now time.Time) []Hint {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := now.UnixMilli()
	out := make([]Hint, 0, len(s.pending))
	for _, h := range s.pending {
		if h.NextRetry <= n {
			out = append(out, h)
		}
	}
	return out
}

// Remove drops a delivered hint (matched by target + doc id).
func (s *Store) Remove(target, collection, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, h := range s.pending {
		if h.Target == target && h.Doc.Collection == collection && h.Doc.ID == id {
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			s.delivered++
			_ = s.saveLocked()
			return true
		}
	}
	return false
}

// Reschedule bumps a failed hint's attempt count and pushes its retry
// out with exponential backoff capped at maxBackoff.
func (s *Store) Reschedule(target, collection, id string, maxBackoff time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.pending {
		h := &s.pending[i]
		if h.Target == target && h.Doc.Collection == collection && h.Doc.ID == id {
			h.Attempts++
			shift := h.Attempts
			if shift > 20 {
				shift = 20 // guard against shift overflow; cap dominates anyway
			}
			backoff := time.Duration(int64(1)<<uint(shift)) * time.Second
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			h.NextRetry = time.Now().Add(backoff).UnixMilli()
			_ = s.saveLocked()
			return
		}
	}
}

// Pending returns the number of hints still owed.
func (s *Store) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// Stats returns counters for metrics/status.
func (s *Store) Stats() (pending int, added, delivered, dropped int64, byTarget map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byTarget = map[string]int{}
	for _, h := range s.pending {
		byTarget[h.Target]++
	}
	return len(s.pending), s.added, s.delivered, s.dropped, byTarget
}

// saveLocked rewrites the whole hint file atomically (tmp + fsync +
// rename). Called with s.mu held. The set is small by construction, so
// a full rewrite is cheaper than tracking appends/deletes separately.
func (s *Store) saveLocked() error {
	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, h := range s.pending {
		b, err := json.Marshal(h)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Flush forces a durable save (used at shutdown/tests).
func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked()
}

var _ = filepath.Join // keep import if unused after refactors
