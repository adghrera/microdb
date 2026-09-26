// Package store is a schema-less document store: documents are plain JSON
// maps, persisted with an append-only JSONL log, merged last-writer-wins
// by (version, timestamp).
package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"microdb/internal/merkle"
)

type Doc struct {
	ID        string                 `json:"id"`
	Ver       int64                  `json:"ver"`
	TS        int64                  `json:"ts"` // unix millis
	Deleted   bool                   `json:"deleted,omitempty"`
	Collection string                `json:"collection"`
	Fields    map[string]interface{} `json:"fields"`
}

type Store struct {
	mu   sync.RWMutex
	docs map[string]*Doc // key = collection + "\x00" + id
	f    *os.File
}

func key(col, id string) string { return col + "\x00" + id }

// Open loads any existing JSONL log into memory and appends future writes.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{docs: map[string]*Doc{}}
	path := filepath.Join(dir, "data.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// Replay existing log.
	buf := make([]byte, 0, 1<<20)
	chunk := make([]byte, 64*1024)
	for {
		n, rerr := f.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if rerr != nil {
			break
		}
	}
	for _, line := range strings.Split(string(buf), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var d Doc
		if json.Unmarshal([]byte(line), &d) == nil {
			s.merge(&d)
		}
	}
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}
	s.f = f
	return s, nil
}

// merge applies a doc only if it is newer (higher ver, tie-break on ts).
// Returns true if the stored state changed. Idempotent — safe to apply
// the same write to the same node many times.
func (s *Store) merge(d *Doc) bool {
	k := key(d.Collection, d.ID)
	cur, ok := s.docs[k]
	if !ok || d.Ver > cur.Ver || (d.Ver == cur.Ver && d.TS > cur.TS) {
		s.docs[k] = d
		return true
	}
	return false
}

// Apply upserts a document (schema-free: any JSON object) and persists it.
func (s *Store) Apply(collection, id string, fields map[string]interface{}) (*Doc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(collection, id)
	ver := int64(1)
	if cur, ok := s.docs[k]; ok {
		ver = cur.Ver + 1
	}
	d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Fields: fields}
	if !s.merge(d) {
		return d, nil // concurrent newer write already applied
	}
	b, _ := json.Marshal(d)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return nil, err
	}
	return d, nil
}

// ApplyRemote merges a replicated doc from a peer without re-propagating.
func (s *Store) ApplyRemote(d *Doc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.merge(d) {
		return false
	}
	b, _ := json.Marshal(d)
	_, err := s.f.Write(append(b, '\n'))
	return err == nil
}

func (s *Store) Get(collection, id string) (*Doc, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.docs[key(collection, id)]
	if !ok || d.Deleted {
		return nil, false
	}
	return d, true
}

func (s *Store) Delete(collection, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(collection, id)
	ver := int64(1)
	if cur, ok := s.docs[k]; ok {
		ver = cur.Ver + 1
	}
	d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Deleted: true}
	if !s.merge(d) {
		return nil
	}
	b, _ := json.Marshal(d)
	_, err := s.f.Write(append(b, '\n'))
	return err
}

// Scan walks a collection applying a filter.
func (s *Store) Scan(collection string, keep func(*Doc) bool) []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []*Doc{}
	for _, d := range s.docs {
		if d.Collection != collection || d.Deleted {
			continue
		}
		if keep == nil || keep(d) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Store) Close() error { return s.f.Close() }

// Compact rewrites the JSONL log keeping only the current version of
// each live document. Tombstones older than gcWindow are dropped
// entirely; tombstones newer than the window are kept so deletes
// cannot be resurrected by a lagging replica during anti-entropy.
//
// The rewrite is crash-safe: new log is written to data.jsonl.tmp,
// fsynced, then atomically renamed over the old log.
func (s *Store) Compact(gcWindow time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-gcWindow).UnixMilli()
	live := make([]*Doc, 0, len(s.docs))
	dropped := 0
	for k, d := range s.docs {
		if d.Deleted && d.TS <= cutoff {
			delete(s.docs, k)
			dropped++
			continue
		}
		live = append(live, d)
	}
	sort.Slice(live, func(i, j int) bool {
		if live[i].Collection != live[j].Collection {
			return live[i].Collection < live[j].Collection
		}
		return live[i].ID < live[j].ID
	})

	tmpPath := s.f.Name() + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	for _, d := range live {
		b, _ := json.Marshal(d)
		if _, err := w.Write(append(b, '\n')); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return 0, err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	oldName := s.f.Name()
	// Windows cannot rename over an open handle — close first (lock held,
	// so no writes can slip through), rename, then reopen for appending.
	if err := s.f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmpPath, oldName); err != nil {
		return 0, err
	}
	nf, err := os.OpenFile(oldName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	s.f = nf
	return dropped, nil
}

// Collections lists all collection names present in the store
// (including ones containing only tombstones).
func (s *Store) Collections() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := map[string]bool{}
	for _, d := range s.docs {
		set[d.Collection] = true
	}
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// Leaves returns (id, hash) pairs for every doc in a collection,
// tombstones included so deletions participate in the Merkle tree.
func (s *Store) Leaves(collection string) []merkle.Leaf {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]merkle.Leaf, 0, 16)
	for _, d := range s.docs {
		if d.Collection != collection {
			continue
		}
		out = append(out, merkle.Leaf{ID: d.ID, Hash: merkle.HashDoc(d)})
	}
	return out
}

// DocsByIDs returns stored docs (tombstones included) for the given ids.
func (s *Store) DocsByIDs(collection string, ids []string) []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Doc, 0, len(ids))
	for _, id := range ids {
		if d, ok := s.docs[key(collection, id)]; ok {
			out = append(out, d)
		}
	}
	return out
}

// Matches evaluates a filter object against a document.
// Supported: {"field": value} exact, {"field": {"$gt":v}}, {"$lt":v}.
// Numbers compare numerically, strings lexicographically.
func Matches(d *Doc, filter map[string]interface{}) bool {
	for field, cond := range filter {
		v := d.Fields[field]
		m, isCond := cond.(map[string]interface{})
		if !isCond {
			if !jsonEq(v, cond) {
				return false
			}
			continue
		}
		for op, operand := range m {
			switch op {
			case "$gt":
				if !cmp(v, operand, func(c int) bool { return c > 0 }) {
					return false
				}
			case "$lt":
				if !cmp(v, operand, func(c int) bool { return c < 0 }) {
					return false
				}
			default:
				return false // unknown operator
			}
		}
	}
	return true
}

func jsonEq(a, b interface{}) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

func cmp(a, b interface{}, pred func(int) bool) bool {
	switch av := a.(type) {
	case float64:
		bv, ok := b.(float64)
		return ok && pred(int(sign(av - bv)))
	case string:
		bv, ok := b.(string)
		return ok && pred(strings.Compare(av, bv))
	}
	return false
}

func sign(f float64) int {
	switch {
	case f > 0:
		return 1
	case f < 0:
		return -1
	}
	return 0
}

var _ = fmt.Sprintf // keep fmt for future errors
