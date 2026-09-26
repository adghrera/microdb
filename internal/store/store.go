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
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"microdb/internal/index"
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
	mu     sync.RWMutex
	docs   map[string]*Doc // key = collection + "\x00" + id
	f      *os.File
	fsync  bool // sync to disk on every write (durability over throughput)
	idx    *index.IndexSet // inverted field indexes (fast exact-match lookups)
}

// Indexes exposes the index set (enabled by default; pass false to OpenOpts to disable).
func (s *Store) Indexes() *index.IndexSet { return s.idx }

// SetFsync enables fsync-on-write. With it enabled every Apply/Delete
// blocks until the record is durable on disk; without it the OS page
// cache decides (fast, but a power loss can lose the log tail).
func (s *Store) SetFsync(on bool) { s.fsync = on }

// sync flushes the log to stable storage if fsync is enabled.
func (s *Store) sync() error {
	if s.fsync {
		return s.f.Sync()
	}
	return nil
}

func key(col, id string) string { return col + "\x00" + id }

// Open loads any existing JSONL log into memory and appends future writes.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{docs: map[string]*Doc{}, idx: index.NewSet(true)}
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
		// Batch records: {"batch":[{doc},{doc},...]}.
		if strings.HasPrefix(line, `{"batch":`) {
			var rec struct {
				Batch []*Doc `json:"batch"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil {
				for _, d := range rec.Batch {
					if s.merge(d) {
						s.idx.For(d.Collection).Remove(d.ID)
						if !d.Deleted {
							s.idx.For(d.Collection).Add(d.ID, d.Fields)
						}
					}
				}
			}
			continue
		}
		var d Doc
		if json.Unmarshal([]byte(line), &d) == nil {
			if s.merge(&d) {
				s.idx.For(d.Collection).Remove(d.ID)
				if !d.Deleted {
					s.idx.For(d.Collection).Add(d.ID, d.Fields)
				}
			}
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
	s.idx.For(collection).Remove(id)
	s.idx.For(collection).Add(id, fields)
	b, _ := json.Marshal(d)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return nil, err
	}
	if err := s.sync(); err != nil {
		return nil, err
	}
	return d, nil
}

// ApplyBatch upserts many documents in one call: one log write (one
// fsync when enabled) and one fanout pass. All docs land or none do —
// the batch is written as a single atomic log record.
func (s *Store) ApplyBatch(collection string, docs map[string]map[string]interface{}) ([]*Doc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Doc, 0, len(docs))
	changed := make([]*Doc, 0, len(docs))
	for id, fields := range docs {
		ver := int64(1)
		if cur, ok := s.docs[key(collection, id)]; ok {
			ver = cur.Ver + 1
		}
		d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Fields: fields}
		out = append(out, d)
		if s.merge(d) {
			s.idx.For(collection).Remove(id)
			s.idx.For(collection).Add(id, fields)
			changed = append(changed, d)
		}
	}
	if len(changed) == 0 {
		return out, nil
	}
	rec := map[string]interface{}{"batch": changed}
	b, _ := json.Marshal(rec)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return nil, err
	}
	if err := s.sync(); err != nil {
		return nil, err
	}
	return out, nil
}

// ApplyRemote merges a replicated doc from a peer without re-propagating.
func (s *Store) ApplyRemote(d *Doc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.merge(d) {
		return false
	}
	s.idx.For(d.Collection).Remove(d.ID)
	if !d.Deleted {
		s.idx.For(d.Collection).Add(d.ID, d.Fields)
	}
	b, _ := json.Marshal(d)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return false
	}
	return s.sync() == nil
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
	s.idx.For(collection).Remove(id)
	b, _ := json.Marshal(d)
	if _, err := s.f.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.sync()
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

// ScanIndexed is Scan with an index fast path: if the filter is a single
// exact-match condition on an indexed field, resolve candidate ids via
// the inverted index instead of scanning the whole collection. Falls
// back to a full scan for compound/comparison filters.
func (s *Store) ScanIndexed(collection string, filter map[string]interface{}) []*Doc {
	if len(filter) == 1 {
		for field, cond := range filter {
			if _, isCmp := cond.(map[string]interface{}); !isCmp {
				// Capture the index set under the store lock: Compact
				// swaps s.idx atomically and we must not read a torn ref.
				s.mu.RLock()
				idx := s.idx
				out := make([]*Doc, 0, 8)
				for _, id := range idx.For(collection).Lookup(field, cond) {
					if d, ok := s.docs[key(collection, id)]; ok && !d.Deleted {
						out = append(out, d)
					}
				}
				s.mu.RUnlock()
				sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
				return out
			}
		}
	}
	return s.Scan(collection, func(d *Doc) bool {
		return len(filter) == 0 || Matches(d, filter)
	})
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
	// Rebuild indexes from scratch: cheap and guarantees no stale entries.
	s.idx = index.NewSet(s.idx.Enabled())
	for _, d := range live {
		if !d.Deleted {
			s.idx.For(d.Collection).Add(d.ID, d.Fields)
		}
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
// Supported: {"field": value} exact, {"field": {"$gt":v}}, {"$lt":v},
// {"$gte":v}, {"$lte":v}, {"$ne":v}, {"$in":[...]}, {"$exists":bool},
// {"$regex":"pattern"} (string fields). Multiple conditions on one
// field AND together; multiple fields AND together.
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
			case "$gte":
				if !cmp(v, operand, func(c int) bool { return c >= 0 }) {
					return false
				}
			case "$lte":
				if !cmp(v, operand, func(c int) bool { return c <= 0 }) {
					return false
				}
			case "$ne":
				if jsonEq(v, operand) {
					return false
				}
			case "$in":
				list, ok := operand.([]interface{})
				if !ok {
					return false
				}
				found := false
				for _, cand := range list {
					if jsonEq(v, cand) {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			case "$exists":
				want, _ := operand.(bool)
				_, have := d.Fields[field]
				if want != have {
					return false
				}
			case "$regex":
				pat, ok := operand.(string)
				if !ok {
					return false
				}
				re, err := regexp.Compile(pat)
				if err != nil {
					return false
				}
				sv, ok := v.(string)
				if !ok || !re.MatchString(sv) {
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

// CompareValues orders two JSON-decoded values: numbers numerically,
// strings lexicographically, missing values sort last. Mixed or
// unorderable types compare equal (stable sort keeps insertion order).
func CompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
	switch av := a.(type) {
	case float64:
		if bv, ok := b.(float64); ok {
			return sign(av - bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			ab, bb := 0, 0
			if av {
				ab = 1
			}
			if bv {
				bb = 1
			}
			return ab - bb
		}
	}
	return 0
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
