// Package index maintains inverted indexes over document fields for
// fast non-id lookups. Indexes are built per collection: field value ->
// set of doc ids. They are updated incrementally on every Apply/Delete
// and rebuilt from the store on startup.
package index

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

// Index is a thread-safe inverted index for one collection.
//
// fields is the classic inverted map: field -> encoded value -> doc ids.
// byID is the reverse map that keeps writes cheap: id -> field ->
// encoded value. Without it, dropping one document would have to walk
// every field and value bucket in the collection, making every write
// O(collection size) — the quadratic path the benchmark harness
// exposed (3.18ms/op point writes at 10k docs).
type Index struct {
	mu      sync.RWMutex
	fields  map[string]map[string]map[string]bool // field -> encoded value -> doc ids
	indexed map[string]bool                       // fields we index (nil = all)
	byID    map[string]map[string]string          // doc id -> field -> encoded value
}

func New() *Index {
	return &Index{
		fields:  map[string]map[string]map[string]bool{},
		indexed: nil, // index all fields by default
		byID:    map[string]map[string]string{},
	}
}

// SetIndexedFields restricts which fields get indexed (nil = all).
// Entries for fields that just stopped being indexed are purged so the
// forward and reverse maps can never disagree.
func (ix *Index) SetIndexedFields(fs []string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if fs == nil {
		ix.indexed = nil
		return
	}
	next := make(map[string]bool, len(fs))
	for _, f := range fs {
		next[f] = true
	}
	for f := range ix.fields {
		if !next[f] {
			delete(ix.fields, f)
		}
	}
	for id, fv := range ix.byID {
		for f := range fv {
			if !next[f] {
				delete(fv, f)
			}
		}
		if len(fv) == 0 {
			delete(ix.byID, id)
		}
	}
	ix.indexed = next
}

func encodeValue(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// Add indexes a doc's fields under its id. Self-correcting: if the
// document previously indexed a different value for a field, that
// stale bucket entry is dropped first, so Add may be called directly
// on updates without a separate Remove.
func (ix *Index) Add(id string, fields map[string]interface{}) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.addLocked(id, fields)
}

func (ix *Index) addLocked(id string, fields map[string]interface{}) {
	rev, haveRev := ix.byID[id]
	for f, v := range fields {
		if ix.indexed != nil && !ix.indexed[f] {
			continue
		}
		val := encodeValue(v)
		if val == "" {
			continue
		}
		// Drop the previous value for this field, if any.
		if haveRev {
			if old, ok := rev[f]; ok && old != val {
				ix.unindex(id, f, old)
			}
		}
		byVal, ok := ix.fields[f]
		if !ok {
			byVal = map[string]map[string]bool{}
			ix.fields[f] = byVal
		}
		ids, ok := byVal[val]
		if !ok {
			ids = map[string]bool{}
			byVal[val] = ids
		}
		ids[id] = true
		if !haveRev {
			rev = map[string]string{}
			ix.byID[id] = rev
			haveRev = true
		}
		rev[f] = val
	}
}

// unindex drops one (field, value) membership. Caller holds the lock.
func (ix *Index) unindex(id, field, val string) {
	byVal := ix.fields[field]
	if byVal == nil {
		return
	}
	ids := byVal[val]
	delete(ids, id)
	if len(ids) == 0 {
		delete(byVal, val)
	}
	if len(byVal) == 0 {
		delete(ix.fields, field)
	}
}

// Remove drops all index entries for a doc id in O(fields on that doc)
// by consulting the reverse map — it never walks the collection.
func (ix *Index) Remove(id string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.removeLocked(id)
}

func (ix *Index) removeLocked(id string) {
	rev := ix.byID[id]
	if rev == nil {
		return // never indexed (or already removed): nothing to scan
	}
	for f, val := range rev {
		ix.unindex(id, f, val)
	}
	delete(ix.byID, id)
}

// Upsert is Remove+Add under a single lock — the write path's index
// maintenance. It drops fields the document no longer has and replaces
// changed values, so one call is always a correct re-index.
func (ix *Index) Upsert(id string, fields map[string]interface{}) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.removeLocked(id)
	ix.addLocked(id, fields)
}

// Lookup returns doc ids whose field equals the encoded value.
func (ix *Index) Lookup(field string, v interface{}) []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	byVal, ok := ix.fields[field]
	if !ok {
		return nil
	}
	ids := byVal[encodeValue(v)]
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// LookupRange returns doc ids whose numeric field falls in [lo, hi]
// (either bound may be nil for open-ended). Only numeric values match.
func (ix *Index) LookupRange(field string, lo, hi *float64) []string {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	byVal, ok := ix.fields[field]
	if !ok {
		return nil
	}
	// Collect numeric values with their ids.
	type nv struct {
		val float64
		ids []string
	}
	var nums []nv
	for val, ids := range byVal {
		var f float64
		if json.Unmarshal([]byte(val), &f) == nil {
			list := make([]string, 0, len(ids))
			for id := range ids {
				list = append(list, id)
			}
			nums = append(nums, nv{f, list})
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i].val < nums[j].val })
	var out []string
	for _, n := range nums {
		if lo != nil && n.val < *lo {
			continue
		}
		if hi != nil && n.val > *hi {
			continue
		}
		out = append(out, n.ids...)
	}
	sort.Strings(out)
	return out
}

// Stats returns indexed field names and their distinct-value counts.
func (ix *Index) Stats() map[string]int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	out := map[string]int{}
	for f, byVal := range ix.fields {
		out[f] = len(byVal)
	}
	return out
}

// IndexSet holds one Index per collection.
type IndexSet struct {
	mu      sync.RWMutex
	byCol   map[string]*Index
	enabled bool
}

func NewSet(enabled bool) *IndexSet {
	return &IndexSet{byCol: map[string]*Index{}, enabled: enabled}
}

func (s *IndexSet) Enabled() bool { return s.enabled }

func (s *IndexSet) For(collection string) *Index {
	s.mu.Lock()
	defer s.mu.Unlock()
	ix, ok := s.byCol[collection]
	if !ok {
		ix = New()
		s.byCol[collection] = ix
	}
	return ix
}

var _ = strings.TrimSpace
