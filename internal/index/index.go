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
type Index struct {
	mu      sync.RWMutex
	fields  map[string]map[string]map[string]bool // field -> encoded value -> doc ids
	indexed map[string]bool                       // fields we index (nil = all)
}

func New() *Index {
	return &Index{
		fields:  map[string]map[string]map[string]bool{},
		indexed: nil, // index all fields by default
	}
}

// SetIndexedFields restricts which fields get indexed (nil = all).
func (ix *Index) SetIndexedFields(fs []string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if fs == nil {
		ix.indexed = nil
		return
	}
	ix.indexed = map[string]bool{}
	for _, f := range fs {
		ix.indexed[f] = true
	}
}

func encodeValue(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// Add indexes a doc's fields under its id.
func (ix *Index) Add(id string, fields map[string]interface{}) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for f, v := range fields {
		if ix.indexed != nil && !ix.indexed[f] {
			continue
		}
		val := encodeValue(v)
		if val == "" {
			continue
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
	}
}

// Remove drops all index entries for a doc id.
func (ix *Index) Remove(id string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	for f, byVal := range ix.fields {
		for val, ids := range byVal {
			delete(ids, id)
			if len(ids) == 0 {
				delete(byVal, val)
			}
		}
		if len(byVal) == 0 {
			delete(ix.fields, f)
		}
	}
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
