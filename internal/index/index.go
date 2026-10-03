// Package index maintains inverted indexes over document fields for
// fast non-id lookups. Indexes are built per collection: field value ->
// set of doc ids. They are updated incrementally on every Apply/Delete
// and rebuilt from the store on startup.
package index

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"

	"microdb/internal/bloom"
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
	// values is a counting Bloom filter over "field=value" tokens. It
	// answers the only question a filter can answer safely: "can I rule
	// this collection out entirely?" — which turns the full-scan
	// fallback of a compound query into an instant empty result when
	// one of its equality conditions matches nothing here.
	values *bloom.Filter
}

// bloomStartItems is the filter's initial capacity; it doubles when
// the live token count passes half of it, so the false-positive rate
// stays near target as the collection grows.
const bloomStartItems = 8192

// valueToken builds the filter token for one indexed field value.
// It must be byte-identical to what Add inserts and Remove deletes.
//
// The value is truncated: a token is only ever compared for exact
// equality with itself, so a prefix is enough to separate values, and
// without truncation a 50KB document field would be copied into the
// filter on every write AND every update (measured: replay allocations
// tripled before this). Two values sharing a 128-byte prefix collapse
// into one token — a false positive, which the filter is allowed to
// make; the reverse would be a false negative, which it is not.
const maxTokenValue = 128

func valueToken(field, encoded string) string {
	if len(encoded) > maxTokenValue {
		encoded = encoded[:maxTokenValue]
	}
	return field + "\x00" + encoded
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
		for f, val := range fv {
			if !next[f] {
				if ix.values != nil {
					ix.values.Remove(valueToken(f, val))
				}
				delete(fv, f)
			}
		}
		if len(fv) == 0 {
			delete(ix.byID, id)
		}
	}
	ix.indexed = next
}

// encodeValue renders an indexed value as its JSON form. Every write
// calls it once per field, and every query calls it again for the
// value it is looking for, so the fast path matters: json.Marshal is
// reflection plus a growing buffer plus an escaping scan, measured at
// 61% of replay allocations before this.
//
// The fast path must be byte-identical to json.Marshal for the values
// it accepts — the index stores one form and queries encode with the
// other, and any divergence silently turns into a missed match. It
// therefore only handles values whose JSON form it can produce without
// an escaping decision: plain ASCII strings with no JSON-significant
// byte (quote, backslash, control, or the HTML escapes Go always
// applies), ints, int64s, bools and nil. Everything else — floats
// (Go's float formatting rules are subtle), non-ASCII, json.Number —
// goes to json.Marshal, which is the correctness answer, not the fast
// one. TestEncodeValueMatchesMarshal pins the equivalence.
func encodeValue(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case string:
		if plainJSONString(x) {
			var b []byte
			b = append(b, '"')
			b = append(b, x...)
			b = append(b, '"')
			return string(b)
		}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// plainJSONString reports whether s needs no JSON escaping at all.
// json.Marshal always escapes quote, backslash, control bytes and
// <, >, & (HTML), and passes other ASCII through untouched; anything
// outside printable ASCII takes the slow path rather than guessing.
func plainJSONString(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c > 0x7e {
			return false
		}
		switch c {
		case '"', '\\', '<', '>', '&':
			return false
		}
	}
	return true
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
		if ix.values == nil {
			ix.values = bloom.New(bloomStartItems, 0.01)
		}
		ix.values.Add(valueToken(f, val))
		if ix.values.Len() > ix.values.Cap()/2 {
			ix.growBloomLocked()
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
	if ix.values != nil {
		ix.values.Remove(valueToken(field, val))
	}
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

// ProbablyHasValue reports whether at least one document in this
// collection MIGHT have field == value. A false answer is guaranteed
// correct (nothing in the collection has it); a true answer only means
// "look". A nil filter answers true — never rule anything out.
func (ix *Index) ProbablyHasValue(field string, v interface{}) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.ProbablyHasEncoded(field, encodeValue(v))
}

// ProbablyHasEncoded is ProbablyHasValue for an already-encoded value.
func (ix *Index) ProbablyHasEncoded(field, encoded string) bool {
	if encoded == "" {
		return true
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return ix.values.Has(valueToken(field, encoded))
}

// growBloomLocked rebuilds the filter at double the capacity so the
// false-positive rate does not decay as the collection fills up.
func (ix *Index) growBloomLocked() {
	if ix.values == nil {
		return
	}
	grown := bloom.New(ix.values.Cap()*2, 0.01)
	for id, fv := range ix.byID {
		_ = id
		for f, val := range fv {
			grown.Add(valueToken(f, val))
		}
	}
	ix.values = grown
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
