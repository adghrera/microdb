package store

import (
	"encoding/json"
	"sort"
	"strconv"
)

// AggKind names the aggregates a shard can compute on its own.
var AggKinds = map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}

// Aggregate is one shard's partial answer for a grouped or ungrouped
// aggregate. Shards never ship documents for these queries — they ship
// these few numbers, and the coordinator merges them.
//
// Sum/count carry the state for avg (E[x] = sum/count) so merging is
// associative: any order of partials gives the same result, which is
// what makes scatter-gather aggregates correct under retries and
// partial results.
type Aggregate struct {
	Kind   string               `json:"kind"`
	Field  string               `json:"field,omitempty"`
	Count  int64                `json:"count"` // rows this partial covers
	Sum    float64              `json:"sum"`
	Min    interface{}          `json:"min,omitempty"`
	Max    interface{}          `json:"max,omitempty"`
	Groups map[string]Aggregate `json:"groups,omitempty"`
}

// Aggregate computes kind over the collection, filtered by filter.
// groupBy (optional) buckets the result by another field.
//
// own must be non-nil in a cluster: every document is REPLICATED to
// every one of its RF owners, so an aggregate over "everything I hold"
// counts each document once per replica. Shards therefore count only
// the documents whose primary owner they are, and the coordinator's
// sum is each document exactly once. A nil own counts everything, which
// is what a single-node store and the unit tests want.
//
// It reuses the exact-scan path — including the Bloom prefilter, so a
// query over a value the collection does not hold answers without
// walking anything.
func (s *Store) Aggregate(collection string, filter map[string]interface{}, kind, field, groupBy string, own func(id string) bool) Aggregate {
	out := Aggregate{Kind: kind, Field: field, Groups: map[string]Aggregate{}}
	if !AggKinds[kind] {
		return out
	}
	if s.Prefilter(collection, filter) {
		return out // cannot possibly match: empty answer, no walk
	}
	accumulate := func(dst *Aggregate, d *Doc) {
		dst.Count++
		if kind == "count" || field == "" {
			return
		}
		v, ok := d.Fields[field]
		if !ok {
			return
		}
		if f, isNum := numOf(v); isNum {
			dst.Sum += f
		}
		if dst.Min == nil || CompareValues(v, dst.Min) < 0 {
			dst.Min = v
		}
		if dst.Max == nil || CompareValues(v, dst.Max) > 0 {
			dst.Max = v
		}
	}
	s.rangeDocs(func(_ string, d *Doc) bool {
		if d.Collection != collection || d.Deleted {
			return true
		}
		if own != nil && !own(d.ID) {
			return true // another node's primary: counted there
		}
		if len(filter) > 0 && !Matches(d, filter) {
			return true
		}
		if groupBy == "" {
			accumulate(&out, d)
			return true
		}
		key := groupKey(d.Fields[groupBy])
		g, ok := out.Groups[key]
		if !ok {
			// A bucket carries its own kind: Value() and Merge() both
			// read it, and an empty kind made every merged bucket
			// count as zero.
			g = Aggregate{Kind: kind, Field: field, Groups: map[string]Aggregate{}}
		}
		accumulate(&g, d)
		out.Groups[key] = g
		return true
	})
	return out
}

// groupKey renders a grouping value as a stable bucket name. Missing
// fields group together rather than being dropped — a NULL bucket is
// information, not an error.
func groupKey(v interface{}) string {
	if v == nil {
		return "null"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return strconv.Quote(toString(v))
	}
	return string(b)
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// Merge folds another shard's partial into p. It is associative and
// commutative for sum/count/min/max, which is what lets the
// coordinator merge whatever came back, in whatever order, including
// partial results after a deadline.
func (p *Aggregate) Merge(o Aggregate) {
	// Never mix different aggregates — but a bucket that lost its kind
	// (an older peer, a partial decoded without it) counts as the one
	// we are building: refusing it silently produced zeroed groups.
	if o.Kind != "" && p.Kind != "" && o.Kind != p.Kind {
		return
	}
	if o.Kind == "" {
		o.Kind = p.Kind
	}
	p.Count += o.Count
	p.Sum += o.Sum
	if o.Min != nil && (p.Min == nil || CompareValues(o.Min, p.Min) < 0) {
		p.Min = o.Min
	}
	if o.Max != nil && (p.Max == nil || CompareValues(o.Max, p.Max) > 0) {
		p.Max = o.Max
	}
	if len(o.Groups) == 0 {
		return
	}
	if p.Groups == nil {
		p.Groups = map[string]Aggregate{}
	}
	for k, g := range o.Groups {
		cur := p.Groups[k]
		if cur.Kind == "" {
			// Fill in the missing kind WITHOUT replacing the bucket:
			// assigning a fresh Aggregate here threw away whatever the
			// local shard had already counted.
			cur.Kind = p.Kind
			if cur.Field == "" {
				cur.Field = p.Field
			}
		}
		cur.Merge(g)
		p.Groups[k] = cur
	}
}

// Value renders the aggregate as the scalar the API returns.
// min/max stay in their original JSON form (they may be strings).
func (p Aggregate) Value() interface{} {
	switch p.Kind {
	case "count":
		return p.Count
	case "sum":
		return p.Sum
	case "avg":
		if p.Count == 0 {
			return nil
		}
		return p.Sum / float64(p.Count)
	case "min":
		return p.Min
	case "max":
		return p.Max
	}
	return nil
}

// SortedGroups returns group keys in a stable order so responses are
// byte-comparable across nodes (map iteration is not).
func (p Aggregate) SortedGroups() []string {
	keys := make([]string, 0, len(p.Groups))
	for k := range p.Groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
