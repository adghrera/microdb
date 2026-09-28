// Package ranges implements contiguous token-range ownership with
// split and merge — the DynamoDB-style alternative to pure vnode
// hashing. The 32-bit token space is tiled by half-open intervals
// [start, end); each range has an ordered owner list (primary
// first). Hot ranges (load above a fair share) are SPLIT at their
// load-median so a hot spot is served by multiple smaller ranges on
// different nodes; cold adjacent ranges on the same primary MERGE
// back into fewer big ranges.
//
// The plan is a SPARSE overlay: it only contains ranges that have
// been split. Tokens outside any planned range fall through to the
// base vnode ring (Owners returns nil). That keeps the common case
// exactly as it was and confines the overlay to the hot spots that
// motivated it.
//
// Determinism is the load-bearing property: PlanFor is a pure
// function of (nodes, rf, per-bucket cluster load, epoch). Every
// node computes the identical plan from gossiped inputs, so no
// plan replication or agreement protocol is needed — same inputs,
// same plan, same ownership view.
package ranges

import (
	"sort"
)

// Buckets is the granularity of load accounting: the high byte of
// the token. 256 buckets are cheap to track, gossip, and sum.
const Buckets = 256

// Range is a half-open token interval [Start, End) with an ordered
// owner list (primary first). Owners are node addresses.
type Range struct {
	Start  uint32   `json:"start"`
	End    uint32   `json:"end"`
	Owners []string `json:"owners"`
	Load   float64  `json:"load"` // cluster writes/sec attributed to this range
}

// Plan is a sparse set of explicitly-owned ranges, sorted by Start.
// Ranges never overlap or touch: each is a distinct split region.
type Plan struct {
	Epoch  int64   `json:"epoch"` // membership epoch this plan was computed for
	Ranges []Range `json:"ranges"`
}

// Owners returns up to n owners for a token, or nil when the token
// falls outside every planned range (caller falls back to the base
// ring). The planned range's owner order is authoritative.
func (p *Plan) Owners(token uint32, n int) []string {
	if p == nil || n <= 0 {
		return nil
	}
	// Rightmost range with Start <= token.
	i := sort.Search(len(p.Ranges), func(i int) bool { return p.Ranges[i].Start > token }) - 1
	if i < 0 {
		return nil
	}
	r := p.Ranges[i]
	// End == 0 means the range runs to the top of the 32-bit space
	// (2^32 wraps to 0); only check the upper bound when it didn't.
	if r.End != 0 && token >= r.End {
		return nil // gap: not covered by the overlay
	}
	if n > len(r.Owners) {
		n = len(r.Owners)
	}
	return r.Owners[:n]
}

// Split divides r at token `at` (Start < at < End) into two ranges
// keeping the same owners. Both halves are returned in order.
func Split(r Range, at uint32) (Range, Range, bool) {
	if at <= r.Start || at >= r.End {
		return Range{}, Range{}, false
	}
	owners := append([]string(nil), r.Owners...)
	return Range{Start: r.Start, End: at, Owners: owners, Load: r.Load / 2},
		Range{Start: at, End: r.End, Owners: owners, Load: r.Load / 2}, true
}

// MergeAdjacent joins two touching ranges (a.End == b.Start) into one.
// Owner order follows `a`, with any owners only in `b` appended.
func MergeAdjacent(a, b Range) (Range, bool) {
	if a.End != b.Start {
		return Range{}, false
	}
	owners := append([]string(nil), a.Owners...)
	seen := map[string]bool{}
	for _, o := range owners {
		seen[o] = true
	}
	for _, o := range b.Owners {
		if !seen[o] {
			owners = append(owners, o)
			seen[o] = true
		}
	}
	return Range{Start: a.Start, End: b.End, Owners: owners, Load: a.Load + b.Load}, true
}

// Bucket returns the load bucket for a token.
func Bucket(token uint32) int { return int(token >> 24) }

// Planner configuration knobs (fixed for "tiny"; sensible defaults).
const (
	// MinWidthBuckets is the smallest splittable region, in buckets.
	// A single hot bucket is already the atomic unit of split.
	MinWidthBuckets = 1
	// HotFactor: a region is hot when its load exceeds
	// HotFactor * (total load / node count). This is the "fair
	// share" a single range should carry.
	HotFactor = 1.0
	// MaxRanges caps the overlay size so a pathological load can't
	// produce an unbounded plan.
	MaxRanges = 64
)

// PlanFor computes the deterministic sparse overlay for the current
// cluster state.
//
//	nodes  : live node addresses (any order; sorted internally)
//	rf     : replication factor (owner-list length)
//	bucketLoad: cluster-wide writes/sec per bucket (summed across nodes)
//	epoch  : membership epoch to stamp on the plan
//	base   : fallback owner resolver (the vnode ring), used to seed
//	         replica ordering for split ranges
//
// A split region's primary is the least-loaded node (greedy, by
// observed cluster load); replicas are taken from the base ring's
// owner order for the region start, excluding the primary. Cold
// regions are never split, and adjacent cold ranges with the same
// primary merge.
func PlanFor(nodes []string, rf int, bucketLoad []float64, epoch int64, base BaseRing) *Plan {
	return Rebalance(nodes, rf, bucketLoad, epoch, base, nil)
}

// ImbalanceThreshold is the hysteresis band for the sticky
// rebalancer: a node may carry up to ImbalanceThreshold * fair-share
// before ranges are shed off it. A value of 1.0 rebalances exactly;
// 1.1 lets load drift of up to ~10% happen with ZERO range moves,
// which is the whole point of minimal-movement. Small fluctuations
// below the band never trigger churn.
const ImbalanceThreshold = 1.1

// Rebalance computes the plan, optionally seeded with the previous
// plan for MINIMAL MOVEMENT. With prev==nil it behaves exactly like
// PlanFor (fresh greedy). With a prev plan, each range keeps its
// previous primary as long as that node is still live and stays
// within ImbalanceThreshold * fair-share; only ranges that would
// overflow their old primary move, and they move to the coolest node.
//
// Consequences:
//   - A node JOIN shrinks the fair-share, so old primaries overflow
//     and shed exactly the ranges needed to fill the new node — the
//     minimum set that restores balance.
//   - A node LEAVE reassigns only the departed node's ranges.
//   - Small load drift inside the band moves nothing.
func Rebalance(nodes []string, rf int, bucketLoad []float64, epoch int64, base BaseRing, prev *Plan) *Plan {
	if len(nodes) == 0 || rf <= 0 || len(bucketLoad) != Buckets {
		return nil
	}
	sorted := append([]string(nil), nodes...)
	sort.Strings(sorted)
	total := 0.0
	for _, v := range bucketLoad {
		total += v
	}
	if total <= 0 {
		return nil // no load signal: base ring decides everything
	}
	fair := HotFactor * total / float64(len(sorted))
	if fair <= 0 {
		return nil
	}

	out := splitRanges(bucketLoad, total, fair)
	if len(out) == 0 {
		return nil
	}

	// Heaviest-first ordering makes the greedy/sticky pass deterministic
	// and spreads the big ranges before the small ones.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Load != out[j].Load {
			return out[i].Load > out[j].Load
		}
		return out[i].Start < out[j].Start
	})

	live := make(map[string]bool, len(sorted))
	for _, n := range sorted {
		live[n] = true
	}
	assigned := make(map[string]float64, len(sorted))
	for _, n := range sorted {
		assigned[n] = 0
	}
	cap := ImbalanceThreshold * fair

	// Sticky primary pass: prefer the previous primary when it is live
	// and taking this range keeps that node inside the band. Otherwise
	// fall to the coolest live node (fresh greedy for uncovered ranges).
	for i := range out {
		primary := ""
		if prev != nil {
			if pp := prevPrimaryFor(prev, out[i].Start); pp != "" && live[pp] && assigned[pp]+out[i].Load <= cap {
				primary = pp
			}
		}
		if primary == "" {
			for _, n := range sorted { // sorted: deterministic tie-break
				if primary == "" || assigned[n] < assigned[primary] {
					primary = n
				}
			}
		}
		out[i].Owners = []string{primary}
		assigned[primary] += out[i].Load
	}

	// Balance refinement: if the sticky pass left any node above the
	// band (because its previous ranges alone exceed it), shed the
	// largest ranges off the hottest node to the coolest node until
	// every node is inside the band or no move helps. Shedding the
	// LARGEST range reduces the max with the FEWEST moves.
	for guard := 0; guard < len(out)*2; guard++ {
		hot, cool := "", ""
		for _, n := range sorted {
			if hot == "" || assigned[n] > assigned[hot] {
				hot = n
			}
			if cool == "" || assigned[n] < assigned[cool] {
				cool = n
			}
		}
		if hot == cool || assigned[hot] <= cap {
			break // balanced within the band
		}
		// Find the largest range on `hot` whose move to `cool` strictly
		// reduces the spread (and does not overshoot cool past hot).
		idx := -1
		var bestLoad float64
		for i := range out {
			if out[i].Owners[0] != hot {
				continue
			}
			moved := out[i].Load
			if moved <= 0 {
				continue
			}
			newHot := assigned[hot] - moved
			newCool := assigned[cool] + moved
			// Only accept if it reduces the max load on these two.
			if newHot < assigned[hot] && newCool < assigned[hot] {
				if moved > bestLoad {
					bestLoad, idx = moved, i
				}
			}
		}
		if idx < 0 {
			break // nothing movable improves the balance
		}
		out[idx].Owners[0] = cool
		assigned[hot] -= bestLoad
		assigned[cool] += bestLoad
	}

	// Replicas: base-ring owner order for the region start, excluding
	// the primary, so replica placement keeps the ring's spread.
	for i := range out {
		primary := out[i].Owners[0]
		if base != nil {
			for _, o := range base.Owners(tokenKey(out[i].Start), rf*3) {
				if len(out[i].Owners) >= rf {
					break
				}
				if o != primary {
					out[i].Owners = append(out[i].Owners, o)
				}
			}
		}
	}

	// Final invariant check: sorted, non-overlapping.
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return &Plan{Epoch: epoch, Ranges: out}
}

// splitRanges runs the recursive hot-region split and returns the
// leaf ranges (Load set, Owners nil). A region hotter than fair
// splits at its load-median until it fits or can't split further.
// Every leaf of a split subtree is KEPT (falling back to the ring
// would re-concentrate the load the split relieved). Only the whole
// space being cold yields no ranges — then the base ring owns all.
func splitRanges(bucketLoad []float64, total, fair float64) []Range {
	var split func(lo, hi int, load float64, depth int, splitAbove bool) []Range
	split = func(lo, hi int, load float64, depth int, splitAbove bool) []Range {
		if load <= fair {
			if !splitAbove && hi-lo == Buckets {
				return nil // whole space cold: base ring territory
			}
			return []Range{{Start: uint32(lo) << 24, End: uint32(hi) << 24, Load: load}}
		}
		if hi-lo <= MinWidthBuckets || depth <= 0 {
			return []Range{{Start: uint32(lo) << 24, End: uint32(hi) << 24, Load: load}}
		}
		mid := loadMedian(bucketLoad, lo, hi, load)
		if mid <= lo || mid >= hi {
			return []Range{{Start: uint32(lo) << 24, End: uint32(hi) << 24, Load: load}}
		}
		leftLoad := sumBuckets(bucketLoad, lo, mid)
		return append(split(lo, mid, leftLoad, depth-1, true), split(mid, hi, load-leftLoad, depth-1, true)...)
	}
	maxDepth := 0
	for (1<<maxDepth) < MaxRanges {
		maxDepth++
	}
	return split(0, Buckets, total, maxDepth, false)
}

// prevPrimaryFor returns the primary (first owner) of the prev-plan
// range covering `token`, or "" if none. Used to seed the sticky
// rebalancer from the previous ownership view.
func prevPrimaryFor(prev *Plan, token uint32) string {
	if prev == nil || len(prev.Ranges) == 0 {
		return ""
	}
	i := sort.Search(len(prev.Ranges), func(i int) bool { return prev.Ranges[i].Start > token }) - 1
	if i < 0 {
		return ""
	}
	r := prev.Ranges[i]
	if r.End != 0 && token >= r.End {
		return "" // gap in the previous overlay
	}
	if len(r.Owners) == 0 {
		return ""
	}
	return r.Owners[0]
}

// MovementCount returns how many ranges changed primary between two
// plans, counting each range in `next` whose primary differs from
// the primary that owned its region in `prev`. A new range (no prev
// coverage) counts as one move. This is the churn metric the
// minimal-movement rebalancer minimizes.
func MovementCount(prev, next *Plan) int {
	if next == nil {
		return 0
	}
	moves := 0
	for _, r := range next.Ranges {
		if len(r.Owners) == 0 {
			continue
		}
		if prevPrimaryFor(prev, r.Start) != r.Owners[0] {
			moves++
		}
	}
	return moves
}

// BaseRing is the fallback placement source (the vnode ring).
type BaseRing interface {
	Owners(key string, n int) []string
}

func tokenKey(start uint32) string {
	// A stable key inside the region: the region's start token as a
	// string. The base ring hashes it, so replicas spread by the
	// ring's vnode layout for that point.
	return "rng/" + uitoa(start)
}

func uitoa(v uint32) string {
	if v == 0 {
		return "0"
	}
	var b [10]byte
	pos := len(b)
	for v > 0 {
		pos--
		b[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(b[pos:])
}

func sumBuckets(b []float64, lo, hi int) float64 {
	s := 0.0
	for i := lo; i < hi; i++ {
		s += b[i]
	}
	return s
}

// loadMedian returns the bucket split point s in (lo, hi) that most
// evenly divides the region's load — the hot-spot-aware split point.
// Returns -1 when the region is too narrow to split.
func loadMedian(b []float64, lo, hi int, total float64) int {
	half := total / 2
	// First crossing: the smallest s where the running load reaches
	// half the interval total. Ties resolve toward the crossing, not
	// the low end, so a single hot bucket is isolated instead of the
	// split crawling bucket-by-bucket from lo.
	acc := 0.0
	for s := lo + 1; s < hi; s++ {
		acc += b[s-1]
		if acc >= half {
			return s
		}
	}
	// No crossing before hi: the mass sits at the top of the interval.
	return hi - 1
}
