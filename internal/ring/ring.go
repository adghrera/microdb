// Package ring implements consistent hashing with virtual nodes so that
// adding or removing a node only moves a 1/N slice of the keyspace.
package ring

import (
	"hash/fnv"
	"sort"
)

const vnodes = 128

type vnode struct {
	h    uint32
	node string
}

type Ring struct {
	vnodes []vnode
	epoch  int64
	// zones maps node -> failure domain (rack/AZ). Empty means no
	// topology information: selection then behaves exactly as before.
	zones map[string]string
}

func hash(s string) uint32 {
	f := fnv.New32a()
	f.Write([]byte(s))
	return f.Sum32()
}

// Hash exposes the ring's token function so other components (the
// range planner) can map keys into the same 32-bit token space.
func Hash(s string) uint32 { return hash(s) }

// Build creates a ring from the given node addresses (epoch 1).
func Build(nodes []string) *Ring { return BuildWithEpoch(nodes, 1) }

// BuildWithEpoch creates a ring carrying a membership epoch. Every
// membership change bumps the epoch; writes carry the epoch so storage
// can fence writes from nodes with stale ownership views.
func BuildWithEpoch(nodes []string, epoch int64) *Ring {
	return BuildWeighted(nodes, nil, epoch)
}

// BuildWeighted creates a ring where each node gets a number of
// vnodes proportional to its weight (observed load). A node carrying
// 2x the cluster-average load gets ~2x the vnodes and therefore
// ~2x the share of the keyspace; a node with no weight info gets the
// baseline. Weights <= 0 or a missing entry fall back to the average
// weight so an unloaded node never loses all its vnodes.
//
// Determinism: every node computes the same ring from the same
// (nodes, weights, epoch) triple — weights must arrive via gossip,
// not local guesses, or ownership views diverge.
func BuildWeighted(nodes []string, weights map[string]float64, epoch int64) *Ring {
	return BuildWeightedIn(nodes, weights, nil, epoch)
}

// BuildWeightedIn is BuildWeighted plus topology: zones spreads
// replicas across failure domains (see Owners). Every node must build
// the same ring from the same inputs, so zones arrive by gossip like
// weights do.
func BuildWeightedIn(nodes []string, weights map[string]float64, zones map[string]string, epoch int64) *Ring {
	r := &Ring{epoch: epoch, zones: zones}
	// Average weight over nodes that have one; baseline for the rest.
	var sum float64
	var cnt int
	for _, n := range nodes {
		if w, ok := weights[n]; ok && w > 0 {
			sum += w
			cnt++
		}
	}
	avg := 1.0
	if cnt > 0 {
		avg = sum / float64(cnt)
	}
	const minVnodes = 8
	for _, n := range nodes {
		w := avg
		if x, ok := weights[n]; ok && x > 0 {
			w = x
		} else if cnt > 0 {
			// A node with no/zero load while others report load is
			// genuinely idle: floor it to 5% of the average rather
			// than the average itself, so the busy nodes absorb the
			// keyspace. (If NO node reports load, everyone keeps the
			// uniform baseline.)
			w = avg * 0.05
		}
		count := int(float64(vnodes) * w / avg)
		if count < minVnodes {
			count = minVnodes
		}
		for i := 0; i < count; i++ {
			r.vnodes = append(r.vnodes, vnode{hash(n + "#" + itoa(i)), n})
		}
	}
	sort.Slice(r.vnodes, func(i, j int) bool { return r.vnodes[i].h < r.vnodes[j].h })
	return r
}

// Epoch returns the membership epoch this ring was built for.
func (r *Ring) Epoch() int64 { return r.epoch }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// Owners returns up to n distinct nodes responsible for a key,
// starting from the first vnode at/after hash(key) and walking
// clockwise — preferring nodes in failure domains not yet used, so
// replicas of one key do not all live in one rack.
//
// Two passes: the first takes only nodes whose zone is new; if the
// topology cannot supply n distinct zones (two AZs with RF=3, or no
// zone data at all) the second pass fills the remainder with any node
// not already chosen. With no zone information the first pass takes
// everything, so this is byte-for-byte the old behaviour.
func (r *Ring) Owners(key string, n int) []string {
	if len(r.vnodes) == 0 || n <= 0 {
		return nil
	}
	h := hash(key)
	start := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].h >= h })
	out := make([]string, 0, n)
	seen := make(map[string]bool, n)
	seenZone := make(map[string]bool, n)
	for pass := 0; pass < 2 && len(out) < n; pass++ {
		for i := 0; i < len(r.vnodes) && len(out) < n; i++ {
			idx := (start + i) % len(r.vnodes)
			node := r.vnodes[idx].node
			if seen[node] {
				continue
			}
			zone := r.zoneOf(node)
			if pass == 0 && zone != "" && seenZone[zone] {
				continue // defer same-domain nodes to the fill pass
			}
			seen[node] = true
			if zone != "" {
				seenZone[zone] = true
			}
			out = append(out, node)
		}
	}
	return out
}

// zoneOf returns a node's failure domain ("" when topology is unknown).
func (r *Ring) zoneOf(node string) string {
	if r.zones == nil {
		return ""
	}
	return r.zones[node]
}
