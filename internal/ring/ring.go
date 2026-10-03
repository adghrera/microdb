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
	// topo maps node -> failure domains (region, then rack/AZ). Empty
	// maps mean no topology: selection then behaves exactly as before.
	topo Topology
}

// Topology is the failure-domain view used to spread replicas: region
// first (surviving a region loss is the expensive property), then
// zone. Either map may be nil — absence of information is not a
// constraint, it is the absence of one.
type Topology struct {
	Zones   map[string]string
	Regions map[string]string
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
	return BuildWeightedTopo(nodes, weights, Topology{Zones: zones}, epoch)
}

// BuildWeightedTopo is BuildWeightedIn with both failure-domain levels.
func BuildWeightedTopo(nodes []string, weights map[string]float64, topo Topology, epoch int64) *Ring {
	r := &Ring{epoch: epoch, topo: topo}
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
// clockwise — preferring failure domains that are not yet used, so
// the replicas of one key do not all live in one rack, or one region.
//
// The preference cascades by the cost of losing it:
//   - pass 0: a node in a region we have not used yet (a new region
//     brings its zones with it, so zone diversity follows for free);
//   - pass 1: a node in a zone we have not used yet, when the topology
//     still has one — two AZs with RF=3 cannot supply three;
//   - final pass: any node not already chosen, so RF is always met.
//
// With no topology at all the first pass takes everything: selection is
// byte-for-byte what it was before regions existed.
func (r *Ring) Owners(key string, n int) []string {
	if len(r.vnodes) == 0 || n <= 0 {
		return nil
	}
	h := hash(key)
	start := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].h >= h })
	out := make([]string, 0, n)
	seen := make(map[string]bool, n)
	seenZone := make(map[string]bool, n)
	seenRegion := make(map[string]bool, n)

	passes := 1
	if r.topo.hasZones() {
		passes = 2
	}
	if r.topo.hasRegions() {
		passes = 3
	}
	zonePass := 0
	if r.topo.hasRegions() {
		zonePass = 1
	}
	for pass := 0; pass < passes && len(out) < n; pass++ {
		for i := 0; i < len(r.vnodes) && len(out) < n; i++ {
			idx := (start + i) % len(r.vnodes)
			node := r.vnodes[idx].node
			if seen[node] {
				continue
			}
			region, zone := r.topo.regionOf(node), r.topo.zoneOf(node)
			ok := true
			if r.topo.hasRegions() && pass == 0 && region != "" && seenRegion[region] {
				ok = false // this pass buys region diversity
			}
			if ok && r.topo.hasZones() && pass == zonePass && zone != "" && seenZone[zone] {
				ok = false
			}
			if !ok {
				continue
			}
			seen[node] = true
			if region != "" {
				seenRegion[region] = true
			}
			if zone != "" {
				seenZone[zone] = true
			}
			out = append(out, node)
		}
	}
	return out
}

// hasZones / hasRegions report whether the topology carries that level.
// hasZones reports whether any node has a non-empty zone. The topology
// maps always carry an entry per known node (including self, whose zone
// may be unset), so emptiness is decided by the VALUES, never the map
// length — otherwise an all-empty zone map would wrongly activate the
// zone pass and, with it, the region pass that suppresses zone
// diversity in the first pass.
func (t Topology) hasZones() bool {
	for _, z := range t.Zones {
		if z != "" {
			return true
		}
	}
	return false
}

// hasRegions reports whether any node has a non-empty region (the same
// value-based rule as hasZones).
func (t Topology) hasRegions() bool {
	for _, r := range t.Regions {
		if r != "" {
			return true
		}
	}
	return false
}

// zoneOf / regionOf return a node's failure domain ("" when unknown).
func (t Topology) zoneOf(node string) string {
	if t.Zones == nil {
		return ""
	}
	return t.Zones[node]
}

func (t Topology) regionOf(node string) string {
	if t.Regions == nil {
		return ""
	}
	return t.Regions[node]
}
