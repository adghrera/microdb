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
}

func hash(s string) uint32 {
	f := fnv.New32a()
	f.Write([]byte(s))
	return f.Sum32()
}

// Build creates a ring from the given node addresses (epoch 1).
func Build(nodes []string) *Ring { return BuildWithEpoch(nodes, 1) }

// BuildWithEpoch creates a ring carrying a membership epoch. Every
// membership change bumps the epoch; writes carry the epoch so storage
// can fence writes from nodes with stale ownership views.
func BuildWithEpoch(nodes []string, epoch int64) *Ring {
	r := &Ring{epoch: epoch}
	for _, n := range nodes {
		for i := 0; i < vnodes; i++ {
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

// Owners returns up to n distinct nodes responsible for a key, starting
// from the first vnode at/after hash(key) and walking clockwise.
func (r *Ring) Owners(key string, n int) []string {
	if len(r.vnodes) == 0 || n <= 0 {
		return nil
	}
	h := hash(key)
	start := sort.Search(len(r.vnodes), func(i int) bool { return r.vnodes[i].h >= h })
	out := make([]string, 0, n)
	seen := make(map[string]bool, n)
	for i := 0; i < len(r.vnodes) && len(out) < n; i++ {
		idx := (start + i) % len(r.vnodes)
		node := r.vnodes[idx].node
		if !seen[node] {
			seen[node] = true
			out = append(out, node)
		}
	}
	return out
}
