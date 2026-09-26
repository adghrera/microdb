// Package merkle implements binary Merkle trees over document hashes for
// anti-entropy sync between replicas.
//
// Leaves are (id, hash) pairs sorted by id, padded to a power of two
// with a fixed empty hash. Comparing roots is O(1): equal roots mean the
// collections are identical. When roots differ, DiffIndices descends the
// tree and returns only the leaf positions that diverge, so replicas
// exchange just the changed documents instead of full collection dumps.
package merkle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

type Leaf struct {
	ID   string `json:"id"`
	Hash string `json:"hash"`
}

// HashDoc produces a deterministic digest of a document. Go's
// encoding/json marshals struct fields in declaration order and sorts
// map keys, so the same logical doc always hashes the same.
func HashDoc(doc interface{}) string {
	b, _ := json.Marshal(doc)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hashString(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

var emptyHash = hashString("")

// Tree is a binary Merkle tree. levels[0] is the root,
// levels[depth] the padded leaf hashes.
type Tree struct {
	leaves []Leaf
	levels [][]string
	depth  int
}

// Build sorts leaves by id, pads to a power of two, and hashes upward.
func Build(leaves []Leaf) *Tree {
	s := make([]Leaf, len(leaves))
	copy(s, leaves)
	sort.Slice(s, func(i, j int) bool { return s[i].ID < s[j].ID })

	n := 1
	for n < len(s) {
		n <<= 1
	}
	level := make([]string, n)
	for i := range level {
		if i < len(s) {
			level[i] = s[i].Hash
		} else {
			level[i] = emptyHash
		}
	}
	levels := [][]string{level}
	for len(level) > 1 {
		next := make([]string, len(level)>>1)
		for i := range next {
			next[i] = hashString(level[i<<1] + level[i<<1|1])
		}
		levels = append(levels, next)
		level = next
	}
	// Reverse so index 0 is the root.
	for i, j := 0, len(levels)-1; i < j; i, j = i+1, j-1 {
		levels[i], levels[j] = levels[j], levels[i]
	}
	return &Tree{leaves: s, levels: levels, depth: len(levels) - 1}
}

func (t *Tree) Root() string          { return t.levels[0][0] }
func (t *Tree) Depth() int           { return t.depth }
func (t *Tree) HashAt(lvl, idx int) string { return t.levels[lvl][idx] }

// LeafAt maps a padded leaf index back to a leaf; ok=false for padding.
func (t *Tree) LeafAt(idx int) (Leaf, bool) {
	if idx < len(t.leaves) {
		return t.leaves[idx], true
	}
	return Leaf{}, false
}

// DiffIndices descends two same-depth trees and returns the padded leaf
// indices whose hashes differ. Returns nil when roots match (or depths
// differ — callers must fall back to DiffIDsByMap in that case).
func DiffIndices(a, b *Tree) []int {
	if a.depth != b.depth || a.Root() == b.Root() {
		return nil
	}
	cur := []int{0}
	for lvl := 0; lvl < a.depth; lvl++ {
		var next []int
		for _, idx := range cur {
			for _, c := range []int{idx << 1, idx<<1 | 1} {
				if a.HashAt(lvl+1, c) != b.HashAt(lvl+1, c) {
					next = append(next, c)
				}
			}
		}
		cur = next
	}
	return cur
}

// DiffIDs returns the document ids at divergent leaf positions for
// same-depth trees, using the tree descent.
func DiffIDs(a, b *Tree) []string {
	seen := map[string]bool{}
	var ids []string
	for _, idx := range DiffIndices(a, b) {
		if l, ok := a.LeafAt(idx); ok && !seen[l.ID] {
			seen[l.ID] = true
			ids = append(ids, l.ID)
		}
		if l, ok := b.LeafAt(idx); ok && !seen[l.ID] {
			seen[l.ID] = true
			ids = append(ids, l.ID)
		}
	}
	return ids
}

// DiffIDsByMap compares trees of any depth by id→hash maps. Used as the
// fallback when padding differs (different document counts).
func DiffIDsByMap(a, b *Tree) []string {
	am := make(map[string]string, len(a.leaves))
	for _, l := range a.leaves {
		am[l.ID] = l.Hash
	}
	bm := make(map[string]string, len(b.leaves))
	for _, l := range b.leaves {
		bm[l.ID] = l.Hash
	}
	seen := map[string]bool{}
	var ids []string
	for id, h := range am {
		if bm[id] != h && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	for id, h := range bm {
		if am[id] != h && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// DiffIDsAuto picks the descent when depths match, map fallback otherwise.
func DiffIDsAuto(a, b *Tree) []string {
	if a.depth == b.depth {
		return DiffIDs(a, b)
	}
	return DiffIDsByMap(a, b)
}
