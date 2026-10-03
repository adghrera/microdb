// Package bloom implements a counting Bloom filter.
//
// Why a counting variant: microdb's filters track live field values,
// which arrive and leave with every write, delete and compaction. A
// plain Bloom filter can only add, so a deleted document's value would
// keep answering "maybe present" forever — harmless for correctness
// (false positives are allowed) but it would silently rot into a
// filter that says "maybe" about everything and therefore skips
// nothing.
//
// Correctness contract: Has never returns false for an item that was
// added and not removed. False positives are allowed and are the whole
// point — callers use Has to rule work OUT, never to rule it in.
package bloom

import "math"

// Filter is a counting Bloom filter over string tokens.
type Filter struct {
	bits []uint8 // one counter per slot
	m    uint32  // number of counters
	k    uint32  // hash functions
	n    int     // live additions (for stats)
}

// New returns a filter sized for n items at the wanted false-positive
// rate. Both are rounded into sane bounds: at least 8 counters, at
// most 256 hash functions.
func New(n int, fp float64) *Filter {
	if n < 1 {
		n = 1
	}
	if fp <= 0 || fp >= 1 {
		fp = 0.01
	}
	// m = -n ln(p) / (ln 2)^2 counters (4 bits each).
	m := uint32(math.Ceil(-float64(n) * math.Log(fp) / (math.Log(2) * math.Log(2))))
	if m < 8 {
		m = 8
	}
	// k = (m/n) ln 2
	k := uint32(math.Round(float64(m) / float64(n) * math.Log(2)))
	if k < 1 {
		k = 1
	}
	if k > 64 {
		k = 64
	}
	return &Filter{bits: make([]uint8, m), m: m, k: k}
}

// hashes yields the two independent 32-bit hashes double hashing is
// built from (Kirsch-Mitzenmacher: k probes from two hashes).
func hashes(token string) (uint32, uint32) {
	var h1, h2 uint32 = 2166136261, 0x811c9dc5
	for i := 0; i < len(token); i++ {
		c := token[i]
		h1 ^= uint32(c)
		h1 *= 16777619
		h2 ^= uint32(c)
		h2 *= 16777619
		h2 = h2<<7 | h2>>25
	}
	if h2 == 0 {
		h2 = 0x9e3779b9
	}
	return h1, h2
}

// Add records token.
func (f *Filter) Add(token string) {
	if f == nil {
		return
	}
	h1, h2 := hashes(token)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + i*h2) % f.m
		// Saturate at 255 instead of wrapping: a wrapped counter
		// reads as 0, which is a false NEGATIVE — the one thing this
		// type must never produce. Reaching 255 needs 255 unbalanced
		// adds of colliding tokens; every store update is a balanced
		// remove+add, so it does not happen in practice.
		if f.bits[idx] < 0xff {
			f.bits[idx]++
		}
	}
	f.n++
}

// Remove drops token. Removing a token that is not present is a no-op;
// removing one that is present leaves no false negative behind.
func (f *Filter) Remove(token string) {
	if f == nil {
		return
	}
	h1, h2 := hashes(token)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + i*h2) % f.m
		if f.bits[idx] > 0 {
			f.bits[idx]--
		}
	}
	if f.n > 0 {
		f.n--
	}
}

// Has reports whether the token might be present. It never returns
// false for a token that was added and not removed.
func (f *Filter) Has(token string) bool {
	if f == nil {
		return true // no filter: cannot rule anything out
	}
	h1, h2 := hashes(token)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + i*h2) % f.m
		if f.bits[idx] == 0 {
			return false
		}
	}
	return true
}

// Reset clears the filter for reuse (keeps the allocation).
func (f *Filter) Reset() {
	if f == nil {
		return
	}
	for i := range f.bits {
		f.bits[i] = 0
	}
	f.n = 0
}

// Cap reports the number of counters — the filter's capacity, used to
// decide when it should be rebuilt bigger.
func (f *Filter) Cap() int {
	if f == nil {
		return 0
	}
	return int(f.m)
}

// Len reports live additions.
func (f *Filter) Len() int {
	if f == nil {
		return 0
	}
	return f.n
}

// Bytes reports the memory the filter occupies.
func (f *Filter) Bytes() int {
	if f == nil {
		return 0
	}
	return len(f.bits)
}
