package merkle

import (
	"testing"
)

func leaves(pairs ...[2]string) []Leaf {
	out := make([]Leaf, len(pairs))
	for i, p := range pairs {
		out[i] = Leaf{ID: p[0], Hash: p[1]}
	}
	return out
}

func TestEqualTreesSameRoot(t *testing.T) {
	a := Build(leaves([2]string{"a", "h1"}, [2]string{"b", "h2"}, [2]string{"c", "h3"}))
	b := Build(leaves([2]string{"c", "h3"}, [2]string{"a", "h1"}, [2]string{"b", "h2"})) // shuffled input
	if a.Root() != b.Root() {
		t.Fatalf("roots differ for same content in different order: %s vs %s", a.Root(), b.Root())
	}
	if d := DiffIDsAuto(a, b); len(d) != 0 {
		t.Fatalf("expected no diff, got %v", d)
	}
}

func TestSingleDocChangeDetected(t *testing.T) {
	a := Build(leaves([2]string{"a", "h1"}, [2]string{"b", "h2"}, [2]string{"c", "h3"}, [2]string{"d", "h4"}))
	b := Build(leaves([2]string{"a", "h1"}, [2]string{"b", "CHANGED"}, [2]string{"c", "h3"}, [2]string{"d", "h4"}))
	if a.Root() == b.Root() {
		t.Fatal("roots should differ")
	}
	d := DiffIDsAuto(a, b)
	if len(d) != 1 || d[0] != "b" {
		t.Fatalf("expected exactly [b] divergent, got %v", d)
	}
}

func TestDiffFindsAllChanges(t *testing.T) {
	base := leaves([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"},
		[2]string{"d", "4"}, [2]string{"e", "5"}, [2]string{"f", "6"},
		[2]string{"g", "7"}, [2]string{"h", "8"})
	mod := leaves([2]string{"a", "1"}, [2]string{"b", "X"}, [2]string{"c", "3"},
		[2]string{"d", "4"}, [2]string{"e", "Y"}, [2]string{"f", "6"},
		[2]string{"g", "Z"}, [2]string{"h", "8"})
	a := Build(base)
	b := Build(mod)
	got := map[string]bool{}
	for _, id := range DiffIDsAuto(a, b) {
		got[id] = true
	}
	for _, want := range []string{"b", "e", "g"} {
		if !got[want] {
			t.Fatalf("missing divergent id %q in %v", want, got)
		}
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 divergent, got %v", got)
	}
}

func TestDepthMismatchFallsBackToMap(t *testing.T) {
	// 3 leaves pads to 4; 5 leaves pads to 8 — different depths.
	a := Build(leaves([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"}))
	b := Build(leaves([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"},
		[2]string{"d", "4"}, [2]string{"e", "5"}))
	if a.Depth() == b.Depth() {
		t.Fatal("test setup: depths should differ")
	}
	got := map[string]bool{}
	for _, id := range DiffIDsAuto(a, b) {
		got[id] = true
	}
	if !got["d"] || !got["e"] || len(got) != 2 {
		t.Fatalf("expected {d,e}, got %v", got)
	}
}

func TestEmptyTree(t *testing.T) {
	a := Build(nil)
	b := Build([]Leaf{})
	if a.Root() != b.Root() {
		t.Fatal("two empty trees should have equal roots")
	}
	if d := DiffIDsAuto(a, b); len(d) != 0 {
		t.Fatalf("expected no diff, got %v", d)
	}
}

func TestDiffAgainstRemote(t *testing.T) {
	a := Build(leaves([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"}, [2]string{"d", "4"}))
	b := Build(leaves([2]string{"a", "1"}, [2]string{"b", "X"}, [2]string{"c", "3"}, [2]string{"d", "4"}))

	fetchCount := 0
	rt := NewRemoteTree(b.Root(), b.Depth(),
		func(level, index int) (string, error) {
			fetchCount++
			return b.HashAt(level, index), nil
		},
		func(index int) (Leaf, bool, error) {
			l, ok := b.LeafAt(index)
			return l, ok, nil
		})

	divs, err := DiffAgainstRemote(a, rt)
	if err != nil {
		t.Fatal(err)
	}
	if len(divs) != 1 {
		t.Fatalf("expected 1 divergence, got %d: %+v", len(divs), divs)
	}
	if divs[0].Local.ID != "b" || divs[0].Remote.ID != "b" {
		t.Fatalf("wrong divergence: %+v", divs[0])
	}
	// Descent must fetch far fewer hashes than the full tree size.
	// Tree of 4 leaves, depth 2: divergent path costs 1+2+1=4 fetches;
	// the full tree has 7 nodes.
	if fetchCount >= 7 {
		t.Fatalf("subtree diff fetched %d hashes — not bounded to divergent paths", fetchCount)
	}
}

func TestDiffAgainstRemoteEqualRootsNoFetch(t *testing.T) {
	a := Build(leaves([2]string{"a", "1"}, [2]string{"b", "2"}))
	b := Build(leaves([2]string{"a", "1"}, [2]string{"b", "2"}))
	fetches := 0
	rt := NewRemoteTree(b.Root(), b.Depth(),
		func(level, index int) (string, error) { fetches++; return b.HashAt(level, index), nil },
		func(index int) (Leaf, bool, error) { l, ok := b.LeafAt(index); return l, ok, nil })
	divs, err := DiffAgainstRemote(a, rt)
	if err != nil || len(divs) != 0 || fetches != 0 {
		t.Fatalf("equal roots must short-circuit with zero fetches: divs=%v fetches=%d err=%v", divs, fetches, err)
	}
}

func TestDiffAgainstRemoteMissingRemoteDoc(t *testing.T) {
	// We have 4 docs, remote has 3 (pads to 4, same depth).
	a := Build(leaves([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"}, [2]string{"d", "4"}))
	b := Build(leaves([2]string{"a", "1"}, [2]string{"b", "2"}, [2]string{"c", "3"}))
	rt := NewRemoteTree(b.Root(), b.Depth(),
		func(level, index int) (string, error) { return b.HashAt(level, index), nil },
		func(index int) (Leaf, bool, error) { l, ok := b.LeafAt(index); return l, ok, nil })
	divs, err := DiffAgainstRemote(a, rt)
	if err != nil {
		t.Fatal(err)
	}
	// The 'd' leaf diverges: we have it, remote is padding.
	if len(divs) != 1 || !divs[0].HasLoc || divs[0].Local.ID != "d" || divs[0].HasRem {
		t.Fatalf("expected d-only divergence, got %+v", divs)
	}
}

func TestHashDocDeterministic(t *testing.T) {
	type doc struct {
		A string `json:"a"`
		B int    `json:"b"`
	}
	h1 := HashDoc(doc{"x", 1})
	h2 := HashDoc(doc{"x", 1})
	h3 := HashDoc(doc{"x", 2})
	if h1 != h2 {
		t.Fatal("same doc hashed differently")
	}
	if h1 == h3 {
		t.Fatal("different docs hashed the same")
	}
}
