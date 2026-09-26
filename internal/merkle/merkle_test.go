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
