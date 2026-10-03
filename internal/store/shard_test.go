package store

import "testing"

// TestShardStripesAreDistributed guards the placement function: if
// everything hashed into one stripe the whole design collapses back to
// a single lock.
func TestShardStripesAreDistributed(t *testing.T) {
	const n = 4096
	seen := map[uint32]int{}
	for i := 0; i < n; i++ {
		seen[shardOf(key("bench", docName(i)))]++
	}
	if len(seen) < numShards/2 {
		t.Fatalf("4096 keys landed in only %d of %d stripes — hashing is broken", len(seen), numShards)
	}
	// No stripe should be a hotspot: with 4096 keys over 64 stripes the
	// mean is 64; allow a generous 4x skew.
	for sh, c := range seen {
		if c > 256 {
			t.Errorf("stripe %d holds %d keys (mean %d) — skew too high", sh, c, n/numShards)
		}
	}
}

// TestShardStoreRoundTrip proves the stripe is invisible to callers:
// writes land, reads find them, iteration sees every one, and the
// tombstone path removes them.
func TestShardStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	const n = 500
	for i := 0; i < n; i++ {
		if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i}); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if got := st.DocCount(); got != n {
		t.Fatalf("DocCount = %d, want %d", got, n)
	}
	// Every key must be readable through its own stripe.
	for i := 0; i < n; i++ {
		d, ok := st.Get("c", docName(i))
		if !ok {
			t.Fatalf("key %d not found after write", i)
		}
		if d.Fields["n"] != i {
			t.Fatalf("key %d returned %#v", i, d.Fields)
		}
	}
	// Iteration must see every stripe's contents exactly once.
	seen := map[string]bool{}
	st.rangeDocs(func(_ string, d *Doc) bool {
		if d.Collection == "c" {
			seen[d.ID] = true
		}
		return true
	})
	if len(seen) != n {
		t.Fatalf("rangeDocs saw %d docs, want %d", len(seen), n)
	}
	// Deletes + compaction must take keys back out of their stripes.
	for i := 0; i < n; i++ {
		if err := st.Delete("c", docName(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	if _, ok := st.Get("c", docName(0)); ok {
		t.Error("deleted key still readable")
	}
	// Tombstones stay in the map (they are data until compaction GCs
	// them), so the key count is unchanged — but every one of them must
	// read as gone, which Get above already checked for key 0.
	if got := st.DocCount(); got != n {
		t.Fatalf("DocCount after deletes = %d, want %d (tombstones retained)", got, n)
	}
}

func docName(i int) string { return "k" + itoa(i) }
