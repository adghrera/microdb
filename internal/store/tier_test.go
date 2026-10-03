package store

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"microdb/internal/tier"
)

// failingTarget stands in for a cold tier that is unreachable (bad
// credentials, network partition, bucket policy change).
type failingTarget struct{}

func (failingTarget) Name() string                       { return "failing://x" }
func (failingTarget) Put(string, io.Reader, int64) error { return errors.New("network is down") }
func (failingTarget) Get(string) (io.ReadCloser, error)  { return nil, errors.New("network is down") }
func (failingTarget) List() ([]string, error)            { return nil, errors.New("network is down") }
func (failingTarget) Delete(string) error                { return errors.New("network is down") }

func writeHistory(t *testing.T, st *Store, n int) {
	t.Helper()
	// Two versions per document: compaction only keeps the latest, so
	// the fixture actually has history to throw away (and therefore
	// something worth archiving).
	for i := 0; i < n; i++ {
		for v := 0; v < 2; v++ {
			if _, err := st.Apply("c", docName(i), map[string]interface{}{"n": i, "v": v}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestCompactArchivesToTier: compaction is the moment history is
// destroyed — that is exactly when the cold tier must receive it.
func TestCompactArchivesToTier(t *testing.T) {
	dir := t.TempDir()
	cold := filepath.Join(dir, "cold")
	st, err := Open(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	writeHistory(t, st, 40)

	target, err := tier.NewDirTarget(cold)
	if err != nil {
		t.Fatal(err)
	}
	st.AttachTier(&tier.Archiver{Target: target, Prefix: "log"})
	st.SetFsync(true)

	before, err := os.Stat(st.path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() < 1000 {
		t.Fatalf("fixture too small: %d bytes", before.Size())
	}

	if _, err := st.Compact(24 * time.Hour); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if got := st.TierArchives(); got != 1 {
		t.Fatalf("TierArchives = %d, want 1", got)
	}

	// The archive must hold the PRE-compaction history, byte for byte.
	keys, err := target.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("cold tier holds %d objects, want 1: %v", len(keys), keys)
	}
	rc, err := target.Get(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	archived, _ := io.ReadAll(rc)
	rc.Close()
	if int64(len(archived)) != before.Size() {
		t.Errorf("archived %d bytes, want the %d byte original", len(archived), before.Size())
	}
	if !strings.Contains(string(archived), `"n":17`) {
		t.Error("archive does not contain the history compaction just discarded")
	}

	// ...and the local log is now the compacted one.
	after, err := os.Stat(st.path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Errorf("local log did not shrink: %d -> %d", before.Size(), after.Size())
	}
	// Data still reads back after the swap.
	if d, ok := st.Get("c", docName(0)); !ok || d.Fields["n"] != 0 {
		t.Fatalf("data lost across compaction: %v", ok)
	}
}

// TestCompactRefusesWithoutTier: if the cold tier is down, compaction
// must NOT run — the local log is the only remaining copy of that
// history, and the rewrite would destroy it.
func TestCompactRefusesWithoutTier(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	writeHistory(t, st, 20)

	st.AttachTier(&tier.Archiver{Target: failingTarget{}})
	st.SetFsync(true)
	before, err := os.ReadFile(st.path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.Compact(24 * time.Hour); err == nil {
		t.Fatal("compaction must fail when the archive cannot be written")
	} else if !strings.Contains(err.Error(), "refusing to compact") {
		t.Errorf("error should explain the refusal, got: %v", err)
	}

	after, err := os.ReadFile(st.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("compaction changed the log despite failing: %d -> %d bytes", len(before), len(after))
	}
	// No partial artefacts left behind.
	if _, err := os.Stat(st.path + ".tmp"); err == nil {
		t.Error("failed compaction left a .tmp file behind")
	}
	// The store is still fully usable.
	if _, err := st.Apply("c", "still-works", map[string]interface{}{"v": 1}); err != nil {
		t.Fatalf("store unusable after refused compaction: %v", err)
	}
}
