package cluster

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// newSeedTestCluster builds a Cluster that is never Start()ed — enough to
// exercise the seed bookkeeping, which touches no transport or store.
func newSeedTestCluster(t *testing.T, self string) *Cluster {
	t.Helper()
	c, err := New(self, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNoteSeedSkipsSelfAndDeduplicates(t *testing.T) {
	c := newSeedTestCluster(t, "self")
	c.noteSeed("self") // our own address is never a reconnection target
	c.noteSeed("")
	c.noteSeed("a")
	c.noteSeed("a") // duplicate
	c.mu.RLock()
	n := len(c.seeds)
	c.mu.RUnlock()
	if n != 1 {
		t.Fatalf("seeds = %d, want 1 (only %q)", n, "a")
	}
}

func TestSeedCandidatesExcludeLiveAndTombstoned(t *testing.T) {
	c := newSeedTestCluster(t, "self")
	c.noteSeed("live")
	c.noteSeed("gone")
	c.noteSeed("tomb")
	// Mark "live" as a current member so it is never a reconnection target.
	c.peers["live"] = time.Now()
	// "tomb" gracefully departed moments ago: reconnect must not resurrect it.
	c.left["tomb"] = time.Now()
	// An old tombstone has expired and is a fair game again.
	c.left["gone"] = time.Now().Add(-time.Hour)

	got := map[string]bool{}
	for _, s := range c.seedCandidates() {
		got[s] = true
	}
	if got["live"] {
		t.Error("live peer offered as a reconnection candidate")
	}
	if got["tomb"] {
		t.Error("recently departed address offered (would resurrect a tombstone)")
	}
	if !got["gone"] {
		t.Error("stale seed not offered as a reconnection candidate")
	}
}

func TestSeedsPersistAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seeds.json")

	c := newSeedTestCluster(t, "self")
	c.SetSeedFile(path)
	c.noteSeed("alpha")
	c.noteSeed("beta")
	c.maybeSaveSeeds(true) // force flush (the write is throttled)

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("seeds file not written: %v", err)
	}
	if len(b) == 0 {
		t.Fatal("seeds file is empty")
	}

	// A fresh process (new Cluster) on the same data dir must recover the
	// remembered peers so it can find the cluster after a restart.
	c2 := newSeedTestCluster(t, "self")
	c2.SetSeedFile(path)
	c2.mu.RLock()
	hasAlpha, hasBeta := false, false
	for a := range c2.seeds {
		if a == "alpha" {
			hasAlpha = true
		}
		if a == "beta" {
			hasBeta = true
		}
	}
	c2.mu.RUnlock()
	if !hasAlpha || !hasBeta {
		t.Fatalf("reloaded seeds missing entries: alpha=%v beta=%v", hasAlpha, hasBeta)
	}
	// Self must never be loaded as its own seed.
	if _, ok := c2.seeds["self"]; ok {
		t.Error("self loaded as a seed")
	}
}

func TestSeedsAreCapped(t *testing.T) {
	c := newSeedTestCluster(t, "self")
	for i := 0; i < maxSeeds+50; i++ {
		c.noteSeed("n" + strconv.Itoa(i))
	}
	c.mu.RLock()
	n := len(c.seeds)
	c.mu.RUnlock()
	if n > maxSeeds {
		t.Fatalf("seeds grew to %d, want <= %d", n, maxSeeds)
	}
}
