package backup

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"microdb/internal/store"
	"microdb/internal/tier"
)

func seedStore(t *testing.T, n int) (*store.Store, func()) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := st.Apply("c", "k"+itoa(i), map[string]interface{}{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	return st, func() { st.Close() }
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for ; i > 0; i /= 10 {
		b = append([]byte{byte('0' + i%10)}, b...)
	}
	for l, r := 0, len(b)-1; l < r; l, r = l+1, r-1 {
		b[l], b[r] = b[r], b[l]
	}
	return string(b)
}

// TestRunOnceProducesVerifiableBackup is the whole point: a scheduled
// backup must land on the target WITH a manifest that verifies it.
func TestRunOnceProducesVerifiableBackup(t *testing.T) {
	st, done := seedStore(t, 7)
	defer done()
	target, err := tier.NewDirTarget(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	key, manifest, err := RunOnce(Config{St: st, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Docs != 7 {
		t.Errorf("manifest docs = %d, want 7", manifest.Docs)
	}
	keys, err := target.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("target holds %v, want archive + manifest", keys)
	}

	// Fetch both back and verify the archive against its manifest, the
	// same way an operator would after a restore drill.
	fetch := func(k string) string {
		rc, err := target.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	local := filepath.Join(t.TempDir(), "fetched.jsonl")
	if err := os.WriteFile(local, []byte(fetch(key)), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.ManifestPath(local), []byte(fetch(key+".manifest.json")), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err := store.VerifyBackup(local)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		t.Fatalf("scheduled backup did not verify: %v", rep.Problems)
	}
	if rep.Docs != 7 {
		t.Errorf("verified docs = %d, want 7", rep.Docs)
	}
}

// TestPruneKeepsNewest: offsite growth must be bounded, and a manifest
// must never outlive the archive it describes.
func TestPruneKeepsNewest(t *testing.T) {
	st, done := seedStore(t, 3)
	defer done()
	target, err := tier.NewDirTarget(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n := 0
	cfg := Config{
		St: st, Target: target, Keep: 2,
		Now: func() time.Time { n++; return base.Add(time.Duration(n) * time.Second) },
	}
	for i := 0; i < 5; i++ {
		if _, _, err := RunOnce(cfg); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	keys, err := target.List()
	if err != nil {
		t.Fatal(err)
	}
	var archives, manifests int
	for _, k := range keys {
		// NOTE: filepath.Ext only reports the last suffix, so
		// "backup-1.jsonl.manifest.json" ends in ".json" — classify by
		// full suffix, not by Ext.
		switch {
		case strings.HasSuffix(k, ".manifest.json"):
			manifests++
		case strings.HasSuffix(k, ".jsonl"):
			archives++
		}
	}
	if archives != 2 {
		t.Errorf("archives after prune = %d, want 2 (Keep)", archives)
	}
	if manifests != archives {
		t.Errorf("manifests = %d for %d archives — an orphan manifest is a trap", manifests, archives)
	}
	// The survivors must be the NEWEST keys.
	var sorted []string
	for _, k := range keys {
		if strings.HasSuffix(k, ".jsonl") {
			sorted = append(sorted, k)
		}
	}
	if len(sorted) == 2 && sorted[0] > sorted[1] {
		t.Errorf("survivors are not in time order: %v", sorted)
	}
}

type failingPut struct{ *tier.DirTarget }

func (f failingPut) Put(string, io.Reader, int64) error { return errors.New("network is down") }

// TestRunOnceFailurePublishesNothing: if the archive cannot be
// written, no manifest is published either — half a backup is worse
// than none because it looks restorable.
func TestRunOnceFailurePublishesNothing(t *testing.T) {
	st, done := seedStore(t, 3)
	defer done()
	dir, err := tier.NewDirTarget(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := RunOnce(Config{St: st, Target: failingPut{dir}}); err == nil {
		t.Fatal("expected the target failure to surface")
	}
	keys, err := dir.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("failed run left %v on the target", keys)
	}
}

// TestRunKeepsGoingAfterAFailure: one failed tick must not stop the
// scheduler — the next tick retries.
func TestRunKeepsGoingAfterAFailure(t *testing.T) {
	st, done := seedStore(t, 3)
	defer done()
	dir, err := tier.NewDirTarget(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bad := failingPut{dir}
	good := tier.DirTarget{}
	_ = good

	stop := make(chan struct{})
	defer close(stop)
	var mu sync.Mutex
	failing := true
	target := targetFunc(func() tier.Target {
		mu.Lock()
		defer mu.Unlock()
		if failing {
			return bad
		}
		return dir
	})

	go Run(stop, 30*time.Millisecond, Config{St: st, Target: target}, func(string, ...interface{}) {})
	time.Sleep(70 * time.Millisecond) // a few failing ticks
	mu.Lock()
	failing = false
	mu.Unlock()
	time.Sleep(90 * time.Millisecond) // then good ones

	keys, err := dir.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) < 2 {
		t.Fatalf("scheduler did not recover after the target came back: %v", keys)
	}
}

// targetFunc lets a test swap the target between ticks while keeping
// the Config interface simple.
type targetFunc func() tier.Target

func (f targetFunc) Name() string                             { return f().Name() }
func (f targetFunc) Put(k string, r io.Reader, n int64) error { return f().Put(k, r, n) }
func (f targetFunc) Get(k string) (io.ReadCloser, error)      { return f().Get(k) }
func (f targetFunc) List() ([]string, error)                  { return f().List() }
func (f targetFunc) Delete(k string) error                    { return f().Delete(k) }
