package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPITRReplayUntil: replaying the raw log up to an intermediate
// timestamp restores the state as it was then — later versions and
// later deletes are invisible.
func TestPITRReplayUntil(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// v1 at T1
	st.Apply("c", "k1", map[string]interface{}{"v": "first"})
	time.Sleep(10 * time.Millisecond)
	t1 := time.Now().UnixMilli()
	time.Sleep(10 * time.Millisecond)

	// v2 + a second doc + a delete, all AFTER T1
	st.Apply("c", "k1", map[string]interface{}{"v": "second"})
	st.Apply("c", "k2", map[string]interface{}{"v": "added-later"})
	st.Apply("c", "k3", map[string]interface{}{"v": "doomed"})
	time.Sleep(10 * time.Millisecond)
	st.Delete("c", "k3")

	// Archive the raw log (full history).
	var raw bytes.Buffer
	if err := st.ArchiveRaw(&raw); err != nil {
		t.Fatal(err)
	}
	st.Close()

	// PITR to T1: only the first version of k1 exists; k2 and k3
	// never happened yet.
	recDir := filepath.Join(t.TempDir(), "recovered")
	rec, applied, err := ReplayUntil(bytes.NewReader(raw.Bytes()), t1, recDir)
	if err != nil {
		t.Fatal(err)
	}
	if applied < 1 {
		t.Fatalf("applied=%d want >=1", applied)
	}
	d, ok := rec.Get("c", "k1")
	if !ok {
		t.Fatal("k1 missing after PITR")
	}
	if d.Fields["v"] != "first" {
		t.Fatalf("PITR restored wrong version: %v want 'first'", d.Fields["v"])
	}
	if _, ok := rec.Get("c", "k2"); ok {
		t.Fatal("k2 should not exist at T1")
	}
	if _, ok := rec.Get("c", "k3"); ok {
		t.Fatal("k3 should not exist at T1")
	}
	rec.Close()

	// PITR to "now": full current state, including the delete.
	rec2, _, err := ReplayUntil(bytes.NewReader(raw.Bytes()), time.Now().UnixMilli()+1000, filepath.Join(t.TempDir(), "full"))
	if err != nil {
		t.Fatal(err)
	}
	defer rec2.Close()
	d2, ok := rec2.Get("c", "k1")
	if !ok || d2.Fields["v"] != "second" {
		t.Fatalf("full replay wrong k1: %v", d2)
	}
	if _, ok := rec2.Get("c", "k2"); !ok {
		t.Fatal("k2 missing in full replay")
	}
	if d3, ok := rec2.Get("c", "k3"); ok && !d3.Deleted {
		t.Fatal("k3 should be deleted in full replay")
	}
}

// TestPITRRecoveredStatePersists: the recovered store must survive a
// restart with NO post-recovery writes — the replay itself has to be
// durable, not just in-memory.
func TestPITRRecoveredStatePersists(t *testing.T) {
	var raw bytes.Buffer
	src, _ := Open(t.TempDir())
	src.Apply("c", "a", map[string]interface{}{"v": "keep"})
	src.Apply("c", "b", map[string]interface{}{"v": "gone"})
	src.Delete("c", "b")
	if err := src.ArchiveRaw(&raw); err != nil {
		t.Fatal(err)
	}
	src.Close()

	recDir := t.TempDir()
	rec, _, err := ReplayUntil(bytes.NewReader(raw.Bytes()), time.Now().UnixMilli()+1000, recDir)
	if err != nil {
		t.Fatal(err)
	}
	rec.Close() // no new writes at all

	// Reopen cold: 'a' must be there, 'b' must be a persisted tombstone.
	reopened, err := Open(recDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if d, ok := reopened.Get("c", "a"); !ok || d.Fields["v"] != "keep" {
		t.Fatalf("recovered 'a' lost across restart: %v", d)
	}
	if d, ok := reopened.Get("c", "b"); ok && !d.Deleted {
		t.Fatal("'b' should be a tombstone after recovery")
	}
}

// TestPITRRecoveredStoreIsLive: the recovered store can take new
// writes and persists them (it's a real store, not a read-only view).
func TestPITRRecoveredStoreIsLive(t *testing.T) {
	var raw bytes.Buffer
	src, _ := Open(t.TempDir())
	src.Apply("c", "k", map[string]interface{}{"v": 1})
	if err := src.ArchiveRaw(&raw); err != nil {
		t.Fatal(err)
	}
	src.Close()

	recDir := t.TempDir()
	rec, _, err := ReplayUntil(bytes.NewReader(raw.Bytes()), time.Now().UnixMilli()+1000, recDir)
	if err != nil {
		t.Fatal(err)
	}
	rec.Apply("c", "k", map[string]interface{}{"v": 2})
	rec.Close()

	// Reopen from disk: the post-recovery write persisted.
	reopened, err := Open(recDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	d, ok := reopened.Get("c", "k")
	if !ok || d.Fields["v"] != float64(2) {
		t.Fatalf("recovered store not live: %v", d)
	}
}

// TestPITRArchiveTimestamps: archives land in the archive dir with
// sortable timestamped names (mirrors the server loop).
func TestArchiveNaming(t *testing.T) {
	archiveDir := t.TempDir()
	src, _ := Open(t.TempDir())
	src.Apply("c", "k", map[string]interface{}{"v": 1})
	name := time.Now().UTC().Format("20060102T150405.000") + ".jsonl"
	f, err := os.Create(filepath.Join(archiveDir, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := src.ArchiveRaw(f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	src.Close()
	entries, _ := os.ReadDir(archiveDir)
	if len(entries) != 1 || entries[0].Name() != name {
		t.Fatalf("archive naming wrong: %v", entries)
	}
}

func TestDurableFeedSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EnableDurableFeed(); err != nil {
		t.Fatal(err)
	}
	st.Apply("c", "a", map[string]interface{}{"v": 1})
	st.Apply("c", "b", map[string]interface{}{"v": 2})
	st.Delete("c", "a")
	head := st.ChangeLog().Head()
	if head != 3 {
		t.Fatalf("head=%d want 3", head)
	}
	st.Close()

	// Reopen: docs replay from data.jsonl, feed events from feed.jsonl.
	st2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if err := st2.EnableDurableFeed(); err != nil {
		t.Fatal(err)
	}
	evs := st2.ChangeLog().Since(0)
	if len(evs) != 3 {
		t.Fatalf("feed events lost across restart: %v", evs)
	}
	if evs[0].ID != "a" || evs[0].Kind != "upsert" || evs[2].Kind != "delete" {
		t.Fatalf("feed order/kind wrong: %v", evs)
	}
	// New writes continue the sequence.
	st2.Apply("c", "c", map[string]interface{}{"v": 3})
	if st2.ChangeLog().Head() != 4 {
		t.Fatalf("post-restart seq: %d want 4", st2.ChangeLog().Head())
	}
}
