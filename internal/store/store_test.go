package store

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func countLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			n++
		}
	}
	return n
}

func TestCompactKeepsLatestVersionOnly(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// 3 versions of one doc + 1 other doc => 4 log lines.
	s.Apply("c", "a", map[string]interface{}{"v": 1})
	s.Apply("c", "a", map[string]interface{}{"v": 2})
	s.Apply("c", "a", map[string]interface{}{"v": 3})
	s.Apply("c", "b", map[string]interface{}{"v": 1})
	logPath := filepath.Join(dir, "data.jsonl")
	if got := countLines(t, logPath); got != 4 {
		t.Fatalf("expected 4 lines pre-compact, got %d", got)
	}

	dropped, err := s.Compact(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 0 {
		t.Fatalf("no tombstones yet, dropped=%d", dropped)
	}
	if got := countLines(t, logPath); got != 2 {
		t.Fatalf("expected 2 lines post-compact, got %d", got)
	}
	// Latest version survived. (Apply stores Go ints directly; only
	// JSON-decoded docs carry float64 — compare via Ver and string form.)
	d, ok := s.Get("c", "a")
	if !ok || d.Ver != 3 || fmt.Sprint(d.Fields["v"]) != "3" {
		t.Fatalf("latest version lost: %+v", d)
	}
	// Reopen replays compacted log correctly.
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if d, ok := s2.Get("c", "a"); !ok || d.Ver != 3 {
		t.Fatalf("replay after compact lost state: %+v", d)
	}
}

func TestCompactTombstoneGC(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Apply("c", "gone", map[string]interface{}{"v": 1})
	s.Delete("c", "gone") // tombstone with current TS
	s.Apply("c", "kept", map[string]interface{}{"v": 1})

	// GC window of 0 => the tombstone (TS <= now) is droppable.
	dropped, err := s.Compact(0)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 1 {
		t.Fatalf("expected 1 tombstone dropped, got %d", dropped)
	}
	if _, ok := s.Get("c", "gone"); ok {
		t.Fatal("gone should be purged")
	}
	if _, ok := s.Get("c", "kept"); !ok {
		t.Fatal("kept should survive")
	}
	// Log now has exactly the one live doc.
	if got := countLines(t, filepath.Join(dir, "data.jsonl")); got != 1 {
		t.Fatalf("expected 1 line after GC, got %d", got)
	}
}

func TestScanIndexedFastPathMatchesFullScan(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.Apply("u", "a", map[string]interface{}{"city": "Lisbon"})
	s.Apply("u", "b", map[string]interface{}{"city": "Berlin"})
	s.Apply("u", "c", map[string]interface{}{"city": "Lisbon"})
	s.Delete("u", "c") // tombstoned — must not appear via index

	fast := s.ScanIndexed("u", map[string]interface{}{"city": "Lisbon"})
	if len(fast) != 1 || fast[0].ID != "a" {
		t.Fatalf("index fast path wrong: %v", fast)
	}
	// Comparison filter falls back to scan and still works.
	fb := s.ScanIndexed("u", map[string]interface{}{"city": map[string]interface{}{"$gt": "A"}})
	if len(fb) != 2 {
		t.Fatalf("fallback scan wrong: %v", fb)
	}
	// Index survives replay: reopen and query again.
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	fast2 := s2.ScanIndexed("u", map[string]interface{}{"city": "Lisbon"})
	if len(fast2) != 1 || fast2[0].ID != "a" {
		t.Fatalf("index not rebuilt on replay: %v", fast2)
	}
}

func TestFsyncModePersistsWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.SetFsync(true)
	if _, err := s.Apply("c", "dur", map[string]interface{}{"v": 1}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// After close+reopen the fsynced write must be present.
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, ok := s2.Get("c", "dur"); !ok {
		t.Fatal("fsynced write lost across reopen")
	}
}

func TestCompactKeepsFreshTombstones(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Apply("c", "x", map[string]interface{}{"v": 1})
	s.Delete("c", "x") // tombstone TS = now

	// 1h window: tombstone is fresh, must be kept so lagging replicas
	// don't resurrect it.
	dropped, err := s.Compact(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 0 {
		t.Fatalf("fresh tombstone must not be GC'd, dropped=%d", dropped)
	}
	// Tombstone still in log (deleted docs are written as deleted entries).
	lines := countLines(t, filepath.Join(dir, "data.jsonl"))
	if lines != 1 {
		t.Fatalf("expected 1 tombstone line kept, got %d", lines)
	}
}
