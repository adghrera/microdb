package store

import (
	"bufio"
	"bytes"
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

func TestMatchesOperators(t *testing.T) {
	d := &Doc{Fields: map[string]interface{}{
		"age":   float64(30),
		"name":  "alice",
		"tags":  []interface{}{"a"},
		"score": float64(9.5),
	}}
	cases := []struct {
		filter map[string]interface{}
		want   bool
	}{
		{map[string]interface{}{"age": map[string]interface{}{"$gte": float64(30)}}, true},
		{map[string]interface{}{"age": map[string]interface{}{"$gte": float64(31)}}, false},
		{map[string]interface{}{"age": map[string]interface{}{"$lte": float64(30)}}, true},
		{map[string]interface{}{"age": map[string]interface{}{"$lte": float64(29)}}, false},
		{map[string]interface{}{"name": map[string]interface{}{"$ne": "bob"}}, true},
		{map[string]interface{}{"name": map[string]interface{}{"$ne": "alice"}}, false},
		{map[string]interface{}{"name": map[string]interface{}{"$in": []interface{}{"x", "alice"}}}, true},
		{map[string]interface{}{"name": map[string]interface{}{"$in": []interface{}{"x", "y"}}}, false},
		{map[string]interface{}{"score": map[string]interface{}{"$in": []interface{}{float64(9.5)}}}, true},
		{map[string]interface{}{"name": map[string]interface{}{"$exists": true}}, true},
		{map[string]interface{}{"missing": map[string]interface{}{"$exists": false}}, true},
		{map[string]interface{}{"missing": map[string]interface{}{"$exists": true}}, false},
		{map[string]interface{}{"name": map[string]interface{}{"$regex": "^a[lc]"}}, true},
		{map[string]interface{}{"name": map[string]interface{}{"$regex": "^b"}}, false},
		{map[string]interface{}{"age": map[string]interface{}{"$regex": "x"}}, false},  // non-string
		{map[string]interface{}{"name": map[string]interface{}{"$regex": "["}}, false}, // bad pattern
		// compound: both conditions must hold
		{map[string]interface{}{"age": map[string]interface{}{"$gte": float64(20), "$lte": float64(30)}}, true},
		{map[string]interface{}{"age": map[string]interface{}{"$gte": float64(20), "$lte": float64(29)}}, false},
		{map[string]interface{}{"unknown_op": map[string]interface{}{"$bogus": 1}}, false},
	}
	for i, c := range cases {
		if got := Matches(d, c.filter); got != c.want {
			t.Errorf("case %d: filter %v got %v want %v", i, c.filter, got, c.want)
		}
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

func TestApplyBatchAtomicAndReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := s.ApplyBatch("b", map[string]map[string]interface{}{
		"x": {"v": 1},
		"y": {"v": 2},
		"z": {"v": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 3 {
		t.Fatalf("expected 3 docs, got %d", len(docs))
	}
	// One log line for the whole batch.
	if got := countLines(t, filepath.Join(dir, "data.jsonl")); got != 1 {
		t.Fatalf("batch should be 1 log line, got %d", got)
	}
	// Index sees all three.
	if ids := s.ScanIndexed("b", map[string]interface{}{"v": 2}); len(ids) != 1 || ids[0].ID != "y" {
		t.Fatalf("batch index miss: %v", ids)
	}
	// Replay restores all three.
	s.Close()
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	for _, id := range []string{"x", "y", "z"} {
		if _, ok := s2.Get("b", id); !ok {
			t.Fatalf("batch doc %s lost on replay", id)
		}
	}
	// Batch update bumps versions.
	docs2, _ := s2.ApplyBatch("b", map[string]map[string]interface{}{"x": {"v": 10}})
	if docs2[0].Ver != 2 {
		t.Fatalf("batch update should bump ver to 2, got %d", docs2[0].Ver)
	}
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	src := t.TempDir()
	s, err := Open(src)
	if err != nil {
		t.Fatal(err)
	}
	s.Apply("a", "1", map[string]interface{}{"v": "one"})
	s.Apply("a", "2", map[string]interface{}{"v": "two"})
	s.Apply("b", "1", map[string]interface{}{"v": "bee"})
	s.Apply("a", "2", map[string]interface{}{"v": "TWO"}) // v2 wins
	s.Delete("b", "1")                                    // tombstone in backup

	var buf bytes.Buffer
	if err := s.Backup(&buf); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Restore into a fresh store.
	dst := t.TempDir()
	s2, err := Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	n, err := s2.Restore(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("expected 3 records applied (2 live + 1 tombstone), got %d", n)
	}
	if d, ok := s2.Get("a", "2"); !ok || d.Fields["v"] != "TWO" {
		t.Fatalf("latest version not restored: %+v", d)
	}
	if _, ok := s2.Get("b", "1"); ok {
		t.Fatal("deleted doc should not be readable after restore")
	}
	// Index works post-restore.
	if ids := s2.ScanIndexed("a", map[string]interface{}{"v": "one"}); len(ids) != 1 {
		t.Fatalf("index broken after restore: %v", ids)
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
