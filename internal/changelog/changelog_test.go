package changelog

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestAppendAndSince(t *testing.T) {
	l := New(100, time.Minute)
	e1 := l.Append("c1", "a", "upsert", nil)
	e2 := l.Append("c1", "b", "delete", nil)
	if e1.Seq != 1 || e2.Seq != 2 {
		t.Fatalf("seq numbering wrong: %d %d", e1.Seq, e2.Seq)
	}
	got := l.Since(1)
	if len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("Since(1) wrong: %v", got)
	}
	if l.Head() != 2 {
		t.Fatalf("head should be 2, got %d", l.Head())
	}
}

func TestSizeTrim(t *testing.T) {
	l := New(3, time.Hour)
	for i := 0; i < 10; i++ {
		l.Append("c", "x", "upsert", nil)
	}
	all := l.Since(0)
	if len(all) != 3 {
		t.Fatalf("expected 3 retained, got %d", len(all))
	}
	if all[0].Seq != 8 {
		t.Fatalf("oldest retained should be seq 8, got %d", all[0].Seq)
	}
}

// TestAppendPastCapIsCheap pins the allocation cost of trimming: once
// the feed is at its size cap, every append used to rebuild the whole
// retained window (~800KB for a 10k cap), which made every write pay
// for the feed's history. Re-slicing keeps it near-zero.
func TestAppendPastCapIsCheap(t *testing.T) {
	const feedCap = 10000
	l := New(feedCap, time.Hour)
	for i := 0; i < feedCap*2; i++ {
		l.Append("c", "x", "upsert", nil)
	}
	if len(l.Since(0)) != feedCap {
		t.Fatalf("window should be capped at %d", feedCap)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const n = 200
	for i := 0; i < n; i++ {
		l.Append("c", "x", "upsert", nil)
	}
	runtime.ReadMemStats(&after)
	got := after.TotalAlloc - before.TotalAlloc
	// Budget: 200 appends must not rebuild a 10k window. The old
	// behaviour allocated ~80MB here; anything near that is a regression.
	if got > 4<<20 {
		t.Fatalf("%d appends past the cap allocated %d bytes (window copy is back?)", n, got)
	}
	if len(l.Since(0)) != feedCap {
		t.Fatalf("window should still be capped at %d, got %d", feedCap, len(l.Since(0)))
	}
}

func TestAgeTrim(t *testing.T) {
	l := New(1000, 50*time.Millisecond)
	l.Append("c", "old", "upsert", nil)
	time.Sleep(80 * time.Millisecond)
	l.Append("c", "new", "upsert", nil)
	all := l.Since(0)
	if len(all) != 1 || all[0].ID != "new" {
		t.Fatalf("aged event should be trimmed: %v", all)
	}
}

func TestWaitReleasesOnAppend(t *testing.T) {
	l := New(100, time.Minute)
	done := make(chan []Event, 1)
	go func() {
		done <- l.WaitFiltered("c", 0, 5*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	l.Append("c", "x", "upsert", nil)
	select {
	case evs := <-done:
		if len(evs) != 1 || evs[0].ID != "x" {
			t.Fatalf("wait returned wrong events: %v", evs)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wait did not release on append")
	}
}

func TestWaitFilteredIgnoresOtherCollections(t *testing.T) {
	l := New(100, time.Minute)
	go func() {
		time.Sleep(50 * time.Millisecond)
		l.Append("other", "x", "upsert", nil)
	}()
	evs := l.WaitFiltered("mine", 0, 300*time.Millisecond)
	if len(evs) != 0 {
		t.Fatalf("other-collection events must not release this wait: %v", evs)
	}
}

func TestDurableReopenRestoresWindow(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/feed.jsonl"
	l1, err := Open(path, 1000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !l1.Durable() {
		t.Fatal("Open feed must report durable")
	}
	for i := 0; i < 5; i++ {
		l1.Append("c", string(rune('a'+i)), "upsert", nil)
	}
	l1.Close()

	l2, err := Open(path, 1000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if l2.Head() != 5 {
		t.Fatalf("seq not restored: %d want 5", l2.Head())
	}
	evs := l2.Since(0)
	if len(evs) != 5 || evs[0].ID != "a" || evs[4].ID != "e" {
		t.Fatalf("window not restored: %v", evs)
	}
	// Appending after reopen continues the sequence.
	ev := l2.Append("c", "f", "upsert", nil)
	if ev.Seq != 6 {
		t.Fatalf("post-reopen seq: %d want 6", ev.Seq)
	}
}

func TestDurableTornTailTolerated(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/feed.jsonl"
	l1, _ := Open(path, 1000, time.Hour)
	l1.Append("c", "a", "upsert", nil)
	l1.Close()
	// Simulate a crash mid-append: garbage at EOF.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"seq":2,"ts":999,"collection":"c","id":"b","kin`)
	f.Close()

	l2, err := Open(path, 1000, time.Hour)
	if err != nil {
		t.Fatalf("torn tail must be tolerated, got: %v", err)
	}
	defer l2.Close()
	if l2.Head() != 1 {
		t.Fatalf("head after torn-tail recovery: %d want 1", l2.Head())
	}
	if evs := l2.Since(0); len(evs) != 1 {
		t.Fatalf("want 1 event, got %v", evs)
	}
}

func TestDurableRetentionTrimOnOpen(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/feed.jsonl"
	// Write one fresh and one ancient event by hand.
	old := time.Now().Add(-2 * time.Hour).UnixMilli()
	fresh := time.Now().UnixMilli()
	os.WriteFile(path, []byte(
		`{"seq":1,"ts":`+strconv.FormatInt(old, 10)+`,"collection":"c","id":"old","kind":"upsert"}`+"\n"+
			`{"seq":2,"ts":`+strconv.FormatInt(fresh, 10)+`,"collection":"c","id":"new","kind":"upsert"}`+"\n"), 0o644)
	l, err := Open(path, 1000, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	evs := l.Since(0)
	if len(evs) != 1 || evs[0].ID != "new" {
		t.Fatalf("expired event should be dropped on open: %v", evs)
	}
	if l.Head() != 2 {
		t.Fatalf("head must track max seq even for expired events: %d", l.Head())
	}
}
