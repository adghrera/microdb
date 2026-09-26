package changelog

import (
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
