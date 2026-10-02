package store

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countingFlush is a slow, counting stand-in for Write+fsync.
type countingFlush struct {
	mu    sync.Mutex
	calls int
	err   error // when set, flushes fail
	maxIn atomic.Int64
	in    atomic.Int64
	lines [][]byte
}

func (c *countingFlush) flush(lines [][]byte) error {
	cur := c.in.Add(1)
	for {
		m := c.maxIn.Load()
		if cur <= m || c.maxIn.CompareAndSwap(m, cur) {
			break
		}
	}
	time.Sleep(20 * time.Millisecond) // the expensive disk part
	c.in.Add(-1)

	c.mu.Lock()
	c.calls++
	c.lines = append(c.lines, lines...)
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *countingFlush) stats() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, len(c.lines)
}

// TestLogWriterBatches is the core claim: N writers that arrive
// together cost far fewer log passes than N, because one pass covers
// every record queued before it started.
func TestLogWriterBatches(t *testing.T) {
	f := &countingFlush{}
	w := newLogWriter(f.flush, 1024)
	w.start()
	defer w.close()

	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.submit([]byte(`{"id":"k` + itoa(i) + `"}` + "\n")); err != nil {
				t.Errorf("submit: %v", err)
			}
		}()
	}
	wg.Wait()

	passes, records, failed := w.Stats()
	calls, lines := f.stats()
	if records != n || lines != n {
		t.Fatalf("records=%d lines=%d, want %d (every record exactly once)", records, lines, n)
	}
	if failed != 0 {
		t.Errorf("failed passes = %d, want 0", failed)
	}
	if int64(calls) != passes {
		t.Errorf("stats say %d passes, flush ran %d times", passes, calls)
	}
	if passes >= n/2 {
		t.Errorf("no batching: %d log passes for %d writes", passes, n)
	}
	t.Logf("group commit: %d writes -> %d log passes (%.1fx batching)",
		records, passes, float64(records)/float64(passes))
}

// TestLogWriterNeverOverlapsPasses: exactly one pass at a time, so a
// record can never be acknowledged by a pass that started before it
// was queued.
func TestLogWriterNeverOverlapsPasses(t *testing.T) {
	f := &countingFlush{}
	w := newLogWriter(f.flush, 1024)
	w.start()
	defer w.close()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.submit([]byte(itoa(i) + "\n")); err != nil {
				t.Errorf("submit: %v", err)
			}
		}()
	}
	wg.Wait()
	if m := f.maxIn.Load(); m != 1 {
		t.Fatalf("saw %d concurrent log passes, want exactly 1", m)
	}
}

func TestLogWriterPropagatesErrors(t *testing.T) {
	f := &countingFlush{}
	w := newLogWriter(f.flush, 1024)
	w.start()
	defer w.close()

	boom := errors.New("disk full")
	f.mu.Lock()
	f.err = boom
	f.mu.Unlock()

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.submit([]byte("x\n"))
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, boom) {
			t.Errorf("waiter %d got %v, want the pass error", i, err)
		}
	}
	if _, _, failed := w.Stats(); failed == 0 {
		t.Error("failed pass was not counted")
	}

	// A failed pass must not wedge the writer: the next one runs.
	f.mu.Lock()
	f.err = nil
	f.mu.Unlock()
	if err := w.submit([]byte("y\n")); err != nil {
		t.Fatalf("writer wedged after an error: %v", err)
	}
}

func TestLogWriterCloseReleasesWaiters(t *testing.T) {
	f := &countingFlush{}
	w := newLogWriter(f.flush, 1024)
	w.start()

	// Submissions racing with shutdown must neither hang nor be
	// silently acknowledged.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = w.submit([]byte("z\n")) // error is acceptable; a hang is not
			}
		}()
	}
	time.Sleep(30 * time.Millisecond)
	w.close()
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("close left a submitter hanging")
	case <-waitDone(w):
	}
	close(stop)
	wg.Wait()

	if err := w.submit([]byte("after-close\n")); !errors.Is(err, errLogWriterStopped) {
		t.Errorf("submit after close = %v, want errLogWriterStopped", err)
	}
}

func waitDone(w *logWriter) <-chan struct{} { return w.done }

func TestLogWriterCommitWindowGathersBatch(t *testing.T) {
	f := &countingFlush{}
	w := newLogWriter(f.flush, 1024)
	w.start()
	defer w.close()
	w.setWindow(30 * time.Millisecond)

	start := time.Now()
	if err := w.submit([]byte("x\n")); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("commit window ignored: returned after %v, want >= 30ms", elapsed)
	}

	w.setWindow(0)
	start = time.Now()
	if err := w.submit([]byte("x\n")); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 25*time.Millisecond {
		t.Errorf("window=0 should flush immediately, took %v", elapsed)
	}
}

// TestLogWriterPreservesBytes: whatever the batching, every submitted
// record must reach the log intact and exactly once — replay depends
// on it.
func TestLogWriterPreservesBytes(t *testing.T) {
	f := &countingFlush{}
	w := newLogWriter(f.flush, 1024)
	w.start()
	defer w.close()

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.submit([]byte(itoa(i) + "\n")); err != nil {
				t.Errorf("submit: %v", err)
			}
		}()
	}
	wg.Wait()

	_, lines := f.stats()
	if lines != n {
		t.Fatalf("log holds %d records, want %d", lines, n)
	}
	got := map[string]bool{}
	f.mu.Lock()
	for _, line := range f.lines {
		got[string(bytes.TrimSuffix(line, []byte("\n")))] = true
	}
	f.mu.Unlock()
	if len(got) != n {
		t.Fatalf("log holds %d distinct records, want %d", len(got), n)
	}
	if _, records, _ := w.Stats(); records != n {
		t.Errorf("records covered = %d, want %d", records, n)
	}
}

// TestLogWriterStoreUnderConcurrency exercises the real path: fsync
// enabled, many writers, and every acknowledged write durable on
// reopen.
func TestLogWriterStoreUnderConcurrency(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.SetFsync(true)

	const (
		writers = 16
		per     = 10
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := "k-" + string(rune('a'+w)) + "-" + itoa(i)
				if _, err := st.Apply("gc", id, map[string]interface{}{"w": w, "i": i}); err != nil {
					t.Errorf("apply: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	passes, records, failed := st.CommitStats()
	if failed != 0 {
		t.Errorf("failed log passes = %d, want 0", failed)
	}
	if records != writers*per {
		t.Errorf("records covered = %d, want %d", records, writers*per)
	}
	if passes >= records {
		t.Errorf("no batching on the real path: %d log passes for %d writes", passes, records)
	}
	t.Logf("store: %d writes -> %d log passes (%.1fx batching)",
		records, passes, float64(records)/float64(passes))

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.DocCount(); got != writers*per {
		t.Fatalf("after reopen doc count = %d, want %d", got, writers*per)
	}
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
