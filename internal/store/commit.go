package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// logReq is one writer's claim on the log: the encoded record it
// submitted and the channel it waits on for durability.
type logReq struct {
	line  []byte
	ready chan struct{}
	err   error
}

// errLogWriterStopped is returned to a writer that submits after the
// store has been closed.
var errLogWriterStopped = errors.New("store: log writer stopped")

// logWriter owns every append to the commit log: it batches records
// that arrive together into ONE write and ONE fsync, then releases the
// waiters.
//
// Why a dedicated goroutine rather than "the writer that happens to
// start the fsync runs it":
//
//   - Writers never touch the file. They hand their record to a queue
//     and block. So while a pass is flushing, other writers keep
//     appending to the queue instead of queueing behind the disk —
//     which is exactly what makes the batch grow. (On Windows the
//     kernel serialises WriteFile and FlushFileBuffers on one handle,
//     so a writer that writes directly during a flush stalls for the
//     whole flush; queueing sidesteps that entirely.)
//   - One Write syscall per batch instead of one per record: the log
//     becomes sequential, and the fsync cost — the expensive part — is
//     paid once per batch instead of once per write.
//   - Ordering is safe: replay is LWW by (ver, ts), so the file order
//     of two concurrent writes does not change the recovered state.
//
// Durability contract: submit returns only after a pass that started
// after the record was queued has completed, so an acknowledged write
// is on stable storage.
type logWriter struct {
	reqs  chan *logReq
	stop  chan struct{}
	done  chan struct{}
	flush func(lines [][]byte) error
	// window holds the first queued record open briefly so more can
	// join before the pass runs (0 = flush as soon as the queue has
	// something). Operator knob: --commit-window.
	window atomic.Int64 // time.Duration as int64

	// Observability for /metrics: passes and records covered.
	passes  atomic.Int64
	records atomic.Int64
	failed  atomic.Int64

	startOnce sync.Once
	stopOnce  sync.Once
}

func newLogWriter(flush func([][]byte) error, queue int) *logWriter {
	if queue <= 0 {
		queue = 1024
	}
	return &logWriter{
		reqs:  make(chan *logReq, queue),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
		flush: flush,
	}
}

func (w *logWriter) start() {
	w.startOnce.Do(func() { go w.loop() })
}

// setWindow changes the commit window (0 = flush immediately).
func (w *logWriter) setWindow(d time.Duration) { w.window.Store(int64(d)) }

// submit queues a record and blocks until it is durable.
func (w *logWriter) submit(line []byte) error {
	req := &logReq{line: line, ready: make(chan struct{})}
	select {
	case w.reqs <- req:
	case <-w.done:
		return errLogWriterStopped
	}
	select {
	case <-req.ready:
		return req.err
	case <-w.done:
		// The writer died mid-batch (Close racing a submit): never
		// hang a client on a shutdown that cannot acknowledge it.
		return errLogWriterStopped
	}
}

func (w *logWriter) loop() {
	defer close(w.done)
	batch := make([]*logReq, 0, 64)
	for {
		batch = batch[:0]
		// Block for the first record.
		select {
		case req := <-w.reqs:
			batch = append(batch, req)
		case <-w.stop:
			w.drain(&batch)
			return
		}
		// Optional commit window: give the rest of the cohort time to
		// arrive before paying for the flush.
		if win := time.Duration(w.window.Load()); win > 0 {
			time.Sleep(win)
		}
		// Take everything already queued without blocking again.
	watch:
		for len(batch) < cap(batch) {
			select {
			case req := <-w.reqs:
				batch = append(batch, req)
			default:
				break watch
			}
		}
		w.runPass(batch)
	}
}

// drain flushes whatever is still queued during shutdown so no waiter
// is left blocked forever.
func (w *logWriter) drain(batch *[]*logReq) {
	for {
		select {
		case req := <-w.reqs:
			*batch = append(*batch, req)
			continue
		default:
		}
		break
	}
	if len(*batch) > 0 {
		w.runPass(*batch)
	}
}

func (w *logWriter) runPass(batch []*logReq) {
	lines := make([][]byte, len(batch))
	for i, r := range batch {
		lines[i] = r.line
	}
	err := w.flush(lines)
	w.passes.Add(1)
	w.records.Add(int64(len(batch)))
	if err != nil {
		w.failed.Add(1)
	}
	for _, r := range batch {
		r.err = err
		close(r.ready)
	}
}

// Stats returns (passes, records covered, failed passes).
func (w *logWriter) Stats() (int64, int64, int64) {
	return w.passes.Load(), w.records.Load(), w.failed.Load()
}

// close stops the writer after draining queued records.
func (w *logWriter) close() {
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
}
