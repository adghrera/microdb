// Package store is a schema-less document store: documents are plain JSON
// maps, persisted with an append-only JSONL log, merged last-writer-wins
// by (version, timestamp).
package store

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"hash/maphash"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"microdb/internal/changelog"
	"microdb/internal/index"
	"microdb/internal/merkle"
	"microdb/internal/metrics"
	"microdb/internal/migrate"
	"microdb/internal/sharedlog"
	"microdb/internal/storage"
	"microdb/internal/tier"
)

type Doc struct {
	ID         string                 `json:"id"`
	Ver        int64                  `json:"ver"`
	TS         int64                  `json:"ts"` // unix millis
	Deleted    bool                   `json:"deleted,omitempty"`
	Collection string                 `json:"collection"`
	Fields     map[string]interface{} `json:"fields"`
}

type Store struct {
	mu sync.RWMutex
	// shards holds every document (including tombstones), keyed by
	// collection+keySep+id. Striping the map is what keeps the point
	// read path off the store mutex: a reader locks ONE shard (a
	// cache line of its own) instead of contending with every write
	// and with other readers, and compaction no longer stalls reads
	// for its whole run. Writers still serialise on mu, which is the
	// barrier compaction needs.
	shards [numShards]docShard
	nDocs  atomic.Int64 // keys in shards, tombstones included
	// tier is the cold half of storage: when attached, every
	// compaction archives the raw log there BEFORE the rewrite throws
	// history away, so local disk tracks the live dataset while PITR
	// history lives in object storage.
	tier   *tier.Archiver
	tiered atomic.Int64 // segments archived since open
	// compress deflates log records before encryption (opt-in: CPU on
	// every write in exchange for fewer bytes on disk and less to
	// replay at startup).
	compress atomic.Bool
	path   string       // absolute path of the JSONL commit log
	f      *os.File
	key    []byte      // encryption-at-rest key (nil = plaintext log)
	fsync  atomic.Bool // sync to disk on every write (durability over throughput)
	// lw owns every append to the commit log: it batches records that
	// arrive together into one write and one fsync (group commit), then
	// releases the waiters (see commit.go).
	lw *logWriter
	// wmu is the single-writer slot for the log file. Inline appends
	// (fsync off) and log-writer batches (fsync on) both take it, so
	// exactly one writer advances the file position at a time even if
	// fsync is toggled at runtime. Lock order: wmu before mu.
	wmu    sync.Mutex
	idx    *index.IndexSet // inverted field indexes (fast exact-match lookups)
	log    *changelog.Log  // bounded mutation feed for watchers
	writes atomic.Int64    // client writes applied (load signal for placement)
	// Per-collection live stats for isolation: ops counters and an
	// approximate live-doc count per collection. A hot collection
	// must be visible (metrics) and containable (quota) without
	// touching other collections.
	colStats map[string]*ColStats
	// Durable shared commit log (C3): when attached, every client
	// mutation is appended to the shared log BEFORE it touches local
	// state. A write the log cannot persist quorum-durable fails and
	// is not applied locally — durability comes from the log's own
	// replication, not this node's disk.
	shared      *sharedlog.Client
	sharedState string // path holding the last durable LSN checkpoint
	sharedLSN   int64
}

// ColStats tracks live per-collection activity.
type ColStats struct {
	Writes  atomic.Int64
	Deletes atomic.Int64
	Reads   atomic.Int64
	Queries atomic.Int64
	Live    atomic.Int64 // live (non-deleted) docs
}

// StatsFor returns (creating if needed) the stats bucket for a
// collection.
func (s *Store) StatsFor(col string) *ColStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.colStats == nil {
		s.colStats = map[string]*ColStats{}
	}
	st, ok := s.colStats[col]
	if !ok {
		st = &ColStats{}
		s.colStats[col] = st
	}
	return st
}

// ColSnapshot is a plain-value snapshot of ColStats (map-safe).
type ColSnapshot struct {
	Writes  int64 `json:"writes"`
	Deletes int64 `json:"deletes"`
	Reads   int64 `json:"reads"`
	Queries int64 `json:"queries"`
	Live    int64 `json:"live_docs"`
}

// AllColStats snapshots per-collection stats.
func (s *Store) AllColStats() map[string]ColSnapshot {
	s.mu.RLock()
	cols := make([]string, 0, len(s.colStats))
	for c := range s.colStats {
		cols = append(cols, c)
	}
	s.mu.RUnlock()
	out := map[string]ColSnapshot{}
	for _, c := range cols {
		st := s.StatsFor(c)
		out[c] = ColSnapshot{
			Writes:  st.Writes.Load(),
			Deletes: st.Deletes.Load(),
			Reads:   st.Reads.Load(),
			Queries: st.Queries.Load(),
			Live:    st.Live.Load(),
		}
	}
	return out
}

// WriteCount returns the number of client writes applied since start.
// The cluster samples this each gossip round to derive writes/sec.
func (s *Store) WriteCount() int64 { return s.writes.Load() }

// ChangeLog exposes the mutation feed (nil-safe: watchers just see no events).
func (s *Store) ChangeLog() *changelog.Log { return s.log }

// EnableDurableFeed switches the mutation feed from the in-memory
// window to a durable JSONL-backed feed at <dir>/feed.jsonl. Must be
// called right after Open, before any writes (events already recorded
// in this process would not be on disk). Once enabled, watcher
// cursors survive node restarts within the retention window.
func (s *Store) EnableDurableFeed() error {
	d := filepath.Dir(s.path)
	dl, err := changelog.Open(filepath.Join(d, "feed.jsonl"), 10000, 24*time.Hour)
	if err != nil {
		return err
	}
	s.log = dl
	return nil
}

// Indexes exposes the index set (enabled by default; pass false to OpenOpts to disable).
func (s *Store) Indexes() *index.IndexSet { return s.idx }

// AttachSharedLog makes every client mutation write-through to a
// durable shared commit log before touching local state. A write
// the log cannot persist quorum-durable returns an error and is NOT
// applied locally. The local JSONL log stays as the node's own
// materialized view; recovery of lost local state uses
// RecoverSharedLog. Call before serving writes.
func (s *Store) AttachSharedLog(c *sharedlog.Client) {
	s.shared = c
	s.sharedState = filepath.Join(filepath.Dir(s.path), "sharedlog.state")
	if b, err := os.ReadFile(s.sharedState); err == nil {
		n, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		s.sharedLSN = n
	}
}

// SharedAttached reports whether a shared commit log is wired in.
func (s *Store) SharedAttached() bool { return s.shared != nil }

// SharedLSN returns the last LSN this store durably appended.
func (s *Store) SharedLSN() int64 { return s.sharedLSN }

// sharedAppend appends one plaintext record (single-doc or batch
// framing, same bytes as the local log line) to the shared log and
// checkpoints the LSN. No-op when no shared log is attached.
// Caller holds the write lock (checkpoint ordering).
func (s *Store) sharedAppend(plain []byte) error {
	if s.shared == nil {
		return nil
	}
	lsn, err := s.shared.Append(plain)
	if err != nil {
		atomic.AddInt64(metrics.Default.Counter("microdb_sharedlog_failures_total", "Shared-log appends that failed (quorum not reached)"), 1)
		return fmt.Errorf("shared log append: %w", err)
	}
	s.sharedLSN = lsn
	_ = os.WriteFile(s.sharedState, []byte(strconv.FormatInt(lsn, 10)), 0o644)
	atomic.AddInt64(metrics.Default.Counter("microdb_sharedlog_appends_total", "Shared-log appends committed"), 1)
	return nil
}

// RecoverSharedLog applies shared-log entries newer than this
// store's checkpoint through the normal merge path, resuming where
// the last attach left off. Call BEFORE AttachSharedLog so replayed
// records are not re-appended to the log. Returns entries applied.
func RecoverSharedLog(s *Store, c *sharedlog.Client) (int, error) {
	statePath := filepath.Join(filepath.Dir(s.path), "sharedlog.state")
	var after int64
	if b, err := os.ReadFile(statePath); err == nil {
		after, _ = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	}
	applied := 0
	for {
		entries := c.Tail(after, 500)
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			trimmed := strings.TrimSpace(string(e.Payload))
			if trimmed != "" {
				if strings.HasPrefix(trimmed, `{"batch":`) {
					var rec struct {
						Batch []*Doc `json:"batch"`
					}
					if json.Unmarshal([]byte(trimmed), &rec) == nil {
						for _, d := range rec.Batch {
							if s.ApplyRemote(d) {
								applied++
							}
						}
					}
				} else {
					var d Doc
					if json.Unmarshal([]byte(trimmed), &d) == nil {
						if s.ApplyRemote(&d) {
							applied++
						}
					}
				}
			}
			if e.LSN > after {
				after = e.LSN
			}
		}
		if len(entries) < 500 {
			break
		}
	}
	_ = os.WriteFile(statePath, []byte(strconv.FormatInt(after, 10)), 0o644)
	return applied, nil
}

// numShards is the stripe count for the document map (power of two).
// Each shard has its own RWMutex and map, so concurrent point reads
// land on independent cache lines instead of one shared lock word.
const numShards = 64

// docShard is one stripe of the document map.
type docShard struct {
	mu   sync.RWMutex
	docs map[string]*Doc
}

// shardSeed is a per-process hash seed: it randomises stripe
// placement (so hot keys cannot be predicted onto one shard) and lets
// shardOf use the runtime's SIMD hash instead of a byte-at-a-time one.
var shardSeed = maphash.MakeSeed()

// shardOf maps a document key to its stripe.
func shardOf(k string) uint32 {
	return uint32(maphash.String(shardSeed, k)) & (numShards - 1)
}

// applyReplayRecord merges one decoded record (single doc or batch)
// back into the store during log replay, taking the same index
// shortcuts as the live write path. It works on the caller's read
// buffer, so replaying copies nothing it does not have to keep.
func (s *Store) applyReplayRecord(plain []byte) {
	if bytes.HasPrefix(plain, batchPrefix) {
		var rec struct {
			Batch []*Doc `json:"batch"`
		}
		if json.Unmarshal(plain, &rec) == nil {
			for _, d := range rec.Batch {
				if s.merge(d) {
					s.idx.For(d.Collection).Upsert(d.ID, indexable(d))
				}
			}
		}
		return
	}
	var d Doc
	if json.Unmarshal(plain, &d) == nil {
		if s.merge(&d) {
			s.idx.For(d.Collection).Upsert(d.ID, indexable(&d))
		}
	}
}

var batchPrefix = []byte(`{"batch":`)

// decodeLogLineBytes decrypts an ENC1 record. Plaintext passes through
// with no copy, so the streaming replay parses directly out of its
// read buffer.
func (s *Store) decodeLogLineBytes(line []byte) ([]byte, error) {
	out := line
	if bytes.HasPrefix(out, []byte(encPrefix)) {
		plain, err := decryptRecord(s.key, string(out))
		if err != nil {
			return nil, err
		}
		out = plain
	}
	return inflateIfCompressed(out)
}

// lookup reads one document under a single shard's read lock. It never
// touches the store mutex, so point reads keep running while writes,
// replay or compaction hold it.
func (s *Store) lookup(k string) (*Doc, bool) {
	sh := &s.shards[shardOf(k)]
	sh.mu.RLock()
	d, ok := sh.docs[k]
	sh.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return d, true
}

// put installs a document in its shard (caller holds the store lock).
func (s *Store) put(k string, d *Doc) {
	sh := &s.shards[shardOf(k)]
	sh.mu.Lock()
	sh.docs[k] = d
	sh.mu.Unlock()
}

// rangeDocs walks every document under each shard's read lock. Locking
// per stripe (rather than the whole map) keeps writers moving; the
// caller normally already holds the store lock, which is what makes
// the walk a consistent snapshot.
func (s *Store) rangeDocs(fn func(k string, d *Doc) bool) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k, d := range sh.docs {
			if !fn(k, d) {
				sh.mu.RUnlock()
				return
			}
		}
		sh.mu.RUnlock()
	}
}

// mutateDocs walks every document with the stripe write-locked, so the
// callback may delete what it is looking at.
func (s *Store) mutateDocs(fn func(k string, d *Doc) bool) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, d := range sh.docs {
			if !fn(k, d) {
				sh.mu.Unlock()
				return
			}
		}
		sh.mu.Unlock()
	}
}

// isNew reports whether doc d would change stored state under the
// LWW merge rule. Caller holds the lock; used to decide whether a
// write is worth a durable shared-log append before applying it.
func (s *Store) isNew(d *Doc) bool {
	cur, ok := s.lookup(key(d.Collection, d.ID))
	return !ok || d.Ver > cur.Ver || (d.Ver == cur.Ver && d.TS > cur.TS)
}

// SetCompress turns on per-record log compression. It only pays for
// itself on records that are big or repetitive enough to shrink, so
// encoding falls back to the raw record whenever compression does not
// actually save bytes. Reading is unconditional: a store can always
// replay a compressed log even when compression is currently off.
func (s *Store) SetCompress(on bool) { s.compress.Store(on) }

// Compressing reports whether new records are compressed.
func (s *Store) Compressing() bool { return s.compress.Load() }

// AttachTier wires the cold storage tier: compaction archives the raw
// log to it before rewriting. A nil target disables tiering (default).
func (s *Store) AttachTier(a *tier.Archiver) { s.tier = a }

// TierArchives reports how many segments this store has pushed cold
// since it opened.
func (s *Store) TierArchives() int64 { return s.tiered.Load() }

// archiveToTier streams the current log file to the cold tier under a
// timestamped key. Called by Compact while the file is still intact.
func (s *Store) archiveToTier(path string) error {
	if s.tier == nil {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("tier archive: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("tier archive: %w", err)
	}
	seg, err := s.tier.Put(f, fi.Size())
	if err != nil {
		return err
	}
	s.tiered.Add(1)
	atomic.AddInt64(metrics.Default.Counter("microdb_tier_archives_total", "Raw log segments archived to the cold tier"), 1)
	atomic.AddInt64(metrics.Default.Counter("microdb_tier_bytes_total", "Bytes pushed to the cold tier"), seg.Size)
	return nil
}

// SetFsync enables fsync-on-write. With it enabled every Apply/Delete
// blocks until the record is durable on disk; without it the OS page
// cache decides (fast, but a power loss can lose the log tail).
// Concurrent writes share fsync passes (group commit), so the cost is
// amortised across the batch rather than paid per write.
func (s *Store) SetFsync(on bool) { s.fsync.Store(on) }

// SetCommitWindow sets how long the first queued record holds the
// batch open for company before the pass runs (0 = flush as soon as
// the queue has something). Only meaningful with fsync enabled.
func (s *Store) SetCommitWindow(d time.Duration) {
	if s.lw == nil {
		return
	}
	s.lw.setWindow(d)
}

// CommitStats reports (log passes, records covered, failed passes) so
// operators can watch the group-commit batch size on /metrics.
func (s *Store) CommitStats() (int64, int64, int64) {
	if s.lw == nil {
		return 0, 0, 0
	}
	return s.lw.Stats()
}

// sync flushes the log to stable storage if fsync is enabled.
func (s *Store) sync() error {
	if s.fsync.Load() {
		return s.f.Sync()
	}
	return nil
}

// appendDurable hands one encoded record to the log writer and blocks
// until a pass that started AFTER it was queued has completed — i.e.
// until the record is on stable storage.
//
// This runs with the store lock released, which is what lets other
// writers keep appending while a pass is in flight.
func (s *Store) appendDurable(line []byte) error {
	if s.lw == nil {
		// Bare store (PITR replay builds one directly): no writer
		// goroutine, so append inline as before.
		return s.appendInline(line)
	}
	if !s.fsync.Load() {
		// Nothing to batch: with fsync off the expensive part does not
		// exist, and one uncontended write costs less than the queue
		// round trip (measured: 8us inline vs 14us queued).
		return s.appendInline(line)
	}
	return s.lw.submit(line)
}

// appendInline writes one record straight to the log, holding the
// single-writer slot so it can never race the log writer's batch.
func (s *Store) appendInline(line []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.mu.Lock()
	f := s.f
	s.mu.Unlock()
	if f == nil {
		return errLogWriterStopped
	}
	if _, err := f.Write(line); err != nil {
		return err
	}
	if s.fsync.Load() {
		return f.Sync()
	}
	return nil
}

// appendIf is appendDurable for a possibly-nil line: no line means the
// write was a no-op and owes no durability pass.
func (s *Store) appendIf(line []byte) error {
	if line == nil {
		return nil
	}
	return s.appendDurable(line)
}

// flushLog writes one batch of encoded records and, when fsync is on,
// makes the whole batch durable before returning. It runs on the log
// writer goroutine — the only writer of the live log file outside
// compaction and replay.
func (s *Store) flushLog(lines [][]byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.mu.Lock()
	f := s.f
	s.mu.Unlock()
	if f == nil {
		return errLogWriterStopped
	}
	total := 0
	for _, l := range lines {
		total += len(l)
	}
	buf := make([]byte, 0, total)
	for _, l := range lines {
		buf = append(buf, l...)
	}
	if _, err := f.Write(buf); err != nil {
		return err
	}
	if s.fsync.Load() {
		return f.Sync()
	}
	return nil
}

func key(col, id string) string { return col + "\x00" + id }

// indexable returns the fields to keep in the inverted index for a
// document: deleted docs are removed from the index entirely rather
// than indexed under their stale fields.
func indexable(d *Doc) map[string]interface{} {
	if d == nil || d.Deleted {
		return nil
	}
	return d.Fields
}

// --- encryption at rest ------------------------------------------
//
// When a key is set, every log record is written as
//   ENC1:<base64(nonce || AES-256-GCM ciphertext)>
// with a fresh random nonce per record. Plaintext records remain
// readable, so enabling encryption on an existing db encrypts all
// NEW records going forward (a rewrite happens naturally on the next
// compaction). A key is 32 bytes, hex-encoded (64 hex chars).

const encPrefix = "ENC1:"

// SetEncryptionKey enables encryption-at-rest for all future log
// records. keyHex is 64 hex characters (32 bytes). Must be called
// right after Open, before any writes.
func (s *Store) SetEncryptionKey(keyHex string) error {
	k, err := decodeHexKey(keyHex)
	if err != nil {
		return err
	}
	s.key = k
	return nil
}

func decodeHexKey(h string) ([]byte, error) {
	if len(h) != 64 {
		return nil, fmt.Errorf("encryption key must be 64 hex chars (32 bytes), got %d", len(h))
	}
	k := make([]byte, 32)
	for i := 0; i < 32; i++ {
		var b int
		if _, err := fmt.Sscanf(h[i*2:i*2+2], "%02x", &b); err != nil {
			return nil, fmt.Errorf("invalid hex at position %d", i*2)
		}
		k[i] = byte(b)
	}
	return k, nil
}

// encryptRecord seals one log line (without newline) into ENC1 form.
func encryptRecord(key, plain []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ct := gcm.Seal(nonce, nonce, plain, nil)
	return []byte(encPrefix + base64.StdEncoding.EncodeToString(ct)), nil
}

// decryptRecord opens an ENC1 line back to plaintext JSON.
func decryptRecord(key []byte, line string) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("log record is encrypted but no key configured")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(line, encPrefix))
	if err != nil {
		return nil, fmt.Errorf("bad base64 in encrypted record: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(raw) < gcm.NonceSize() {
		return nil, fmt.Errorf("encrypted record too short")
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt failed (wrong key?): %w", err)
	}
	return plain, nil
}

// appendRecord writes one log line, encrypting if a key is set.
// Caller holds the write lock.
// appendRecord encodes one log line and writes it immediately.
// Used by compaction, which owns its own file handle; the live write
// path goes through the log writer instead.
func (s *Store) appendRecord(w io.Writer, plain []byte) error {
	line, err := s.encodeRecord(plain)
	if err != nil {
		return err
	}
	_, err = w.Write(line)
	return err
}

// encodeRecord renders one log line (including its newline): encrypt
// when a key is set, then wrap in the CRC32C envelope. It does no I/O
// — the log writer owns the file — so the expensive part of a write
// (the fsync) can be shared by every record queued while it runs.
//
// The checksum sits OUTSIDE the encryption so integrity can be checked
// without the key: a bit-rotted record is reported as corruption, not
// as a decryption failure.
func (s *Store) encodeRecord(plain []byte) ([]byte, error) {
	payload := plain
	if s.compress.Load() {
		if c := compressRecord(plain); c != nil {
			payload = c
			atomic.AddInt64(metrics.Default.Counter("microdb_compressed_records_total", "Log records written compressed"), 1)
		}
	}
	if s.key != nil {
		enc, err := encryptRecord(s.key, plain)
		if err != nil {
			return nil, err
		}
		payload = enc
	}
	out := make([]byte, 0, len(payload)+24)
	out = append(out, crcPrefix...)
	out = strconv.AppendUint(out, uint64(crc32.Checksum(payload, castagnoli)), 16)
	out = append(out, ':')
	out = append(out, payload...)
	out = append(out, '\n')
	return out, nil
}

// decodeLogLine turns one raw log line into plaintext JSON,
// transparently decrypting ENC1 records.
func (s *Store) decodeLogLine(line string) (string, error) {
	if strings.HasPrefix(line, encPrefix) {
		plain, err := decryptRecord(s.key, line)
		if err != nil {
			return "", err
		}
		line = string(plain)
	}
	out, err := inflateIfCompressed([]byte(line))
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// Open loads any existing JSONL log into memory and appends future writes.
func Open(dir string) (*Store, error) {
	return OpenWithKey(dir, "")
}

// OpenWithKey opens a store with encryption-at-rest enabled: the key
// (64 hex chars) is installed BEFORE the log replay so encrypted
// records can be read back. Empty keyHex = plaintext store.
func OpenWithKey(dir, keyHex string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "data.jsonl")
	s := &Store{path: path, idx: index.NewSet(true), log: changelog.New(10000, 5*time.Minute), colStats: map[string]*ColStats{}}
	for i := range s.shards {
		s.shards[i].docs = map[string]*Doc{}
	}
	if keyHex != "" {
		if err := s.SetEncryptionKey(keyHex); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// Replay the log by streaming it: one reusable buffer, records
	// parsed straight out of the bytes that were read. The previous
	// implementation kept a full copy of the file, then a full string
	// copy of it, before parsing a single record — 11x the file size
	// in allocations for a 2MB log (measured by
	// TestOpenReplayBoundedMemory). Startup cost now tracks the data
	// that has to stay resident, not the size of the file it came in.
	br := bufio.NewReaderSize(f, 1<<20)
	var (
		corrupt  int64
		lastGood int64 // end offset of the last COMPLETE record
		off      int64
	)
	for {
		line, rerr := br.ReadSlice('\n')
		start := off
		off += int64(len(line))
		if len(line) == 0 {
			break // clean EOF (or a read error handled below)
		}
		if rerr == bufio.ErrBufferFull {
			// A single record larger than the read window: it cannot be
			// a record this node ever wrote, and resynchronising
			// mid-record would fabricate one. Skip to the next newline.
			corrupt++
			for {
				chunk, e2 := br.ReadSlice('\n')
				off += int64(len(chunk))
				if e2 == bufio.ErrBufferFull {
					continue
				}
				if len(chunk) > 0 && chunk[len(chunk)-1] == '\n' {
					lastGood = off
				}
				break
			}
			continue
		}
		if rerr != nil && rerr != io.EOF {
			f.Close()
			return nil, fmt.Errorf("log replay: %w", rerr)
		}
		complete := line[len(line)-1] == '\n'
		rec := line
		if complete {
			rec = rec[:len(rec)-1]
			if len(rec) > 0 && rec[len(rec)-1] == '\r' {
				rec = rec[:len(rec)-1]
			}
		}
		if len(bytes.TrimSpace(rec)) == 0 {
			if complete {
				lastGood = off
			}
			if rerr == io.EOF {
				break
			}
			continue
		}
		// Integrity first: a record whose CRC does not match is not
		// data. Damage in the middle of the log is skipped (counted +
		// metered) so one bad sector cannot brick the node — anti-
		// entropy refills it from peers. An incomplete record at EOF is
		// a torn write from a crash; it is dropped and the file is
		// truncated back to the last complete record below.
		payload, intact := verifyRecordBytes(rec)
		if intact {
			// Transparently decrypt ENC1 records (error = wrong/missing
			// key: fail loudly rather than silently dropping data).
			plain, err := s.decodeLogLineBytes(payload)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("log replay: %w", err)
			}
			s.applyReplayRecord(plain)
		} else if complete {
			corrupt++
		}
		if complete {
			lastGood = off
		}
		if rerr == io.EOF {
			break
		}
		_ = start
	}
	if corrupt > 0 {
		atomic.AddInt64(metrics.Default.Counter("microdb_corrupt_records_total",
			"Corrupt log records skipped during replay"), corrupt)
	}
	// Self-healing tail: anything after the last complete record is a
	// partial write from a crash. Drop it so the file starts clean
	// (and so VerifyLog stops reporting the same torn tail forever).
	if off > lastGood {
		if err := f.Truncate(lastGood); err != nil {
			f.Close()
			return nil, fmt.Errorf("truncate torn tail: %w", err)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	s.f = f
	// The log writer owns every append from here on: it batches
	// records that arrive together into one write + one fsync and
	// releases the waiters (group commit).
	s.lw = newLogWriter(s.flushLog, 1024)
	s.lw.start()
	return s, nil
}

// merge applies a doc only if it is newer (higher ver, tie-break on ts).
// Returns true if the stored state changed. Idempotent — safe to apply
// the same write to the same node many times.
func (s *Store) merge(d *Doc) bool {
	k := key(d.Collection, d.ID)
	cur, ok := s.lookup(k)
	if !ok || d.Ver > cur.Ver || (d.Ver == cur.Ver && d.TS > cur.TS) {
		// Maintain the per-collection live count across the state
		// transition (this is the only place docs change state).
		// colStats may be nil on bare stores (e.g. PITR replay
		// builds one directly) — lazily initialize.
		if s.colStats == nil {
			s.colStats = map[string]*ColStats{}
		}
		st := s.colStats[d.Collection]
		if st == nil {
			st = &ColStats{}
			s.colStats[d.Collection] = st
		}
		wasLive := ok && !cur.Deleted
		nowLive := !d.Deleted
		if wasLive && !nowLive {
			st.Live.Add(-1)
		} else if !wasLive && nowLive {
			st.Live.Add(1)
		}
		s.put(k, d)
		if !ok {
			s.nDocs.Add(1) // first record for this key
		}
		return true
	}
	return false
}

// Apply upserts a document (schema-free: any JSON object) and persists it.
//
// The store lock covers only the mutation; the encoded line is then
// handed to the log writer and awaited WITHOUT the lock, which is what
// lets concurrent writers share one fsync (group commit) instead of
// queueing on the disk one behind another.
func (s *Store) Apply(collection, id string, fields map[string]interface{}) (*Doc, error) {
	d, line, err := s.applyLocked(collection, id, fields)
	if err != nil {
		return nil, err
	}
	if err := s.appendIf(line); err != nil {
		return nil, err
	}
	return d, nil
}

// applyLocked does the mutation under the store lock and encodes the
// log line for it. A nil line means nothing was appended (no-op merge),
// so no durability pass is owed by the caller.
func (s *Store) applyLocked(collection, id string, fields map[string]interface{}) (d *Doc, line []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(collection, id)
	ver := int64(1)
	if cur, ok := s.lookup(k); ok {
		ver = cur.Ver + 1
	}
	d = &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Fields: fields}
	b, _ := json.Marshal(d)
	// Durability first: the record must be quorum-durable in the
	// shared log before it touches local state (no-op without a log).
	if s.isNew(d) {
		if err := s.sharedAppend(b); err != nil {
			return nil, nil, err
		}
	}
	if !s.merge(d) {
		return d, nil, nil // concurrent newer write already applied
	}
	s.idx.For(collection).Upsert(id, fields)
	line, err = s.encodeRecord(b)
	if err != nil {
		return nil, nil, err
	}
	s.log.Append(collection, id, "upsert", d)
	s.writes.Add(1)
	atomic.AddInt64(metrics.Default.Counter("microdb_writes_total", "Client writes applied"), 1)
	return d, line, nil
}

// ApplyBatch upserts many documents in one call: one log write (one
// fsync when enabled) and one fanout pass. All docs land or none do —
// the batch is written as a single atomic log record.
func (s *Store) ApplyBatch(collection string, docs map[string]map[string]interface{}) ([]*Doc, error) {
	out, line, err := s.applyBatchLocked(collection, docs)
	if err != nil {
		return nil, err
	}
	if err := s.appendIf(line); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) applyBatchLocked(collection string, docs map[string]map[string]interface{}) (out []*Doc, line []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out = make([]*Doc, 0, len(docs))
	changed := make([]*Doc, 0, len(docs))
	for id, fields := range docs {
		ver := int64(1)
		if cur, ok := s.lookup(key(collection, id)); ok {
			ver = cur.Ver + 1
		}
		d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Fields: fields}
		out = append(out, d)
		if s.isNew(d) {
			changed = append(changed, d)
		}
	}
	if len(changed) == 0 {
		return out, nil, nil
	}
	rec := map[string]interface{}{"batch": changed}
	b, _ := json.Marshal(rec)
	// Durability first: the whole batch lands in the shared log (one
	// append, one quorum) before any local state changes.
	if err := s.sharedAppend(b); err != nil {
		return nil, nil, err
	}
	for _, d := range changed {
		s.idx.For(collection).Upsert(d.ID, d.Fields)
		s.merge(d)
	}
	line, err = s.encodeRecord(b)
	if err != nil {
		return nil, nil, err
	}
	for _, d := range changed {
		s.log.Append(d.Collection, d.ID, "upsert", d)
	}
	return out, line, nil
}

// ApplyRemote merges a replicated doc from a peer without re-propagating.
func (s *Store) ApplyRemote(d *Doc) bool {
	line, err := s.applyRemoteLocked(d)
	if err != nil {
		return false
	}
	return s.appendIf(line) == nil
}

func (s *Store) applyRemoteLocked(d *Doc) (line []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.merge(d) {
		return nil, nil
	}
	s.idx.For(d.Collection).Upsert(d.ID, indexable(d))
	b, _ := json.Marshal(d)
	line, err = s.encodeRecord(b)
	if err != nil {
		return nil, err
	}
	kind := "upsert"
	if d.Deleted {
		kind = "delete"
	}
	s.log.Append(d.Collection, d.ID, kind, d)
	atomic.AddInt64(metrics.Default.Counter("microdb_replicated_applies_total", "Docs applied from replication/anti-entropy"), 1)
	return line, nil
}

// Get returns a live document. It takes no store lock: the map read
// itself is lock-free, so point reads scale with cores instead of
// queueing behind every write.
func (s *Store) Get(collection, id string) (*Doc, bool) {
	d, ok := s.lookup(key(collection, id))
	if !ok || d.Deleted {
		return nil, false
	}
	return d, true
}

func (s *Store) Delete(collection, id string) error {
	line, err := s.deleteLocked(collection, id)
	if err != nil {
		return err
	}
	return s.appendIf(line)
}

func (s *Store) deleteLocked(collection, id string) (line []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(collection, id)
	ver := int64(1)
	if cur, ok := s.lookup(k); ok {
		ver = cur.Ver + 1
	}
	d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Deleted: true}
	b, _ := json.Marshal(d)
	// Durability first (same contract as Apply).
	if s.isNew(d) {
		if err := s.sharedAppend(b); err != nil {
			return nil, err
		}
	}
	if !s.merge(d) {
		return nil, nil
	}
	s.idx.For(collection).Remove(id)
	line, err = s.encodeRecord(b)
	if err != nil {
		return nil, err
	}
	s.log.Append(collection, id, "delete", nil)
	s.writes.Add(1)
	atomic.AddInt64(metrics.Default.Counter("microdb_deletes_total", "Client deletes applied"), 1)
	return line, nil
}

// Scan walks a collection applying a filter.
func (s *Store) Scan(collection string, keep func(*Doc) bool) []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []*Doc{}
	s.rangeDocs(func(_ string, d *Doc) bool {
		if d.Collection != collection || d.Deleted {
			return true
		}
		if keep == nil || keep(d) {
			out = append(out, d)
		}
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Prefilter answers one question cheaply: can this collection be ruled
// out for this filter WITHOUT scanning it? It returns true only when
// some exact-equality condition matches nothing in the collection's
// Bloom filter — a "no" that cannot be wrong. Everything else (ranges,
// regex, compound without an equality, an unfiltered scan) returns
// false and lets the normal path run.
//
// This is what turns the full-scan fallback of a compound query — the
// one the inverted-index fast path cannot serve — into an instant empty
// result on a shard that simply does not hold the value.
func (s *Store) Prefilter(collection string, filter map[string]interface{}) bool {
	if len(filter) == 0 {
		return false
	}
	// Compact swaps the index set; read the reference under the lock
	// the same way ScanIndexed does.
	s.mu.RLock()
	idx := s.idx
	s.mu.RUnlock()
	ix := idx.For(collection)
	for field, cond := range filter {
		if _, isCond := cond.(map[string]interface{}); isCond {
			continue // operators have no Bloom answer
		}
		if !ix.ProbablyHasValue(field, cond) {
			return true
		}
	}
	return false
}

// ScanIndexed is Scan with an index fast path: if the filter is a single
// exact-match condition on an indexed field, resolve candidate ids via
// the inverted index instead of scanning the whole collection. Falls
// back to a full scan for compound/comparison filters.
func (s *Store) ScanIndexed(collection string, filter map[string]interface{}) []*Doc {
	if len(filter) == 1 {
		for field, cond := range filter {
			if _, isCmp := cond.(map[string]interface{}); !isCmp {
				// Capture the index set under the store lock: Compact
				// swaps s.idx atomically and we must not read a torn ref.
				s.mu.RLock()
				idx := s.idx
				out := make([]*Doc, 0, 8)
				for _, id := range idx.For(collection).Lookup(field, cond) {
					if d, ok := s.lookup(key(collection, id)); ok && !d.Deleted {
						out = append(out, d)
					}
				}
				s.mu.RUnlock()
				sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
				return out
			}
		}
	}
	if s.Prefilter(collection, filter) {
		// Nothing in this collection can satisfy an equality the Bloom
		// filter says we never stored: skip the walk entirely.
		atomic.AddInt64(metrics.Default.Counter("microdb_scan_shortcircuits_total",
			"Full scans skipped because the collection cannot match"), 1)
		return nil
	}
	return s.Scan(collection, func(d *Doc) bool {
		return len(filter) == 0 || Matches(d, filter)
	})
}

func (s *Store) Close() error {
	// Stop the log writer first: it drains queued records (releasing
	// any waiters) before the file handle goes away.
	if s.lw != nil {
		s.lw.close()
	}
	if s.log != nil {
		s.log.Close()
	}
	// Flush before closing so a clean shutdown leaves everything
	// durable even when fsync-per-write is off.
	if err := s.sync(); err != nil {
		s.f.Close()
		return err
	}
	return s.f.Close()
}

// Compact rewrites the JSONL log keeping only the current version of
// each live document. Tombstones older than gcWindow are dropped
// entirely; tombstones newer than the window are kept so deletes
// cannot be resurrected by a lagging replica during anti-entropy.
//
// The rewrite is crash-safe: new log is written to data.jsonl.tmp,
// fsynced, then atomically renamed over the old log.
func (s *Store) Compact(gcWindow time.Duration) (int, error) {
	// Exclusive write slot for the whole swap: the log writer must not
	// touch the file handle while compaction closes and replaces it.
	// Lock order matches appendInline/flushLog: wmu before mu.
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-gcWindow).UnixMilli()
	live := make([]*Doc, 0, s.nDocs.Load())
	dropped := 0
	s.mutateDocs(func(k string, d *Doc) bool {
		if d.Deleted && d.TS <= cutoff {
			delete(s.shards[shardOf(k)].docs, k)
			s.nDocs.Add(-1)
			dropped++
			return true
		}
		live = append(live, d)
		return true
	})
	// Rebuild indexes from scratch: cheap and guarantees no stale entries.
	s.idx = index.NewSet(s.idx.Enabled())
	for _, d := range live {
		if !d.Deleted {
			s.idx.For(d.Collection).Add(d.ID, d.Fields)
		}
	}
	sort.Slice(live, func(i, j int) bool {
		if live[i].Collection != live[j].Collection {
			return live[i].Collection < live[j].Collection
		}
		return live[i].ID < live[j].ID
	})

	tmpPath := s.f.Name() + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	for _, d := range live {
		b, _ := json.Marshal(d)
		if err := s.appendRecord(w, b); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return 0, err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	oldName := s.f.Name()
	// Cold tier: preserve history BEFORE the rewrite discards it. If
	// the tier is unreachable we refuse to compact — the local log is
	// the only remaining copy of that history, and a compaction that
	// cannot archive is a compaction that would destroy it.
	if err := s.archiveToTier(oldName); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return 0, fmt.Errorf("refusing to compact without archiving: %w", err)
	}
	// Windows cannot rename over an open handle — close first (lock held,
	// so no writes can slip through), rename, then reopen for appending.
	if err := s.f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmpPath, oldName); err != nil {
		return 0, err
	}
	nf, err := os.OpenFile(oldName, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}
	s.f = nf
	return dropped, nil
}

// DocCount returns the number of docs held (including tombstones).
func (s *Store) DocCount() int {
	return int(s.nDocs.Load())
}

// Collections lists all collection names present in the store
// (including ones containing only tombstones).
func (s *Store) Collections() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := map[string]bool{}
	s.rangeDocs(func(_ string, d *Doc) bool {
		set[d.Collection] = true
		return true
	})
	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// StreamCollection returns up to `limit` docs from a collection
// (tombstones included) with ID > after, sorted by ID. Paginated
// snapshot for bootstrap streaming; taken under the read lock so it
// is consistent while writes continue.
func (s *Store) StreamCollection(collection, after string, limit int) []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := make([]*Doc, 0, 64)
	s.rangeDocs(func(_ string, d *Doc) bool {
		if d.Collection != collection {
			return true
		}
		if after != "" && d.ID <= after {
			return true
		}
		all = append(all, d)
		return true
	})
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}

// Leaves returns (id, hash) pairs for every doc in a collection,
// tombstones included so deletions participate in the Merkle tree.
func (s *Store) Leaves(collection string) []merkle.Leaf {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]merkle.Leaf, 0, 16)
	s.rangeDocs(func(_ string, d *Doc) bool {
		if d.Collection != collection {
			return true
		}
		out = append(out, merkle.Leaf{ID: d.ID, Hash: merkle.HashDoc(d)})
		return true
	})
	return out
}

// DocsByIDs returns stored docs (tombstones included) for the given ids.
func (s *Store) DocsByIDs(collection string, ids []string) []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Doc, 0, len(ids))
	for _, id := range ids {
		if d, ok := s.lookup(key(collection, id)); ok {
			out = append(out, d)
		}
	}
	return out
}

// Backup writes a snapshot of the current in-memory state to w as
// JSONL (one doc per line, current versions only — no history).
// Safe to take while writes continue: the snapshot is taken under the
// read lock.
// AllDocs returns a snapshot of every stored doc (tombstones included)
// sorted by (collection, id). Used by graceful decommission to hand
// this node's data off to the remaining cluster.
func (s *Store) AllDocs() []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	docs := make([]*Doc, 0, s.nDocs.Load())
	s.rangeDocs(func(_ string, d *Doc) bool {
		docs = append(docs, d)
		return true
	})
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].Collection != docs[j].Collection {
			return docs[i].Collection < docs[j].Collection
		}
		return docs[i].ID < docs[j].ID
	})
	return docs
}

// ConfigCollection is a reserved collection whose docs hold
// per-collection settings. Storing config as ordinary data means it
// replicates, merges (LWW), and survives restarts for free.
const ConfigCollection = "_config"

// CollectionConfig is the per-collection settings doc.
type CollectionConfig struct {
	RF             int    `json:"rf,omitempty"`              // 0 = use the node default
	PartitionField string `json:"partition_field,omitempty"` // route by this field's value, not doc id
	SortField      string `json:"sort_field,omitempty"`      // within-partition ordering field
	MaxDocs        int64  `json:"max_docs,omitempty"`        // live-doc quota for this collection (0 = unlimited)
}

// SetCollectionConfig stores per-collection settings. The returned
// doc must be fanned out by the caller (the store itself doesn't
// replicate — that's the API layer's job). rf <= 0 clears the override.
func (s *Store) SetCollectionConfig(col string, cfg CollectionConfig) (*Doc, error) {
	fields := map[string]interface{}{"rf": cfg.RF}
	if cfg.PartitionField != "" {
		fields["partition_field"] = cfg.PartitionField
	}
	if cfg.SortField != "" {
		fields["sort_field"] = cfg.SortField
	}
	if cfg.MaxDocs > 0 {
		fields["max_docs"] = cfg.MaxDocs
	}
	return s.Apply(ConfigCollection, col, fields)
}

// GetCollectionConfig returns the stored settings for a collection
// (zero value if none).
func (s *Store) GetCollectionConfig(col string) CollectionConfig {
	d, ok := s.Get(ConfigCollection, col)
	if !ok {
		return CollectionConfig{}
	}
	var cfg CollectionConfig
	switch v := d.Fields["rf"].(type) {
	case float64: // came through a JSON round-trip (replica / restart)
		cfg.RF = int(v)
	case int: // stored in-process by SetCollectionConfig
		cfg.RF = v
	}
	if v, ok := d.Fields["partition_field"].(string); ok {
		cfg.PartitionField = v
	}
	if v, ok := d.Fields["sort_field"].(string); ok {
		cfg.SortField = v
	}
	switch v := d.Fields["max_docs"].(type) {
	case float64:
		cfg.MaxDocs = int64(v)
	case int:
		cfg.MaxDocs = int64(v)
	case int64:
		cfg.MaxDocs = v
	}
	return cfg
}

// EffectiveRF resolves the replication factor for a collection:
// per-collection override if set and positive, else the fallback.
func (s *Store) EffectiveRF(col string, fallback int) int {
	if cfg := s.GetCollectionConfig(col); cfg.RF > 0 {
		return cfg.RF
	}
	return fallback
}

// ApplyVersioned stores a record with an explicit (ver, ts) supplied
// by the caller — the storage-tier write path. LWW merge semantics
// match ApplyRemote: the record lands iff it's newer than what we
// hold. Persists, indexes, and feeds the change log like any write.
func (s *Store) ApplyVersioned(rec *storage.Record) error {
	line, err := s.applyVersionedLocked(rec)
	if err != nil {
		return err
	}
	return s.appendIf(line)
}

func (s *Store) applyVersionedLocked(rec *storage.Record) (line []byte, err error) {
	d := &Doc{
		ID: rec.ID, Ver: rec.Ver, TS: rec.TS,
		Deleted: rec.Deleted, Collection: rec.Collection, Fields: rec.Fields,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.merge(d) {
		return nil, nil // older or equal: idempotent no-op
	}
	s.idx.For(rec.Collection).Upsert(rec.ID, indexable(d))
	b, _ := json.Marshal(d)
	line, err = s.encodeRecord(b)
	if err != nil {
		return nil, err
	}
	kind := "upsert"
	if rec.Deleted {
		kind = "delete"
	}
	s.log.Append(rec.Collection, rec.ID, kind, d)
	atomic.AddInt64(metrics.Default.Counter("microdb_storage_applies_total", "Records applied via the storage-tier API"), 1)
	return line, nil
}

// GetDoc returns a live doc as a storage-tier Record.
func (s *Store) GetDoc(collection, id string) (*storage.Record, bool) {
	d, ok := s.Get(collection, id)
	if !ok {
		return nil, false
	}
	return &storage.Record{
		Collection: d.Collection, ID: d.ID, Ver: d.Ver, TS: d.TS,
		Deleted: d.Deleted, Fields: d.Fields,
	}, true
}

// ScanPage returns live records in id order, resuming after afterID.
func (s *Store) ScanPage(collection, afterID string, limit int) []*storage.Record {
	docs := s.StreamCollection(collection, afterID, limit)
	out := make([]*storage.Record, 0, len(docs))
	for _, d := range docs {
		if d.Deleted {
			continue
		}
		out = append(out, &storage.Record{
			Collection: d.Collection, ID: d.ID, Ver: d.Ver, TS: d.TS,
			Fields: d.Fields,
		})
	}
	return out
}

// CountUnder counts live (non-deleted) docs in every collection
// whose name starts with prefix (e.g. "acme." counts acme.users,
// acme.orders). Used for tenant storage quotas.
func (s *Store) CountUnder(prefix string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	s.rangeDocs(func(_ string, d *Doc) bool {
		if !d.Deleted && strings.HasPrefix(d.Collection, prefix) {
			n++
		}
		return true
	})
	return n
}

// SetCollectionSchema stores the collection's migration schema in
// the same reserved _config doc (field "schema"), so it replicates
// and survives restarts like any other data. Existing settings (rf)
// are preserved.
func (s *Store) SetCollectionSchema(col string, sch migrate.Schema) (*Doc, error) {
	if err := sch.Validate(); err != nil {
		return nil, err
	}
	cfg := s.GetCollectionConfig(col)
	b, err := json.Marshal(sch)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return s.Apply(ConfigCollection, col, map[string]interface{}{"rf": cfg.RF, "schema": m})
}

// GetCollectionSchema returns the stored schema for a collection
// (ok=false when none is set).
func (s *Store) GetCollectionSchema(col string) (migrate.Schema, bool) {
	d, ok := s.Get(ConfigCollection, col)
	if !ok {
		return migrate.Schema{}, false
	}
	raw, ok := d.Fields["schema"].(map[string]interface{})
	if !ok {
		return migrate.Schema{}, false
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return migrate.Schema{}, false
	}
	var sch migrate.Schema
	if err := json.Unmarshal(b, &sch); err != nil {
		return migrate.Schema{}, false
	}
	return sch, true
}

// ArchiveRaw copies the raw commit log (full version history, not the
// current-state snapshot) to w. Because the log is append-only, any
// copy taken at any instant is a valid prefix of history — i.e. a
// point-in-time snapshot. Safe under concurrent writes (read lock).
func (s *Store) ArchiveRaw(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Re-read the log from the start: s.f is positioned at the tail
	// for appending, so use a separate handle.
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
}

// ReplayUntil replays a commit log (as produced by ArchiveRaw) into a
// fresh store, applying only records whose TS is <= until. Returns the
// new store and the number of records applied. The target dir must be
// empty or new. This is the point-in-time recovery primitive: the
// state at time T = replay of every log record with TS <= T.
//
// NOTE: the recoverable window is bounded by compaction — compaction
// rewrites the log keeping only current versions, so history older
// than the last compaction is gone. Archive frequently to widen it.
func ReplayUntil(r io.Reader, until int64, dir string) (*Store, int, error) {
	return ReplayUntilKey(r, until, dir, "")
}

// ReplayUntilKey is ReplayUntil with an encryption key (64 hex chars)
// for archives taken from encrypted databases. The recovered store
// inherits the key, so its new log is encrypted too.
func ReplayUntilKey(r io.Reader, until int64, dir, keyHex string) (*Store, int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, 0, err
	}
	s := &Store{idx: index.NewSet(true), log: changelog.New(10000, 5*time.Minute), path: filepath.Join(dir, "data.jsonl")}
	for i := range s.shards {
		s.shards[i].docs = map[string]*Doc{}
	}
	if keyHex != "" {
		if err := s.SetEncryptionKey(keyHex); err != nil {
			return nil, 0, err
		}
	}
	// Fresh log file for the recovered store.
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, 0, err
	}
	s.f = f
	bw := bufio.NewWriterSize(f, 1<<20)
	br := bufio.NewReaderSize(r, 1<<20)
	applied := 0
	for {
		line, rerr := br.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := strings.TrimSpace(string(line))
			if trimmed != "" {
				// Verify the archive record's checksum before doing anything
				// else with it: the envelope sits outside the ciphertext, so
				// no key is needed to spot a bit-rotted record.
				payload, intact := verifyRecord(trimmed)
				if !intact {
					continue
				}
				trimmed, derr := s.decodeLogLine(payload)
				if derr != nil {
					f.Close()
					return nil, 0, fmt.Errorf("archive replay: %w", derr)
				}
				// Batch record or single doc — filter each doc by TS.
				if strings.HasPrefix(trimmed, `{"batch":`) {
					var rec struct {
						Batch []*Doc `json:"batch"`
					}
					if json.Unmarshal([]byte(trimmed), &rec) == nil {
						for _, d := range rec.Batch {
							if d.TS > until {
								continue
							}
							if s.merge(d) {
								s.idx.For(d.Collection).Remove(d.ID)
								if !d.Deleted {
									s.idx.For(d.Collection).Add(d.ID, d.Fields)
								}
								kind := "put"
								var fld map[string]interface{}
								if d.Deleted {
									kind = "delete"
								} else {
									fld = d.Fields
								}
								s.log.Append(d.Collection, d.ID, kind, fld)
								// Persist the replayed record so the
								// recovered store survives restart.
								if b, err := json.Marshal(d); err == nil {
									if ln, lerr := s.encodeRecord(b); lerr == nil {
										bw.Write(ln)
									}
								}
								applied++
							}
						}
					}
				} else {
					var d Doc
					if json.Unmarshal([]byte(trimmed), &d) == nil && d.TS <= until {
						if s.merge(&d) {
							s.idx.For(d.Collection).Remove(d.ID)
							if !d.Deleted {
								s.idx.For(d.Collection).Add(d.ID, d.Fields)
							}
							kind := "put"
							var fld map[string]interface{}
							if d.Deleted {
								kind = "delete"
							} else {
								fld = d.Fields
							}
							s.log.Append(d.Collection, d.ID, kind, fld)
							if b, err := json.Marshal(d); err == nil {
								if ln, lerr := s.encodeRecord(b); lerr == nil {
									bw.Write(ln)
								}
							}
							applied++
						}
					}
				}
			}
		}
		if rerr != nil {
			break
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return nil, 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, 0, err
	}
	return s, applied, nil
}

// Backup writes a snapshot of the current in-memory state to w as
// JSONL (one doc per line, current versions only — no history).
// Safe to take while writes continue: the snapshot is taken under the
// read lock.
func (s *Store) Backup(w io.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	docs := make([]*Doc, 0, s.nDocs.Load())
	s.rangeDocs(func(_ string, d *Doc) bool {
		docs = append(docs, d)
		return true
	})
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].Collection != docs[j].Collection {
			return docs[i].Collection < docs[j].Collection
		}
		return docs[i].ID < docs[j].ID
	})
	bw := bufio.NewWriterSize(w, 1<<20)
	enc := json.NewEncoder(bw)
	for _, d := range docs {
		if err := enc.Encode(d); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// Restore reads a JSONL backup (as produced by Backup) and applies
// every record through the normal merge path, so restoring into a
// non-empty store is safe (LWW decides) and indexes stay consistent.
func (s *Store) Restore(r io.Reader) (int, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	applied := 0
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := strings.TrimSpace(string(line))
			if trimmed != "" {
				var d Doc
				if json.Unmarshal([]byte(trimmed), &d) != nil {
					return applied, fmt.Errorf("bad backup line: %.80s", trimmed)
				}
				if s.ApplyRemote(&d) {
					applied++
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return applied, err
		}
	}
	return applied, nil
}

// Matches evaluates a filter object against a document.
// Supported: {"field": value} exact, {"field": {"$gt":v}}, {"$lt":v},
// {"$gte":v}, {"$lte":v}, {"$ne":v}, {"$in":[...]}, {"$exists":bool},
// {"$regex":"pattern"} (string fields). Multiple conditions on one
// field AND together; multiple fields AND together.
func Matches(d *Doc, filter map[string]interface{}) bool {
	for field, cond := range filter {
		v := d.Fields[field]
		m, isCond := cond.(map[string]interface{})
		if !isCond {
			if !jsonEq(v, cond) {
				return false
			}
			continue
		}
		for op, operand := range m {
			switch op {
			case "$gt":
				if !cmp(v, operand, func(c int) bool { return c > 0 }) {
					return false
				}
			case "$lt":
				if !cmp(v, operand, func(c int) bool { return c < 0 }) {
					return false
				}
			case "$gte":
				if !cmp(v, operand, func(c int) bool { return c >= 0 }) {
					return false
				}
			case "$lte":
				if !cmp(v, operand, func(c int) bool { return c <= 0 }) {
					return false
				}
			case "$ne":
				if jsonEq(v, operand) {
					return false
				}
			case "$in":
				list, ok := operand.([]interface{})
				if !ok {
					return false
				}
				found := false
				for _, cand := range list {
					if jsonEq(v, cand) {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			case "$exists":
				want, _ := operand.(bool)
				_, have := d.Fields[field]
				if want != have {
					return false
				}
			case "$regex":
				pat, ok := operand.(string)
				if !ok {
					return false
				}
				re, err := regexp.Compile(pat)
				if err != nil {
					return false
				}
				sv, ok := v.(string)
				if !ok || !re.MatchString(sv) {
					return false
				}
			default:
				return false // unknown operator
			}
		}
	}
	return true
}

func jsonEq(a, b interface{}) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

// CompareValues orders two JSON-decoded values: numbers numerically,
// strings lexicographically, missing values sort last. Mixed or
// unorderable types compare equal (stable sort keeps insertion order).
func CompareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
		return -1
	}
	switch av := a.(type) {
	case float64:
		if bv, ok := b.(float64); ok {
			return sign(av - bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			ab, bb := 0, 0
			if av {
				ab = 1
			}
			if bv {
				bb = 1
			}
			return ab - bb
		}
	}
	return 0
}

func cmp(a, b interface{}, pred func(int) bool) bool {
	switch av := a.(type) {
	case float64:
		bv, ok := b.(float64)
		return ok && pred(int(sign(av-bv)))
	case string:
		bv, ok := b.(string)
		return ok && pred(strings.Compare(av, bv))
	}
	return false
}

func sign(f float64) int {
	switch {
	case f > 0:
		return 1
	case f < 0:
		return -1
	}
	return 0
}

var _ = fmt.Sprintf // keep fmt for future errors
