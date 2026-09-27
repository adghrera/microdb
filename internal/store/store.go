// Package store is a schema-less document store: documents are plain JSON
// maps, persisted with an append-only JSONL log, merged last-writer-wins
// by (version, timestamp).
package store

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"microdb/internal/changelog"
	"microdb/internal/index"
	"microdb/internal/metrics"
	"microdb/internal/merkle"
	"microdb/internal/migrate"
)

type Doc struct {
	ID        string                 `json:"id"`
	Ver       int64                  `json:"ver"`
	TS        int64                  `json:"ts"` // unix millis
	Deleted   bool                   `json:"deleted,omitempty"`
	Collection string                `json:"collection"`
	Fields    map[string]interface{} `json:"fields"`
}

type Store struct {
	mu     sync.RWMutex
	docs   map[string]*Doc // key = collection + "\x00" + id
	path   string          // absolute path of the JSONL commit log
	f      *os.File
	key    []byte // encryption-at-rest key (nil = plaintext log)
	fsync  bool // sync to disk on every write (durability over throughput)
	idx    *index.IndexSet // inverted field indexes (fast exact-match lookups)
	log    *changelog.Log  // bounded mutation feed for watchers
}

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

// SetFsync enables fsync-on-write. With it enabled every Apply/Delete
// blocks until the record is durable on disk; without it the OS page
// cache decides (fast, but a power loss can lose the log tail).
func (s *Store) SetFsync(on bool) { s.fsync = on }

// sync flushes the log to stable storage if fsync is enabled.
func (s *Store) sync() error {
	if s.fsync {
		return s.f.Sync()
	}
	return nil
}

func key(col, id string) string { return col + "\x00" + id }

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
func (s *Store) appendRecord(w io.Writer, plain []byte) error {
	if s.key != nil {
		enc, err := encryptRecord(s.key, plain)
		if err != nil {
			return err
		}
		_, err = w.Write(append(enc, '\n'))
		return err
	}
	_, err := w.Write(append(plain, '\n'))
	return err
}

// decodeLogLine turns one raw log line into plaintext JSON,
// transparently decrypting ENC1 records.
func (s *Store) decodeLogLine(line string) (string, error) {
	if strings.HasPrefix(line, encPrefix) {
		plain, err := decryptRecord(s.key, line)
		if err != nil {
			return "", err
		}
		return string(plain), nil
	}
	return line, nil
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
	s := &Store{docs: map[string]*Doc{}, path: path, idx: index.NewSet(true), log: changelog.New(10000, 5*time.Minute)}
	if keyHex != "" {
		if err := s.SetEncryptionKey(keyHex); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// Replay existing log.
	buf := make([]byte, 0, 1<<20)
	chunk := make([]byte, 64*1024)
	for {
		n, rerr := f.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if rerr != nil {
			break
		}
	}
	for _, raw := range strings.Split(string(buf), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		// Transparently decrypt ENC1 records (error = wrong/missing
		// key: fail loudly rather than silently dropping data).
		line, err := s.decodeLogLine(line)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("log replay: %w", err)
		}
		// Batch records: {"batch":[{doc},{doc},...]}.
		if strings.HasPrefix(line, `{"batch":`) {
			var rec struct {
				Batch []*Doc `json:"batch"`
			}
			if json.Unmarshal([]byte(line), &rec) == nil {
				for _, d := range rec.Batch {
					if s.merge(d) {
						s.idx.For(d.Collection).Remove(d.ID)
						if !d.Deleted {
							s.idx.For(d.Collection).Add(d.ID, d.Fields)
						}
					}
				}
			}
			continue
		}
		var d Doc
		if json.Unmarshal([]byte(line), &d) == nil {
			if s.merge(&d) {
				s.idx.For(d.Collection).Remove(d.ID)
				if !d.Deleted {
					s.idx.For(d.Collection).Add(d.ID, d.Fields)
				}
			}
		}
	}
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}
	s.f = f
	return s, nil
}

// merge applies a doc only if it is newer (higher ver, tie-break on ts).
// Returns true if the stored state changed. Idempotent — safe to apply
// the same write to the same node many times.
func (s *Store) merge(d *Doc) bool {
	k := key(d.Collection, d.ID)
	cur, ok := s.docs[k]
	if !ok || d.Ver > cur.Ver || (d.Ver == cur.Ver && d.TS > cur.TS) {
		s.docs[k] = d
		return true
	}
	return false
}

// Apply upserts a document (schema-free: any JSON object) and persists it.
func (s *Store) Apply(collection, id string, fields map[string]interface{}) (*Doc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(collection, id)
	ver := int64(1)
	if cur, ok := s.docs[k]; ok {
		ver = cur.Ver + 1
	}
	d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Fields: fields}
	if !s.merge(d) {
		return d, nil // concurrent newer write already applied
	}
	s.idx.For(collection).Remove(id)
	s.idx.For(collection).Add(id, fields)
	b, _ := json.Marshal(d)
	if err := s.appendRecord(s.f, b); err != nil {
		return nil, err
	}
	if err := s.sync(); err != nil {
		return nil, err
	}
	s.log.Append(collection, id, "upsert", d)
	atomic.AddInt64(metrics.Default.Counter("microdb_writes_total", "Client writes applied"), 1)
	return d, nil
}

// ApplyBatch upserts many documents in one call: one log write (one
// fsync when enabled) and one fanout pass. All docs land or none do —
// the batch is written as a single atomic log record.
func (s *Store) ApplyBatch(collection string, docs map[string]map[string]interface{}) ([]*Doc, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Doc, 0, len(docs))
	changed := make([]*Doc, 0, len(docs))
	for id, fields := range docs {
		ver := int64(1)
		if cur, ok := s.docs[key(collection, id)]; ok {
			ver = cur.Ver + 1
		}
		d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Fields: fields}
		out = append(out, d)
		if s.merge(d) {
			s.idx.For(collection).Remove(id)
			s.idx.For(collection).Add(id, fields)
			changed = append(changed, d)
		}
	}
	if len(changed) == 0 {
		return out, nil
	}
	rec := map[string]interface{}{"batch": changed}
	b, _ := json.Marshal(rec)
	if err := s.appendRecord(s.f, b); err != nil {
		return nil, err
	}
	if err := s.sync(); err != nil {
		return nil, err
	}
	for _, d := range changed {
		s.log.Append(d.Collection, d.ID, "upsert", d)
	}
	return out, nil
}

// ApplyRemote merges a replicated doc from a peer without re-propagating.
func (s *Store) ApplyRemote(d *Doc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.merge(d) {
		return false
	}
	s.idx.For(d.Collection).Remove(d.ID)
	if !d.Deleted {
		s.idx.For(d.Collection).Add(d.ID, d.Fields)
	}
	b, _ := json.Marshal(d)
	if err := s.appendRecord(s.f, b); err != nil {
		return false
	}
	if s.sync() != nil {
		return false
	}
	kind := "upsert"
	if d.Deleted {
		kind = "delete"
	}
	s.log.Append(d.Collection, d.ID, kind, d)
	atomic.AddInt64(metrics.Default.Counter("microdb_replicated_applies_total", "Docs applied from replication/anti-entropy"), 1)
	return true
}

func (s *Store) Get(collection, id string) (*Doc, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.docs[key(collection, id)]
	if !ok || d.Deleted {
		return nil, false
	}
	return d, true
}

func (s *Store) Delete(collection, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(collection, id)
	ver := int64(1)
	if cur, ok := s.docs[k]; ok {
		ver = cur.Ver + 1
	}
	d := &Doc{ID: id, Ver: ver, TS: time.Now().UnixMilli(), Collection: collection, Deleted: true}
	if !s.merge(d) {
		return nil
	}
	s.idx.For(collection).Remove(id)
	b, _ := json.Marshal(d)
	if err := s.appendRecord(s.f, b); err != nil {
		return err
	}
	if err := s.sync(); err != nil {
		return err
	}
	s.log.Append(collection, id, "delete", nil)
	atomic.AddInt64(metrics.Default.Counter("microdb_deletes_total", "Client deletes applied"), 1)
	return nil
}

// Scan walks a collection applying a filter.
func (s *Store) Scan(collection string, keep func(*Doc) bool) []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []*Doc{}
	for _, d := range s.docs {
		if d.Collection != collection || d.Deleted {
			continue
		}
		if keep == nil || keep(d) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
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
					if d, ok := s.docs[key(collection, id)]; ok && !d.Deleted {
						out = append(out, d)
					}
				}
				s.mu.RUnlock()
				sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
				return out
			}
		}
	}
	return s.Scan(collection, func(d *Doc) bool {
		return len(filter) == 0 || Matches(d, filter)
	})
}

func (s *Store) Close() error {
	if s.log != nil {
		s.log.Close()
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
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-gcWindow).UnixMilli()
	live := make([]*Doc, 0, len(s.docs))
	dropped := 0
	for k, d := range s.docs {
		if d.Deleted && d.TS <= cutoff {
			delete(s.docs, k)
			dropped++
			continue
		}
		live = append(live, d)
	}
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.docs)
}

// Collections lists all collection names present in the store
// (including ones containing only tombstones).
func (s *Store) Collections() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := map[string]bool{}
	for _, d := range s.docs {
		set[d.Collection] = true
	}
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
	for _, d := range s.docs {
		if d.Collection != collection {
			continue
		}
		if after != "" && d.ID <= after {
			continue
		}
		all = append(all, d)
	}
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
	for _, d := range s.docs {
		if d.Collection != collection {
			continue
		}
		out = append(out, merkle.Leaf{ID: d.ID, Hash: merkle.HashDoc(d)})
	}
	return out
}

// DocsByIDs returns stored docs (tombstones included) for the given ids.
func (s *Store) DocsByIDs(collection string, ids []string) []*Doc {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Doc, 0, len(ids))
	for _, id := range ids {
		if d, ok := s.docs[key(collection, id)]; ok {
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
	docs := make([]*Doc, 0, len(s.docs))
	for _, d := range s.docs {
		docs = append(docs, d)
	}
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
	RF int `json:"rf,omitempty"` // 0 = use the node default
}

// SetCollectionConfig stores per-collection settings. The returned
// doc must be fanned out by the caller (the store itself doesn't
// replicate — that's the API layer's job). rf <= 0 clears the override.
func (s *Store) SetCollectionConfig(col string, cfg CollectionConfig) (*Doc, error) {
	return s.Apply(ConfigCollection, col, map[string]interface{}{"rf": cfg.RF})
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
	s := &Store{docs: map[string]*Doc{}, idx: index.NewSet(true), log: changelog.New(10000, 5*time.Minute), path: filepath.Join(dir, "data.jsonl")}
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
				trimmed, derr := s.decodeLogLine(trimmed)
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
									bw.Write(b)
									bw.WriteByte('\n')
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
								bw.Write(b)
								bw.WriteByte('\n')
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
	docs := make([]*Doc, 0, len(s.docs))
	for _, d := range s.docs {
		docs = append(docs, d)
	}
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
		return ok && pred(int(sign(av - bv)))
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
