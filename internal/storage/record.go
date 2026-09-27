// Package storage defines the storage-tier record boundary: the
// minimal, epoch-fenced interface a storage node exposes and a
// (possibly remote) compute engine speaks. This is the seam that
// disaggregates storage from query: a compute node never touches
// JSONL, indexes, or the local store — it speaks RecordStore, either
// through the in-process Local adapter or over HTTP via Remote.
//
// Fencing: every mutation carries the ring epoch the writer used to
// route. A storage node rejects (ErrStaleEpoch) any mutation whose
// epoch is below the highest epoch it has seen — the same fencing
// rule the API layer applies to forwarded writes, now enforced at
// the record boundary itself.
package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// ErrStaleEpoch means the writer's ring view is behind this storage
// node's; the caller must refresh its view and retry.
var ErrStaleEpoch = errors.New("stale epoch")

// Record is one versioned storage record.
type Record struct {
	Collection string                 `json:"collection"`
	ID         string                 `json:"id"`
	Ver        int64                  `json:"ver"`
	TS         int64                  `json:"ts"`
	Deleted    bool                   `json:"deleted,omitempty"`
	Fields     map[string]interface{} `json:"fields"`
	Epoch      int64                  `json:"epoch"`
}

// RecordStore is the storage-tier interface.
type RecordStore interface {
	// Put stores one record (upsert or tombstone via Deleted).
	// Fenced by rec.Epoch.
	Put(rec *Record) error
	// Get fetches one live record.
	Get(collection, id string) (*Record, bool)
	// Scan pages a collection in id order, resuming after afterID.
	Scan(collection, afterID string, limit int) []*Record
	// Epoch reports the storage node's current fencing epoch.
	Epoch() int64
}

// Local adapts the on-node store to the RecordStore boundary.
// Fencing is tracked here (the highest epoch ever accepted).
type Local struct {
	st    Store
	mu    sync.Mutex
	epoch int64
}

// Store is the minimal store surface Local needs (store.Store
// satisfies it; kept narrow so the boundary is explicit).
type Store interface {
	ApplyVersioned(rec *Record) error
	GetDoc(collection, id string) (*Record, bool)
	ScanPage(collection, afterID string, limit int) []*Record
}

func NewLocal(st Store) *Local { return &Local{st: st} }

// ObserveEpoch raises the fence without writing (called when the
// local cluster epoch advances).
func (l *Local) ObserveEpoch(e int64) {
	l.mu.Lock()
	if e > l.epoch {
		l.epoch = e
	}
	l.mu.Unlock()
}

func (l *Local) Epoch() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epoch
}

func (l *Local) Put(rec *Record) error {
	l.mu.Lock()
	fence := l.epoch
	l.mu.Unlock()
	if rec.Epoch < fence {
		return ErrStaleEpoch
	}
	if err := l.st.ApplyVersioned(rec); err != nil {
		return err
	}
	l.ObserveEpoch(rec.Epoch)
	return nil
}

func (l *Local) Get(collection, id string) (*Record, bool) {
	return l.st.GetDoc(collection, id)
}

func (l *Local) Scan(collection, afterID string, limit int) []*Record {
	return l.st.ScanPage(collection, afterID, limit)
}

// Remote speaks RecordStore over HTTP to a storage node's
// /internal/record endpoints.
type Remote struct {
	Addr  string
	Token string // optional bearer token
	http  *http.Client
}

func NewRemote(addr string) *Remote {
	return &Remote{Addr: addr, http: &http.Client{Timeout: 15 * time.Second}}
}

func (r *Remote) do(method, path string, body []byte, epoch int64) (*http.Response, error) {
	req, err := http.NewRequest(method, r.Addr+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if epoch > 0 {
		req.Header.Set("X-Microdb-Epoch", strconv.FormatInt(epoch, 10))
	}
	if r.Token != "" {
		req.Header.Set("Authorization", "Bearer "+r.Token)
	}
	return r.http.Do(req)
}

func (r *Remote) Put(rec *Record) error {
	b, _ := json.Marshal(rec)
	resp, err := r.do("PUT", "/internal/record/"+rec.Collection+"/"+rec.ID, b, rec.Epoch)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return ErrStaleEpoch
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("record put: %d", resp.StatusCode)
	}
	return nil
}

func (r *Remote) Get(collection, id string) (*Record, bool) {
	resp, err := r.do("GET", "/internal/record/"+collection+"/"+id, nil, 0)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	var rec Record
	if json.NewDecoder(resp.Body).Decode(&rec) != nil {
		return nil, false
	}
	return &rec, true
}

func (r *Remote) Scan(collection, afterID string, limit int) []*Record {
	if limit <= 0 {
		limit = 1000
	}
	u := "/internal/record/" + collection + "/scan?after=" + afterID + "&limit=" + strconv.Itoa(limit)
	resp, err := r.do("GET", u, nil, 0)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var out struct {
		Records []*Record `json:"records"`
		Epoch   int64     `json:"epoch"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return nil
	}
	return out.Records
}

func (r *Remote) Epoch() int64 {
	resp, err := r.do("GET", "/internal/record/_epoch", nil, 0)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var out struct {
		Epoch int64 `json:"epoch"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	return out.Epoch
}
