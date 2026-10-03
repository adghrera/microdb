// Package compute implements a stateless query engine: a process
// that holds NO authoritative data. It parses client requests,
// resolves placement (which storage node owns a key), and routes
// reads/writes through the storage-tier RecordStore boundary. A
// compute node can die or scale out with zero data movement — the
// payoff of the C2 record boundary.
//
// The engine needs two things to be stateless-but-correct:
//
//  1. A placement oracle: given (collection, id), which storage
//     node is primary? Here that's a ring built from the live node
//     list, refreshed from any storage node's /cluster view.
//  2. Epoch discipline: every write carries the placement epoch;
//     a 409/ErrStaleEpoch from the storage tier means "your map is
//     stale" -> refresh placement, retry once.
//
// This is deliberately a thin engine: point reads/writes, batch
// writes, and scatter-gather queries across the storage nodes.
package compute

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"microdb/internal/ring"
	"microdb/internal/storage"
)

// Engine is a stateless compute node.
type Engine struct {
	mu      sync.RWMutex
	ring    *ring.Ring
	nodes   []string // storage nodes, sorted for determinism
	stores  map[string]*storage.Remote
	self    string // this engine's advertised address (for /health only)
	refresh time.Time
}

// New creates an engine seeded with storage node addresses.
func New(nodes []string) *Engine {
	e := &Engine{stores: map[string]*storage.Remote{}}
	for _, n := range nodes {
		e.stores[n] = storage.NewRemote(n)
	}
	e.setNodes(nodes)
	return e
}

func (e *Engine) setNodes(nodes []string) {
	sorted := append([]string(nil), nodes...)
	sort.Strings(sorted)
	e.mu.Lock()
	e.nodes = sorted
	e.ring = ring.BuildWeighted(sorted, nil, time.Now().Unix())
	e.refresh = time.Now()
	e.mu.Unlock()
}

// Nodes returns the current storage node list.
func (e *Engine) Nodes() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]string(nil), e.nodes...)
}

// RefreshPlacement re-reads the cluster view from any known storage
// node and rebuilds the placement ring.
func (e *Engine) RefreshPlacement() error {
	e.mu.RLock()
	nodes := append([]string(nil), e.nodes...)
	e.mu.RUnlock()
	for _, n := range nodes {
		resp, err := http.Get(n + "/api/cluster")
		if err != nil {
			continue
		}
		var out struct {
			Self  string   `json:"self"`
			Peers []string `json:"peers"`
		}
		err = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if err != nil || out.Self == "" {
			continue
		}
		all := append([]string{out.Self}, out.Peers...)
		e.setNodes(all)
		for _, n := range all {
			e.mu.Lock()
			if _, ok := e.stores[n]; !ok {
				e.stores[n] = storage.NewRemote(n)
			}
			e.mu.Unlock()
		}
		return nil
	}
	return errors.New("no storage node reachable for placement refresh")
}

// storeFor resolves the primary storage node for a key.
func (e *Engine) storeFor(collection, id string) (*storage.Remote, string, error) {
	e.mu.RLock()
	r := e.ring
	stores := e.stores
	e.mu.RUnlock()
	if r == nil || len(e.nodes) == 0 {
		return nil, "", errors.New("no storage nodes")
	}
	owners := r.Owners(collection+"/"+id, 1)
	if len(owners) == 0 {
		return nil, "", errors.New("empty ring")
	}
	st, ok := stores[owners[0]]
	if !ok {
		return nil, owners[0], fmt.Errorf("no client for %s", owners[0])
	}
	return st, owners[0], nil
}

// Put writes a document, routing to the primary storage node with
// the placement epoch. On ErrStaleEpoch it refreshes placement and
// retries once.
func (e *Engine) Put(collection, id string, fields map[string]interface{}) error {
	st, node, err := e.storeFor(collection, id)
	if err != nil {
		return err
	}
	ep := st.Epoch()
	ver := int64(1)
	if cur, ok := st.Get(collection, id); ok {
		ver = cur.Ver + 1
	}
	rec := &storage.Record{
		Collection: collection, ID: id, Ver: ver, TS: time.Now().UnixMilli(),
		Fields: fields, Epoch: ep,
	}
	err = st.Put(rec)
	if errors.Is(err, storage.ErrStaleEpoch) {
		if rerr := e.RefreshPlacement(); rerr != nil {
			return err
		}
		st2, _, err2 := e.storeFor(collection, id)
		if err2 != nil {
			return err
		}
		ver = 1
		if cur, ok := st2.Get(collection, id); ok {
			ver = cur.Ver + 1
		}
		rec.Ver = ver
		rec.TS = time.Now().UnixMilli()
		rec.Epoch = st2.Epoch()
		return st2.Put(rec)
	}
	_ = node
	return err
}

// Get reads a document from its primary storage node.
func (e *Engine) Get(collection, id string) (map[string]interface{}, bool) {
	st, _, err := e.storeFor(collection, id)
	if err != nil {
		return nil, false
	}
	rec, ok := st.Get(collection, id)
	if !ok {
		// Placement may be stale (node moved); refresh once.
		if rerr := e.RefreshPlacement(); rerr != nil {
			return nil, false
		}
		st2, _, err2 := e.storeFor(collection, id)
		if err2 != nil {
			return nil, false
		}
		rec, ok = st2.Get(collection, id)
		if !ok {
			return nil, false
		}
	}
	return rec.Fields, true
}

// Query fans out to every storage node, merges by id (highest ver
// wins), and applies a simple equality filter + sort/limit locally.
func (e *Engine) Query(collection string, filter map[string]interface{}, sortField string, limit int) []map[string]interface{} {
	e.mu.RLock()
	stores := make([]*storage.Remote, 0, len(e.stores))
	for _, s := range e.stores {
		stores = append(stores, s)
	}
	e.mu.RUnlock()

	merged := map[string]*storage.Record{}
	for _, st := range stores {
		after := ""
		for {
			recs := st.Scan(collection, after, 1000)
			if len(recs) == 0 {
				break
			}
			for _, rec := range recs {
				if cur, ok := merged[rec.ID]; !ok || rec.Ver > cur.Ver || (rec.Ver == cur.Ver && rec.TS > cur.TS) {
					merged[rec.ID] = rec
				}
			}
			after = recs[len(recs)-1].ID
			if len(recs) < 1000 {
				break
			}
		}
	}
	out := make([]map[string]interface{}, 0, len(merged))
	for _, rec := range merged {
		if matches(rec.Fields, filter) {
			out = append(out, rec.Fields)
		}
	}
	if sortField != "" {
		sort.Slice(out, func(i, j int) bool {
			return fmt.Sprint(out[i][sortField]) < fmt.Sprint(out[j][sortField])
		})
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

func matches(fields, filter map[string]interface{}) bool {
	for k, want := range filter {
		got, ok := fields[k]
		if !ok || fmt.Sprint(got) != fmt.Sprint(want) {
			return false
		}
	}
	return true
}

// --- HTTP surface: the compute node's client-facing API ----------

// Handler returns the engine's HTTP handler. Routes mirror the
// storage API shape but route through placement.
func (e *Engine) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok", "role": "compute", "storage_nodes": strconv.Itoa(len(e.Nodes()))})
	})
	mux.HandleFunc("GET /api/collections/{col}/docs/{id}", func(w http.ResponseWriter, r *http.Request) {
		f, ok := e.Get(r.PathValue("col"), r.PathValue("id"))
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, 200, map[string]interface{}{"fields": f})
	})
	mux.HandleFunc("PUT /api/collections/{col}/docs/{id}", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var fields map[string]interface{}
		if json.Unmarshal(body, &fields) != nil {
			writeJSON(w, 400, map[string]string{"error": "body must be a JSON object"})
			return
		}
		if err := e.Put(r.PathValue("col"), r.PathValue("id"), fields); err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /api/collections/{col}/docs", func(w http.ResponseWriter, r *http.Request) {
		var filter map[string]interface{}
		if f := r.URL.Query().Get("filter"); f != "" {
			json.Unmarshal([]byte(f), &filter)
		}
		limit := 0
		if l := r.URL.Query().Get("limit"); l != "" {
			limit, _ = strconv.Atoi(l)
		}
		docs := e.Query(r.PathValue("col"), filter, r.URL.Query().Get("sort"), limit)
		writeJSON(w, 200, map[string]interface{}{"count": len(docs), "docs": docs})
	})
	mux.HandleFunc("POST /api/placement/refresh", func(w http.ResponseWriter, r *http.Request) {
		if err := e.RefreshPlacement(); err != nil {
			writeJSON(w, 502, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]interface{}{"nodes": e.Nodes()})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
