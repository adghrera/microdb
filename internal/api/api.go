// Package api exposes the client-facing HTTP API and routes writes to
// ring-owners, fanning out replication in the background.
package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"microdb/internal/cluster"
	"microdb/internal/ring"
	"microdb/internal/store"
)

type Server struct {
	st      *store.Store
	cl      *cluster.Cluster
	ringMu  sync.RWMutex
	ring    *ring.Ring
	self    string
	repl    chan replTask
	mux     *http.ServeMux
	fwdClient *http.Client
}

type replTask struct {
	peer string
	doc  *store.Doc
}

func New(self string, st *store.Store, cl *cluster.Cluster) *Server {
	s := &Server{st: st, cl: cl, self: self, repl: make(chan replTask, 256), mux: http.NewServeMux(), fwdClient: &http.Client{Timeout: 5 * time.Second}}
	s.ring = ring.Build([]string{self})
	cl.OnPeersChanged = s.rebuildRing

	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /api/collections/{col}/docs/{id}", s.handleGet)
	s.mux.HandleFunc("PUT /api/collections/{col}/docs/{id}", s.handlePut)
	s.mux.HandleFunc("DELETE /api/collections/{col}/docs/{id}", s.handleDelete)
	s.mux.HandleFunc("GET /api/collections/{col}/docs", s.handleQuery)
	s.mux.HandleFunc("GET /api/cluster", s.handleCluster)

	// internal replication + membership endpoints
	s.mux.HandleFunc("POST /internal/join", cl.HandleJoin)
	s.mux.HandleFunc("POST /internal/gossip", cl.HandleGossip)
	s.mux.HandleFunc("POST /internal/replicate", cl.HandleReplicate)
	s.mux.HandleFunc("POST /internal/replicate_bulk", cl.HandleReplicateBulk)
	s.mux.HandleFunc("GET /internal/collections", cl.HandleCollections)
	s.mux.HandleFunc("GET /internal/merkle/{col}", cl.HandleMerkle)
	s.mux.HandleFunc("GET /internal/merkle/{col}/meta", cl.HandleMerkleMeta)
	s.mux.HandleFunc("GET /internal/merkle/{col}/node", cl.HandleMerkleNode)
	s.mux.HandleFunc("GET /internal/merkle/{col}/leaf", cl.HandleMerkleLeaf)
	s.mux.HandleFunc("POST /internal/antientropy", cl.HandleAntiEntropy)
	s.mux.HandleFunc("GET /internal/scan/{col}", s.handleInternalScan)

	go s.replicationWorker()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) rebuildRing(addrs []string) {
	nodes := append([]string{s.self}, addrs...)
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.ring = ring.Build(nodes)
}

func (s *Server) currentRing() *ring.Ring {
	s.ringMu.RLock()
	defer s.ringMu.RUnlock()
	return s.ring
}

func (s *Server) replicationWorker() {
	for t := range s.repl {
		if err := s.cl.Replicate(t.peer, t.doc); err != nil {
			log.Printf("replication to %s failed: %v", t.peer, err)
		}
	}
}

// owns returns true if this node is the primary owner of the key.
func (s *Server) owns(key string) bool {
	owners := s.currentRing().Owners(key, 1)
	return len(owners) > 0 && owners[0] == s.self
}

func (s *Server) fanout(d *store.Doc) {
	key := d.Collection + "/" + d.ID
	for _, peer := range s.currentRing().Owners(key, 3) {
		if peer != s.self {
			select {
			case s.repl <- replTask{peer: peer, doc: d}:
			default: // queue full — drop; converges via anti-entropy
			}
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok", "node": s.self})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	d, ok := s.st.Get(col, id)
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, 200, d)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "read body"})
		return
	}
	var fields map[string]interface{}
	if err := json.Unmarshal(body, &fields); err != nil {
		writeJSON(w, 400, map[string]string{"error": "body must be a JSON object"})
		return
	}
	key := col + "/" + id
	if !s.owns(key) {
		s.forwardBytes(w, r, "PUT", "/api/collections/"+col+"/docs/"+id, body)
		return
	}
	d, err := s.st.Apply(col, id, fields)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	s.fanout(d)
	writeJSON(w, 200, d)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	key := col + "/" + id
	if !s.owns(key) {
		s.forwardBytes(w, r, "DELETE", "/api/collections/"+col+"/docs/"+id, nil)
		return
	}
	if err := s.st.Delete(col, id); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]bool{"deleted": true})
}

// forwardBytes proxies a write to the current ring-owner so any node can
// accept any write. The body is passed as bytes (already read) so it can
// be replayed to the owner. Bounded to one hop to avoid loops.
func (s *Server) forwardBytes(w http.ResponseWriter, r *http.Request, method, path string, body []byte) {
	if r.Header.Get("X-Microdb-Hop") != "" {
		writeJSON(w, 503, map[string]string{"error": "ownership unstable, retry"})
		return
	}
	owner := s.currentRing().Owners(r.PathValue("col")+"/"+r.PathValue("id"), 1)
	if len(owner) == 0 {
		writeJSON(w, 503, map[string]string{"error": "no owner known"})
		return
	}
	req, err := http.NewRequest(method, owner[0]+path, bytes.NewReader(body))
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Microdb-Hop", "1")
	resp, err := s.fwdClient.Do(req)
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": "forward failed: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// handleQuery: GET /api/collections/{col}/docs?filter=<url-encoded JSON>
// Scatter-gather: the receiving node fans the query out to every live
// cluster member (including itself), merges results by doc id keeping
// the highest (ver, ts), and returns the union.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	var filter map[string]interface{}
	if f := r.URL.Query().Get("filter"); f != "" {
		if err := json.Unmarshal([]byte(f), &filter); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad filter JSON"})
			return
		}
	}
	keep := func(d *store.Doc) bool {
		return len(filter) == 0 || store.Matches(d, filter)
	}

	// Gather from every member: self + peers.
	members := append([]string{s.self}, s.cl.Peers()...)
	merged := map[string]*store.Doc{}
	var errs []string
	for _, m := range members {
		var docs []*store.Doc
		if m == s.self {
			docs = s.st.Scan(col, keep)
		} else {
			got, err := s.gatherFrom(m, col, r.URL.Query().Get("filter"))
			if err != nil {
				errs = append(errs, m+": "+err.Error())
				continue
			}
			docs = got
		}
		for _, d := range docs {
			if cur, ok := merged[d.ID]; !ok || d.Ver > cur.Ver || (d.Ver == cur.Ver && d.TS > cur.TS) {
				merged[d.ID] = d
			}
		}
	}
	out := make([]*store.Doc, 0, len(merged))
	for _, d := range merged {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	resp := map[string]interface{}{"count": len(out), "docs": out, "nodes_queried": len(members)}
	if len(errs) > 0 {
		resp["partial"] = true
		resp["errors"] = errs
	}
	writeJSON(w, 200, resp)
}

// gatherFrom asks one peer for its local matching docs for a collection.
func (s *Server) gatherFrom(peer, col, filter string) ([]*store.Doc, error) {
	u := peer + "/internal/scan/" + col
	if filter != "" {
		u += "?filter=" + filter
	}
	resp, err := s.fwdClient.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Docs []*store.Doc `json:"docs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Docs, nil
}

// handleInternalScan serves one shard of a scatter-gather query:
// local matching docs only, no further fanout.
func (s *Server) handleInternalScan(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	var filter map[string]interface{}
	if f := r.URL.Query().Get("filter"); f != "" {
		if err := json.Unmarshal([]byte(f), &filter); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad filter JSON"})
			return
		}
	}
	docs := s.st.Scan(col, func(d *store.Doc) bool {
		return len(filter) == 0 || store.Matches(d, filter)
	})
	writeJSON(w, 200, map[string]interface{}{"docs": docs})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"self": s.self, "peers": s.cl.Peers()})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
