// Package api exposes the client-facing HTTP API and routes writes to
// ring-owners, fanning out replication in the background.
package api

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"microdb/internal/cluster"
	"microdb/internal/metrics"
	"microdb/internal/migrate"
	"microdb/internal/ring"
	"microdb/internal/store"
	"microdb/internal/tenants"
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
	rf      int // replication factor (ring owners consulted per write)
	idem    *idempotencyCache
	tenants *tenants.Registry
}

// SetTenants enables multi-tenant admission control on the public
// API: bearer tokens resolve to tenants, collections must live under
// the tenant's "<name>." prefix, requests are rate-limited per
// tenant, and writes are checked against the tenant's doc quota.
func (s *Server) SetTenants(r *tenants.Registry) { s.tenants = r }

// ReplicationFactor returns the configured RF.
func (s *Server) ReplicationFactor() int { return s.rf }

// rfFor resolves the effective replication factor for a collection:
// a per-collection override (stored in the reserved _config
// collection) wins over the node-level --rf default.
func (s *Server) rfFor(col string) int {
	return s.st.EffectiveRF(col, s.rf)
}

// handleConfigCollection sets per-collection settings over the API:
// PUT /api/collections/{col}/config {"rf": 5}
func (s *Server) handleConfigCollection(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "read body"})
		return
	}
	var in struct {
		RF int `json:"rf"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		writeJSON(w, 400, map[string]string{"error": "body must be {\"rf\": n}"})
		return
	}
	if in.RF < 0 || in.RF > 9 {
		writeJSON(w, 400, map[string]string{"error": "rf must be 0..9 (0 clears the override)"})
		return
	}
	cfgDoc, err := s.st.SetCollectionConfig(col, store.CollectionConfig{RF: in.RF})
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	// Replicate the config change like any other write.
	s.fanout(cfgDoc)
	writeJSON(w, 200, map[string]interface{}{"collection": col, "rf": in.RF, "effective": s.rfFor(col)})
}

// handleGetConfigCollection reads the current settings.
func (s *Server) handleGetConfigCollection(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	cfg := s.st.GetCollectionConfig(col)
	writeJSON(w, 200, map[string]interface{}{"collection": col, "rf": cfg.RF, "effective": s.rfFor(col)})
}

type replTask struct {
	peer string
	doc  *store.Doc
}

// Version is the build identity; stamped at release time.
const Version = "0.9.0"

var startTime = time.Now()

func goVersion() string {
	return runtime.Version()
}

func New(self string, st *store.Store, cl *cluster.Cluster) *Server {
	return NewWithRF(self, st, cl, 3)
}

// NewWithRF builds a server with an explicit replication factor.
func NewWithRF(self string, st *store.Store, cl *cluster.Cluster, rf int) *Server {
	if rf < 1 {
		rf = 1
	}
	s := &Server{st: st, cl: cl, self: self, repl: make(chan replTask, 256), mux: http.NewServeMux(), fwdClient: &http.Client{Timeout: 5 * time.Second}, rf: rf, idem: newIdempotencyCache(10 * time.Minute)}
	s.ring = ring.Build([]string{self})
	cl.OnPeersChanged = s.rebuildRing

	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /ready", s.handleReady)
	s.mux.HandleFunc("GET /version", s.handleVersion)
	// Public API is registered twice: unversioned (canonical) and
	// under /v1 for clients that pin a version.
	for _, prefix := range []string{"/api", "/v1/api"} {
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/docs/{id}", s.handleGet)
		s.mux.HandleFunc("PUT "+prefix+"/collections/{col}/docs/{id}", s.handlePut)
		s.mux.HandleFunc("POST "+prefix+"/collections/{col}/docs/batch", s.handleBatch)
		s.mux.HandleFunc("DELETE "+prefix+"/collections/{col}/docs/{id}", s.handleDelete)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/docs", s.handleQuery)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/watch", s.handleWatch)
		s.mux.HandleFunc("PUT "+prefix+"/collections/{col}/config", s.handleConfigCollection)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/config", s.handleGetConfigCollection)
		s.mux.HandleFunc("PUT "+prefix+"/collections/{col}/schema", s.handlePutSchema)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/schema", s.handleGetSchema)
		s.mux.HandleFunc("GET "+prefix+"/cluster", s.handleCluster)
	}
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)

	// internal replication + membership endpoints
	s.mux.HandleFunc("POST /internal/join", cl.HandleJoin)
	s.mux.HandleFunc("POST /internal/gossip", cl.HandleGossip)
	s.mux.HandleFunc("POST /internal/replicate", cl.HandleReplicate)
	s.mux.HandleFunc("POST /internal/replicate_bulk", cl.HandleReplicateBulk)
	s.mux.HandleFunc("GET /internal/collections", cl.HandleCollections)
	s.mux.HandleFunc("GET /internal/doc/{col}/{id}", s.handleInternalDoc)
	s.mux.HandleFunc("GET /internal/merkle/{col}", cl.HandleMerkle)
	s.mux.HandleFunc("GET /internal/merkle/{col}/meta", cl.HandleMerkleMeta)
	s.mux.HandleFunc("GET /internal/merkle/{col}/node", cl.HandleMerkleNode)
	s.mux.HandleFunc("GET /internal/merkle/{col}/leaf", cl.HandleMerkleLeaf)
	s.mux.HandleFunc("POST /internal/antientropy", cl.HandleAntiEntropy)
	s.mux.HandleFunc("POST /internal/leave", cl.HandleLeave)
	s.mux.HandleFunc("POST /internal/decommission", cl.HandleDecommission)
	s.mux.HandleFunc("GET /internal/stream/{col}", cl.HandleStream)
	s.mux.HandleFunc("GET /internal/bootstrap", cl.HandleBootstrapStatus)
	s.mux.HandleFunc("GET /internal/owners/{col}/{id}", s.handleOwners)
	s.mux.HandleFunc("GET /internal/hints", cl.HandleHints)
	s.mux.HandleFunc("GET /internal/scan/{col}", s.handleInternalScan)

	go s.replicationWorker()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Cluster identity guard: internal traffic must carry our name.
	// Prevents a misconfigured node from silently merging two
	// clusters' data and rings.
	if expected := s.cl.ClusterName(); expected != "" && strings.HasPrefix(r.URL.Path, "/internal/") {
		got := r.Header.Get("X-Microdb-Cluster")
		if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
			atomic.AddInt64(metrics.Default.Counter("microdb_cluster_mismatch_total", "Internal requests rejected for cluster mismatch"), 1)
			writeJSON(w, 403, map[string]interface{}{
				"error":  "cluster name mismatch — this node belongs to a different cluster",
				"wanted": expected,
				"got":    got,
			})
			return
		}
	}
	// Multi-tenant admission: on the public API, a bearer token must
	// resolve to a tenant, the collection must live under the
	// tenant's "<name>." namespace, and the tenant's rate bucket
	// must have a token. (Doc quota is checked per-write in the
	// handlers, where the exact delta is known.)
	if s.tenants != nil && !s.tenants.Empty() && (strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/v1/api/")) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		t, ok := s.tenants.Lookup(tok)
		if !ok {
			writeJSON(w, 401, map[string]string{"error": "unknown tenant token"})
			return
		}
		// Extract the collection from the path (/api/collections/{col}/...)
		// — PathValue is only populated after mux routing, which hasn't
		// happened yet at this point in ServeHTTP.
		if col := collectionFromPath(r.URL.Path); col != "" && !tenants.PrefixAllowed(t, col) {
			writeJSON(w, 403, map[string]string{"error": fmt.Sprintf("collection %q is outside tenant %q namespace", col, t.Name)})
			return
		}
		if allowed, retry := s.tenants.AllowRate(t); !allowed {
			w.Header().Set("Retry-After", fmt.Sprintf("%d", int(retry.Seconds())+1))
			writeJSON(w, 429, map[string]string{"error": "tenant rate limit exceeded"})
			return
		}
	}
	// Idempotency: mutating requests carrying an Idempotency-Key are
	// executed at most once per TTL; replays get the cached response.
	key := r.Header.Get("Idempotency-Key")
	if key == "" || s.idem.ttl <= 0 || (r.Method != "PUT" && r.Method != "POST" && r.Method != "DELETE") {
		s.mux.ServeHTTP(w, r)
		return
	}
	if e, ok := s.idem.get(key); ok {
		atomic.AddInt64(metrics.Default.Counter("microdb_idempotent_replays_total", "Requests answered from idempotency cache"), 1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Idempotent-Replay", "true")
		w.WriteHeader(e.status)
		w.Write(e.body)
		return
	}
	rec := &recordingWriter{hdr: http.Header{}, body: &bytes.Buffer{}}
	s.mux.ServeHTTP(rec, r)
	s.idem.put(key, idemEntry{status: rec.status, body: rec.body.Bytes(), created: time.Now()})
	for k, v := range rec.hdr {
		for _, vv := range v {
			w.Header().Add(k, vv)
		}
	}
	w.WriteHeader(rec.status)
	w.Write(rec.body.Bytes())
}

// SetIdempotencyTTL configures the dedupe window for Idempotency-Key
// retries. 0 disables deduplication entirely.
func (s *Server) SetIdempotencyTTL(d time.Duration) { s.idem = newIdempotencyCache(d) }

// RequireInternalTLS wraps a handler so that /internal/* paths must
// present a verified client certificate (mutual TLS). Public API
// paths are unaffected. Use when serving with TLS; without TLS the
// internal endpoints remain open (dev mode).
func RequireInternalTLS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/") {
			if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
				writeJSON(w, 403, map[string]string{"error": "internal endpoints require a client certificate"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// idempotencyCache provides a short-lived dedupe cache for client
// retries: a PUT/POST carrying an Idempotency-Key header is executed
// at most once within the TTL; replays get the original response
// replayed from the cache. This makes client retries safe under
// network flakiness (no double-apply, no version churn).
type idempotencyCache struct {
	mu      sync.Mutex
	entries map[string]idemEntry
	ttl     time.Duration
}

type idemEntry struct {
	status  int
	body    []byte
	created time.Time
}

func newIdempotencyCache(ttl time.Duration) *idempotencyCache {
	return &idempotencyCache{entries: map[string]idemEntry{}, ttl: ttl}
}

func (c *idempotencyCache) get(key string) (idemEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Since(e.created) > c.ttl {
		if ok {
			delete(c.entries, key)
		}
		return idemEntry{}, false
	}
	return e, true
}

func (c *idempotencyCache) put(key string, e idemEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Opportunistic GC of expired entries.
	for k, v := range c.entries {
		if time.Since(v.created) > c.ttl {
			delete(c.entries, k)
		}
	}
	c.entries[key] = e
}

// recordingWriter captures a response for later replay.
type recordingWriter struct {
	hdr    http.Header
	body   *bytes.Buffer
	status int
	wrote  bool
}

func (r *recordingWriter) Header() http.Header { return r.hdr }
func (r *recordingWriter) Write(b []byte) (int, error) {
	if !r.wrote {
		r.status = 200
		r.wrote = true
	}
	return r.body.Write(b)
}
func (r *recordingWriter) WriteHeader(code int) {
	if !r.wrote {
		r.status = code
		r.wrote = true
	}
}

// Backpressure sheds load before the cluster melts: at most maxInflight
// requests are processed concurrently; over-cap requests get an
// immediate 429 + Retry-After instead of queueing until everything
// times out. /health is never shed (load balancers must see the node).
func Backpressure(maxInflight int64, next http.Handler) http.Handler {
	var inflight atomic.Int64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		cur := inflight.Add(1)
		if cur > maxInflight {
			inflight.Add(-1)
			atomic.AddInt64(metrics.Default.Counter("microdb_shed_total", "Requests shed by backpressure"), 1)
			w.Header().Set("Retry-After", "1")
			writeJSON(w, 429, map[string]interface{}{
				"error":    "server busy, retry later",
				"inflight": cur - 1,
				"limit":    maxInflight,
			})
			return
		}
		atomic.StoreInt64(metrics.Default.Gauge("microdb_inflight", "Requests currently in flight"), inflight.Load())
		defer func() {
			inflight.Add(-1)
			atomic.StoreInt64(metrics.Default.Gauge("microdb_inflight", "Requests currently in flight"), inflight.Load())
		}()
		next.ServeHTTP(w, r)
	})
}

// Tracing assigns a trace ID to every request (honoring an incoming
// X-Trace-Id from the client for cross-service correlation), times the
// request, logs slow requests, and echoes the ID back. Latency is
// bucketed into metrics so tail causes are findable.
func Tracing(slowMs int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := r.Header.Get("X-Trace-Id")
		if traceID == "" {
			traceID = newTraceID()
		}
		w.Header().Set("X-Trace-Id", traceID)
		next.ServeHTTP(w, r)
		elapsed := time.Since(start)
		atomic.AddInt64(metrics.Default.Counter("microdb_requests_total", "Requests served"), 1)
		// Rough latency buckets: fast (<50ms), slow (<500ms), very slow.
		switch {
		case elapsed < 50*time.Millisecond:
			atomic.AddInt64(metrics.Default.Counter("microdb_latency_fast_total", "Requests under 50ms"), 1)
		case elapsed < 500*time.Millisecond:
			atomic.AddInt64(metrics.Default.Counter("microdb_latency_slow_total", "Requests 50-500ms"), 1)
		default:
			atomic.AddInt64(metrics.Default.Counter("microdb_latency_very_slow_total", "Requests over 500ms"), 1)
			log.Printf("slow request trace=%s %s %s took %s", traceID, r.Method, r.URL.Path, elapsed)
		}
		if elapsed > time.Duration(slowMs)*time.Millisecond {
			log.Printf("trace=%s SLOW-MARK %s %s %s", traceID, r.Method, r.URL.Path, elapsed)
		}
	})
}

var traceCounter atomic.Int64

func newTraceID() string {
	n := traceCounter.Add(1)
	return fmt.Sprintf("%x-%x", time.Now().UnixNano(), n)
}

// RequireAPIAuth protects /api/* with a bearer token (constant-time
// compared). /health stays open for load balancers. Empty token
// disables the check entirely.
func RequireAPIAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || !(strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/v1/api/")) {
			next.ServeHTTP(w, r)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			writeJSON(w, 401, map[string]string{"error": "missing or invalid bearer token"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) rebuildRing(addrs []string) {
	nodes := append([]string{s.self}, addrs...)
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.ring = ring.BuildWithEpoch(nodes, s.cl.Epoch())
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

// Owns reports whether this node is the primary owner of col/id.
func (s *Server) Owns(col, id string) bool {
	return s.owns(col + "/" + id)
}

// OwnerSet returns the RF-owner set for col/id under the current ring
// (exposed for tests and admin tooling).
func (s *Server) OwnerSet(col, id string) []string {
	return s.currentRing().Owners(col+"/"+id, s.rfFor(col))
}

// owns returns true if this node is the primary owner of the key.
func (s *Server) owns(key string) bool {
	owners := s.currentRing().Owners(key, 1)
	return len(owners) > 0 && owners[0] == s.self
}

func (s *Server) fanout(d *store.Doc) {
	key := d.Collection + "/" + d.ID
	for _, peer := range s.currentRing().Owners(key, s.rfFor(d.Collection)) {
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

// handleReady is the load-balancer readiness probe: 200 only when
// this node can actually serve traffic — bootstrapped (has its data),
// not draining, and (if the cluster expects peers) has seen the mesh.
// /health stays 200 whenever the process is up; /ready gates traffic.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.cl.Draining() {
		writeJSON(w, 503, map[string]interface{}{"ready": false, "reason": "decommissioning"})
		return
	}
	if s.cl.Streaming() {
		writeJSON(w, 503, map[string]interface{}{"ready": false, "reason": "bootstrapping"})
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ready": true, "node": s.self, "peers": len(s.cl.Peers())})
}

// handleVersion reports build identity for ops dashboards.
func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"version":   Version,
		"go":        goVersion(),
		"node":      s.self,
		"rf":        s.rf,
		"cluster":   s.cl.ClusterName(),
		"uptime_s":  time.Since(startTime).Seconds(),
	})
}

// migrateDoc returns a schema-migrated COPY of d if the collection
// has a schema and d is behind it; otherwise returns d unchanged.
// The stored doc is never mutated on the read path — the transform
// happens per-response, so migration is invisible, lazy, and safe
// under concurrent readers.
func (s *Server) migrateDoc(col string, d *store.Doc) *store.Doc {
	if d == nil || col == store.ConfigCollection {
		return d
	}
	sch, ok := s.st.GetCollectionSchema(col)
	if !ok || sch.Version <= migrate.DocVer(d.Fields) {
		return d
	}
	cp := *d
	cp.Fields = make(map[string]interface{}, len(d.Fields)+1)
	for k, v := range d.Fields {
		cp.Fields[k] = v
	}
	if sch.Apply(cp.Fields) {
		return &cp
	}
	return d
}

// migrateDocs maps migrateDoc over a slice.
func (s *Server) migrateDocs(col string, docs []*store.Doc) []*store.Doc {
	out := make([]*store.Doc, len(docs))
	for i, d := range docs {
		out[i] = s.migrateDoc(col, d)
	}
	return out
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	consistency := r.URL.Query().Get("consistency")
	switch consistency {
	case "quorum", "all":
		s.quorumGet(w, col, id, consistency == "all")
		return
	case "one", "local_quorum", "local_one", "":
		// 'one' / 'local_quorum' on a read both mean: answer from the
		// local replica, no cross-node round-trips. (Cassandra's
		// LOCAL_QUORUM read differs from ONE only across replicas of
		// the local DC; single-DC microdb collapses them.)
	default:
		writeJSON(w, 400, map[string]string{"error": "unknown consistency: use one|quorum|all|local_quorum"})
		return
	}
	d, ok := s.st.Get(col, id)
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, 200, s.migrateDoc(col, d))
}

// quorumGet reads a doc from self + all live peers, merges by
// (ver, ts), and returns the newest only if enough replicas answered.
// Any replica seen to lag gets the newest version pushed to it
// (read-repair) so the next read is consistent everywhere.
func (s *Server) quorumGet(w http.ResponseWriter, col, id string, readAll bool) {
	members := append([]string{s.self}, s.cl.Peers()...)
	need := len(members)/2 + 1
	if readAll {
		need = len(members)
	}
	var newest *store.Doc
	answered := 0
	var errs []string
	got := make(map[string]*store.Doc, len(members)) // member -> doc (nil = not found)
	for _, m := range members {
		d, err := s.fetchDocFrom(m, col, id)
		if err != nil {
			errs = append(errs, m+": "+err.Error())
			continue
		}
		answered++
		got[m] = d
		if d != nil && (newest == nil || d.Ver > newest.Ver || (d.Ver == newest.Ver && d.TS > newest.TS)) {
			newest = d
		}
	}
	if answered < need {
		writeJSON(w, 503, map[string]interface{}{
			"error":    "quorum not reached",
			"answered": answered,
			"need":     need,
			"errors":   errs,
		})
		return
	}
	if newest == nil {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	// Read-repair: push the newest version to any replica that lacks it
	// or holds a stale copy (using results already fetched — no re-reads).
	for _, m := range members {
		d := got[m]
		if d == nil || d.Ver < newest.Ver || (d.Ver == newest.Ver && d.TS < newest.TS) {
			go func(peer string) { _ = s.cl.Replicate(peer, newest) }(m)
		}
	}
	writeJSON(w, 200, s.migrateDoc(col, newest))
}

// fetchDocFrom reads one doc from a member (self or peer) without
// forwarding — direct local read via the internal scan-by-id endpoint.
func (s *Server) fetchDocFrom(member, col, id string) (*store.Doc, error) {
	if member == s.self {
		if d, ok := s.st.Get(col, id); ok {
			return d, nil
		}
		return nil, nil
	}
	u := member + "/internal/doc/" + col + "/" + id
	resp, err := s.fwdClient.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil, nil
	}
	var d store.Doc
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	return &d, nil
}

// collectionFromPath pulls {col} out of /api/collections/{col}/...
// (or the /v1 prefix). Returns "" when the path doesn't carry one.
func collectionFromPath(p string) string {
	segs := strings.Split(strings.Trim(p, "/"), "/")
	for i := 0; i+1 < len(segs); i++ {
		if segs[i] == "collections" {
			return segs[i+1]
		}
	}
	return ""
}

// tenantQuotaOK checks the tenant that owns `col` against its doc
// quota for admitting `delta` new live docs. No tenants configured,
// no tenant owning the collection, or unlimited quota -> true.
func (s *Server) tenantQuotaOK(col string, delta int64) (bool, *tenants.Tenant) {
	if s.tenants == nil || s.tenants.Empty() {
		return true, nil
	}
	name := tenants.TenantFromCollection(col)
	if name == "" {
		return true, nil
	}
	// Find the tenant by name (not token) for quota accounting.
	for _, t := range s.tenants.All() {
		if t.Name == name {
			return s.tenants.CheckQuota(t, s.st.CountUnder(name+"."), delta), t
		}
	}
	return true, nil
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
	if s.cl.Draining() {
		writeJSON(w, 503, map[string]string{"error": "node is decommissioning, retry elsewhere"})
		return
	}
	if !s.owns(key) {
		s.forwardBytes(w, r, "PUT", "/api/collections/"+col+"/docs/"+id, body, 0)
		return
	}
	// Fencing: reject writes from a forwarder whose ring epoch is
	// behind ours — its ownership view is stale and it must refresh.
	if h := r.Header.Get("X-Microdb-Epoch"); h != "" {
		if fwdEpoch, err := strconv.ParseInt(h, 10, 64); err == nil && fwdEpoch < s.currentRing().Epoch() {
			writeJSON(w, 409, map[string]interface{}{"error": "stale ring epoch", "epoch": s.currentRing().Epoch()})
			return
		}
	}
	if _, existed := s.st.Get(col, id); !existed {
		if ok, t := s.tenantQuotaOK(col, 1); !ok {
			writeJSON(w, 403, map[string]interface{}{"error": "tenant storage quota exceeded", "tenant": t.Name, "max_docs": t.MaxDocs})
			return
		}
	}
	d, err := s.st.Apply(col, id, fields)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	// Tunable write consistency:
	//   one          — local apply only; replicas converge async (default)
	//   quorum       — block until W = majority of the RF-owner set acked
	//   local_quorum — same as quorum in single-DC microdb
	//   all          — block until every RF owner acked
	consistency := r.URL.Query().Get("consistency")
	if consistency == "quorum" || consistency == "all" || consistency == "local_quorum" {
		members := s.currentRing().Owners(key, s.rfFor(col))
		need := len(members)/2 + 1 // W = majority of the RF-owner set
		if consistency == "all" {
			need = len(members)
		}
		acked := 1 // local apply counts
		var werrs []string
		for _, peer := range members {
			if peer == s.self || acked >= need {
				continue
			}
			if err := s.cl.Replicate(peer, d); err != nil {
				werrs = append(werrs, err.Error())
				continue
			}
			acked++
		}
		if acked < need {
			// The write IS applied locally and will converge via
			// anti-entropy/hints, but we cannot promise durability — say so.
			writeJSON(w, 503, map[string]interface{}{
				"error":    "write quorum not reached",
				"acked":    acked,
				"need":     need,
				"applied":  true,
				"errors":   werrs,
			})
			return
		}
	} else if consistency != "" && consistency != "one" {
		writeJSON(w, 400, map[string]string{"error": "unknown consistency: use one|quorum|all|local_quorum"})
		return
	}
	s.fanout(d)
	writeJSON(w, 200, d)
}

// handleBatch: POST /api/collections/{col}/docs/batch
// Body: {"docs": {"id1": {fields}, "id2": {fields}, ...}}
// Applies all docs in one atomic log record, then fans each changed
// doc out to its ring owners. One round-trip for bulk imports.
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	if s.cl.Draining() {
		writeJSON(w, 503, map[string]string{"error": "node is decommissioning, retry elsewhere"})
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20)) // 32 MiB cap
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "read body"})
		return
	}
	var in struct {
		Docs map[string]map[string]interface{} `json:"docs"`
	}
	if err := json.Unmarshal(body, &in); err != nil || len(in.Docs) == 0 {
		writeJSON(w, 400, map[string]string{"error": `body must be {"docs": {id: fields, ...}}`})
		return
	}
	var newCount int64
	for id := range in.Docs {
		if _, exists := s.st.Get(col, id); !exists {
			newCount++
		}
	}
	if ok, t := s.tenantQuotaOK(col, newCount); !ok {
		writeJSON(w, 403, map[string]interface{}{"error": "tenant storage quota exceeded", "tenant": t.Name, "max_docs": t.MaxDocs})
		return
	}
	docs, err := s.st.ApplyBatch(col, in.Docs)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	for _, d := range docs {
		s.fanout(d)
	}
	writeJSON(w, 200, map[string]interface{}{"applied": len(docs), "docs": docs})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	key := col + "/" + id
	if s.cl.Draining() {
		writeJSON(w, 503, map[string]string{"error": "node is decommissioning, retry elsewhere"})
		return
	}
	if !s.owns(key) {
		s.forwardBytes(w, r, "DELETE", "/api/collections/"+col+"/docs/"+id, nil, 0)
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
// be replayed to the owner. The forwarder's ring epoch travels in a
// header; if the owner is on a newer epoch it answers 409 + its epoch,
// we adopt the epoch, rebuild our ring, and retry once. Bounded to one
// hop otherwise to avoid loops.
func (s *Server) forwardBytes(w http.ResponseWriter, r *http.Request, method, path string, body []byte, attempt int) {
	if r.Header.Get("X-Microdb-Hop") != "" {
		writeJSON(w, 503, map[string]string{"error": "ownership unstable, retry"})
		return
	}
	owner := s.currentRing().Owners(r.PathValue("col")+"/"+r.PathValue("id"), 1)
	if len(owner) == 0 {
		writeJSON(w, 503, map[string]string{"error": "no owner known"})
		return
	}
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, owner[0]+path, rdr)
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	// The hop marker guards against forwarding loops on the FIRST hop.
	// A post-fencing retry must NOT carry it: we adopted a fresher ring
	// view, so this attempt is a fresh first-hop, not a loop.
	if attempt == 0 {
		req.Header.Set("X-Microdb-Hop", "1")
	}
	req.Header.Set("X-Microdb-Epoch", strconv.FormatInt(s.currentRing().Epoch(), 10))
	resp, err := s.fwdClient.Do(req)
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": "forward failed: " + err.Error()})
		return
	}
	// Fencing: the owner says our ownership view is stale.
	if resp.StatusCode == 409 {
		var fence struct {
			Epoch int64 `json:"epoch"`
		}
		json.NewDecoder(resp.Body).Decode(&fence)
		resp.Body.Close()
		// Retry with a small budget: gossip can bump the epoch again
		// between our adopt and our retry, so one shot isn't enough.
		if fence.Epoch > s.currentRing().Epoch() && attempt < 3 {
			s.cl.AdoptEpoch(fence.Epoch) // rebuilds ring via callback
			time.Sleep(time.Duration(20*(attempt+1)) * time.Millisecond)
			s.forwardBytes(w, r, method, path, body, attempt+1)
			return
		}
		writeJSON(w, 503, map[string]interface{}{"error": "ownership unstable, retry", "epoch": fence.Epoch})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// handleQuery: GET /api/collections/{col}/docs?filter=<url-encoded JSON>
//   &sort=<field>&desc=true&limit=N&offset=M
//
// Scatter-gather: the receiving node fans the query out to every live
// cluster member (including itself), merges results by doc id keeping
// the highest (ver, ts), then applies sort and pagination to the
// merged set.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	var filter map[string]interface{}
	if f := r.URL.Query().Get("filter"); f != "" {
		if err := json.Unmarshal([]byte(f), &filter); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad filter JSON"})
			return
		}
	}
	sortField := r.URL.Query().Get("sort")
	desc := r.URL.Query().Get("desc") == "true"
	limit := -1
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 0 {
			writeJSON(w, 400, map[string]string{"error": "bad limit"})
			return
		}
		limit = n
	}
	offset := 0
	if o := r.URL.Query().Get("offset"); o != "" {
		n, err := strconv.Atoi(o)
		if err != nil || n < 0 {
			writeJSON(w, 400, map[string]string{"error": "bad offset"})
			return
		}
		offset = n
	}

	// Gather from every member concurrently: self + peers. Peer
	// gathers run in parallel; if a peer hasn't answered within the
	// hedge window we fire a duplicate gather at another member and
	// take whichever response arrives first (hedged reads — the
	// standard tail-latency trick for cross-shard scans).
	members := append([]string{s.self}, s.cl.Peers()...)
	hedgeMs := 0
	if h := r.URL.Query().Get("hedge_ms"); h != "" {
		n, err := strconv.Atoi(h)
		if err != nil || n < 10 {
			writeJSON(w, 400, map[string]string{"error": "bad hedge_ms (min 10)"})
			return
		}
		hedgeMs = n
	}

	type gatherResult struct {
		docs      []*store.Doc
		truncated bool
		err       error
	}
	// Pushdown: when the client asked for a bounded window, each shard
	// returns only its top (offset+limit) docs in the requested order.
	// The global top window is always contained in that union, so we
	// transfer O(shards * window) instead of O(all docs).
	pushdown := limit >= 0
	capN := -1
	if pushdown {
		capN = offset + limit
	}
	gatherOne := func(m string) gatherResult {
		if m == s.self {
			docs := s.migrateDocs(col, s.st.ScanIndexed(col, filter))
			trunc := false
			if pushdown {
				sort.SliceStable(docs, func(i, j int) bool {
					c := store.CompareValues(docs[i].Fields[sortField], docs[j].Fields[sortField])
					if desc {
						return c > 0
					}
					return c < 0
				})
				if len(docs) > capN {
					docs = docs[:capN]
					trunc = true
				}
			}
			return gatherResult{docs: docs, truncated: trunc}
		}
		docs, trunc, err := s.gatherFrom(m, col, r.URL.Query().Get("filter"), sortField, desc, capN)
		return gatherResult{docs: docs, truncated: trunc, err: err}
	}
	hedged := hedgeMs > 0 && len(members) >= 2
	results := make(chan gatherResult, len(members)*2)
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Add(1)
		go func(m string, idx int) {
			defer wg.Done()
			if !hedged {
				results <- gatherOne(m)
				return
			}
			// Primary with a hedge timer.
			primary := make(chan gatherResult, 1)
			go func() { primary <- gatherOne(m) }()
			timer := time.NewTimer(time.Duration(hedgeMs) * time.Millisecond)
			defer timer.Stop()
			select {
			case res := <-primary:
				results <- res
			case <-timer.C:
				// Slow tail: duplicate the gather at another member.
				hedgeTarget := members[(idx+1)%len(members)]
				secondary := make(chan gatherResult, 1)
				go func() {
					res := gatherOne(hedgeTarget)
					// A failed hedge must not mark the whole response
					// partial — the primary still reports its own
					// outcome, and the hedge target's own primary
					// gather covers its shard.
					if res.err == nil {
						secondary <- res
					}
				}()
				select {
				case res := <-primary:
					results <- res
				case res := <-secondary:
					results <- res
				}
			}
		}(m, i)
	}
	go func() { wg.Wait(); close(results) }()

	merged := map[string]*store.Doc{}
	var errs []string
	anyTruncated := false
	for res := range results {
		if res.err != nil {
			errs = append(errs, res.err.Error())
			continue
		}
		if res.truncated {
			anyTruncated = true
		}
		for _, d := range res.docs {
			if cur, ok := merged[d.ID]; !ok || d.Ver > cur.Ver || (d.Ver == cur.Ver && d.TS > cur.TS) {
				merged[d.ID] = d
			}
		}
	}
	out := make([]*store.Doc, 0, len(merged))
	for _, d := range merged {
		out = append(out, d)
	}
	if sortField != "" {
		sort.SliceStable(out, func(i, j int) bool {
			c := store.CompareValues(out[i].Fields[sortField], out[j].Fields[sortField])
			if desc {
				return c > 0
			}
			return c < 0
		})
	} else {
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	}
	total := len(out)
	if offset > 0 {
		if offset >= len(out) {
			out = nil
		} else {
			out = out[offset:]
		}
	}
	if limit >= 0 && len(out) > limit {
		out = out[:limit]
	}
	resp := map[string]interface{}{
		"count":         len(out),
		"total":       total,
		"docs":        out,
		"nodes_queried": len(members),
	}
	// With pushdown the merged set may be a truncated view, so the
	// pre-pagination total is a lower bound, not exact.
	if anyTruncated {
		resp["total_exact"] = false
	} else {
		resp["total_exact"] = true
	}
	if len(errs) > 0 {
		resp["partial"] = true
		resp["errors"] = errs
	}
	writeJSON(w, 200, resp)
}

// gatherFrom asks one peer for its local matching docs for a collection.
// When cap >= 0 the peer sorts by sortField (desc per desc) and returns
// only its top `cap` docs plus whether it truncated — the global
// top-K always fits inside the union of per-shard top-K sets.
func (s *Server) gatherFrom(peer, col, filter string, sortField string, desc bool, cap int) ([]*store.Doc, bool, error) {
	u := peer + "/internal/scan/" + col
	sep := "?"
	if filter != "" {
		u += sep + "filter=" + filter
		sep = "&"
	}
	if cap >= 0 {
		u += sep + "cap=" + strconv.Itoa(cap)
		if sortField != "" {
			u += "&sort=" + sortField
		}
		if desc {
			u += "&desc=true"
		}
	}
	resp, err := s.fwdClient.Get(u)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	var out struct {
		Docs      []*store.Doc `json:"docs"`
		Truncated bool       `json:"truncated"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, false, err
	}
	return out.Docs, out.Truncated, nil
}

// handleWatch: GET /api/collections/{col}/watch?since=<seq>&wait=30
// Long-poll feed of local mutations (direct writes + replicated
// applies observed by this node). Returns events with seq > since as
// NDJSON, flushing as they arrive until the wait window closes.
// Clients pass the last seq they saw; first call: since=0.
func (s *Server) handleWatch(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	since := int64(0)
	if v := r.URL.Query().Get("since"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad since"})
			return
		}
		since = n
	}
	wait := 30 * time.Second
	if v := r.URL.Query().Get("wait"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 300 {
			writeJSON(w, 400, map[string]string{"error": "wait must be 1..300 seconds"})
			return
		}
		wait = time.Duration(n) * time.Second
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, 500, map[string]string{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(200)
	flusher.Flush() // send headers immediately so the client can start reading

	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		evs := s.st.ChangeLog().WaitFiltered(col, since, remaining)
		if len(evs) == 0 {
			continue
		}
		for _, e := range evs {
			if e.Kind == "upsert" && e.Doc != nil {
				if d, ok := e.Doc.(*store.Doc); ok {
					e.Doc = s.migrateDoc(col, d)
				}
			}
			b, _ := json.Marshal(e)
			w.Write(append(b, '\n'))
			since = e.Seq
		}
		flusher.Flush()
	}
}

// handleOwners serves the RF-owner set + ring epoch for a key so
// shard-map-aware clients can route directly to the primary instead
// of relying on server-side forwarding.
func (s *Server) handleOwners(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	writeJSON(w, 200, map[string]interface{}{
		"owners": s.OwnerSet(col, id),
		"epoch":  s.currentRing().Epoch(),
	})
}

// handleInternalDoc serves a single local doc for quorum reads by
// peers. 404 when absent or tombstoned.
func (s *Server) handleInternalDoc(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	d, ok := s.st.Get(col, id)
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, 200, s.migrateDoc(col, d))
}

// handleInternalScan serves one shard of a scatter-gather query:
// local matching docs only, no further fanout.
//
// Pushdown: when the coordinator passes sort/desc/cap, this shard
// sorts locally and returns only its top `cap` docs — the global
// top-(offset+limit) is always contained in the union of the
// per-shard top-(offset+limit), so the coordinator can merge a
// small set instead of full shards. `total` reports the shard's FULL
// match count (before truncation) so the coordinator's global total
// stays exact.
func (s *Server) handleInternalScan(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	var filter map[string]interface{}
	if f := r.URL.Query().Get("filter"); f != "" {
		if err := json.Unmarshal([]byte(f), &filter); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad filter JSON"})
			return
		}
	}
	docs := s.migrateDocs(col, s.st.ScanIndexed(col, filter))
	sortField := r.URL.Query().Get("sort")
	desc := r.URL.Query().Get("desc") == "true"
	capN := -1
	if c := r.URL.Query().Get("cap"); c != "" {
		n, err := strconv.Atoi(c)
		if err != nil || n < 0 {
			writeJSON(w, 400, map[string]string{"error": "bad cap"})
			return
		}
		capN = n
	}
	total := len(docs)
	truncated := false
	if sortField != "" {
		sort.SliceStable(docs, func(i, j int) bool {
			c := store.CompareValues(docs[i].Fields[sortField], docs[j].Fields[sortField])
			if desc {
				return c > 0
			}
			return c < 0
		})
	} else {
		sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })
	}
	if capN >= 0 && len(docs) > capN {
		docs = docs[:capN]
		truncated = true
	}
	writeJSON(w, 200, map[string]interface{}{"docs": docs, "total": total, "truncated": truncated})
}

// handlePutSchema installs/updates a collection's migration schema:
//
//	PUT /api/collections/{col}/schema
//	{"transforms":[{"op":"rename","from":"city","to":"town"},
//	              {"op":"retype","from":"age","to":"int"}]}
//
// The schema version is len(transforms). Transforms are append-only:
// the new list must extend the current one (same prefix, version >=
// current), so already-migrated docs stay correct. Docs behind the
// new version are transformed lazily on read — no rewrite needed.
func (s *Server) handlePutSchema(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "read body"})
		return
	}
	var in struct {
		Transforms []migrate.Transform `json:"transforms"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		writeJSON(w, 400, map[string]string{"error": `body must be {"transforms":[...]}`})
		return
	}
	newSch := migrate.Schema{Version: len(in.Transforms), Transforms: in.Transforms}
	if err := newSch.Validate(); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if cur, ok := s.st.GetCollectionSchema(col); ok {
		if newSch.Version < cur.Version {
			writeJSON(w, 400, map[string]string{"error": "schema is append-only: new version must be >= current"})
			return
		}
		for i := 0; i < cur.Version && i < len(newSch.Transforms); i++ {
			if newSch.Transforms[i] != cur.Transforms[i] {
				writeJSON(w, 400, map[string]string{"error": fmt.Sprintf("transform %d already applied — cannot rewrite history", i)})
				return
			}
		}
	}
	doc, err := s.st.SetCollectionSchema(col, newSch)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	s.fanout(doc)
	writeJSON(w, 200, map[string]interface{}{"collection": col, "version": newSch.Version})
}

// handleGetSchema returns the collection's current schema (or
// {"version":0} when none is set).
func (s *Server) handleGetSchema(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	sch, ok := s.st.GetCollectionSchema(col)
	if !ok {
		writeJSON(w, 200, map[string]interface{}{"version": 0, "transforms": []migrate.Transform{}})
		return
	}
	writeJSON(w, 200, sch)
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"self": s.self, "peers": s.cl.Peers()})
}

// handleMetrics renders Prometheus text format with live gauges
// refreshed from current state before rendering.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	atomic.StoreInt64(metrics.Default.Gauge("microdb_docs", "Documents currently held (incl. tombstones)"), int64(s.st.DocCount()))
	atomic.StoreInt64(metrics.Default.Gauge("microdb_collections", "Collections currently held"), int64(len(s.st.Collections())))
	atomic.StoreInt64(metrics.Default.Gauge("microdb_peers", "Live cluster peers"), int64(len(s.cl.Peers())))
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(metrics.Default.Render()))
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
