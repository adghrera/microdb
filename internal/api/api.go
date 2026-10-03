// Package api exposes the client-facing HTTP API and routes writes to
// ring-owners, fanning out replication in the background.
package api

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"microdb/internal/audit"
	"microdb/internal/changelog"
	"microdb/internal/cluster"
	"microdb/internal/gsi"
	"microdb/internal/metrics"
	"microdb/internal/migrate"
	"microdb/internal/protocol"
	"microdb/internal/ranges"
	"microdb/internal/readcache"
	"microdb/internal/ring"
	"microdb/internal/storage"
	"microdb/internal/store"
	"microdb/internal/tenants"
)

// maxInternalBody bounds a checksummed internal payload: enough for
// the largest replication batch, small enough that a hostile or
// broken peer cannot make us buffer without limit.
const maxInternalBody = 64 << 20

type Server struct {
	st        *store.Store
	cl        *cluster.Cluster
	ringMu    sync.RWMutex
	ring      *ring.Ring
	self      string
	repl      chan replTask
	mux       *http.ServeMux
	fwdClient *http.Client
	rf        int // replication factor (ring owners consulted per write)
	idem      *idempotencyCache
	tenants   *tenants.Registry
	rcache    *readcache.Cache
	rec       *storage.Local // storage-tier record boundary (fenced)
	gsi       *gsi.Manager
	// Range map (load-adaptive contiguous ownership): nil until
	// EnableRangeMap. rangeBuckets counts writes per token high
	// byte; rangeEWMA is the smoothed writes/sec; rangePlan is the
	// last computed plan.
	rangeMu      *sync.RWMutex
	rangeBuckets []int64
	rangeEWMA    []float64
	rangePlan    *ranges.Plan
}

// EnableGSI starts the async global-secondary-index service: a
// background worker subscribed to the change feed maintains index
// collections off the write path.
func (s *Server) EnableGSI() {
	s.gsi = gsi.NewManager(s.st)
	s.gsi.Attach()
}

// GSI exposes the index manager (for tests/ops).
func (s *Server) GSI() *gsi.Manager { return s.gsi }

// EnableReadCache turns on a read-through cache for local (consistency
// = one) point reads. Entries are invalidated synchronously by every
// mutation observed in the change feed — local writes AND replicated
// applies — so a cached read is never staler than this node's own
// mutation stream. Capacity is total entries; maxAge is a safety-net
// freshness bound (0 = rely on invalidation only).
func (s *Server) EnableReadCache(capacity int, maxAge time.Duration) {
	s.rcache = readcache.New(capacity, maxAge)
	s.st.ChangeLog().OnAppend(func(ev changelog.Event) {
		s.rcache.Invalidate(ev.Collection + "\x00" + ev.ID)
	})
}

// CacheStats exposes read-cache counters (0s when disabled).
func (s *Server) CacheStats() (hits, misses, invalidations int64) {
	if s.rcache == nil {
		return 0, 0, 0
	}
	return s.rcache.Stats()
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
		RF             int    `json:"rf"`
		PartitionField string `json:"partition_field"`
		SortField      string `json:"sort_field"`
		MaxDocs        int64  `json:"max_docs"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		writeJSON(w, 400, map[string]string{"error": "body must be {\"rf\": n, \"partition_field\": \"x\", \"sort_field\": \"y\"}"})
		return
	}
	if in.RF < 0 || in.RF > 9 {
		writeJSON(w, 400, map[string]string{"error": "rf must be 0..9 (0 clears the override)"})
		return
	}
	// Preserve existing partition/sort settings when the caller only
	// sends rf (and vice versa).
	cur := s.st.GetCollectionConfig(col)
	pf := in.PartitionField
	if pf == "" {
		pf = cur.PartitionField
	}
	sf := in.SortField
	if sf == "" {
		sf = cur.SortField
	}
	md := in.MaxDocs
	if md == 0 {
		md = cur.MaxDocs
	}
	cfgDoc, err := s.st.SetCollectionConfig(col, store.CollectionConfig{RF: in.RF, PartitionField: pf, SortField: sf, MaxDocs: md})
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	// Replicate the config change like any other write.
	s.fanout(cfgDoc)
	writeJSON(w, 200, map[string]interface{}{"collection": col, "rf": in.RF, "effective": s.rfFor(col),
		"partition_field": pf, "sort_field": sf, "max_docs": md})
}

// handleGetConfigCollection reads the current settings.
func (s *Server) handleGetConfigCollection(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	cfg := s.st.GetCollectionConfig(col)
	writeJSON(w, 200, map[string]interface{}{
		"collection":      col,
		"rf":              cfg.RF,
		"effective":       s.rfFor(col),
		"partition_field": cfg.PartitionField,
		"sort_field":      cfg.SortField,
	})
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
	s := &Server{st: st, cl: cl, self: self, repl: make(chan replTask, 256), mux: http.NewServeMux(), fwdClient: &http.Client{Timeout: 5 * time.Second, Transport: &protoStampTransport{base: http.DefaultTransport}}, rf: rf, idem: newIdempotencyCache(10 * time.Minute), rec: storage.NewLocal(st)}
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
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/partition/{pk}", s.handlePartitionQuery)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/watch", s.handleWatch)
		s.mux.HandleFunc("PUT "+prefix+"/collections/{col}/config", s.handleConfigCollection)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/config", s.handleGetConfigCollection)
		s.mux.HandleFunc("PUT "+prefix+"/collections/{col}/schema", s.handlePutSchema)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/schema", s.handleGetSchema)
		s.mux.HandleFunc("PUT "+prefix+"/collections/{col}/gsi", s.handleDeclareGSI)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/gsi", s.handleListGSI)
		s.mux.HandleFunc("GET "+prefix+"/collections/{col}/gsi/{name}", s.handleLookupGSI)
		s.mux.HandleFunc("GET "+prefix+"/cluster", s.handleCluster)
	}
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /api/stats/collections", s.handleCollectionStats)

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
	s.mux.HandleFunc("POST /internal/repair", cl.HandleRepair)
	s.mux.HandleFunc("GET /internal/agg/{col}", s.handleInternalAgg)
	s.mux.HandleFunc("POST /internal/leave", cl.HandleLeave)
	s.mux.HandleFunc("POST /internal/decommission", cl.HandleDecommission)
	s.mux.HandleFunc("GET /internal/stream/{col}", cl.HandleStream)
	s.mux.HandleFunc("GET /internal/bootstrap", cl.HandleBootstrapStatus)
	s.mux.HandleFunc("GET /internal/owners/{col}/{id}", s.handleOwners)
	s.mux.HandleFunc("GET /internal/ranges", s.handleRangePlan)
	s.mux.HandleFunc("POST /internal/ranges/replan", s.handleRangeReplan)
	s.mux.HandleFunc("GET /internal/loadbuckets", s.handleLoadBuckets)
	s.mux.HandleFunc("GET /internal/hints", cl.HandleHints)
	s.mux.HandleFunc("GET /internal/scan/{col}", s.handleInternalScan)

	// Storage-tier record API: the disaggregation boundary. A
	// compute node (or a peer) speaks these instead of touching the
	// store directly. Fenced by X-Microdb-Epoch.
	for _, prefix := range []string{"/internal"} {
		s.mux.HandleFunc("PUT "+prefix+"/record/{col}/{id}", s.handleRecordPut)
		s.mux.HandleFunc("GET "+prefix+"/record/{col}/{id}", s.handleRecordGet)
		s.mux.HandleFunc("GET "+prefix+"/record/{col}/scan", s.handleRecordScan)
		s.mux.HandleFunc("GET "+prefix+"/record/_epoch", s.handleRecordEpoch)
	}

	go s.replicationWorker()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Audit: record mutations (and every denial below) with the status
	// they ended up with. Wrapping w and deferring covers every return
	// path in this function, including the early ones.
	if audit.Default.Enabled() && isAuditable(r) {
		aw := &auditRW{ResponseWriter: w, status: http.StatusOK}
		w = aw
		col, id := apiPathParts(r.URL.Path)
		e := audit.Event{
			Kind:       "mutation",
			Action:     r.Method,
			Decision:   "allow",
			Remote:     r.RemoteAddr,
			Collection: col,
			ID:         id,
			Trace:      r.Header.Get("X-Trace-Id"),
		}
		if tok := r.Header.Get("Authorization"); tok != "" {
			e.Actor = "bearer"
			// Name the tenant when the bearer token is one: an audit
			// record that cannot say WHO wrote something is half a
			// record.
			if s.tenants != nil {
				if tn, ok := s.tenants.Lookup(strings.TrimPrefix(tok, "Bearer ")); ok && tn != nil {
					e.Tenant = tn.Name
					e.Actor = "tenant:" + tn.Name
				}
			}
		} else {
			e.Actor = "anonymous"
		}
		defer func() {
			e.Status = aw.status
			if aw.status >= 400 {
				e.Decision = "deny"
				if e.Detail == "" {
					e.Detail = "rejected"
				}
			}
			audit.Default.Log(e)
		}()
	}

	// Wire-protocol negotiation (rolling upgrades). Internal traffic
	// outside our version window is refused loudly, before any of it
	// can be half-parsed into membership or data — a mixed-version mesh
	// must fail as "incompatible", never as silent corruption.
	if strings.HasPrefix(r.URL.Path, "/internal/") {
		pv, perr := protocol.Parse(r.Header.Get(protocol.Header))
		if perr != nil {
			pv = -1 // unparseable: report it as out of window
		}
		if perr != nil || protocol.CheckRequest(pv) != nil {
			atomic.AddInt64(metrics.Default.Counter("microdb_protocol_rejections_total", "Internal requests refused for wire-version mismatch"), 1)
			w.Header().Set(protocol.Header, protocol.String())
			writeJSON(w, 505, map[string]interface{}{
				"error":          "incompatible wire protocol — upgrade or roll back this node",
				"peer_version":   pv,
				"min_supported":  protocol.MinSupported,
				"max_supported":  protocol.Version,
				"server_version": protocol.Version,
			})
			return
		}
	}
	// Integrity: an internal body that declares a CRC32C must match it.
	// The body is buffered and re-wrapped, so handlers see no
	// difference — a truncated or corrupted payload is rejected before
	// it reaches replication, anti-entropy or membership state.
	if r.Body != nil && r.Header.Get(protocol.CRCHeader) != "" &&
		(r.Method == http.MethodPost || r.Method == http.MethodPut) {
		body, err := protocol.VerifyBody(r, maxInternalBody)
		if err != nil {
			atomic.AddInt64(metrics.Default.Counter("microdb_checksum_rejections_total",
				"Internal requests rejected for body checksum mismatch"), 1)
			w.Header().Set(protocol.Header, protocol.String())
			writeJSON(w, 400, map[string]interface{}{
				"error":  "internal request body failed its checksum",
				"detail": err.Error(),
			})
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	// Advertise our version on every reply so a peer can detect a
	// mismatch even on a 200, and so operators can curl it.
	w.Header().Set(protocol.Header, protocol.String())
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
			auditDeny(r, "auth", "", "", "unknown tenant token", 401)
			writeJSON(w, 401, map[string]string{"error": "unknown tenant token"})
			return
		}
		// Extract the collection from the path (/api/collections/{col}/...)
		// — PathValue is only populated after mux routing, which hasn't
		// happened yet at this point in ServeHTTP.
		if col := collectionFromPath(r.URL.Path); col != "" && !tenants.PrefixAllowed(t, col) {
			auditDeny(r, "auth", t.Name, col, "collection outside tenant namespace", 403)
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
// sloMs is the latency objective the SLO gauges are measured against
// (--slo-ms). Requests beyond it are counted so /metrics can report
// the percentage of traffic that violated it.
var sloMs int64 = 500

// SetSLO sets the latency objective in milliseconds (<=0 keeps the
// default of 500).
func SetSLO(ms int64) {
	if ms > 0 {
		atomic.StoreInt64(&sloMs, ms)
	}
}

// SLO returns the current objective in milliseconds.
func SLO() int64 { return atomic.LoadInt64(&sloMs) }

// routeLabel collapses a request path into the handful of route
// classes operators chart: one histogram per class keeps /metrics
// bounded while still separating reads, writes, queries and internal
// traffic.
func routeLabel(method, path string) string {
	switch {
	case path == "/health" || path == "/ready":
		return "health"
	case path == "/version":
		return "version"
	case path == "/metrics":
		return "metrics"
	case strings.HasPrefix(path, "/internal/"):
		return "internal"
	case strings.HasSuffix(path, "/docs"):
		return "query"
	case strings.HasSuffix(path, "/watch"):
		return "watch"
	case strings.Contains(path, "/docs/batch"):
		return "batch"
	case strings.Contains(path, "/docs/"):
		switch {
		case strings.HasSuffix(path, "/schema"):
			return "schema"
		case strings.HasSuffix(path, "/gsi"):
			return "gsi"
		}
		return strings.ToLower(method) + "_doc"
	case strings.Contains(path, "/collections"):
		return "collection_admin"
	case strings.Contains(path, "/cluster"):
		return "cluster"
	case strings.Contains(path, "/stats"):
		return "stats"
	default:
		return "other"
	}
}

// Tracing stamps a trace id and records RED metrics: a duration
// histogram per route class, error and over-SLO counters, and the
// gauges derived from them at scrape time.
func Tracing(slowMs int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := r.Header.Get("X-Trace-Id")
		if traceID == "" {
			traceID = newTraceID()
		}
		w.Header().Set("X-Trace-Id", traceID)
		rec := &auditRW{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start)

		route := routeLabel(r.Method, r.URL.Path)
		// Duration histogram for this route class, in milliseconds.
		metrics.Default.Histogram(
			"microdb_request_duration_ms_"+route,
			"Request duration in ms for the "+route+" route class",
			metrics.DefaultLatencyBounds,
		).Observe(float64(elapsed) / float64(time.Millisecond))

		atomic.AddInt64(metrics.Default.Counter("microdb_requests_total", "Requests served"), 1)
		if rec.status >= 500 {
			atomic.AddInt64(metrics.Default.Counter("microdb_request_errors_total", "Requests that failed (5xx)"), 1)
			atomic.AddInt64(metrics.Default.Counter("microdb_request_errors_"+route+"_total", "5xx on the "+route+" route class"), 1)
		}
		if elapsed > time.Duration(SLO())*time.Millisecond {
			atomic.AddInt64(metrics.Default.Counter("microdb_requests_over_slo_total", "Requests slower than the latency SLO"), 1)
		}
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

// auditRW captures the status a handler chose so the deferred audit
// record can report what actually happened.
type auditRW struct {
	http.ResponseWriter
	status int
}

func (a *auditRW) WriteHeader(code int) {
	a.status = code
	a.ResponseWriter.WriteHeader(code)
}

// isAuditable is true for client-facing mutations: admin commands and
// document writes. Reads and liveness probes are not audited (they
// would drown the log in noise).
func isAuditable(r *http.Request) bool {
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodDelete:
	default:
		return false
	}
	return strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/v1/api/")
}

// apiPathParts pulls (collection, id) out of the canonical document
// routes: /api/collections/{col}/docs/{id}. Anything else yields
// empty strings rather than a wrong guess.
func apiPathParts(p string) (col, id string) {
	parts := strings.Split(strings.Trim(p, "/"), "/")
	// .../collections/{col}/docs/{id}
	if len(parts) >= 4 && parts[len(parts)-2] == "docs" && parts[len(parts)-4] == "collections" {
		return parts[len(parts)-3], parts[len(parts)-1]
	}
	if len(parts) >= 3 && parts[len(parts)-3] == "collections" {
		return parts[len(parts)-2], ""
	}
	return "", ""
}

// auditDeny records an authorization failure (always on: denials are
// the events an audit log exists for).
func auditDeny(r *http.Request, kind, tenant, collection, detail string, status int) {
	if !audit.Default.Enabled() {
		return
	}
	audit.Default.Log(audit.Event{
		Kind:       kind,
		Action:     r.Method,
		Decision:   "deny",
		Status:     status,
		Remote:     r.RemoteAddr,
		Tenant:     tenant,
		Collection: collection,
		Trace:      r.Header.Get("X-Trace-Id"),
		Detail:     detail,
	})
}

// Token holds the API bearer token so it can be rotated at runtime.
// Secrets that only change on restart are secrets operators avoid
// rotating; the holder lets a reloaded token file take effect on the
// next request without dropping connections.
type Token struct {
	v atomic.Value // string
}

// NewToken returns a Token seeded with initial ("" = open API).
func NewToken(initial string) *Token {
	t := &Token{}
	t.Set(initial)
	return t
}

// Set installs a new token value ("" disables auth).
func (t *Token) Set(v string) {
	if t == nil {
		return
	}
	t.v.Store(v)
}

// Get returns the current token.
func (t *Token) Get() string {
	if t == nil {
		return ""
	}
	s, _ := t.v.Load().(string)
	return s
}

// RequireAPIAuth protects /api/* with a bearer token (constant-time
// compared). /health stays open for load balancers. Empty token
// disables the check entirely.
func RequireAPIAuth(tok *Token, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := tok.Get()
		if token == "" || !(strings.HasPrefix(r.URL.Path, "/api/") || strings.HasPrefix(r.URL.Path, "/v1/api/")) {
			next.ServeHTTP(w, r)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			auditDeny(r, "auth", "", "", "invalid bearer token", 401)
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
	// Load-aware placement: vnodes are distributed proportional to
	// each node's gossiped write-rate, so a hot node takes a bigger
	// share of the keyspace and a cold node a smaller one. With no
	// load data (or equal loads) this is the uniform ring.
	// Topology: gossiped zones spread a key's replicas across failure
	// domains (rack/AZ) instead of stacking them on one rack.
	s.ring = ring.BuildWeightedIn(nodes, s.cl.Loads(), s.cl.Zones(), s.cl.Epoch())
	// Keep the storage-tier fence at least as high as the ring epoch.
	s.rec.ObserveEpoch(s.cl.Epoch())
}

// RingSnapshot exposes the current placement ring (for admin/CLI use).
func (s *Server) RingSnapshot() *ring.Ring { return s.currentRing() }

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
	return s.owns(s.routingKey(col, id))
}

// OwnerSet returns the RF-owner set for col/id under the current ring
// (exposed for tests and admin tooling).
func (s *Server) OwnerSet(col, id string) []string {
	return s.placementOwners(s.routingKey(col, id), s.rfFor(col))
}

// owns returns true if this node is the primary owner of the key.
// routingKey computes the ring key for a document. For collections
// with a partition_field configured, the doc id must be
// "partitionValue|sortValue" — the ring key becomes col/partitionValue
// so ALL docs of one partition live on the same shard (locality:
// one-shard reads, one-shard range queries, no scatter). Without a
// partition_field the key is col/id as always.
func (s *Server) routingKey(col, id string) string {
	if cfg := s.st.GetCollectionConfig(col); cfg.PartitionField != "" {
		pk := id
		if i := strings.Index(id, "|"); i >= 0 {
			pk = id[:i]
		}
		return col + "/" + pk
	}
	return col + "/" + id
}

// checkPartitionID validates that a write's partition field value
// matches the id prefix convention for partitioned collections.
func (s *Server) checkPartitionID(col, id string, fields map[string]interface{}) string {
	cfg := s.st.GetCollectionConfig(col)
	if cfg.PartitionField == "" {
		return ""
	}
	pk := id
	if i := strings.Index(id, "|"); i >= 0 {
		pk = id[:i]
	}
	v, ok := fields[cfg.PartitionField]
	if !ok {
		return fmt.Sprintf("collection %q is partitioned by %q: field required", col, cfg.PartitionField)
	}
	if fmt.Sprintf("%v", v) != pk {
		return fmt.Sprintf("id prefix %q does not match %s=%v", pk, cfg.PartitionField, v)
	}
	return ""
}

func (s *Server) owns(key string) bool {
	owners := s.placementOwners(key, 1)
	return len(owners) > 0 && owners[0] == s.self
}

func (s *Server) fanout(d *store.Doc) {
	key := d.Collection + "/" + d.ID
	for _, peer := range s.placementOwners(key, s.rfFor(d.Collection)) {
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
		"version":         Version,
		"go":              goVersion(),
		"node":            s.self,
		"rf":              s.rf,
		"cluster":         s.cl.ClusterName(),
		"uptime_s":        time.Since(startTime).Seconds(),
		"protocol":        protocol.Version,
		"protocol_window": protocol.Window(),
	})
}

// protoStampTransport stamps our wire version on outbound /internal/*
// requests. Public /api traffic is unversioned by design: the HTTP API
// is versioned by its /v1 path prefix instead.
type protoStampTransport struct {
	base http.RoundTripper
}

func (t *protoStampTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasPrefix(req.URL.Path, "/internal/") {
		req.Header.Set(protocol.Header, protocol.String())
	}
	return t.base.RoundTrip(req)
}

// serveAggregate answers an aggregate query by computing it on every
// shard (including this one) and folding the partials. Same deadline
// rule as the document path: answer within the budget with what
// arrived, name what did not.
func (s *Server) serveAggregate(w http.ResponseWriter, r *http.Request, col string,
	filter map[string]interface{}, kind, field, groupBy string, members []string, timeoutMs int) {
	if !store.AggKinds[kind] {
		writeJSON(w, 400, map[string]string{"error": "unknown agg (count|sum|avg|min|max)"})
		return
	}
	if kind != "count" && field == "" {
		writeJSON(w, 400, map[string]string{"error": "agg " + kind + " requires &field="})
		return
	}
	filterJSON, _ := json.Marshal(filter)

	type partial struct {
		member string
		agg    store.Aggregate
		err    error
	}
	results := make(chan partial, len(members)*2)
	var wg sync.WaitGroup
	for _, m := range members {
		m := m
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m == s.self {
				results <- partial{member: m, agg: s.st.Aggregate(col, filter, kind, field, groupBy, s.primaryOwner(col))}
				return
			}
			agg, err := s.gatherAggFrom(m, col, string(filterJSON), kind, field, groupBy)
			results <- partial{member: m, agg: agg, err: err}
		}()
	}
	go func() { wg.Wait(); close(results) }()

	merged := store.Aggregate{Kind: kind, Field: field}
	var errs []string
	answered := map[string]bool{}
	timedOut := false
	merge := func(p partial) {
		answered[p.member] = true
		if p.err != nil {
			errs = append(errs, p.err.Error())
			return
		}
		merged.Merge(p.agg)
	}
	deadline := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer deadline.Stop()
collect:
	for {
		select {
		case p, ok := <-results:
			if !ok {
				break collect
			}
			merge(p)
		case <-deadline.C:
			timedOut = true
			atomic.AddInt64(metrics.Default.Counter("microdb_query_timeouts_total",
				"Queries that returned partial results at their deadline"), 1)
			for {
				select {
				case p, ok := <-results:
					if !ok {
						break collect
					}
					merge(p)
				default:
					break collect
				}
			}
		}
	}
	if timedOut {
		for _, m := range members {
			if !answered[m] {
				errs = append(errs, fmt.Sprintf("timeout after %dms waiting for %s", timeoutMs, m))
			}
		}
	}

	resp := map[string]interface{}{
		"aggregate":     merged.Value(),
		"agg":           merged,
		"count":         merged.Count,
		"nodes_queried": len(members),
	}
	if groupBy != "" {
		groups := map[string]interface{}{}
		for _, k := range merged.SortedGroups() {
			g := merged.Groups[k]
			groups[k] = g.Value()
		}
		resp["groups"] = groups
		resp["group_totals"] = merged.Groups
	}
	if len(errs) > 0 {
		resp["partial"] = true
		resp["errors"] = errs
	}
	if timedOut {
		resp["timed_out"] = true
		resp["timeout_ms"] = timeoutMs
	}
	writeJSON(w, 200, resp)
}

// primaryOwner reports whether this node is the FIRST ring owner of a
// document — the one copy allowed to count it. Replicas must stay out
// of the sum or every document would be counted RF times.
func (s *Server) primaryOwner(col string) func(id string) bool {
	return func(id string) bool {
		owners := s.OwnerSet(col, id)
		return len(owners) > 0 && owners[0] == s.self
	}
}

// gatherAggFrom asks one peer to compute the same aggregate locally.
func (s *Server) gatherAggFrom(peer, col, filterJSON, kind, field, groupBy string) (store.Aggregate, error) {
	u := peer + "/internal/agg/" + col + "?kind=" + url.QueryEscape(kind) +
		"&field=" + url.QueryEscape(field) + "&group_by=" + url.QueryEscape(groupBy)
	if filterJSON != "" && filterJSON != "null" {
		u += "&filter=" + url.QueryEscape(filterJSON)
	}
	resp, err := s.fwdClient.Get(u)
	if err != nil {
		return store.Aggregate{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return store.Aggregate{}, fmt.Errorf("%s: HTTP %d: %s", peer, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var out struct {
		Agg store.Aggregate `json:"agg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return store.Aggregate{}, fmt.Errorf("%s: %w", peer, err)
	}
	return out.Agg, nil
}

// handleInternalAgg is the shard-side aggregate worker: compute over
// this node's data only and return the partial.
func (s *Server) handleInternalAgg(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	var filter map[string]interface{}
	if f := r.URL.Query().Get("filter"); f != "" {
		if err := json.Unmarshal([]byte(f), &filter); err != nil {
			writeJSON(w, 400, map[string]string{"error": "bad filter JSON"})
			return
		}
	}
	kind := r.URL.Query().Get("kind")
	agg := s.st.Aggregate(col, filter, kind, r.URL.Query().Get("field"), r.URL.Query().Get("group_by"), s.primaryOwner(col))
	writeJSON(w, 200, map[string]interface{}{"agg": agg})
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
	s.colOp(col, "read")
	if s.rcache != nil {
		ckey := col + "\x00" + id
		if v, ok := s.rcache.Get(ckey); ok {
			writeJSON(w, 200, v)
			return
		}
		d, ok := s.st.Get(col, id)
		if !ok {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}
		md := s.migrateDoc(col, d)
		s.rcache.Put(ckey, md)
		writeJSON(w, 200, md)
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
	if perr := s.checkPartitionID(col, id, fields); perr != "" {
		writeJSON(w, 400, map[string]string{"error": perr})
		return
	}
	// Per-collection quota: a runaway collection can't grow past its
	// max_docs without affecting neighbors' capacity headroom.
	if cfg := s.st.GetCollectionConfig(col); cfg.MaxDocs > 0 {
		if _, exists := s.st.Get(col, id); !exists {
			if s.st.StatsFor(col).Live.Load() >= cfg.MaxDocs {
				writeJSON(w, 403, map[string]interface{}{"error": "collection quota exceeded", "collection": col, "max_docs": cfg.MaxDocs})
				return
			}
		}
	}
	s.colOp(col, "write")
	key := s.routingKey(col, id)
	if s.cl.Draining() {
		writeJSON(w, 503, map[string]string{"error": "node is decommissioning, retry elsewhere"})
		return
	}
	if !s.owns(key) && r.Header.Get("X-Microdb-Forwarded") == "" {
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
		members := s.placementOwners(key, s.rfFor(col))
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
				"error":   "write quorum not reached",
				"acked":   acked,
				"need":    need,
				"applied": true,
				"errors":  werrs,
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
	// Partitioned collections: every doc in the batch must belong to
	// the same partition, and this node must own it — otherwise the
	// batch can't be one atomic log record on one shard.
	if pfield := s.st.GetCollectionConfig(col).PartitionField; pfield != "" {
		partition := ""
		for id, fields := range in.Docs {
			if perr := s.checkPartitionID(col, id, fields); perr != "" {
				writeJSON(w, 400, map[string]string{"error": perr})
				return
			}
			pk := id
			if i := strings.Index(id, "|"); i >= 0 {
				pk = id[:i]
			}
			if partition == "" {
				partition = pk
			} else if pk != partition {
				writeJSON(w, 400, map[string]string{"error": fmt.Sprintf("batch spans partitions %q and %q — one batch = one partition", partition, pk)})
				return
			}
		}
		if !s.owns(col + "/" + partition) {
			s.forwardBytes(w, r, "POST", "/api/collections/"+col+"/docs/batch", body, 0)
			return
		}
	}
	var newCount int64
	for id := range in.Docs {
		if _, exists := s.st.Get(col, id); !exists {
			newCount++
		}
	}
	if cfg := s.st.GetCollectionConfig(col); cfg.MaxDocs > 0 {
		if s.st.StatsFor(col).Live.Load()+newCount > cfg.MaxDocs {
			writeJSON(w, 403, map[string]interface{}{"error": "collection quota exceeded", "collection": col, "max_docs": cfg.MaxDocs})
			return
		}
	}
	s.st.StatsFor(col).Writes.Add(int64(len(in.Docs)))
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
	s.colOp(col, "delete")
	key := s.routingKey(col, id)
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
	owner := s.placementOwners(s.routingKey(r.PathValue("col"), r.PathValue("id")), 1)
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
	// Tell the receiver this is an already-routed write: under the
	// range map the primary can shift between ticks, so a forwarded
	// write must be applied by whoever receives it instead of being
	// bounced back in a ping-pong.
	req.Header.Set("X-Microdb-Forwarded", "1")
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
//
//	&sort=<field>&desc=true&limit=N&offset=M
//
// Scatter-gather: the receiving node fans the query out to every live
// cluster member (including itself), merges results by doc id keeping
// the highest (ver, ts), then applies sort and pagination to the
// merged set.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	s.colOp(col, "query")
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

	// Shared deadline: every gather runs concurrently, and the query
	// waits for all of them FOR AT MOST this long. Without it one
	// unreachable peer holds the whole result hostage until the HTTP
	// client's own timeout — and a query that answers in 3s with
	// partial results beats one that answers in 30s with all of them.
	timeoutMs := 2000
	if t := r.URL.Query().Get("timeout_ms"); t != "" {
		n, err := strconv.Atoi(t)
		if err != nil || n < 10 || n > 60000 {
			writeJSON(w, 400, map[string]string{"error": "bad timeout_ms (10..60000)"})
			return
		}
		timeoutMs = n
	}

	// Aggregate pushdown: count/sum/avg/min/max are computed ON each
	// shard and merged here. The wire carries a handful of numbers
	// instead of every matching document — O(shards) bytes instead of
	// O(matches), and no coordinator-side sort at all.
	if agg := r.URL.Query().Get("agg"); agg != "" {
		s.serveAggregate(w, r, col, filter, agg, r.URL.Query().Get("field"),
			r.URL.Query().Get("group_by"), members, timeoutMs)
		return
	}

	type gatherResult struct {
		member    string
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
			// Pushdown: sort and window happen INSIDE the scan (a
			// bounded heap over the walk), so a 20-row query never
			// materialises or sorts every match on this shard. Schema
			// migration runs on each candidate as it is ordered, so
			// the sort sees the fields the client will see.
			if pushdown {
				docs, trunc := s.st.ScanTopK(col, filter,
					func(d *store.Doc) *store.Doc { return s.migrateDoc(col, d) },
					sortField, desc, capN)
				return gatherResult{member: m, docs: docs, truncated: trunc}
			}
			return gatherResult{member: m, docs: s.migrateDocs(col, s.st.ScanIndexed(col, filter)), truncated: false}
		}
		docs, trunc, err := s.gatherFrom(m, col, r.URL.Query().Get("filter"), sortField, desc, capN)
		return gatherResult{member: m, docs: docs, truncated: trunc, err: err}
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
	answered := map[string]bool{}
	merge := func(res gatherResult) {
		answered[res.member] = true
		if res.err != nil {
			errs = append(errs, res.err.Error())
			return
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

	deadline := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer deadline.Stop()
	timedOut := false
collect:
	for {
		select {
		case res, ok := <-results:
			if !ok {
				break collect
			}
			merge(res)
		case <-deadline.C:
			// Out of budget: take whatever has already arrived, report
			// the rest as missing, and answer instead of hanging.
			timedOut = true
			atomic.AddInt64(metrics.Default.Counter("microdb_query_timeouts_total",
				"Queries that returned partial results at their deadline"), 1)
			for {
				select {
				case res, ok := <-results:
					if !ok {
						break collect
					}
					merge(res)
				default:
					break collect
				}
			}
		}
	}
	if timedOut {
		for _, m := range members {
			if !answered[m] {
				errs = append(errs, fmt.Sprintf("timeout after %dms waiting for %s", timeoutMs, m))
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
		"total":         total,
		"docs":          out,
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
	if timedOut {
		resp["timed_out"] = true
		resp["timeout_ms"] = timeoutMs
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
		Truncated bool         `json:"truncated"`
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

// --- storage-tier record API handlers --------------------------------
//
// These implement the storage side of storage.RecordStore. Writes are
// fenced: a mutation carrying an epoch below the highest epoch this
// node has accepted is rejected 409 (ErrStaleEpoch), so a compute
// node with a stale ring view can never write to the wrong shard.

func (s *Server) handleRecordPut(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	id := r.PathValue("id")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "read body"})
		return
	}
	var rec storage.Record
	if err := json.Unmarshal(body, &rec); err != nil {
		writeJSON(w, 400, map[string]string{"error": "bad record JSON"})
		return
	}
	rec.Collection, rec.ID = col, id
	// Fence against the node's live ring epoch too: a writer behind
	// the cluster's membership view is stale even if its own fence
	// was fine.
	if rec.Epoch < s.currentRing().Epoch() {
		writeJSON(w, 409, map[string]interface{}{"error": "stale epoch", "epoch": s.currentRing().Epoch()})
		return
	}
	if err := s.rec.Put(&rec); err != nil {
		if errors.Is(err, storage.ErrStaleEpoch) {
			writeJSON(w, 409, map[string]interface{}{"error": "stale epoch", "epoch": s.rec.Epoch()})
			return
		}
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]bool{"ok": true})
}

func (s *Server) handleRecordGet(w http.ResponseWriter, r *http.Request) {
	rec, ok := s.st.GetDoc(r.PathValue("col"), r.PathValue("id"))
	if !ok {
		writeJSON(w, 404, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, 200, rec)
}

func (s *Server) handleRecordScan(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	after := r.URL.Query().Get("after")
	limit := 1000
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > 10000 {
			writeJSON(w, 400, map[string]string{"error": "limit must be 1..10000"})
			return
		}
		limit = n
	}
	recs := s.st.ScanPage(col, after, limit)
	writeJSON(w, 200, map[string]interface{}{"records": recs, "epoch": s.currentRing().Epoch()})
}

func (s *Server) handleRecordEpoch(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"epoch": s.currentRing().Epoch()})
}

// --- global secondary indexes (async, off the write path) ----------

// handleDeclareGSI: PUT /api/collections/{col}/gsi
//
//	{"name":"by_city","field":"city"}
//
// Registers the index and backfills existing docs. New writes are
// indexed asynchronously by the GSI worker — the response includes
// the current lag so clients know how fresh the index is.
func (s *Server) handleDeclareGSI(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "read body"})
		return
	}
	var in gsi.Def
	if err := json.Unmarshal(body, &in); err != nil {
		writeJSON(w, 400, map[string]string{"error": `body must be {"name":"...","field":"..."}`})
		return
	}
	if s.gsi == nil {
		writeJSON(w, 503, map[string]string{"error": "GSI service not enabled (start with --gsi)"})
		return
	}
	n, err := s.gsi.Declare(col, in)
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	processed, head, _ := s.gsi.Lag()
	writeJSON(w, 200, map[string]interface{}{
		"collection": col, "index": in.Name, "field": in.Field,
		"backfilled": n, "lag_processed": processed, "feed_head": head,
	})
}

// handleListGSI: GET /api/collections/{col}/gsi
func (s *Server) handleListGSI(w http.ResponseWriter, r *http.Request) {
	if s.gsi == nil {
		writeJSON(w, 200, map[string]interface{}{"indexes": []gsi.Def{}})
		return
	}
	writeJSON(w, 200, map[string]interface{}{"indexes": s.gsi.Defs(r.PathValue("col"))})
}

// handleLookupGSI: GET /api/collections/{col}/gsi/{name}?value=X
// Returns matching doc ids plus the index lag (processed vs feed
// head) so the caller can judge staleness.
func (s *Server) handleLookupGSI(w http.ResponseWriter, r *http.Request) {
	if s.gsi == nil {
		writeJSON(w, 503, map[string]string{"error": "GSI service not enabled (start with --gsi)"})
		return
	}
	value := r.URL.Query().Get("value")
	if value == "" {
		writeJSON(w, 400, map[string]string{"error": "value query param required"})
		return
	}
	ids, processed, head := s.gsi.Lookup(r.PathValue("col"), r.PathValue("name"), value)
	writeJSON(w, 200, map[string]interface{}{
		"ids": ids, "count": len(ids),
		"lag_processed": processed, "feed_head": head,
		"lag_events": head - processed,
	})
}

// handlePartitionQuery: GET /api/collections/{col}/partition/{pk}
//
//	?sort_gte=A&sort_lte=B&sort_lt=..&sort_gt=..&desc=true&limit=N
//
// Dynamo-style Query(): for collections with a partition_field, all
// docs of one partition live on one shard, so this is served from a
// single node with NO scatter-gather. If this node doesn't own the
// partition, the request is forwarded to the owner. Results are
// ordered by the sort component of the doc id (the part after '|').
func (s *Server) handlePartitionQuery(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	pk := r.PathValue("pk")
	cfg := s.st.GetCollectionConfig(col)
	if cfg.PartitionField == "" {
		writeJSON(w, 400, map[string]string{"error": "collection has no partition_field — use /docs scatter query"})
		return
	}
	if !s.owns(col + "/" + pk) {
		// Forward straight to the partition owner (forwardBytes
		// derives the owner from the doc-id path value, which this
		// route doesn't have).
		owner := s.placementOwners(col+"/"+pk, 1)
		if len(owner) == 0 {
			writeJSON(w, 503, map[string]string{"error": "no owner known"})
			return
		}
		req, _ := http.NewRequest("GET", owner[0]+r.URL.String(), nil)
		resp, err := s.fwdClient.Do(req)
		if err != nil {
			writeJSON(w, 502, map[string]string{"error": "partition forward failed"})
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}
	// Sort bounds on the sort component (id suffix).
	type bound struct {
		op    string
		value string
	}
	var bounds []bound
	for _, b := range []struct {
		param string
		op    string
	}{{"sort_gte", "gte"}, {"sort_lte", "lte"}, {"sort_gt", "gt"}, {"sort_lt", "lt"}} {
		if v := r.URL.Query().Get(b.param); v != "" {
			bounds = append(bounds, bound{b.op, v})
		}
	}
	limit := 0
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 0 {
			writeJSON(w, 400, map[string]string{"error": "bad limit"})
			return
		}
		limit = n
	}
	desc := r.URL.Query().Get("desc") == "true"

	docs := s.st.Scan(col, func(d *store.Doc) bool {
		idPk := d.ID
		if i := strings.Index(d.ID, "|"); i >= 0 {
			idPk = d.ID[:i]
		}
		if idPk != pk {
			return false
		}
		sv := ""
		if i := strings.Index(d.ID, "|"); i >= 0 {
			sv = d.ID[i+1:]
		}
		for _, b := range bounds {
			switch b.op {
			case "gte":
				if sv < b.value {
					return false
				}
			case "lte":
				if sv > b.value {
					return false
				}
			case "gt":
				if sv <= b.value {
					return false
				}
			case "lt":
				if sv >= b.value {
					return false
				}
			}
		}
		return true
	})
	sort.Slice(docs, func(i, j int) bool {
		si, sj := sortPart(docs[i].ID), sortPart(docs[j].ID)
		if desc {
			return si > sj
		}
		return si < sj
	})
	total := len(docs)
	if limit > 0 && len(docs) > limit {
		docs = docs[:limit]
	}
	docs = s.migrateDocs(col, docs)
	writeJSON(w, 200, map[string]interface{}{
		"count": len(docs), "total": total, "docs": docs,
		"partition": pk, "shards_queried": 1,
	})
}

func sortPart(id string) string {
	if i := strings.Index(id, "|"); i >= 0 {
		return id[i+1:]
	}
	return ""
}

// colOp bumps a per-collection operation counter (isolation
// visibility: one hot collection is obvious in the stats and can be
// quota-limited without touching its neighbors).
func (s *Server) colOp(col, kind string) {
	st := s.st.StatsFor(col)
	switch kind {
	case "read":
		st.Reads.Add(1)
	case "write":
		st.Writes.Add(1)
	case "delete":
		st.Deletes.Add(1)
	case "query":
		st.Queries.Add(1)
	}
}

// handleCollectionStats: GET /api/stats/collections — per-collection
// ops + live docs (isolation observability).
func (s *Server) handleCollectionStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"collections": s.st.AllColStats()})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	// Incompatible peers are reported separately so an operator can
	// tell "no peers" from "peers I refuse to talk to".
	writeJSON(w, 200, map[string]interface{}{
		"self":               s.self,
		"peers":              s.cl.Peers(),
		"protocol":           protocol.Version,
		"protocol_window":    protocol.Window(),
		"incompatible_peers": s.cl.Incompatible(),
		"zones":              s.cl.Zones(),
	})
}

// handleMetrics renders Prometheus text format with live gauges
// refreshed from current state before rendering.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	atomic.StoreInt64(metrics.Default.Gauge("microdb_docs", "Documents currently held (incl. tombstones)"), int64(s.st.DocCount()))
	atomic.StoreInt64(metrics.Default.Gauge("microdb_collections", "Collections currently held"), int64(len(s.st.Collections())))
	atomic.StoreInt64(metrics.Default.Gauge("microdb_peers", "Live cluster peers"), int64(len(s.cl.Peers())))
	if s.rcache != nil {
		h, m, inv := s.rcache.Stats()
		atomic.StoreInt64(metrics.Default.Counter("microdb_cache_hits_total", "Read cache hits"), h)
		atomic.StoreInt64(metrics.Default.Counter("microdb_cache_misses_total", "Read cache misses"), m)
		atomic.StoreInt64(metrics.Default.Counter("microdb_cache_invalidations_total", "Read cache invalidations from the change feed"), inv)
		atomic.StoreInt64(metrics.Default.Gauge("microdb_cache_entries", "Live read cache entries"), int64(s.rcache.Len()))
	}
	// SLO and error budgets: fractions of traffic, not raw counts, so
	// an alert can fire on user-visible pain instead of on CPU.
	if total := atomic.LoadInt64(metrics.Default.Counter("microdb_requests_total", "Requests served")); total > 0 {
		overSLO := atomic.LoadInt64(metrics.Default.Counter("microdb_requests_over_slo_total", "Requests slower than the latency SLO"))
		errs := atomic.LoadInt64(metrics.Default.Counter("microdb_request_errors_total", "Requests that failed (5xx)"))
		metrics.Default.SetFloat("microdb_slo_violation_pct",
			"Percentage of requests slower than the latency SLO since start", 100*float64(overSLO)/float64(total))
		metrics.Default.SetFloat("microdb_error_pct",
			"Percentage of requests that returned 5xx since start", 100*float64(errs)/float64(total))
		atomic.StoreInt64(metrics.Default.Gauge("microdb_slo_ms", "Latency SLO in milliseconds"), SLO())
	}

	// Group commit: writes/fsync is the batch size — the number that
	// shows whether writes are actually sharing durability passes.
	passes, writes, failed := s.st.CommitStats()
	atomic.StoreInt64(metrics.Default.Counter("microdb_group_fsyncs_total", "Group-commit fsync passes"), passes)
	atomic.StoreInt64(metrics.Default.Counter("microdb_group_fsync_writes_total", "Writes made durable by those passes"), writes)
	atomic.StoreInt64(metrics.Default.Counter("microdb_group_fsync_errors_total", "Group-commit fsync passes that failed"), failed)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(metrics.Default.Render()))
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
