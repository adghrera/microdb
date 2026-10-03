// Package cluster keeps nodes aware of each other via periodic gossip and
// replicates writes to the ring-owners of each key. Membership converges
// through push/pull of the node list; failed nodes are evicted after a
// timeout. This is deliberately simple: eventual consistency, no quorum
// reads, last-writer-wins by (ver, ts) at the store layer.
package cluster

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"microdb/internal/hints"
	"microdb/internal/merkle"
	"microdb/internal/metrics"
	"microdb/internal/protocol"
	"microdb/internal/store"
)

// MerkleMeta is a peer tree's root + depth.
type MerkleMeta struct {
	Root  string `json:"root"`
	Depth int    `json:"depth"`
}

// TLSOptions configures mutual-TLS for internal (gossip/replication)
// traffic. All three files are PEM: server cert + key, and a CA bundle
// used to verify peers.
type TLSOptions struct {
	CertFile string
	KeyFile  string
	CAFile   string
	// cache holds the leaf certificate and re-reads the files when
	// they change (see tlsreload.go).
	cache *certCache
}

// ClientTLS builds the tls.Config used for peer-to-peer calls:
// verifies peer certs against the CA and presents our cert so peers
// can verify us back (mutual TLS).
func (o *TLSOptions) ClientTLS() (*tls.Config, error) {
	if _, err := o.loadEager(); err != nil {
		return nil, fmt.Errorf("client cert/key: %w", err)
	}
	caPEM, err := os.ReadFile(o.CAFile)
	if err != nil {
		return nil, fmt.Errorf("ca file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certs in CA bundle")
	}
	// Present the CURRENT leaf on every handshake: a cert renewed on
	// disk is picked up on the next connection, no restart.
	return &tls.Config{
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return o.certs().get()
		},
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}, nil
}

// ServerTLS builds the tls.Config for serving internal endpoints:
// requires and verifies client certs against the CA.
func (o *TLSOptions) ServerTLS() (*tls.Config, error) {
	if _, err := o.loadEager(); err != nil {
		return nil, fmt.Errorf("server cert/key: %w", err)
	}
	caPEM, err := os.ReadFile(o.CAFile)
	if err != nil {
		return nil, fmt.Errorf("ca file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certs in CA bundle")
	}
	// Serve the CURRENT leaf on every handshake (see tlsreload.go):
	// certificate renewal is a file write, not a restart.
	return &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return o.certs().get()
		},
		ClientCAs:  pool,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
	}, nil
}

type Member struct {
	Addr string  `json:"addr"`           // http://host:port
	TTL  int64   `json:"ttl"`            // unix millis; expired members are dropped
	Load float64 `json:"load,omitempty"` // recent writes/sec (EWMA)
	Zone string  `json:"zone,omitempty"` // failure domain (rack/AZ)
}

type Cluster struct {
	self   string
	peers  map[string]time.Time // addr -> last seen (alive)
	mu     sync.RWMutex
	epoch  int64 // membership epoch; bumped on every membership change
	st     *store.Store
	client *http.Client
	stop   chan struct{}
	// hints is the hinted-handoff store; nil until EnableHints is called.
	hints *hints.Store
	// draining marks a node that has begun graceful decommission: it
	// refuses new writes while its data is handed off to the remaining
	// peers.
	draining bool
	// bootstrapped is false from process start until the first join
	// has completed its immediate full sync ("streaming" phase).
	// While false the node accepts writes but reports streaming=true
	// so clients/ops know reads may be stale.
	bootstrapped atomic.Bool
	// activeBootstraps counts join requests we are currently serving
	// with an immediate sync (admission control on the seed side).
	activeBootstraps atomic.Int64
	maxBootstraps    int64
	// left holds addresses that gracefully departed, with the time of
	// departure. Gossip must not resurrect them for the tombstone
	// window (longer than gossip convergence), but an explicit Join
	// clears the tombstone so a restarted node on the same address is
	// accepted.
	left map[string]time.Time
	// ae schedules anti-entropy contacts: a shuffled lap over the peer
	// set, `fanout` peers per round. Without it every node synced with
	// EVERY peer every round — O(N^2) messages cluster-wide per
	// interval, which is the first wall a few hundred nodes hit.
	ae       *aeSchedule
	aeFanout int
	// incompatible holds peers that answered outside our wire-version
	// window [protocol.MinSupported, protocol.Version]; they are
	// skipped rather than retried forever.
	incompatible map[string]int
	// clusterName guards against cross-cluster contamination.
	clusterName string
	// zone is this node's failure domain (--zone). It is gossiped so
	// every node can build a topology-aware ring.
	zone  string
	zones map[string]string // peer addr -> zone, from gossip
	// loads holds the most recently gossiped write-rate (writes/sec)
	// per node, including self. Used for load-aware weighted vnode
	// placement: a node with 2x the average load gets ~2x the vnodes.
	loads map[string]float64
	// writeTicks counts client writes since the last EWMA update;
	// selfLoad is the EWMA of writes/sec fed into the gossiped load.
	lastWrites atomic.Int64  // last sampled store.WriteCount
	selfLoad   atomic.Uint64 // float64 bits
	// OnPeersChanged is called with the live peer list whenever membership changes.
	OnPeersChanged func(addrs []string)
}

func New(self string, st *store.Store, tlsOpts *TLSOptions) (*Cluster, error) {
	transport := &http.Transport{}
	if tlsOpts != nil {
		tlsCfg, err := tlsOpts.ClientTLS()
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = tlsCfg
	}
	c := &Cluster{
		self:          self,
		peers:         map[string]time.Time{},
		left:          map[string]time.Time{},
		loads:         map[string]float64{},
		incompatible:  map[string]int{},
		zones:         map[string]string{},
		ae:            &aeSchedule{},
		aeFanout:      defaultAEFanout,
		st:            st,
		client:        &http.Client{Timeout: 30 * time.Second},
		stop:          make(chan struct{}),
		maxBootstraps: 2, // bootstrap streams can be large: 30s client
		// timeout covers them; serve at most 2 concurrent streams
		// as a seed (admission control on the join path).
	}
	// Every internal request this node makes carries the cluster name
	// (when configured) and the wire protocol version, and every reply
	// is checked for compatibility. Unconditional — not only when a
	// cluster name happens to be set.
	c.client.Transport = &clusterStampTransport{base: transport, c: c}
	return c, nil
}

// SetClusterName stamps every internal request this node makes with
// the cluster name. Peers that belong to a different cluster reject
// the request outright — this prevents the classic operational
// disaster of a misconfigured node joining the wrong production
// cluster and starting to replicate/fence against it.
// SetZone records this node's failure domain and makes it visible to
// gossip from the next round on. Empty means "no topology" (default).
func (c *Cluster) SetZone(z string) {
	c.mu.Lock()
	c.zone = z
	c.mu.Unlock()
}

// Zone returns this node's configured failure domain.
func (c *Cluster) Zone() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.zone
}

// ZoneOf returns a node's failure domain ("" when unknown), including
// this node's own.
func (c *Cluster) ZoneOf(addr string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if addr == c.self {
		return c.zone
	}
	return c.zones[addr]
}

// Zones returns addr -> zone for every peer we know, plus self. Used
// to build the topology-aware ring; also reported by /api/cluster so
// an operator can see where replicas landed.
func (c *Cluster) Zones() map[string]string {
	c.mu.RLock()
	out := make(map[string]string, len(c.zones)+1)
	for k, v := range c.zones {
		out[k] = v
	}
	out[c.self] = c.zone
	c.mu.RUnlock()
	return out
}

func (c *Cluster) SetClusterName(name string) {
	c.clusterName = name
	if t, ok := c.client.Transport.(*clusterStampTransport); ok {
		t.name = name
		return
	}
	c.client.Transport = &clusterStampTransport{base: c.client.Transport, name: name, c: c}
}

// --- wire-protocol compatibility -----------------------------------

// markIncompatible records a peer speaking a version we cannot talk
// to, so gossip and replication skip it instead of retrying a
// conversation that can never succeed.
func (c *Cluster) markIncompatible(addr string, peerVersion int) {
	c.mu.Lock()
	_, known := c.incompatible[addr]
	if !known {
		c.incompatible[addr] = peerVersion
	}
	c.mu.Unlock()
	if !known {
		log.Printf("wire protocol: %s speaks version %d, outside our window [%d, %d] — skipping until an operator upgrades or rolls back",
			addr, peerVersion, protocol.MinSupported, protocol.Version)
		atomic.AddInt64(metrics.Default.Counter("microdb_incompatible_peers_total", "Peers skipped for wire-version mismatch"), 1)
	}
}

// isIncompatible reports whether addr was seen speaking an
// out-of-window version.
func (c *Cluster) isIncompatible(addr string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.incompatible[addr]
	return ok
}

// Incompatible returns addr -> peer version for every peer skipped for
// a wire-version mismatch, so an operator can see WHY a node stopped
// participating.
func (c *Cluster) Incompatible() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]int, len(c.incompatible))
	for a, v := range c.incompatible {
		out[a] = v
	}
	return out
}

// markFromResponse inspects a peer reply: an explicit 505 means it
// refused OUR version, an out-of-window advertised version means it
// ignored our header (a pre-versioning binary). Either way the peer is
// unusable for internal traffic.
func (c *Cluster) markFromResponse(addr string, resp *http.Response) {
	pv, err := protocol.Parse(resp.Header.Get(protocol.Header))
	if err != nil {
		pv = -1
	}
	if resp.StatusCode == 505 || protocol.CheckResponse(pv) != nil {
		c.markIncompatible(addr, pv)
	}
}

// clusterNameOf returns the configured cluster name ("" = unguarded).
func (c *Cluster) ClusterName() string { return c.clusterName }

// clusterStampTransport adds the X-Microdb-Cluster and
// X-Microdb-Protocol headers to every outbound request, whatever the
// inner transport, and checks every reply for wire compatibility.
type clusterStampTransport struct {
	base http.RoundTripper
	name string
	c    *Cluster
}

func (t *clusterStampTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.name != "" {
		req.Header.Set("X-Microdb-Cluster", t.name)
	}
	if strings.HasPrefix(req.URL.Path, "/internal/") {
		req.Header.Set(protocol.Header, protocol.String())
	}
	resp, err := t.base.RoundTrip(req)
	if err == nil && resp != nil && t.c != nil && strings.HasPrefix(req.URL.Path, "/internal/") {
		t.c.markFromResponse(req.URL.Scheme+"://"+req.URL.Host, resp)
	}
	return resp, err
}

// minCompressBody is the size below which gzip framing costs more than
// it saves. Internal messages are JSON: batches, bootstrap pages and
// gossip with a few hundred members all compress well above this.
const minCompressBody = 1024

// maybeCompress gzips a body that is large enough to benefit. The
// return value is the payload to send plus whether it was compressed —
// small bodies go out as-is (no framing overhead, no decompressor on
// the far side for a 200-byte gossip message).
func maybeCompress(body []byte) ([]byte, bool) {
	if len(body) < minCompressBody {
		return body, false
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(body); err != nil {
		return body, false
	}
	if err := gz.Close(); err != nil {
		return body, false
	}
	if buf.Len() >= len(body) {
		return body, false // incompressible: don't pay framing for nothing
	}
	return buf.Bytes(), true
}

// post sends an internal request, stamping the body's CRC32C so the
// receiver can prove the bytes that arrived are the bytes that were
// sent — over the ORIGINAL body, before compression, so the checksum
// still answers "did the right bytes arrive?" rather than "did gzip
// round-trip?". Bodies above minCompressBody are gzipped: replication
// and bootstrap used to ship raw JSON, which is the largest wire cost
// a write amplification factor has.
func (c *Cluster) post(url string, body []byte) (*http.Response, error) {
	payload, compressed := maybeCompress(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(body) > 0 {
		req.Header.Set(protocol.CRCHeader, protocol.Checksum(body))
		if compressed {
			req.Header.Set("Content-Encoding", "gzip")
		}
	}
	return c.client.Do(req)
}

// Join contacts a known seed node and asks for its member list.
func (c *Cluster) Join(seed string) error {
	// We are (re)joining under our own address: clear any local
	// tombstone for it so a restarted node is accepted cluster-wide
	// once the seed propagates our presence.
	c.ClearLeave(c.self)
	resp, err := c.post(seed+"/internal/join", []byte(`{"addr":"`+c.self+`"}`))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 505 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("join %s refused: incompatible wire protocol (this node speaks %d, seed window is not compatible): %s",
			seed, protocol.Version, strings.TrimSpace(string(body)))
	}
	// Non-2xx means the seed refused us (e.g. cluster name mismatch).
	// Surface it instead of decoding the error body as a member list.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("join %s refused (%d): %s", seed, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		Members []Member `json:"members"`
		Epoch   int64    `json:"epoch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	c.mu.Lock()
	c.adoptEpochLocked(out.Epoch)
	c.mu.Unlock()
	for _, m := range out.Members {
		if m.Addr != c.self && m.TTL > time.Now().UnixMilli() {
			c.seen(m.Addr)
		}
	}
	// Seed join discovered peers — rebuild the ring immediately.
	if c.OnPeersChanged != nil {
		c.OnPeersChanged(c.Peers())
	}
	// Bootstrap streaming: pull the seed's data NOW instead of
	// waiting for background anti-entropy rounds. The node stays in
	// "streaming" state (accepts writes, reads may be stale) until
	// the pull completes.
	if err := c.bootstrapFrom(seed); err != nil {
		log.Printf("bootstrap from %s incomplete: %v (anti-entropy will finish the job)", seed, err)
		return err
	}
	c.bootstrapped.Store(true)
	return nil
}

func (c *Cluster) seen(addr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A gracefully-departed address must not be resurrected by stale
	// gossip until its tombstone window passes (2x the TTL, so every
	// node's member list has turned over at least once).
	if t, ok := c.left[addr]; ok && time.Since(t) < 30*time.Second {
		return false
	}
	_, had := c.peers[addr]
	c.peers[addr] = time.Now()
	if !had {
		c.epoch++ // membership changed — new fencing epoch
	}
	return !had
}

// ClearLeave removes a departure tombstone so the address may rejoin
// (explicit Join calls do this for the joiner).
func (c *Cluster) ClearLeave(addr string) {
	c.mu.Lock()
	delete(c.left, addr)
	c.mu.Unlock()
}

// Epoch returns the current membership epoch. Writes carry it so a node
// whose view is behind can be fenced (EPOCH_MISMATCH) by the owner.
func (c *Cluster) Epoch() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.epoch
}

// adoptEpochLocked raises our epoch to at least the peer's, returning
// whether our view changed (caller may need to rebuild its ring).
func (c *Cluster) adoptEpochLocked(peerEpoch int64) bool {
	if peerEpoch > c.epoch {
		c.epoch = peerEpoch
		return true
	}
	return false
}

// Peers returns currently-alive peer addresses (excluding self).
func (c *Cluster) Peers() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	out := make([]string, 0, len(c.peers))
	for a, t := range c.peers {
		if now.Sub(t) < 15*time.Second {
			out = append(out, a)
		}
	}
	return out
}

// Start begins the gossip loop: every second, push our member list to a
// random peer and merge its reply; evict members we haven't heard from.
// A slower anti-entropy loop runs every 10s: for each peer and each
// collection, compare Merkle roots; on mismatch, exchange leaves, find
// divergent doc ids, and swap just those docs (both directions, LWW).
func (c *Cluster) Start() {
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-t.C:
				c.gossipRound()
				c.evict()
				c.replayHints()
			}
		}
	}()
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-t.C:
				c.antiEntropyAll()
			}
		}
	}()
	// Compaction: hourly, dropping tombstones older than 24h. The GC
	// window must exceed max expected replica downtime or a very late
	// replica could resurrect a deleted doc from its own old log.
	go func() {
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-t.C:
				if dropped, err := c.st.Compact(24 * time.Hour); err != nil {
					log.Printf("compaction failed: %v", err)
				} else if dropped > 0 {
					log.Printf("compaction dropped %d expired tombstones", dropped)
				}
			}
		}
	}()
}

// antiEntropyAll syncs every collection with every live peer.
// defaultAEFanout bounds anti-entropy traffic per round: small
// clusters (the ones in the test suite) still cover every peer in one
// round, while a 300-node cluster repairs with three meetings per node
// per interval instead of three hundred.
const defaultAEFanout = 3

// SetAEFanout sets how many peers one anti-entropy round contacts
// (<=0 restores the default).
func (c *Cluster) SetAEFanout(n int) {
	c.mu.Lock()
	if n <= 0 {
		n = defaultAEFanout
	}
	c.aeFanout = n
	c.mu.Unlock()
}

// AEFanout returns the current anti-entropy fanout.
func (c *Cluster) AEFanout() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.aeFanout
}

// antiEntropyAll runs one scheduled round: the next `fanout` peers of a
// shuffled lap, so every peer is visited exactly once per lap and a
// round costs a bounded number of messages no matter how large the
// cluster grows.
func (c *Cluster) antiEntropyAll() {
	collections := c.st.Collections()
	c.mu.RLock()
	fanout := c.aeFanout
	c.mu.RUnlock()
	myZone := c.Zone()
	// Prefer peers in our own failure domain: a cross-AZ exchange
	// costs money and buys no correctness when a local peer exists. A
	// lap still covers every peer — locals first, remote after.
	prefer := func(a, b string) bool {
		if myZone == "" {
			return false
		}
		za, zb := c.ZoneOf(a) == myZone, c.ZoneOf(b) == myZone
		return za && !zb
	}
	for _, peer := range c.ae.nextPref(c.Peers(), fanout, prefer) {
		atomic.AddInt64(metrics.Default.Counter("microdb_ae_peers_contacted_total",
			"Peers contacted by anti-entropy rounds"), 1)
		if myZone != "" && c.ZoneOf(peer) != myZone {
			atomic.AddInt64(metrics.Default.Counter("microdb_ae_cross_zone_contacts_total",
				"Anti-entropy contacts that had to cross a failure domain"), 1)
		}
		for _, col := range collections {
			if err := c.syncCollection(peer, col); err != nil {
				log.Printf("anti-entropy %s <-> %s: %v", col, peer, err)
			}
		}
		// The peer may hold collections we don't know about yet.
		if extra, err := c.fetchCollectionsList(peer); err == nil {
			for _, col := range extra {
				if col != "" && !contains(collections, col) {
					if err := c.syncCollection(peer, col); err != nil {
						log.Printf("anti-entropy %s <-> %s: %v", col, peer, err)
					}
				}
			}
		}
	}
}

// RepairReport is what an explicit repair pass did.
type RepairReport struct {
	Peers       int   `json:"peers"`
	Collections int   `json:"collections_attempted"`
	Synced      int   `json:"collections_synced"`
	Failed      int   `json:"collections_failed"`
	DurationMs  int64 `json:"duration_ms"`
}

// RepairAll runs a full anti-entropy pass against every peer RIGHT
// NOW. This is the "re-fetch anything this node lost" button: after a
// corrupt record is dropped at replay (or a disk is replaced), one
// repair brings every missing document back from the peers that still
// hold it.
func (c *Cluster) RepairAll() RepairReport {
	start := time.Now()
	rep := RepairReport{}
	peers := c.Peers()
	collections := c.st.Collections()
	rep.Peers = len(peers)
	for _, peer := range peers {
		cols := collections
		if extra, err := c.fetchCollectionsList(peer); err == nil {
			for _, col := range extra {
				if col != "" && !contains(cols, col) {
					cols = append(cols, col)
				}
			}
		}
		for _, col := range cols {
			rep.Collections++
			if err := c.syncCollection(peer, col); err != nil {
				rep.Failed++
				log.Printf("repair %s <-> %s: %v", col, peer, err)
			} else {
				rep.Synced++
			}
		}
	}
	rep.DurationMs = time.Since(start).Milliseconds()
	return rep
}

// HandleRepair is POST /internal/repair: run the anti-entropy pass on
// demand instead of waiting for the timer.
func (c *Cluster) HandleRepair(w http.ResponseWriter, r *http.Request) {
	rep := c.RepairAll()
	writeJSON(w, 200, rep)
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func (c *Cluster) fetchCollectionsList(peer string) ([]string, error) {
	resp, err := c.client.Get(peer + "/internal/collections")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Collections []string `json:"collections"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out.Collections, err
}

// syncCollection runs one anti-entropy round against a peer for one
// collection using a subtree diff:
//  1. probe the peer's root+depth (tiny)
//  2. roots equal -> done
//  3. depths equal -> descend our tree against the remote, fetching
//     only hashes along divergent paths (O(k*depth) small requests)
//     depths differ -> fall back to full leaf exchange
//  4. for each divergent leaf: pull their doc, push ours if newer
//  5. both sides merge by (ver, ts)
func (c *Cluster) syncCollection(peer, col string) error {
	ourLeaves := c.st.Leaves(col)
	ourTree := merkle.Build(ourLeaves)

	// Step 1: cheap root probe.
	meta, err := c.fetchMerkleMeta(peer, col)
	if err != nil {
		return err
	}
	if ourTree.Root() == meta.Root {
		return nil // healthy
	}

	var divergent []merkle.Divergence
	if ourTree.Depth() == meta.Depth {
		// Step 3a: subtree descent — fetch only divergent hashes.
		rt := merkle.NewRemoteTree(meta.Root, meta.Depth,
			func(level, index int) (string, error) {
				return c.fetchMerkleNode(peer, col, level, index)
			},
			func(index int) (merkle.Leaf, bool, error) {
				return c.fetchMerkleLeaf(peer, col, index)
			})
		divergent, err = merkle.DiffAgainstRemote(ourTree, rt)
		if err != nil {
			return err
		}
	} else {
		// Step 3b: depth mismatch fallback — full leaf exchange.
		theirLeaves, err := c.fetchMerkleLeaves(peer, col)
		if err != nil {
			return err
		}
		theirTree := merkle.Build(theirLeaves)
		theirHash := make(map[string]string, len(theirLeaves))
		for _, l := range theirLeaves {
			theirHash[l.ID] = l.Hash
		}
		ourHash := make(map[string]string, len(ourLeaves))
		for _, l := range ourLeaves {
			ourHash[l.ID] = l.Hash
		}
		for _, id := range merkle.DiffIDsByMap(ourTree, theirTree) {
			d := merkle.Divergence{}
			if h, ok := ourHash[id]; ok {
				d.Local = merkle.Leaf{ID: id, Hash: h}
				d.HasLoc = true
			}
			if h, ok := theirHash[id]; ok {
				d.Remote = merkle.Leaf{ID: id, Hash: h}
				d.HasRem = true
			}
			divergent = append(divergent, d)
		}
	}
	if len(divergent) == 0 {
		return nil
	}

	// Step 4: exchange docs for divergent ids.
	ids := make([]string, 0, len(divergent))
	seen := map[string]bool{}
	for _, d := range divergent {
		if d.HasLoc && !seen[d.Local.ID] {
			seen[d.Local.ID] = true
			ids = append(ids, d.Local.ID)
		}
		if d.HasRem && !seen[d.Remote.ID] {
			seen[d.Remote.ID] = true
			ids = append(ids, d.Remote.ID)
		}
	}

	// Pull their versions.
	body, _ := json.Marshal(map[string]interface{}{"collection": col, "ids": ids})
	presp, err := c.post(peer+"/internal/antientropy", body)
	if err != nil {
		return err
	}
	var pulled struct {
		Docs []*store.Doc `json:"docs"`
	}
	decErr := json.NewDecoder(presp.Body).Decode(&pulled)
	presp.Body.Close()
	if decErr != nil {
		return decErr
	}
	applied := 0
	for _, d := range pulled.Docs {
		if c.st.ApplyRemote(d) {
			applied++
		}
	}

	// Push our versions they lack or hold stale.
	theirHash := make(map[string]string)
	for _, d := range divergent {
		if d.HasRem {
			theirHash[d.Remote.ID] = d.Remote.Hash
		}
	}
	var toPush []*store.Doc
	for _, d := range c.st.DocsByIDs(col, ids) {
		if theirHash[d.ID] != merkle.HashDoc(d) {
			toPush = append(toPush, d)
		}
	}
	if len(toPush) > 0 {
		pb, _ := json.Marshal(map[string]interface{}{"docs": toPush})
		sresp, err := c.post(peer+"/internal/replicate_bulk", pb)
		if err != nil {
			return err
		}
		sresp.Body.Close()
	}
	log.Printf("anti-entropy %s with %s: %d divergent leaves, pulled %d, pushed %d",
		col, peer, len(divergent), applied, len(toPush))
	return nil
}

func (c *Cluster) fetchMerkleMeta(peer, col string) (MerkleMeta, error) {
	var out struct {
		Root  string `json:"root"`
		Depth int    `json:"depth"`
	}
	resp, err := c.client.Get(peer + "/internal/merkle/" + col + "/meta")
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out, err
}

func (c *Cluster) fetchMerkleNode(peer, col string, level, index int) (string, error) {
	u := fmt.Sprintf("%s/internal/merkle/%s/node?level=%d&index=%d", peer, col, level, index)
	resp, err := c.client.Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		Hash string `json:"hash"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out.Hash, err
}

func (c *Cluster) fetchMerkleLeaf(peer, col string, index int) (merkle.Leaf, bool, error) {
	u := fmt.Sprintf("%s/internal/merkle/%s/leaf?index=%d", peer, col, index)
	resp, err := c.client.Get(u)
	if err != nil {
		return merkle.Leaf{}, false, err
	}
	defer resp.Body.Close()
	var out struct {
		Leaf merkle.Leaf `json:"leaf"`
		OK   bool        `json:"ok"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return merkle.Leaf{}, false, err
	}
	return out.Leaf, out.OK, nil
}

func (c *Cluster) fetchMerkleLeaves(peer, col string) ([]merkle.Leaf, error) {
	resp, err := c.client.Get(peer + "/internal/merkle/" + col)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out struct {
		Leaves []merkle.Leaf `json:"leaves"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	return out.Leaves, err
}

func (c *Cluster) Stop() { close(c.stop) }

func (c *Cluster) gossipRound() {
	c.updateSelfLoad()
	// Refresh placement every tick: the weighted ring is rebuilt from
	// the latest gossiped loads (ours + peers'), so a node that gets
	// busier gradually takes a larger share of the keyspace. Rebuild
	// is same-epoch and cheap (a few hundred vnodes), so no fencing
	// churn and every node converges on the same view.
	if c.OnPeersChanged != nil {
		c.OnPeersChanged(c.Peers())
	}
	peers := c.Peers()
	if len(peers) == 0 {
		return
	}
	peer := peers[rand.Intn(len(peers))]
	if c.isIncompatible(peer) {
		return // known to speak an out-of-window wire version
	}
	changed := false
	if c.seen(peer) {
		changed = true
	}
	// Push our view + epoch + departure tombstones, pull theirs.
	members := c.memberList()
	c.mu.RLock()
	left := make(map[string]int64, len(c.left))
	for a, t := range c.left {
		left[a] = t.UnixMilli()
	}
	c.mu.RUnlock()
	body, _ := json.Marshal(map[string]interface{}{"addr": c.self, "members": members, "epoch": c.Epoch(), "left": left})
	resp, err := c.post(peer+"/internal/gossip", body)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	// 505 = the peer refused our wire version (or advertised one we
	// cannot read). The transport has already recorded it; stop here so
	// its payload can never be merged into our membership view.
	if resp.StatusCode == 505 || resp.StatusCode != http.StatusOK {
		return
	}
	var out struct {
		Members []Member         `json:"members"`
		Epoch   int64            `json:"epoch"`
		Left    map[string]int64 `json:"left"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return
	}
	// Merge departure tombstones (keep the latest timestamp per addr)
	// so the whole cluster refuses to resurrect a gracefully-departed
	// node, not just the peers it notified directly.
	if len(out.Left) > 0 {
		c.mu.Lock()
		for a, ms := range out.Left {
			t := time.UnixMilli(ms)
			if cur, ok := c.left[a]; !ok || t.After(cur) {
				c.left[a] = t
			}
		}
		c.mu.Unlock()
	}
	now := time.Now().UnixMilli()
	for _, m := range out.Members {
		if m.Addr != c.self && m.TTL > now {
			c.mu.Lock()
			c.loads[m.Addr] = m.Load
			c.mu.Unlock()
			if c.seen(m.Addr) {
				changed = true
			}
		}
	}
	// Adopt a higher epoch even if the member set looks identical to ours:
	// a peer that saw a join/eviction we missed must pull us forward.
	c.mu.Lock()
	if c.adoptEpochLocked(out.Epoch) {
		changed = true
	}
	c.mu.Unlock()
	if changed && c.OnPeersChanged != nil {
		c.OnPeersChanged(c.Peers())
	}
}

func (c *Cluster) memberList() []Member {
	peers := c.Peers()
	out := make([]Member, 0, len(peers)+1)
	out = append(out, Member{Addr: c.self, TTL: time.Now().UnixMilli() + 15000, Load: c.SelfLoad(), Zone: c.Zone()})
	for _, p := range peers {
		out = append(out, Member{Addr: p, TTL: time.Now().UnixMilli() + 15000, Load: c.LoadOf(p), Zone: c.ZoneOf(p)})
	}
	return out
}

// SelfLoad returns this node's EWMA of client writes/sec.
func (c *Cluster) SelfLoad() float64 {
	return math.Float64frombits(c.selfLoad.Load())
}

// LoadOf returns the last gossiped write-rate for a peer.
func (c *Cluster) LoadOf(addr string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loads[addr]
}

// Loads returns a snapshot of all known node loads (self included).
func (c *Cluster) Loads() map[string]float64 {
	c.mu.RLock()
	out := make(map[string]float64, len(c.loads)+1)
	for k, v := range c.loads {
		out[k] = v
	}
	c.mu.RUnlock()
	out[c.self] = c.SelfLoad()
	return out
}

// updateSelfLoad samples the store's client-write counter and folds
// the per-second rate into an EWMA (alpha=0.3: ~3s effective window).
// Called once per gossip tick.
func (c *Cluster) updateSelfLoad() {
	cur := c.st.WriteCount()
	prev := c.lastWrites.Swap(cur)
	delta := cur - prev
	ewma := c.SelfLoad()*0.7 + float64(delta)*0.3
	c.selfLoad.Store(math.Float64bits(ewma))
}

// Eviction also bumps the epoch and notifies: a node leaving is a
// membership change exactly like one joining.
func (c *Cluster) evict() {
	c.mu.Lock()
	changed := false
	now := time.Now()
	for a, t := range c.peers {
		if now.Sub(t) > 15*time.Second {
			delete(c.peers, a)
			c.epoch++
			changed = true
		}
	}
	cb := c.OnPeersChanged
	peers := c.peersLocked()
	c.mu.Unlock()
	if changed && cb != nil {
		cb(peers)
	}
}

// AdoptEpoch raises our epoch to at least n (never lowers) and ALWAYS
// fires the membership callback so the ring is rebuilt with the new
// epoch. It must not skip the rebuild when the cluster epoch already
// equals n: the ring can lag the cluster epoch (epoch adopted via one
// path, ring rebuilt at an older value), and a stale ring epoch would
// make us fence ourselves forever.
func (c *Cluster) AdoptEpoch(n int64) {
	c.mu.Lock()
	c.adoptEpochLocked(n)
	cb := c.OnPeersChanged
	peers := c.peersLocked()
	c.mu.Unlock()
	if cb != nil {
		cb(peers)
	}
}

// ForcePeer injects a peer address into the live set (test hook for
// simulating unreachable members in quorum scenarios). Fires the
// membership callback so the ring actually includes the injected peer.
func (c *Cluster) ForcePeer(addr string) {
	c.mu.Lock()
	c.peers[addr] = time.Now()
	c.epoch++
	cb := c.OnPeersChanged
	peers := c.peersLocked()
	c.mu.Unlock()
	if cb != nil {
		cb(peers)
	}
}

// BumpEpoch raises the membership epoch without changing the member
// set (test hook: simulates having observed a membership change that
// a peer hasn't seen yet). Fires the ring-rebuild callback.
func (c *Cluster) BumpEpoch() {
	c.mu.Lock()
	c.epoch++
	cb := c.OnPeersChanged
	peers := c.peersLocked()
	c.mu.Unlock()
	if cb != nil {
		cb(peers)
	}
}

// peersLocked returns alive peers; caller holds c.mu (any mode).
func (c *Cluster) peersLocked() []string {
	now := time.Now()
	out := make([]string, 0, len(c.peers))
	for a, t := range c.peers {
		if now.Sub(t) < 15*time.Second {
			out = append(out, a)
		}
	}
	return out
}

// EnableHints turns on hinted handoff: failed Replicate calls to a peer
// are stored durably in the hint file under dir and replayed when the
// peer becomes reachable again. Call before Start.
func (c *Cluster) EnableHints(dir string) error {
	hs, err := hints.Load(filepath.Join(dir, "hints.jsonl"))
	if err != nil {
		return err
	}
	c.hints = hs
	atomic.StoreInt64(metrics.Default.Gauge("microdb_hints_pending", "Hinted-handoff records pending"), int64(hs.Pending()))
	return nil
}

// Hints exposes the hint store for status/metrics (nil if disabled).
func (c *Cluster) Hints() *hints.Store { return c.hints }

// hintFailed records a failed replication as a hint (no-op when hints
// are disabled).
func (c *Cluster) hintFailed(peer string, d *store.Doc) {
	if c.hints == nil || d == nil {
		return
	}
	c.hints.Add(peer, d)
	atomic.StoreInt64(metrics.Default.Gauge("microdb_hints_pending", "Hinted-handoff records pending"), int64(c.hints.Pending()))
}

// replayHints tries to deliver every due hint. Called from the gossip
// loop; delivery failures reschedule with exponential backoff so a dead
// peer doesn't burn the network.
func (c *Cluster) replayHints() {
	if c.hints == nil {
		return
	}
	due := c.hints.Due(time.Now())
	for _, h := range due {
		if err := c.postReplicate(h.Target, h.Doc); err != nil {
			c.hints.Reschedule(h.Target, h.Doc.Collection, h.Doc.ID, 5*time.Minute)
			continue
		}
		c.hints.Remove(h.Target, h.Doc.Collection, h.Doc.ID)
	}
	if len(due) > 0 {
		atomic.StoreInt64(metrics.Default.Gauge("microdb_hints_pending", "Hinted-handoff records pending"), int64(c.hints.Pending()))
	}
}

// postReplicate is Replicate without the hint-recording wrapper (used
// by the replay loop to avoid recursion on failure).
func (c *Cluster) postReplicate(peer string, d *store.Doc) error {
	b, _ := json.Marshal(d)
	resp, err := c.post(peer+"/internal/replicate", b)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return &PeerError{peer}
	}
	return nil
}

// HandleHints reports the sender-side hinted-handoff state.
func (c *Cluster) HandleHints(w http.ResponseWriter, r *http.Request) {
	if c.hints == nil {
		writeJSON(w, 200, map[string]interface{}{"enabled": false})
		return
	}
	pending, added, delivered, dropped, byTarget := c.hints.Stats()
	writeJSON(w, 200, map[string]interface{}{
		"enabled":   true,
		"pending":   pending,
		"added":     added,
		"delivered": delivered,
		"dropped":   dropped,
		"by_target": byTarget,
	})
}

// Draining reports whether this node is mid-decommission.
func (c *Cluster) Draining() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.draining
}

// Decommission gracefully removes this node from the cluster:
//  1. mark draining (new writes to this node are refused with 503)
//  2. push every local doc to every remaining peer in bulk so no
//     range depends on us any more
//  3. broadcast a leave so peers evict us immediately and rebuild
//     their rings, instead of waiting out the 15s TTL
//
// Returns how many docs were handed off. After it returns the process
// can exit; remaining nodes converge on the new ring.
// SetDraining marks this node as not accepting new work — /ready
// flips to 503 while /health stays 200 — without touching any data.
// It is the first step of both a decommission and a graceful shutdown.
func (c *Cluster) SetDraining(on bool) {
	c.mu.Lock()
	c.draining = on
	c.mu.Unlock()
}

// Leave tells every peer we're departing: they drop us from their
// member set and bump the epoch immediately, instead of waiting out the
// 15s TTL. It moves no data (that is Decommission's job) — it is the
// cheap half, used by graceful shutdown where the replicas already
// hold everything. Returns how many peers were reached.
func (c *Cluster) Leave() int {
	peers := c.Peers()
	for _, p := range peers {
		b, _ := json.Marshal(map[string]interface{}{"addr": c.self, "epoch": c.Epoch()})
		resp, err := c.post(p+"/internal/leave", b)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return len(peers)
}

func (c *Cluster) Decommission() (int, error) {
	c.mu.Lock()
	c.draining = true
	peers := c.peersLocked()
	c.mu.Unlock()
	if len(peers) == 0 {
		return 0, nil // standalone node: nothing to hand off
	}
	docs := c.st.AllDocs()
	// Hand off in chunks so a huge store doesn't build one giant request.
	const chunk = 500
	handedOff := 0
	for i := 0; i < len(docs); i += chunk {
		end := i + chunk
		if end > len(docs) {
			end = len(docs)
		}
		b, _ := json.Marshal(map[string]interface{}{"docs": docs[i:end]})
		for _, p := range peers {
			resp, err := c.post(p+"/internal/replicate_bulk", b)
			if err != nil {
				// Peer down mid-drain: stash as hints so a later
				// return still gets the data.
				for _, d := range docs[i:end] {
					c.hintFailed(p, d)
				}
				continue
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		handedOff = end
	}
	// Tell everyone we're leaving: they drop us from their member set
	// right away and bump the epoch.
	c.Leave()
	return handedOff, nil
}

// HandleDecommission triggers graceful shutdown of this node over the
// wire (used by `microctl decommission`). The node marks itself
// draining, hands off its data, and broadcasts leave; the caller then
// stops the process.
func (c *Cluster) HandleDecommission(w http.ResponseWriter, r *http.Request) {
	handedOff, err := c.Decommission()
	if err != nil {
		writeJSON(w, 500, map[string]interface{}{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "handed_off": handedOff})
}

// HandleLeave processes a graceful-departure notice: remove the peer
// now (with an epoch bump) and rebuild our ring. A leave for an
// unknown address is a no-op.
func (c *Cluster) HandleLeave(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Addr  string `json:"addr"`
		Epoch int64  `json:"epoch"`
	}
	json.NewDecoder(r.Body).Decode(&in)
	c.mu.Lock()
	c.adoptEpochLocked(in.Epoch)
	// Record the tombstone BEFORE deleting so a concurrent gossip
	// carrying the old member list can't re-add the address.
	if in.Addr != "" && in.Addr != c.self {
		c.left[in.Addr] = time.Now()
	}
	_, had := c.peers[in.Addr]
	delete(c.peers, in.Addr)
	if had {
		c.epoch++
	}
	cb := c.OnPeersChanged
	peers := c.peersLocked()
	c.mu.Unlock()
	if had && cb != nil {
		cb(peers)
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "removed": had, "epoch": c.Epoch()})
}

// Streaming reports whether this node is still backfilling data from
// the cluster after a join (bootstrap phase). While true, reads may
// be stale — the node accepts writes but hasn't received the ranges
// it owns yet.
func (c *Cluster) Streaming() bool { return !c.bootstrapped.Load() }

// MarkBootstrapped flips the node out of streaming state (used when
// the node starts standalone with no seed to sync from).
func (c *Cluster) MarkBootstrapped() { c.bootstrapped.Store(true) }

// SetMaxBootstraps caps how many concurrent bootstrap streams this
// node will serve as a seed (admission control; default 2).
func (c *Cluster) SetMaxBootstraps(n int64) {
	if n > 0 {
		c.maxBootstraps = n
	}
}

// bootstrapFrom streams ALL of the seed's data into the local store.
// This is the fast path that replaces waiting for background
// anti-entropy rounds after a join: the joiner pulls every
// collection in paginated chunks and applies them locally. Retries
// on 429 (seed admission full) with backoff.
func (c *Cluster) bootstrapFrom(seed string) error {
	cols, err := c.fetchCollectionsList(seed)
	if err != nil {
		return fmt.Errorf("bootstrap list collections: %w", err)
	}
	for _, col := range cols {
		if err := c.bootstrapCollection(seed, col); err != nil {
			return fmt.Errorf("bootstrap %s: %w", col, err)
		}
	}
	return nil
}

func (c *Cluster) bootstrapCollection(seed, col string) error {
	const page = 500
	after := ""
	for {
		var docs []*store.Doc
		err := c.withRetry(func() error {
			u := fmt.Sprintf("%s/internal/stream/%s?limit=%d", seed, col, page)
			if after != "" {
				u += "&after=" + after
			}
			resp, err := c.client.Get(u)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode == 429 {
				return errBootstrapBusy
			}
			if resp.StatusCode != 200 {
				return fmt.Errorf("stream status %d", resp.StatusCode)
			}
			var out struct {
				Docs  []*store.Doc `json:"docs"`
				After string       `json:"after"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				return err
			}
			docs = out.Docs
			after = out.After
			return nil
		})
		if err != nil {
			return err
		}
		applied := 0
		for _, d := range docs {
			if c.st.ApplyRemote(d) {
				applied++
			}
		}
		log.Printf("bootstrap %s: pulled %d docs (%d applied)", col, len(docs), applied)
		if after == "" || len(docs) == 0 {
			return nil
		}
	}
}

var errBootstrapBusy = fmt.Errorf("seed bootstrap admission full")

func (c *Cluster) withRetry(fn func() error) error {
	delay := 500 * time.Millisecond
	for attempt := 0; attempt < 10; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if err != errBootstrapBusy {
			// One retry for transient errors, then give up.
			if attempt > 0 {
				return err
			}
		}
		time.Sleep(delay)
		if delay < 8*time.Second {
			delay *= 2
		}
	}
	return fmt.Errorf("bootstrap retries exhausted")
}

// HandleStream serves one page of a collection's docs (tombstones
// included) for bootstrap streaming. Admission-controlled: at most
// maxBootstraps concurrent stream requests are served cluster-wide
// per node; others get 429 so a stampede of joining nodes can't
// melt the seed.
func (c *Cluster) HandleStream(w http.ResponseWriter, r *http.Request) {
	if c.activeBootstraps.Add(1) > c.maxBootstraps {
		c.activeBootstraps.Add(-1)
		w.Header().Set("Retry-After", "2")
		writeJSON(w, 429, map[string]interface{}{"error": "bootstrap admission limit reached"})
		return
	}
	defer c.activeBootstraps.Add(-1)
	col := r.PathValue("col")
	limit := 500
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 2000 {
			limit = n
		}
	}
	after := r.URL.Query().Get("after")
	docs := c.st.StreamCollection(col, after, limit)
	nextAfter := ""
	if len(docs) == limit {
		last := docs[len(docs)-1]
		nextAfter = last.ID
	}
	writeJSON(w, 200, map[string]interface{}{"docs": docs, "after": nextAfter})
}

// HandleBootstrapStatus reports this node's bootstrap state.
func (c *Cluster) HandleBootstrapStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"streaming":      c.Streaming(),
		"active_serves":  c.activeBootstraps.Load(),
		"max_bootstraps": c.maxBootstraps,
	})
}

// Replicate sends a doc to a peer; fire-and-forget with one retry.
// On failure the doc is stashed as a hint (if enabled) for later
// handoff, so the write still reaches full RF once the peer returns.
func (c *Cluster) Replicate(peer string, d *store.Doc) error {
	atomic.AddInt64(metrics.Default.Counter("microdb_replication_sends_total", "Replication push attempts"), 1)
	b, _ := json.Marshal(d)
	for attempt := 0; attempt < 2; attempt++ {
		resp, err := c.post(peer+"/internal/replicate", b)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
	}
	c.hintFailed(peer, d)
	return &PeerError{peer}
}

type PeerError struct{ peer string }

func (e *PeerError) Error() string { return "replicate failed: " + e.peer }

// HandleJoin / HandleGossip / HandleReplicate wire into the HTTP mux.
func (c *Cluster) HandleJoin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Addr  string `json:"addr"`
		Epoch int64  `json:"epoch"`
	}
	json.NewDecoder(r.Body).Decode(&in)
	c.mu.Lock()
	changed := c.adoptEpochLocked(in.Epoch)
	cb := c.OnPeersChanged
	// An explicit join overrides any departure tombstone for that
	// address: the node is back on purpose, not stale gossip.
	if in.Addr != "" && in.Addr != c.self {
		delete(c.left, in.Addr)
	}
	c.mu.Unlock()
	if in.Addr != "" && in.Addr != c.self {
		if c.seen(in.Addr) {
			changed = true
		}
	}
	if changed && cb != nil {
		// A new node joined us directly — our ring must learn it now,
		// not only when gossip happens to report a change.
		cb(c.Peers())
	}
	writeJSON(w, 200, map[string]interface{}{"members": c.memberList(), "epoch": c.Epoch()})
}

func (c *Cluster) HandleGossip(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Addr    string           `json:"addr"`
		Members []Member         `json:"members"`
		Epoch   int64            `json:"epoch"`
		Left    map[string]int64 `json:"left"`
	}
	json.NewDecoder(r.Body).Decode(&in)
	c.mu.Lock()
	c.adoptEpochLocked(in.Epoch)
	for a, ms := range in.Left {
		t := time.UnixMilli(ms)
		if cur, ok := c.left[a]; !ok || t.After(cur) {
			c.left[a] = t
		}
	}
	c.mu.Unlock()
	now := time.Now().UnixMilli()
	for _, m := range in.Members {
		if m.Addr != c.self && m.TTL > now {
			c.mu.Lock()
			c.loads[m.Addr] = m.Load
			if m.Zone != "" {
				c.zones[m.Addr] = m.Zone
			}
			c.mu.Unlock()
			c.seen(m.Addr)
		}
	}
	writeJSON(w, 200, map[string]interface{}{"members": c.memberList(), "epoch": c.Epoch()})
}

func (c *Cluster) HandleReplicate(w http.ResponseWriter, r *http.Request) {
	var d store.Doc
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	c.st.ApplyRemote(&d)
	writeJSON(w, 200, map[string]bool{"ok": true})
}

// HandleCollections lists collection names (for anti-entropy discovery).
func (c *Cluster) HandleCollections(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{"collections": c.st.Collections()})
}

// HandleMerkle returns the leaves of a collection so a peer can build
// and diff the tree. Root included for quick comparison/logging.
func (c *Cluster) HandleMerkle(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	leaves := c.st.Leaves(col)
	tree := merkle.Build(leaves)
	writeJSON(w, 200, map[string]interface{}{
		"collection": col,
		"root":       tree.Root(),
		"leaves":     leaves,
	})
}

// HandleMerkleMeta returns just the root and depth — the cheap probe
// that starts a subtree-diff sync without shipping any leaves.
func (c *Cluster) HandleMerkleMeta(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	tree := merkle.Build(c.st.Leaves(col))
	writeJSON(w, 200, map[string]interface{}{"root": tree.Root(), "depth": tree.Depth()})
}

// HandleMerkleNode returns the hash at (level, index) of the tree.
func (c *Cluster) HandleMerkleNode(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	level, err1 := strconv.Atoi(r.URL.Query().Get("level"))
	index, err2 := strconv.Atoi(r.URL.Query().Get("index"))
	if err1 != nil || err2 != nil {
		writeJSON(w, 400, map[string]string{"error": "level and index required"})
		return
	}
	tree := merkle.Build(c.st.Leaves(col))
	if level < 0 || level > tree.Depth() || index < 0 || index >= 1<<(tree.Depth()-level) {
		writeJSON(w, 404, map[string]string{"error": "node out of range"})
		return
	}
	writeJSON(w, 200, map[string]interface{}{"hash": tree.HashAt(level, index)})
}

// HandleMerkleLeaf returns the leaf at padded index (deleted docs
// included; padding returns ok=false).
func (c *Cluster) HandleMerkleLeaf(w http.ResponseWriter, r *http.Request) {
	col := r.PathValue("col")
	index, err := strconv.Atoi(r.URL.Query().Get("index"))
	if err != nil {
		writeJSON(w, 400, map[string]string{"error": "index required"})
		return
	}
	tree := merkle.Build(c.st.Leaves(col))
	leaf, ok := tree.LeafAt(index)
	writeJSON(w, 200, map[string]interface{}{"leaf": leaf, "ok": ok})
}

// HandleAntiEntropy returns our docs for the requested divergent ids.
func (c *Cluster) HandleAntiEntropy(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Collection string   `json:"collection"`
		IDs        []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	docs := c.st.DocsByIDs(in.Collection, in.IDs)
	writeJSON(w, 200, map[string]interface{}{"docs": docs})
}

// HandleReplicateBulk applies a batch of docs (used by anti-entropy push).
func (c *Cluster) HandleReplicateBulk(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Docs []*store.Doc `json:"docs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeJSON(w, 400, map[string]string{"error": err.Error()})
		return
	}
	applied := 0
	for _, d := range in.Docs {
		if c.st.ApplyRemote(d) {
			applied++
		}
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "applied": applied})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

var _ = log.Println
