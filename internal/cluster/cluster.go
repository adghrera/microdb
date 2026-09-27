// Package cluster keeps nodes aware of each other via periodic gossip and
// replicates writes to the ring-owners of each key. Membership converges
// through push/pull of the node list; failed nodes are evicted after a
// timeout. This is deliberately simple: eventual consistency, no quorum
// reads, last-writer-wins by (ver, ts) at the store layer.
package cluster

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"microdb/internal/hints"
	"microdb/internal/merkle"
	"microdb/internal/metrics"
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
}

// ClientTLS builds the tls.Config used for peer-to-peer calls:
// verifies peer certs against the CA and presents our cert so peers
// can verify us back (mutual TLS).
func (o *TLSOptions) ClientTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
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
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// ServerTLS builds the tls.Config for serving internal endpoints:
// requires and verifies client certs against the CA.
func (o *TLSOptions) ServerTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
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
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

type Member struct {
	Addr string `json:"addr"` // http://host:port
	TTL  int64  `json:"ttl"`  // unix millis; expired members are dropped
}

type Cluster struct {
	self    string
	peers   map[string]time.Time // addr -> last seen (alive)
	mu      sync.RWMutex
	epoch   int64 // membership epoch; bumped on every membership change
	st      *store.Store
	client  *http.Client
	stop    chan struct{}
	// hints is the hinted-handoff store; nil until EnableHints is called.
	hints *hints.Store
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
	return &Cluster{
		self:   self,
		peers:  map[string]time.Time{},
		st:     st,
		client: &http.Client{Timeout: 3 * time.Second, Transport: transport},
		stop:   make(chan struct{}),
	}, nil
}

// Join contacts a known seed node and asks for its member list.
func (c *Cluster) Join(seed string) error {
	resp, err := c.client.Post(seed+"/internal/join", "application/json",
		bytes.NewReader([]byte(`{"addr":"`+c.self+`"}`)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
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
	return nil
}

func (c *Cluster) seen(addr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, had := c.peers[addr]
	c.peers[addr] = time.Now()
	if !had {
		c.epoch++ // membership changed — new fencing epoch
	}
	return !had
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
func (c *Cluster) antiEntropyAll() {
	collections := c.st.Collections()
	for _, peer := range c.Peers() {
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
	presp, err := c.client.Post(peer+"/internal/antientropy", "application/json", bytes.NewReader(body))
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
		sresp, err := c.client.Post(peer+"/internal/replicate_bulk", "application/json", bytes.NewReader(pb))
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
	peers := c.Peers()
	if len(peers) == 0 {
		return
	}
	peer := peers[rand.Intn(len(peers))]
	changed := false
	if c.seen(peer) {
		changed = true
	}
	// Push our view + epoch, pull theirs.
	members := c.memberList()
	body, _ := json.Marshal(map[string]interface{}{"addr": c.self, "members": members, "epoch": c.Epoch()})
	resp, err := c.client.Post(peer+"/internal/gossip", "application/json", bytes.NewReader(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var out struct {
		Members []Member `json:"members"`
		Epoch   int64    `json:"epoch"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, m := range out.Members {
		if m.Addr != c.self && m.TTL > now {
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
	out = append(out, Member{Addr: c.self, TTL: time.Now().UnixMilli() + 15000})
	for _, p := range peers {
		out = append(out, Member{Addr: p, TTL: time.Now().UnixMilli() + 15000})
	}
	return out
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

// AdoptEpoch raises our epoch to at least n (never lowers) and fires
// the membership callback so the ring is rebuilt with the new epoch.
func (c *Cluster) AdoptEpoch(n int64) {
	c.mu.Lock()
	changed := c.adoptEpochLocked(n)
	cb := c.OnPeersChanged
	peers := c.peersLocked()
	c.mu.Unlock()
	if changed && cb != nil {
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
	resp, err := c.client.Post(peer+"/internal/replicate", "application/json", bytes.NewReader(b))
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

// Replicate sends a doc to a peer; fire-and-forget with one retry.
// On failure the doc is stashed as a hint (if enabled) for later
// handoff, so the write still reaches full RF once the peer returns.
func (c *Cluster) Replicate(peer string, d *store.Doc) error {
	atomic.AddInt64(metrics.Default.Counter("microdb_replication_sends_total", "Replication push attempts"), 1)
	b, _ := json.Marshal(d)
	for attempt := 0; attempt < 2; attempt++ {
		resp, err := c.client.Post(peer+"/internal/replicate", "application/json", bytes.NewReader(b))
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
	c.adoptEpochLocked(in.Epoch)
	c.mu.Unlock()
	if in.Addr != "" && in.Addr != c.self {
		if c.seen(in.Addr) && c.OnPeersChanged != nil {
			// A new node joined us directly — our ring must learn it now,
			// not only when gossip happens to report a change.
			c.OnPeersChanged(c.Peers())
		}
	}
	writeJSON(w, 200, map[string]interface{}{"members": c.memberList(), "epoch": c.Epoch()})
}

func (c *Cluster) HandleGossip(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Addr    string   `json:"addr"`
		Members []Member `json:"members"`
		Epoch   int64    `json:"epoch"`
	}
	json.NewDecoder(r.Body).Decode(&in)
	c.mu.Lock()
	c.adoptEpochLocked(in.Epoch)
	c.mu.Unlock()
	now := time.Now().UnixMilli()
	for _, m := range in.Members {
		if m.Addr != c.self && m.TTL > now {
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
