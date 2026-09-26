// Package cluster keeps nodes aware of each other via periodic gossip and
// replicates writes to the ring-owners of each key. Membership converges
// through push/pull of the node list; failed nodes are evicted after a
// timeout. This is deliberately simple: eventual consistency, no quorum
// reads, last-writer-wins by (ver, ts) at the store layer.
package cluster

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"microdb/internal/merkle"
	"microdb/internal/store"
)

type Member struct {
	Addr string `json:"addr"` // http://host:port
	TTL  int64  `json:"ttl"`  // unix millis; expired members are dropped
}

type Cluster struct {
	self    string
	peers   map[string]time.Time // addr -> last seen (alive)
	mu      sync.RWMutex
	st      *store.Store
	client  *http.Client
	stop    chan struct{}
	// OnPeersChanged is called with the live peer list whenever membership changes.
	OnPeersChanged func(addrs []string)
}

func New(self string, st *store.Store) *Cluster {
	return &Cluster{
		self:   self,
		peers:  map[string]time.Time{},
		st:     st,
		client: &http.Client{Timeout: 3 * time.Second},
		stop:   make(chan struct{}),
	}
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
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
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
	return !had
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

// syncCollection runs one Merkle round against a peer for one collection:
//  1. ask peer for its leaves (sending our root so it can answer "same" cheaply)
//  2. diff the trees -> divergent ids
//  3. pull their docs for those ids, push our docs for those ids
//  4. both sides merge by (ver, ts)
func (c *Cluster) syncCollection(peer, col string) error {
	ourLeaves := c.st.Leaves(col)
	ourTree := merkle.Build(ourLeaves)

	// Step 1: fetch their leaves (cheap root check happens server-side too,
	// but we need their leaves anyway when roots differ, so just fetch).
	resp, err := c.client.Get(peer + "/internal/merkle/" + col)
	if err != nil {
		return err
	}
	var their struct {
		Collection string        `json:"collection"`
		Root       string        `json:"root"`
		Leaves     []merkle.Leaf `json:"leaves"`
	}
	decErr := json.NewDecoder(resp.Body).Decode(&their)
	resp.Body.Close()
	if decErr != nil {
		return decErr
	}
	theirTree := merkle.Build(their.Leaves)

	// Healthy case: roots equal, nothing to do.
	if ourTree.Root() == theirTree.Root() {
		return nil
	}

	// Step 2: find divergent ids.
	diff := merkle.DiffIDsAuto(ourTree, theirTree)
	if len(diff) == 0 {
		return nil
	}

	// Step 3a: pull their versions of the divergent docs.
	body, _ := json.Marshal(map[string]interface{}{"collection": col, "ids": diff})
	presp, err := c.client.Post(peer+"/internal/antientropy", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	var pulled struct {
		Docs []*store.Doc `json:"docs"`
	}
	decErr = json.NewDecoder(presp.Body).Decode(&pulled)
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

	// Step 3b: push our versions of the divergent docs that they still lack
	// or hold stale copies of (recompute against their leaf map).
	theirHash := make(map[string]string, len(their.Leaves))
	for _, l := range their.Leaves {
		theirHash[l.ID] = l.Hash
	}
	var toPush []*store.Doc
	for _, d := range c.st.DocsByIDs(col, diff) {
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
	log.Printf("anti-entropy %s with %s: %d divergent ids, pulled %d, pushed %d",
		col, peer, len(diff), applied, len(toPush))
	return nil
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
	// Push our view, pull theirs.
	members := c.memberList()
	body, _ := json.Marshal(map[string]interface{}{"addr": c.self, "members": members})
	resp, err := c.client.Post(peer+"/internal/gossip", "application/json", bytes.NewReader(body))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var out struct {
		Members []Member `json:"members"`
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

func (c *Cluster) evict() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for a, t := range c.peers {
		if now.Sub(t) > 15*time.Second {
			delete(c.peers, a)
		}
	}
}

// Replicate sends a doc to a peer; fire-and-forget with one retry.
func (c *Cluster) Replicate(peer string, d *store.Doc) error {
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
	return &PeerError{peer}
}

type PeerError struct{ peer string }

func (e *PeerError) Error() string { return "replicate failed: " + e.peer }

// HandleJoin / HandleGossip / HandleReplicate wire into the HTTP mux.
func (c *Cluster) HandleJoin(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Addr string `json:"addr"`
	}
	json.NewDecoder(r.Body).Decode(&in)
	if in.Addr != "" && in.Addr != c.self {
		if c.seen(in.Addr) && c.OnPeersChanged != nil {
			// A new node joined us directly — our ring must learn it now,
			// not only when gossip happens to report a change.
			c.OnPeersChanged(c.Peers())
		}
	}
	writeJSON(w, 200, map[string]interface{}{"members": c.memberList()})
}

func (c *Cluster) HandleGossip(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Addr    string   `json:"addr"`
		Members []Member `json:"members"`
	}
	json.NewDecoder(r.Body).Decode(&in)
	now := time.Now().UnixMilli()
	for _, m := range in.Members {
		if m.Addr != c.self && m.TTL > now {
			c.seen(m.Addr)
		}
	}
	writeJSON(w, 200, map[string]interface{}{"members": c.memberList()})
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
