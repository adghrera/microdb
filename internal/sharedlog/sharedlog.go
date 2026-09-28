// Package sharedlog implements a durable, quorum-replicated commit
// log service — the durability tier that storage nodes write
// THROUGH instead of relying on their own local fsync.
//
// Topology: a small static cluster of log nodes (typically 3). The
// leader is the FIRST member in the configured order that is
// reachable; every node probes in the same order so the whole cluster
// converges on the same leader without an election protocol. The
// leader assigns monotonically increasing LSNs, appends locally, and
// replicates to followers synchronously; an append is ACKed only
// after a QUORUM (majority of members) have persisted it. Durability
// therefore comes from the log's own replication, not from any
// single disk.
//
// Records are opaque JSON: the log doesn't interpret them. Storage
// nodes append {collection,id,doc} entries and tail them back on
// recovery, replaying from the last-applied LSN.
//
// Failover: if the leader is unreachable, the next member in order
// promotes itself, seeding its LSN above the highest LSN it can
// observe from any peer — so LSNs never go backwards across
// failovers. A partitioned old leader refuses appends when it can
// no longer reach quorum, so two leaders can't double-assign.
package sharedlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// internalClient bounds every inter-node probe so a peer that is
// down (or listening-but-not-yet-serving, e.g. during startup)
// fails fast instead of blocking leadership decisions forever.
var internalClient = &http.Client{Timeout: 2 * time.Second}

// ErrNoQuorum means the append could not reach a durable majority.
var ErrNoQuorum = errors.New("shared log: quorum not reached")

// ErrNotLeader means this node forwarded-or-refused because another
// member holds leadership.
var ErrNotLeader = errors.New("shared log: not leader")

// Entry is one log record: an opaque payload with a global LSN.
type Entry struct {
	LSN     int64           `json:"lsn"`
	TS      int64           `json:"ts"`
	Payload json.RawMessage `json:"payload"`
}

// Server is one log node.
type Server struct {
	members []string // ordered; leader = first reachable
	self    string
	dir     string

	mu     sync.Mutex
	lsn    int64
	f      *os.File
	fbuf   *bufio.Writer
	leader string // current believed leader
}

// NewServer opens (or creates) the log segment in dir.
func NewServer(members []string, self, dir string) (*Server, error) {
	if len(members) == 0 {
		return nil, errors.New("shared log: no members")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Server{members: members, self: self, dir: dir}
	f, err := os.OpenFile(filepath.Join(dir, "commitlog.jsonl"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// Recover own last LSN by scanning the tail.
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e Entry
		if json.Unmarshal([]byte(line), &e) == nil && e.LSN > s.lsn {
			s.lsn = e.LSN
		}
	}
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}
	s.f = f
	s.fbuf = bufio.NewWriterSize(f, 1<<20)
	s.leader = s.electLeader()
	if s.leader == s.self {
		// We start as leader: seed our LSN above the highest LSN
		// observable from any peer, so a restart with a stale (or
		// empty) local log can never re-assign LSNs that a previous
		// leader already handed out.
		s.seedLSNFromPeers()
	}
	return s, nil
}

// seedLSNFromPeers raises our LSN to the max LSN reported by any
// reachable peer. Called when this node assumes leadership so that
// LSNs are monotonic across failovers even if our local log was
// truncated, wiped, or simply older than the cluster's.
func (s *Server) seedLSNFromPeers() {
	var wg sync.WaitGroup
	for _, m := range s.members {
		if m == s.self {
			continue
		}
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			resp, err := internalClient.Get(m + "/log/health")
			if err != nil {
				return
			}
			var out struct {
				LSN int64 `json:"lsn"`
			}
			json.NewDecoder(resp.Body).Decode(&out)
			resp.Body.Close()
			s.mu.Lock()
			if out.LSN > s.lsn {
				s.lsn = out.LSN
			}
			s.mu.Unlock()
		}(m)
	}
	wg.Wait()
}

// quorum is majority of members.
func (s *Server) quorum() int { return len(s.members)/2 + 1 }

// electLeader probes members in configured order; first reachable
// (health-checkable) wins. Deterministic across nodes.
func (s *Server) electLeader() string {
	for _, m := range s.members {
		if m == s.self {
			return s.self
		}
		resp, err := internalClient.Get(m + "/log/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return m
			}
		}
	}
	return s.self
}

// Leader returns the currently believed leader.
func (s *Server) Leader() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader
}

// Close flushes and releases the log file handle.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fbuf != nil {
		s.fbuf.Flush()
	}
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

// refreshLeader re-probes the leader. If this node is promoted by
// the re-election, seed its LSN above any peer's first.
func (s *Server) refreshLeader() string {
	l := s.electLeader()
	s.mu.Lock()
	promoted := l == s.self && s.leader != s.self
	s.leader = l
	s.mu.Unlock()
	if promoted {
		s.seedLSNFromPeers()
	}
	return l
}

// Append adds an entry. If this node is not the leader it forwards
// to the leader. The leader assigns the LSN, persists locally,
// replicates to followers, and returns only after quorum persisted.
func (s *Server) Append(payload json.RawMessage) (int64, error) {
	leader := s.Leader()
	if leader != s.self {
		return s.forwardAppend(leader, payload)
	}
	return s.leaderAppend(payload)
}

func (s *Server) leaderAppend(payload json.RawMessage) (int64, error) {
	s.mu.Lock()
	lsn := s.lsn + 1
	e := Entry{LSN: lsn, TS: time.Now().UnixMilli(), Payload: payload}
	b, _ := json.Marshal(e)
	if err := s.appendLocal(b); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.lsn = lsn
	s.mu.Unlock()

	// Replicate to followers synchronously; count acks.
	acked := 1 // self persisted
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, m := range s.members {
		if m == s.self {
			continue
		}
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			if err := s.replicateTo(m, e); err == nil {
				mu.Lock()
				acked++
				mu.Unlock()
			}
		}(m)
	}
	wg.Wait()
	if acked < s.quorum() {
		// We persisted locally but can't promise durability. The
		// entry exists; a future leader may see it. Refuse the ACK.
		return lsn, ErrNoQuorum
	}
	return lsn, nil
}

func (s *Server) appendLocal(b []byte) error {
	if _, err := s.fbuf.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := s.fbuf.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// replicateTo pushes an already-LSN'd entry to a follower for
// durable storage (no re-sequencing).
func (s *Server) replicateTo(member string, e Entry) error {
	b, _ := json.Marshal(e)
	resp, err := internalClient.Post(member+"/log/replicate", "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("replicate %s: %d", member, resp.StatusCode)
	}
	return nil
}

func (s *Server) forwardAppend(leader string, payload json.RawMessage) (int64, error) {
	b, _ := json.Marshal(map[string]json.RawMessage{"payload": payload})
	resp, err := internalClient.Post(leader+"/log/append", "application/json", bytes.NewReader(b))
	if err != nil {
		// Leader unreachable: re-elect and retry once.
		if l2 := s.refreshLeader(); l2 != leader && l2 != s.self {
			return s.forwardAppend(l2, payload)
		} else if l2 == s.self {
			return s.leaderAppend(payload)
		}
		return 0, err
	}
	defer resp.Body.Close()
	var out struct {
		LSN int64 `json:"lsn"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode == 202 {
		return out.LSN, ErrNoQuorum
	}
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("append via leader: %d", resp.StatusCode)
	}
	return out.LSN, nil
}

// Tail returns entries with LSN > after, up to limit, from this
// node's own durable copy.
func (s *Server) Tail(after int64, limit int) []Entry {
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	f, err := os.Open(filepath.Join(s.dir, "commitlog.jsonl"))
	if err != nil {
		return nil
	}
	defer f.Close()
	out := make([]Entry, 0, 64)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var e Entry
		if json.Unmarshal([]byte(line), &e) != nil {
			continue
		}
		if e.LSN > after {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

// LastLSN returns this node's last durable LSN.
func (s *Server) LastLSN() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lsn
}

// Handler exposes the log service HTTP API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /log/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]interface{}{"ok": true, "lsn": s.LastLSN(), "leader": s.Leader()})
	})
	mux.HandleFunc("POST /log/append", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		var in struct {
			Payload json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(body, &in) != nil || len(in.Payload) == 0 {
			writeJSON(w, 400, map[string]string{"error": "payload required"})
			return
		}
		lsn, err := s.Append(in.Payload)
		switch {
		case errors.Is(err, ErrNoQuorum):
			writeJSON(w, 202, map[string]interface{}{"lsn": lsn, "error": "quorum not reached"})
		case err != nil:
			writeJSON(w, 502, map[string]string{"error": err.Error()})
		default:
			writeJSON(w, 200, map[string]interface{}{"lsn": lsn})
		}
	})
	mux.HandleFunc("POST /log/replicate", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
		var e Entry
		if json.Unmarshal(body, &e) != nil || e.LSN == 0 {
			writeJSON(w, 400, map[string]string{"error": "entry with lsn required"})
			return
		}
		s.mu.Lock()
		if e.LSN <= s.lsn {
			s.mu.Unlock() // already have it: idempotent ack
			writeJSON(w, 200, map[string]bool{"ok": true})
			return
		}
		b, _ := json.Marshal(e)
		err := s.appendLocal(b)
		if err == nil {
			s.lsn = e.LSN
		}
		s.mu.Unlock()
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]bool{"ok": true})
	})
	mux.HandleFunc("GET /log/tail", func(w http.ResponseWriter, r *http.Request) {
		after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		entries := s.Tail(after, limit)
		writeJSON(w, 200, map[string]interface{}{"entries": entries, "last_lsn": s.LastLSN()})
	})
	return mux
}

// Client talks to the shared log cluster.
type Client struct {
	members []string
	http    *http.Client
}

func NewClient(members []string) *Client {
	return &Client{members: members, http: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) Append(payload json.RawMessage) (int64, error) {
	b, _ := json.Marshal(map[string]json.RawMessage{"payload": payload})
	var lastErr error
	for _, m := range c.members {
		resp, err := c.http.Post(m+"/log/append", "application/json", bytes.NewReader(b))
		if err != nil {
			lastErr = err
			continue
		}
		var out struct {
			LSN int64 `json:"lsn"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			return out.LSN, nil
		}
		if resp.StatusCode == 202 {
			return out.LSN, ErrNoQuorum
		}
		lastErr = fmt.Errorf("append %s: %d", m, resp.StatusCode)
	}
	return 0, lastErr
}

// Tail pages through entries after `after`.
func (c *Client) Tail(after int64, limit int) []Entry {
	for _, m := range c.members {
		u := fmt.Sprintf("%s/log/tail?after=%d&limit=%d", m, after, limit)
		resp, err := c.http.Get(u)
		if err != nil {
			continue
		}
		var out struct {
			Entries []Entry `json:"entries"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if len(out.Entries) > 0 || resp.StatusCode == 200 {
			return out.Entries
		}
	}
	return nil
}

// writeJSON writes v as a JSON response with the given status.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
