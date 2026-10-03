//go:build soak

// Soak / chaos harness: the honest answer to "does it survive
// failures". Normal CI runs are seconds long and orderly; production
// is hours long and not. This test runs a cluster under a seeded
// random workload while killing and restarting nodes, then asserts the
// invariants that actually matter:
//
//  1. every write acknowledged with a QUORUM ack is still readable on
//     at least one surviving node — one node may die at any moment, so
//     the majority ack is what makes that provable;
//  2. membership converges among the survivors;
//  3. nothing panics, and unexpected 5xx responses are counted rather
//     than hidden.
//
// Run it with:
//
//	MICRODB_SOAK=5m ./scripts/verify.sh --soak
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// putQuorum writes with a majority ack, so an acknowledged write is on
// more than one node before the client is told it succeeded. Returns
// 0 on a transport error (the node we aimed at may have just died),
// which the harness treats as "not acknowledged" rather than a failure.
func putQuorum(addr, col, id string, fields map[string]interface{}) int {
	b, _ := json.Marshal(fields)
	req, err := http.NewRequest(http.MethodPut,
		addr+"/api/collections/"+col+"/docs/"+id+"?consistency=quorum", bytes.NewReader(b))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// getDoc returns 0 on transport error, else the HTTP status.
func getDoc(addr, col, id string) int {
	resp, err := client.Get(addr + "/api/collections/" + col + "/docs/" + id)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestSoakKillRestart(t *testing.T) {
	dur := 60 * time.Second
	if v := os.Getenv("MICRODB_SOAK"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			dur = d
		}
	}
	const (
		rf     = 3
		total  = 4
		col    = "soak"
		budget = 1 // at most this many nodes may be down at once
	)

	type slot struct {
		n        *node
		dir      string
		alive    bool
		restarts int
	}
	base := t.TempDir()
	slots := make([]*slot, total)
	for i := 0; i < total; i++ {
		dir := fmt.Sprintf("%s/n%d", base, i)
		slots[i] = &slot{n: startNodeRF(t, dir, rf), dir: dir, alive: true}
	}
	for i := 1; i < total; i++ {
		if err := slots[i].n.cl.Join(slots[0].n.addr); err != nil {
			t.Fatalf("join slot %d: %v", i, err)
		}
	}
	eventually(t, 15*time.Second, func() bool {
		for _, s := range slots {
			if s.alive && len(s.n.cl.Peers()) != total-1 {
				return false
			}
		}
		return true
	}, "initial cluster formed")

	rnd := rand.New(rand.NewSource(20260101)) // seeded: a soak failure is reproducible
	acked := map[string]bool{}
	var writes, acked_n, kills, restarts, misses, serverErrs int
	counter := 0

	alive := func() []*slot {
		var out []*slot
		for _, s := range slots {
			if s.alive {
				out = append(out, s)
			}
		}
		return out
	}

	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		up := alive()
		if len(up) == 0 {
			t.Fatal("every node is down — budget must keep one alive")
		}

		// --- workload -------------------------------------------------
		writes++
		id := fmt.Sprintf("d%06d", counter)
		counter++
		target := up[rnd.Intn(len(up))]
		if code := putQuorum(target.n.addr, col, id, map[string]interface{}{"n": counter}); code == 200 {
			acked[id] = true
			acked_n++
		} else if code >= 500 {
			serverErrs++ // counted, not hidden: a 5xx is worth seeing
		}

		// Read back something we acknowledged (misses are legal:
		// repairs are asynchronous; they are logged, not fatal).
		if len(acked) > 0 && rnd.Intn(4) == 0 {
			for id := range acked { // map order is random: fine for a probe
				if code := getDoc(up[rnd.Intn(len(up))].n.addr, col, id); code == 404 {
					misses++
				}
				break
			}
		}

		// --- chaos ----------------------------------------------------
		switch {
		case rnd.Intn(7) == 0 && len(up) > total-budget:
			// Kill one node outright: close the listener, stop gossip,
			// close the store (a crash would not flush either).
			victim := up[rnd.Intn(len(up))]
			victim.n.srv.Close()
			victim.n.cl.Stop()
			victim.n.st.Close()
			victim.alive = false
			kills++
		case rnd.Intn(7) == 0:
			// Restart a killed node on the SAME data directory AND the
			// same address it had before: a real restart comes back on
			// its own endpoint (a pod keeps its port), and that is what
			// lets gossip heal — every survivor still holds that address
			// in its member list, so the node is rediscovered the moment
			// it answers again. Restarting on a fresh random port instead
			// strands the node on an island: no survivor knows its new
			// address, and it knows none of theirs either.
			for _, s := range slots {
				if s.alive {
					continue
				}
				hostPort := strings.TrimPrefix(s.n.addr, "http://")
				s.n = startNodeAt(t, hostPort, s.dir, rf, false)
				s.alive = true
				s.restarts++
				if err := s.n.cl.Join(alive()[0].n.addr); err != nil {
					t.Errorf("rejoin after restart: %v", err)
				}
				restarts++
				break
			}
		}
		time.Sleep(5 * time.Millisecond) // keep the loop from spinning into a syscall storm
	}

	// --- invariants ---------------------------------------------------
	up := alive()
	if len(up) == 0 {
		t.Fatal("cluster ended with no nodes alive")
	}
	// Membership converges among the survivors.
	want := len(up) - 1
	converged := false
	deadline2 := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline2) {
		ok := true
		for _, s := range up {
			if len(s.n.cl.Peers()) != want {
				ok = false
				break
			}
		}
		if ok {
			converged = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !converged {
		aliveSet := map[string]bool{}
		for _, s := range up {
			aliveSet[s.n.addr] = true
		}
		for i, s := range slots {
			if !s.alive {
				t.Logf("slot %d: killed", i)
				continue
			}
			peers := s.n.cl.Peers()
			reachable, phantom := 0, 0
			var missing []string
			seen := map[string]bool{s.n.addr: true}
			for _, p := range peers {
				seen[p] = true
				if probeAlive(p) {
					reachable++
				} else {
					phantom++
				}
			}
			for a := range aliveSet {
				if !seen[a] {
					missing = append(missing, a)
				}
			}
			t.Logf("slot %d self=%s: %d peers (%d reachable, %d phantom) missing=%v: %v",
				i, s.n.addr, len(peers), reachable, phantom, missing, peers)
		}
		t.Fatalf("survivors did not converge: want each alive node to see %d live peers", want)
	}

	// Every acknowledged quorum write is still readable somewhere alive.
	lost := []string{}
	for id := range acked {
		found := false
		for _, s := range up {
			if getDoc(s.n.addr, col, id) == 200 {
				found = true
				break
			}
		}
		if !found {
			lost = append(lost, id)
		}
	}
	if len(lost) > 0 {
		var reads []string
		for _, s := range up {
			reads = append(reads, fmt.Sprintf("%s=%d", s.n.addr, getDoc(s.n.addr, col, lost[0])))
		}
		t.Logf("soak diagnostic: %d/%d acked writes lost; first=%s per-node reads=%v",
			len(lost), len(acked), lost[0], reads)
		t.Fatalf("lost %d acknowledged quorum writes (e.g. %v) — a majority ack must survive one node death",
			len(lost), lost[:min(len(lost), 5)])
	}

	t.Logf("soak %s: %d writes, %d acked, %d kills, %d restarts, %d transient read misses, %d 5xx — %d acknowledged writes all readable",
		dur.Round(time.Second), writes, acked_n, kills, restarts, misses, serverErrs, len(acked))
	if kills == 0 {
		t.Log("warning: the workload never triggered a kill — raise the chaos probability or the duration")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// probeAlive reports whether a listed peer actually answers /health —
// distinguishes "our member list is stale" from "our member list has
// grown phantom addresses", which need different fixes.
func probeAlive(addr string) bool {
	c := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := c.Get(addr + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == 200
}
