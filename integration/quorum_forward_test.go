package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"
)

// qfPutQuorum writes with ?consistency=quorum and returns the status code.
func qfPutQuorum(addr, col, id string, fields map[string]interface{}) int {
	b, _ := json.Marshal(fields)
	req, _ := http.NewRequest(http.MethodPut,
		addr+"/api/collections/"+col+"/docs/"+id+"?consistency=quorum", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// TestQuorumWriteForwardedToNonOwnerStaysDurable guards the durability
// contract across the any-node routing hop. A ?consistency=quorum write
// that lands on a NON-owner is forwarded to the owner. If the forward
// drops the query string (as it once did), the owner treats it as a
// default "one"-consistency write: the doc is applied on ONE node and
// only later drifts to the others via the ASYNC fanout, so a quorum-acked
// write was really a single copy. We assert that at ack time the doc is
// already on a majority of the RF owner set — i.e. the quorum block ran
// and its synchronous Replicate landed, which only happens if the
// forward carried the consistency contract through.
//
// Owner stores are read in-process (store.Get) immediately after the ack
// so the check reflects the SYNCHRONOUS quorum, not the async fanout
// that would mask the bug behind an HTTP round-trip.
func TestQuorumWriteForwardedToNonOwnerStaysDurable(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	c := startNode(t, t.TempDir()+"/c")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	if err := c.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 2 && len(b.cl.Peers()) == 2 && len(c.cl.Peers()) == 2
	}, "mesh")

	col, id := "qfwd", "k"
	nodes := []*node{a, b, c}
	byAddr := map[string]*node{a.addr: a, b.addr: b, c.addr: c}

	// Pick a live node that does NOT own the key, to force the forward.
	var target *node
	for _, n := range nodes {
		if !n.apiSrv.Owns(col, id) {
			target = n
			break
		}
	}
	if target == nil {
		t.Fatal("every node owns the key; cannot exercise the forward path")
	}

	if code := qfPutQuorum(target.addr, col, id, map[string]interface{}{"v": "durable"}); code != 200 {
		t.Fatalf("forwarded quorum write status %d (want 200)", code)
	}

	// Count owners holding the doc right after the ack, reading stores
	// in-process to observe the synchronous quorum (async fanout is a
	// queued background copy and would race an HTTP read).
	owners := a.apiSrv.OwnerSet(col, id)
	need := len(owners)/2 + 1
	present := 0
	var detail []string
	for _, o := range owners {
		n := byAddr[o]
		has := false
		if n != nil {
			_, has = n.st.Get(col, id)
		}
		if has {
			present++
		}
		detail = append(detail, o+"="+map[bool]string{true: "has", false: "missing"}[has])
	}
	if present < need {
		t.Fatalf("forwarded quorum write reached only %d/%d owners at ack (need %d): %v — "+
			"the forward dropped the consistency contract, so quorum degraded to a single copy",
			present, len(owners), need, detail)
	}
}
