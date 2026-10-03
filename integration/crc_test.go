package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"microdb/internal/protocol"
)

func internalPost(t *testing.T, url string, body []byte, crc string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if crc != "" {
		req.Header.Set(protocol.CRCHeader, crc)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestInternalBodyChecksum pins end-to-end integrity on internal
// traffic: a body whose declared CRC32C does not match is rejected
// before it can touch membership or replication state, while a correct
// checksum (and a peer that sends none, i.e. an older binary) passes.
func TestInternalBodyChecksum(t *testing.T) {
	n := startNodeRF(t, t.TempDir(), 1)
	body := []byte(`{"addr":"http://127.0.0.1:1","members":[],"epoch":1,"left":{}}`)
	gossip := n.addr + "/internal/gossip"

	t.Run("mismatch is rejected", func(t *testing.T) {
		code, out := internalPost(t, gossip, body, "deadbeef")
		if code != 400 {
			t.Fatalf("status = %d, want 400 (%s)", code, out)
		}
		if !strings.Contains(out, "checksum") {
			t.Errorf("rejection must name the problem, got: %s", out)
		}
	})

	t.Run("matching checksum is accepted", func(t *testing.T) {
		code, out := internalPost(t, gossip, body, protocol.Checksum(body))
		if code != 200 {
			t.Fatalf("status = %d, want 200 (%s)", code, out)
		}
	})

	t.Run("no checksum means an older peer", func(t *testing.T) {
		code, out := internalPost(t, gossip, body, "")
		if code != 200 {
			t.Fatalf("status = %d, want 200 (%s)", code, out)
		}
	})

	t.Run("tampered body fails its own checksum", func(t *testing.T) {
		// Declare the checksum of the original, then send different
		// bytes: exactly what a truncated or corrupted transfer looks
		// like.
		sum := protocol.Checksum(body)
		tampered := []byte(`{"addr":"http://evil:1","members":[],"epoch":1,"left":{}}`)
		code, out := internalPost(t, gossip, tampered, sum)
		if code != 400 {
			t.Fatalf("status = %d, want 400 (%s)", code, out)
		}
	})
}

// TestRepairPullsMissingDocumentFromPeer: a document this node does not
// hold is fetched by one explicit repair pass — the recovery step after
// verify finds a corrupt record (or a disk is replaced).
func TestRepairPullsMissingDocumentFromPeer(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) >= 1 && len(b.cl.Peers()) >= 1
	}, "both nodes see each other")

	// Give A its own document in the collection first, so both trees
	// are non-empty and the diff has something to walk.
	if code := put(t, a.addr, "docs", "existing", map[string]interface{}{"v": 0}); code != 200 {
		t.Fatalf("seed write: %d", code)
	}
	eventually(t, 10*time.Second, func() bool {
		_, ok := b.st.Get("docs", "existing")
		return ok
	}, "seed document replicated to B")

	// Write straight into B's store: no fanout, no anti-entropy tick
	// yet — A genuinely does not have it.
	if _, err := b.st.Apply("docs", "recovered", map[string]interface{}{"v": 1}); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.st.Get("docs", "recovered"); ok {
		t.Fatal("test setup: A already has the document, repair would prove nothing")
	}

	resp, err := client.Post(a.addr+"/internal/repair", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("repair -> %d: %s", resp.StatusCode, raw)
	}
	var rep struct {
		Peers       int   `json:"peers"`
		Collections int   `json:"collections_attempted"`
		Synced      int   `json:"collections_synced"`
		Failed      int   `json:"collections_failed"`
		DurationMs  int64 `json:"duration_ms"`
	}
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatalf("repair report is not JSON: %v (%s)", err, raw)
	}
	if rep.Peers < 1 {
		t.Errorf("repair saw %d peers, want >= 1", rep.Peers)
	}
	if rep.Synced == 0 || rep.Failed != 0 {
		t.Errorf("repair report: synced=%d failed=%d, want synced>0 and no failures", rep.Synced, rep.Failed)
	}

	eventually(t, 10*time.Second, func() bool {
		d, ok := a.st.Get("docs", "recovered")
		if !ok {
			return false
		}
		// The document crossed the wire, so the number arrives as a
		// JSON float64 — accept either representation.
		switch d.Fields["v"] {
		case 1, float64(1):
			return true
		}
		return false
	}, "repair pulled the missing document from the peer")
}
