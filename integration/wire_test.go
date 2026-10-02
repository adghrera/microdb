package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"microdb/internal/cluster"
	"microdb/internal/protocol"
	"microdb/internal/store"
)

// TestInternalWireVersionNegotiation pins the rolling-upgrade contract:
// internal traffic inside the supported window is served, anything
// outside it is refused loudly with the window in the body, and every
// reply advertises our version so a peer can tell what happened.
func TestInternalWireVersionNegotiation(t *testing.T) {
	n := startNodeRF(t, t.TempDir(), 1)

	cases := []struct {
		name string
		hdr  string
		want int
	}{
		{"legacy peer (no header)", "", http.StatusOK},
		{"current version", "1", http.StatusOK},
		{"newer than us", "99", 505},
		{"older than MinSupported", "0", 505},
		{"malformed header", "banana", 505},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, n.addr+"/internal/bootstrap", nil)
			if err != nil {
				t.Fatal(err)
			}
			if c.hdr != "" {
				req.Header.Set(protocol.Header, c.hdr)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != c.want {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, c.want, body)
			}
			// Every reply advertises what we speak.
			if got := resp.Header.Get(protocol.Header); got != protocol.String() {
				t.Errorf("response %s = %q, want %q", protocol.Header, got, protocol.String())
			}
			if c.want != 505 {
				return
			}
			var errBody struct {
				Error         string `json:"error"`
				PeerVersion   int    `json:"peer_version"`
				MinSupported  int    `json:"min_supported"`
				MaxSupported  int    `json:"max_supported"`
				ServerVersion int    `json:"server_version"`
			}
			if err := json.Unmarshal(body, &errBody); err != nil {
				t.Fatalf("505 body is not structured JSON: %v (%s)", err, body)
			}
			if errBody.Error == "" || !strings.Contains(errBody.Error, "incompatible wire protocol") {
				t.Errorf("505 must name the problem, got %q", errBody.Error)
			}
			if errBody.MinSupported != protocol.MinSupported || errBody.MaxSupported != protocol.Version {
				t.Errorf("505 must state the window, got [%d, %d]",
					errBody.MinSupported, errBody.MaxSupported)
			}
			if errBody.ServerVersion != protocol.Version {
				t.Errorf("505 must state the server version, got %d", errBody.ServerVersion)
			}
		})
	}
}

// TestPublicAPIIgnoresProtocolHeader makes sure the negotiation is
// scoped to internal traffic: /api/* stays reachable for clients and
// older HTTP callers that know nothing about the wire protocol.
func TestPublicAPIIgnoresProtocolHeader(t *testing.T) {
	n := startNodeRF(t, t.TempDir(), 1)
	req, _ := http.NewRequest(http.MethodPut,
		n.addr+"/api/collections/wire/docs/a", strings.NewReader(`{"n":1}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(protocol.Header, "99") // a future client, not a peer
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("public PUT = %d, want 200 (%s)", resp.StatusCode, body)
	}
}

// TestJoinIncompatibleSeedFailsCleanly covers the upgrade mistake that
// actually hurts in production: a node started with a wire version the
// seed cannot talk to. The join must fail with a diagnosis, record the
// peer as incompatible, and leave NO partial membership behind.
func TestJoinIncompatibleSeedFailsCleanly(t *testing.T) {
	var (
		mu      sync.Mutex
		gotHdrs []string
	)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotHdrs = append(gotHdrs, r.Header.Get(protocol.Header))
		mu.Unlock()
		// A seed from a future release: it speaks version 2 and tells
		// us so.
		w.Header().Set(protocol.Header, "2")
		w.WriteHeader(505)
		io.WriteString(w, `{"error":"incompatible wire protocol","min_supported":2,"max_supported":2}`)
	}))
	defer fake.Close()

	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cl, err := cluster.New("http://127.0.0.1:1", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Stop()

	joinErr := cl.Join(fake.URL)
	if joinErr == nil {
		t.Fatal("join against an incompatible seed must fail")
	}
	if !strings.Contains(joinErr.Error(), "incompatible wire protocol") {
		t.Errorf("join error must diagnose the mismatch, got: %v", joinErr)
	}

	mu.Lock()
	sent := append([]string(nil), gotHdrs...)
	mu.Unlock()
	if len(sent) == 0 {
		t.Fatal("join never reached the seed")
	}
	for i, h := range sent {
		if h != protocol.String() {
			t.Errorf("outbound request %d carried protocol %q, want %q", i, h, protocol.String())
		}
	}

	inc := cl.Incompatible()
	if v, ok := inc[fake.URL]; !ok {
		t.Errorf("seed should be recorded as incompatible, got %v", inc)
	} else if v != 2 {
		t.Errorf("recorded peer version = %d, want 2", v)
	}
	if peers := cl.Peers(); len(peers) != 0 {
		t.Errorf("failed join must leave no partial membership, got %v", peers)
	}
}

// TestVersionExposesProtocol gives operators the number they need when
// planning an upgrade.
func TestVersionExposesProtocol(t *testing.T) {
	n := startNodeRF(t, t.TempDir(), 1)
	resp, err := client.Get(n.addr + "/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Protocol       int    `json:"protocol"`
		ProtocolWindow string `json:"protocol_window"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Protocol != protocol.Version {
		t.Errorf("/version protocol = %d, want %d", out.Protocol, protocol.Version)
	}
	if out.ProtocolWindow != protocol.Window() {
		t.Errorf("/version protocol_window = %q, want %q", out.ProtocolWindow, protocol.Window())
	}
}
