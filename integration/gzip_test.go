package integration

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"microdb/internal/protocol"
)

func gzipBytes(t *testing.T, plain []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestGzippedInternalBody: the sender checksums the ORIGINAL bytes and
// then gzips, so the receiver must decompress BEFORE checking the
// checksum — otherwise a valid body fails the moment compression turns
// on. Order matters, so both orders get tested.
func TestGzippedInternalBody(t *testing.T) {
	n := startNodeRF(t, t.TempDir(), 1)
	plain := []byte(`{"addr":"http://127.0.0.1:9","members":[],"epoch":1,"left":{}}`)

	send := func(body []byte, crc string) (int, string) {
		req, err := http.NewRequest(http.MethodPost, n.addr+"/internal/gossip", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
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

	gz := gzipBytes(t, plain)

	// Correct: CRC over the plain body, payload gzipped.
	if code, out := send(gz, protocol.Checksum(plain)); code != 200 {
		t.Fatalf("gzipped body with correct CRC -> %d: %s", code, out)
	}
	// Tampered: the CRC belongs to a DIFFERENT body — must be rejected
	// after decompression (if the check ran on the compressed bytes it
	// would pass, and we would have accepted the wrong membership).
	if code, out := send(gz, protocol.Checksum([]byte(`{"addr":"http://evil:9"}`))); code != 400 {
		t.Fatalf("gzipped body with wrong CRC -> %d, want 400: %s", code, out)
	} else if !strings.Contains(out, "checksum") {
		t.Errorf("rejection should name the checksum, got: %s", out)
	}
	// No CRC at all (an older peer that compresses): accepted.
	if code, out := send(gz, ""); code != 200 {
		t.Fatalf("gzipped body without CRC -> %d: %s", code, out)
	}
	// Corrupt gzip stream: rejected, not a panic or a 500.
	if code, out := send(gz[:len(gz)/2], protocol.Checksum(plain)); code != 400 {
		t.Fatalf("truncated gzip -> %d, want 400: %s", code, out)
	}
}

// TestInternalResponsesCompress: the other half of the wire. Setting
// Accept-Encoding manually makes Go's transport leave the body alone,
// so the header (and the actual gzip bytes) are observable.
func TestInternalResponsesCompress(t *testing.T) {
	n := startNodeRF(t, t.TempDir(), 1)
	if code := put(t, n.addr, "zip", "d", map[string]interface{}{"v": 1}); code != 200 {
		t.Fatalf("seed: %d", code)
	}
	req, err := http.NewRequest(http.MethodGet, n.addr+"/internal/collections", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip (Accept-Encoding was set explicitly)", got)
	}
	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("body is not gzip: %v", err)
	}
	plain, err := io.ReadAll(gz)
	gz.Close()
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Collections []string `json:"collections"`
	}
	if err := json.Unmarshal(plain, &out); err != nil {
		t.Fatalf("decompressed body is not JSON: %v (%s)", err, plain)
	}
}

// TestLargeWriteReplicatesOverTheWire exercises the compressed
// REQUEST path end to end: a multi-kilobyte document crosses between
// two real nodes through cluster.post (which gzips) and a real server
// (which decompresses and checksums it).
func TestLargeWriteReplicatesOverTheWire(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) >= 1 && len(b.cl.Peers()) >= 1
	}, "cluster formed")

	pad := strings.Repeat("payload-", 1024) // 8KB document
	if code := put(t, a.addr, "big", "doc1", map[string]interface{}{"pad": pad}); code != 200 {
		t.Fatalf("write: %d", code)
	}
	eventually(t, 10*time.Second, func() bool {
		d, ok := b.st.Get("big", "doc1")
		return ok && d.Fields["pad"] == pad
	}, "large document replicated to the peer")
}
