package cluster

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"
)

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("payload is not gzip: %v", err)
	}
	defer gz.Close()
	out, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMaybeCompressRoundTrip(t *testing.T) {
	// A realistic JSON body: replication batches and gossip payloads
	// are full of repeated keys, so they compress well.
	body := []byte(`{"docs":[` + strings.Repeat(`{"id":"k1","fields":{"name":"alice","tags":["admin"]}},`, 40) + `]}`)
	if len(body) <= minCompressBody {
		t.Fatalf("fixture too small to exercise the threshold: %d", len(body))
	}
	out, compressed := maybeCompress(body)
	if !compressed {
		t.Fatalf("a %d byte JSON body should compress", len(body))
	}
	if len(out) >= len(body) {
		t.Fatalf("compressed %d -> %d bytes", len(body), len(out))
	}
	if !bytes.Equal(gunzip(t, out), body) {
		t.Fatal("gzip round trip changed the bytes")
	}
	t.Logf("compressed %d -> %d bytes (%.0f%% smaller)", len(body), len(out),
		100*(1-float64(len(out))/float64(len(body))))
}

func TestMaybeCompressSkipsSmallBodies(t *testing.T) {
	body := []byte(`{"addr":"http://127.0.0.1:1","members":[],"epoch":1}`)
	if len(body) >= minCompressBody {
		t.Fatalf("fixture grew past the threshold: %d", len(body))
	}
	out, compressed := maybeCompress(body)
	if compressed {
		t.Error("a small body must not pay gzip framing")
	}
	if !bytes.Equal(out, body) {
		t.Error("uncompressed body must be passed through untouched")
	}
}

func TestMaybeCompressSkipsIncompressibleBodies(t *testing.T) {
	// Random bytes do not compress; sending them gzipped would make the
	// message bigger for no reason.
	body := make([]byte, 4096)
	for i := range body {
		body[i] = byte(i*31 + i*i*7)
	}
	// Force genuine entropy (a repeating pattern would compress).
	for i := 1; i < len(body); i++ {
		body[i] ^= body[i-1] << 1
	}
	out, compressed := maybeCompress(body)
	if compressed && len(out) >= len(body) {
		t.Fatalf("compressed to %d from %d", len(out), len(body))
	}
	if !compressed {
		if !bytes.Equal(out, body) {
			t.Error("skipped compression must pass the original through")
		}
		t.Log("incompressible body passed through raw")
		return
	}
	if !bytes.Equal(gunzip(t, out), body) {
		t.Fatal("round trip changed an incompressible body")
	}
}
