package store

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"fmt"
	"io"
)

// Log-record compression: plain JSON is deflated before encryption
// (compress-then-encrypt — compressing ciphertext would accomplish
// nothing), and the result rides inside the existing CRC envelope:
//
//	CRC1:<crc32c>:Z1:<base64(deflate(plain))>
//
// Compression is opt-in and best-effort per record: if a record is
// small or incompressible it is stored raw, because a "compressed"
// record that is longer than the original only costs CPU on every
// replay. Reading never depends on the flag — inflateIfCompressed
// always understands Z1:, so turning compression off never strands a
// log that was written with it on.
const (
	compressPrefix = "Z1:"
	// minCompress is the size below which flate's framing overhead
	// almost always loses. Records smaller than this are never tried.
	minCompress = 512
)

// compressRecord deflates at BestSpeed and returns the framed result,
// or nil when compression did not actually save bytes.
func compressRecord(plain []byte) []byte {
	if len(plain) < minCompress {
		return nil
	}
	var buf bytes.Buffer
	// Bound the output: a pathological input must not be able to make
	// us allocate more than we started with.
	buf.Grow(len(plain) / 2)
	w, err := flate.NewWriter(&buf, flate.BestSpeed)
	if err != nil {
		return nil
	}
	if _, err := w.Write(plain); err != nil {
		w.Close()
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}
	if buf.Len() >= len(plain) {
		return nil // not worth it
	}
	// The log is line-framed: raw deflate bytes contain 0x0A as often
	// as anything else and would split a record in half. Base64 keeps
	// the envelope single-line (cost: +33% of an already ~90% smaller
	// record).
	out := make([]byte, 0, len(compressPrefix)+base64.StdEncoding.EncodedLen(buf.Len()))
	out = append(out, compressPrefix...)
	out = base64.StdEncoding.AppendEncode(out, buf.Bytes())
	return out
}

// inflateIfCompressed expands a Z1: payload; anything else passes
// through untouched (legacy records, and records written while
// compression was off).
func inflateIfCompressed(b []byte) ([]byte, error) {
	if !bytes.HasPrefix(b, []byte(compressPrefix)) {
		return b, nil
	}
	raw, err := base64.StdEncoding.DecodeString(string(b[len(compressPrefix):]))
	if err != nil {
		return nil, fmt.Errorf("log record decompress: bad base64: %w", err)
	}
	r := flate.NewReader(bytes.NewReader(raw))
	defer r.Close()
	// A record is small by construction; cap the expansion so a
	// corrupt length cannot turn into an unbounded allocation.
	out, err := io.ReadAll(io.LimitReader(r, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("log record decompress: %w", err)
	}
	return out, nil
}
