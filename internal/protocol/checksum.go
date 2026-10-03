package protocol

import (
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
)

// CRCHeader carries a CRC32C of a request or response body over
// internal traffic. The transport already guarantees ordering; this
// guarantees the bytes that arrived are the bytes that were sent — a
// truncated or corrupted payload is rejected before it can be applied
// to membership, replication or anti-entropy state.
const CRCHeader = "X-Microdb-Crc32c"

// castagnoli is the CRC32C polynomial (the one storage systems use:
// hardware-accelerated on amd64/arm64).
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns body's CRC32C as lowercase hex.
func Checksum(body []byte) string {
	return strconv.FormatUint(uint64(crc32.Checksum(body, castagnoli)), 16)
}

// Verify reports whether sum matches body's CRC32C. An empty sum means
// the peer sent no checksum (an older binary or a streaming endpoint)
// and is accepted — the check protects what is declared, it does not
// invent a contract for callers that never made one.
func Verify(body []byte, sum string) bool {
	if sum == "" {
		return true
	}
	want, err := strconv.ParseUint(sum, 16, 32)
	if err != nil {
		return false
	}
	return uint64(crc32.Checksum(body, castagnoli)) == want
}

// VerifyBody reads r (bounded to limit bytes), checks it against the
// CRCHeader value, and returns the bytes. A mismatch returns an error
// naming both values so operators can see a corrupt transfer, not a
// mysterious bad request.
func VerifyBody(r *http.Request, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, limit))
	if err != nil {
		return nil, err
	}
	declared := r.Header.Get(CRCHeader)
	if declared == "" {
		return body, nil
	}
	got := Checksum(body)
	if got != declared {
		return nil, &ChecksumError{Declared: declared, Got: got, Size: len(body)}
	}
	return body, nil
}

// ChecksumError is a body that failed its declared CRC32C.
type ChecksumError struct {
	Declared string
	Got      string
	Size     int
}

func (e *ChecksumError) Error() string {
	return "body checksum mismatch: declared " + e.Declared + ", computed " + e.Got +
		" (" + strconv.Itoa(e.Size) + " bytes)"
}
