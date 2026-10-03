package store

import (
	"bufio"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Log records are wrapped in a CRC32C envelope:
//
//	CRC1:<hex crc32c>:<payload>
//
// where <payload> is the record exactly as it would have been written
// before this feature (optionally an ENC1 encrypted line). The
// envelope is outside the encryption so integrity can be checked
// without the key, and records written by older binaries simply carry
// no envelope and are accepted as-is.
const crcPrefix = "CRC1:"

// castagnoli is CRC32C — hardware-accelerated, and the polynomial
// storage systems standardised on.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// checksumHex returns crc32c(body) as lowercase hex.
func checksumHex(body []byte) string {
	return strconv.FormatUint(uint64(crc32.Checksum(body, castagnoli)), 16)
}

// verifyRecord checks a log line's envelope. It returns the payload to
// decode next and whether the line is intact. Lines without an
// envelope are legacy records and pass through untouched.
func verifyRecord(line string) (payload string, ok bool) {
	if !strings.HasPrefix(line, crcPrefix) {
		return line, true
	}
	rest := line[len(crcPrefix):]
	i := strings.IndexByte(rest, ':')
	if i <= 0 {
		return line, false // malformed envelope
	}
	sumHex, body := rest[:i], rest[i+1:]
	want, err := strconv.ParseUint(sumHex, 16, 32)
	if err != nil {
		return line, false
	}
	if uint64(crc32.Checksum([]byte(body), castagnoli)) != want {
		return line, false // bytes on disk are not the bytes we wrote
	}
	return body, true
}

// CorruptRecord locates one damaged log record.
type CorruptRecord struct {
	Line   int    `json:"line"`
	Offset int64  `json:"offset"`
	Reason string `json:"reason"`
}

// LogReport is the integrity report for one commit log file.
type LogReport struct {
	Path               string          `json:"path"`
	Bytes              int64           `json:"bytes"`
	Records            int             `json:"records"`
	LegacyRecords      int             `json:"records_without_checksum"`
	Corrupt            []CorruptRecord `json:"corrupt,omitempty"`
	CorruptCount       int             `json:"corrupt_total"`
	TornTail           bool            `json:"torn_tail"`
	Encrypted          bool            `json:"encrypted"`
	ChecksummedRecords int             `json:"checksummed_records"`
}

// OK reports whether the log is fully intact (a torn final record from
// an interrupted write is tolerable; anything else is not).
func (r LogReport) OK() bool { return r.CorruptCount == 0 }

// VerifyLog scans a commit log for integrity WITHOUT loading it into
// memory — the offline half of "checksums + verify". It does not need
// the decryption key: the envelope sits outside the ciphertext, so a
// corrupt record is reported as corruption rather than as a
// decryption failure.
func VerifyLog(path string) (LogReport, error) {
	rep := LogReport{Path: path}
	f, err := os.Open(path)
	if err != nil {
		return rep, err
	}
	defer f.Close()
	if fi, err := f.Stat(); err == nil {
		rep.Bytes = fi.Size()
	}

	br := bufio.NewReaderSize(f, 1<<20)
	var (
		offset    int64
		lineNo    int
		lastBad   *CorruptRecord
		lastHadNL = true
	)
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			raw := strings.TrimRight(line, "\n")
			raw = strings.TrimRight(raw, "\r")
			lineNo++
			start := offset
			offset += int64(len(line))

			if strings.TrimSpace(raw) == "" {
				continue
			}
			rep.Records++
			if strings.Contains(raw, encPrefix) {
				rep.Encrypted = true
			}
			if strings.HasPrefix(raw, crcPrefix) {
				rep.ChecksummedRecords++
			} else {
				rep.LegacyRecords++
			}

			truncated := line[len(line)-1] != '\n'
			_, intact := verifyRecord(raw)
			var bad *CorruptRecord
			switch {
			case !intact:
				bad = &CorruptRecord{Line: lineNo, Offset: start, Reason: "crc32c mismatch"}
			case truncated:
				bad = &CorruptRecord{Line: lineNo, Offset: start, Reason: "truncated record (no newline)"}
			}
			if bad != nil {
				rep.CorruptCount++
				rep.Corrupt = append(rep.Corrupt, *bad)
				lastBad = bad
			} else {
				lastBad = nil
			}
			lastHadNL = !truncated
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return rep, err
		}
	}

	// A damaged FINAL record is the signature of a process that died
	// mid-append — the OS wrote half a line. Replay already tolerates
	// exactly that, and peers hold the rest, so it is reported as a
	// torn tail rather than as corruption. Damage earlier in the file
	// is real corruption and stays in the report.
	if lastBad != nil && !lastHadNL {
		rep.TornTail = true
		rep.Corrupt = rep.Corrupt[:len(rep.Corrupt)-1]
		rep.CorruptCount--
	}
	return rep, nil
}

// VerifyDir verifies <dir>/data.jsonl.
func VerifyDir(dir string) (LogReport, error) {
	return VerifyLog(filepath.Join(dir, "data.jsonl"))
}
