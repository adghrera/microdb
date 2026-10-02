// Package protocol defines the versioned wire contract between microdb
// nodes, and the negotiation rules that make a rolling upgrade safe.
//
// The problem this solves: a cluster is upgraded one node at a time, so
// for a while half the mesh is running N and half is running N-1. Any
// message one side would misinterpret must be rejected loudly, before
// it touches state — not half-parsed into a corrupted merge.
//
// Rules:
//
//   - Every internal request and response carries X-Microdb-Protocol.
//   - A request whose version is outside [MinSupported, Version] is
//     refused with 505 and a structured body naming the window, so the
//     operator sees "incompatible", not "data loss".
//   - A request with NO header is treated as LegacyVersion — that is a
//     pre-versioning binary (N-1), which is compatible as long as
//     MinSupported allows it. Absence never means "whatever we are".
//   - Version is bumped only when a change could be misread by an
//     N-1 peer; compatible additions (new optional fields, new routes)
//     do not bump it.
//
// That gives every pair of nodes the N / N-1 window: either side may
// be newer, and both sides validate independently.
package protocol

import (
	"fmt"
	"strconv"
)

const (
	// Version is the wire version this binary speaks.
	Version = 1

	// MinSupported is the oldest peer version this binary can still
	// talk to. Raising it is what ends support for N-1 and must be a
	// deliberate, documented act.
	MinSupported = 1

	// LegacyVersion is what a peer that sends no header speaks. It is
	// fixed forever: an absent header always means the pre-versioning
	// binary, never "current".
	LegacyVersion = 1

	// Header carries the version on every internal request and reply.
	Header = "X-Microdb-Protocol"
)

// IncompatibleError describes a peer speaking a version outside our
// window. Callers should fail the operation cleanly and leave no
// partial state behind.
type IncompatibleError struct {
	PeerVersion int
	Min         int
	Max         int
	Direction   string // "request from" | "response from"
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("%s peer speaks wire version %d; this node supports [%d, %d]",
		e.Direction, e.PeerVersion, e.Min, e.Max)
}

// Parse reads a peer version from a header value. An empty value is
// LegacyVersion (a pre-versioning binary); anything unparseable is an
// error so a corrupted header cannot be silently accepted.
func Parse(v string) (int, error) {
	if v == "" {
		return LegacyVersion, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("malformed %s header %q: %w", Header, v, err)
	}
	return n, nil
}

// Supported reports whether we can talk to a peer at version v.
func Supported(v int) bool {
	return v >= MinSupported && v <= Version
}

// CheckRequest validates the version an inbound internal request
// carries. Returns nil when compatible; otherwise an IncompatibleError
// carrying the window for the response body.
func CheckRequest(v int) error {
	if Supported(v) {
		return nil
	}
	return &IncompatibleError{PeerVersion: v, Min: MinSupported, Max: Version, Direction: "request from"}
}

// CheckResponse validates the version advertised by a peer's reply.
// A peer NEWER than us is fine here — it already accepted our request
// and is speaking within its own window; only a peer older than
// MinSupported is a problem (it ignored our header and may have
// misread the request).
func CheckResponse(v int) error {
	if v >= MinSupported {
		return nil
	}
	return &IncompatibleError{PeerVersion: v, Min: MinSupported, Max: Version, Direction: "response from"}
}

// String renders the window for logs and /version output.
func String() string {
	return strconv.Itoa(Version)
}

// Window renders "min..max" for diagnostics.
func Window() string {
	return fmt.Sprintf("%d..%d", MinSupported, Version)
}
