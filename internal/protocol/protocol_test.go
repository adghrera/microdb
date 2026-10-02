package protocol

import "testing"

func TestParse(t *testing.T) {
	// Absent header = a pre-versioning binary, always LegacyVersion —
	// never "whatever version we happen to be".
	if v, err := Parse(""); err != nil || v != LegacyVersion {
		t.Errorf("Parse(\"\") = %d, %v; want %d, nil", v, err, LegacyVersion)
	}
	if v, err := Parse("1"); err != nil || v != 1 {
		t.Errorf("Parse(\"1\") = %d, %v; want 1, nil", v, err)
	}
	if _, err := Parse("banana"); err == nil {
		t.Error("malformed header must be an error, not silently accepted")
	}
	if _, err := Parse("1.5"); err == nil {
		t.Error("non-integer header must be an error")
	}
}

func TestSupportedWindow(t *testing.T) {
	for v := MinSupported; v <= Version; v++ {
		if !Supported(v) {
			t.Errorf("version %d is inside the window but not supported", v)
		}
	}
	for _, v := range []int{-1, 0, MinSupported - 1, Version + 1, 99} {
		if Supported(v) {
			t.Errorf("version %d is outside the window but reported supported", v)
		}
	}
}

func TestCheckRequestRejectsOutliers(t *testing.T) {
	if err := CheckRequest(Version); err != nil {
		t.Errorf("our own version must be accepted: %v", err)
	}
	err := CheckRequest(Version + 1)
	if err == nil {
		t.Fatal("a newer peer's request must be refused")
	}
	inc, ok := err.(*IncompatibleError)
	if !ok {
		t.Fatalf("want *IncompatibleError, got %T", err)
	}
	if inc.PeerVersion != Version+1 || inc.Min != MinSupported || inc.Max != Version {
		t.Errorf("error must carry the window for the response body: %+v", inc)
	}
	if err := CheckRequest(MinSupported - 1); err == nil {
		t.Error("a peer older than MinSupported must be refused")
	}
}

func TestCheckResponseAllowsNewerPeer(t *testing.T) {
	// A peer NEWER than us already validated our request inside its own
	// window; rejecting its reply here would break every N/N-1 pair.
	if err := CheckResponse(Version + 5); err != nil {
		t.Errorf("newer peer must be tolerated: %v", err)
	}
	if err := CheckResponse(MinSupported); err != nil {
		t.Errorf("oldest supported peer must be tolerated: %v", err)
	}
	if err := CheckResponse(MinSupported - 1); err == nil {
		t.Error("a peer older than MinSupported must be flagged")
	}
}

func TestWindowStrings(t *testing.T) {
	if Window() == "" || String() == "" {
		t.Fatal("diagnostics must not be empty")
	}
	if Window()[0] != String()[0] {
		t.Errorf("window %q should start at version %q", Window(), String())
	}
}
