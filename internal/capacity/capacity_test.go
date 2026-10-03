package capacity

import (
	"strings"
	"testing"
	"time"
)

func TestDiskReportsSaneNumbers(t *testing.T) {
	u, err := Disk(t.TempDir())
	if err != nil {
		t.Fatalf("Disk: %v", err)
	}
	if u.TotalBytes == 0 {
		t.Fatal("total bytes = 0: the platform probe returned nothing usable")
	}
	if u.FreeBytes > u.TotalBytes {
		t.Fatalf("free %d > total %d", u.FreeBytes, u.TotalBytes)
	}
	if u.UsedPct < 0 || u.UsedPct > 100 {
		t.Fatalf("used = %.2f%%", u.UsedPct)
	}
	t.Logf("disk: %d total, %d free (%.1f%% used)", u.TotalBytes, u.FreeBytes, u.UsedPct)
}

// TestShedOnLowDisk uses an impossible watermark (101% free required)
// so the assertion is deterministic on any machine.
func TestShedOnLowDisk(t *testing.T) {
	c := &Checker{Path: t.TempDir(), MinFreePct: 101, Every: time.Millisecond}
	reason := c.ShedReason()
	if reason == "" {
		t.Fatal("expected a disk shed reason")
	}
	if !strings.Contains(reason, "disk free") || !strings.Contains(reason, "watermark") {
		t.Errorf("reason should say what is wrong: %q", reason)
	}
	s := c.Snap()
	if s.Disk.TotalBytes == 0 {
		t.Error("snapshot should carry the disk numbers it shed on")
	}
	if s.ShedReason == "" {
		// Snap does not re-evaluate (it reports), so leave this as a
		// documentation point rather than a failure.
		t.Log("Snap reports state; ShedReason is the decision")
	}
}

func TestDisabledWatermarksNeverShed(t *testing.T) {
	c := &Checker{Path: t.TempDir()} // both watermarks off
	if got := c.ShedReason(); got != "" {
		t.Errorf("disabled watermarks shed with %q", got)
	}
	// A nil checker (node started without capacity wiring) is safe too.
	var nilC *Checker
	if got := nilC.ShedReason(); got != "" {
		t.Errorf("nil checker shed with %q", got)
	}
}

func TestShedOnHeapLimit(t *testing.T) {
	c := &Checker{Path: t.TempDir(), HeapLimitBytes: 1, Every: time.Millisecond}
	reason := c.ShedReason()
	if reason == "" {
		t.Fatal("expected a heap shed reason")
	}
	if !strings.Contains(reason, "heap") {
		t.Errorf("reason should name the heap: %q", reason)
	}
	if c.Snap().HeapBytes == 0 {
		t.Error("snapshot should report the heap it shed on")
	}
}

// TestRefreshIsCached: the whole point of the interval is that the
// watermark costs nothing per write.
func TestRefreshIsCached(t *testing.T) {
	c := &Checker{Path: t.TempDir(), MinFreePct: 101, Every: time.Hour}
	c.ShedReason()
	first := c.Snap().CheckedUnixMs
	c.ShedReason()
	c.ShedReason()
	second := c.Snap().CheckedUnixMs
	if first != second {
		t.Errorf("checker re-probed inside the refresh window: %d != %d", first, second)
	}
	if first == 0 {
		t.Error("first check should have recorded a timestamp")
	}
}
