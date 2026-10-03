package secret

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestResolutionOrder is the contract that keeps secrets off argv:
// file wins over environment, environment wins over the flag.
func TestResolutionOrder(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	writeFile(t, file, "from-file"+"\n")

	t.Setenv("TEST_SECRET", "from-env")

	cases := []struct {
		name string
		src  Source
		want string
	}{
		{"file beats env and flag", Source{Flag: "from-flag", File: file, Env: "TEST_SECRET"}, "from-file"},
		{"env beats flag", Source{Flag: "from-flag", Env: "TEST_SECRET"}, "from-env"},
		{"flag is last resort", Source{Flag: "from-flag"}, "from-flag"},
		{"nothing configured", Source{}, ""},
	}
	for _, c := range cases {
		got, err := c.src.Get()
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestMissingOrEmptyFileFailsLoudly(t *testing.T) {
	dir := t.TempDir()
	if _, err := (Source{File: filepath.Join(dir, "nope")}).Get(); err == nil {
		t.Error("a missing secret file must be an error, not an empty secret")
	}
	empty := filepath.Join(dir, "empty")
	writeFile(t, empty, "   \n")
	if _, err := (Source{File: empty}).Get(); err == nil {
		t.Error("a blank secret file must be an error")
	}
}

// TestRefreshReportsOnlyChanges: the reload loop must fire when a
// rotation happens and stay quiet otherwise.
func TestRefreshReportsOnlyChanges(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "token")
	writeFile(t, f, "v1")
	srcs := map[string]Source{"auth-token": {File: f}}
	last := Snapshot{}

	first, err := RefreshOnce(srcs, last)
	if err != nil {
		t.Fatal(err)
	}
	if first["auth-token"] != "v1" {
		t.Fatalf("first refresh = %v, want v1", first)
	}
	secretApply(first, last)

	// Nothing changed: no event.
	second, err := RefreshOnce(srcs, last)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Errorf("unchanged secret reported a change: %v", second)
	}

	// Rotation: reported once, then quiet again.
	writeFile(t, f, "v2")
	third, err := RefreshOnce(srcs, last)
	if err != nil {
		t.Fatal(err)
	}
	if third["auth-token"] != "v2" {
		t.Fatalf("rotation not reported: %v", third)
	}
	secretApply(third, last)
	fourth, err := RefreshOnce(srcs, last)
	if err != nil || len(fourth) != 0 {
		t.Errorf("steady state reported %v (err %v)", fourth, err)
	}
}

func secretApply(changed, last Snapshot) Snapshot { return Apply(changed, last) }

func TestRefreshKeepsLastGoodValueOnReadError(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "token")
	writeFile(t, f, "v1")
	srcs := map[string]Source{"auth-token": {File: f}}
	last := Snapshot{}
	changed, _ := RefreshOnce(srcs, last)
	Apply(changed, last)

	// The file disappears (deleted mid-rotation): the refresh must
	// report an error and NOT clear the value we already hold.
	os.Remove(f)
	changed, err := RefreshOnce(srcs, last)
	if err == nil {
		t.Fatal("a vanished secret file must be reported")
	}
	if len(changed) != 0 {
		t.Errorf("error produced a value change: %v", changed)
	}
	if last["auth-token"] != "v1" {
		t.Errorf("previous value was lost: %v", last)
	}
	if !strings.Contains(err.Error(), "auth-token") {
		t.Errorf("error should name the secret: %v", err)
	}
}
