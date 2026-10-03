package profile

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStartIsOffByDefaultAndServesWhenAsked(t *testing.T) {
	srv, bound, err := Start("")
	if srv != nil || bound != "" || err != nil {
		t.Fatalf("empty addr should disable pprof: srv=%v addr=%q err=%v", srv, bound, err)
	}

	srv, bound, err = Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if bound == "" {
		t.Fatal("Start must report the address it actually bound (port 0)")
	}
	resp, err := http.Get("http://" + bound + "/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("pprof index -> %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "profile") {
		t.Errorf("pprof index looks wrong: %.200s", body)
	}

	// The goroutine profile answers: proof the handlers are wired, not
	// just the index.
	resp2, err := http.Get("http://" + bound + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Errorf("goroutine profile -> %d", resp2.StatusCode)
	}
}

func TestSamplerWritesProfiles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "profiles")
	s := NewSampler(dir, time.Hour, 3)

	if err := s.Heap(); err != nil {
		t.Fatalf("heap: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "heap-*.pprof"))
	if err != nil || len(files) != 1 {
		t.Fatalf("heap profile not written: %v (err %v)", files, err)
	}
	fi, err := os.Stat(files[0])
	if err != nil || fi.Size() == 0 {
		t.Fatalf("heap profile is empty: %v", err)
	}

	if err := s.CPU(20 * time.Millisecond); err != nil {
		t.Fatalf("cpu: %v", err)
	}
	cpuFiles, _ := filepath.Glob(filepath.Join(dir, "cpu-*.pprof"))
	if len(cpuFiles) != 1 {
		t.Fatalf("cpu profile not written: %v", cpuFiles)
	}
	if fi, err := os.Stat(cpuFiles[0]); err != nil || fi.Size() == 0 {
		t.Fatalf("cpu profile is empty: %v", err)
	}
}

// TestPruneBoundsDisk: profiles are for forensics, not for filling the
// volume they are meant to explain.
func TestPruneBoundsDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "profiles")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	s := NewSampler(dir, time.Hour, 2)

	// Names embed millis; write distinct ones directly.
	for i := 1; i <= 6; i++ {
		for _, kind := range []string{"heap", "cpu"} {
			p := filepath.Join(dir, kind+"-000"+string(rune('0'+i))+".pprof")
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond) // distinct timestamps
		}
	}
	if err := s.Prune(); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"heap", "cpu"} {
		got, _ := filepath.Glob(filepath.Join(dir, kind+"-*.pprof"))
		if len(got) != 2 {
			t.Errorf("%s profiles after prune = %d, want 2 (%v)", kind, len(got), got)
		}
		// The survivors must be the NEWEST ones.
		for _, f := range got {
			if strings.Contains(f, kind+"-0001") || strings.Contains(f, kind+"-0002") {
				t.Errorf("prune kept an old profile: %s", f)
			}
		}
	}
}

func TestSamplerRunAndStop(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "profiles")
	s := NewSampler(dir, 30*time.Millisecond, 5)
	done := make(chan struct{})
	go func() {
		s.Run()
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		files, _ := filepath.Glob(filepath.Join(dir, "heap-*.pprof"))
		if len(files) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sampler never wrote a profile")
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.Stop()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return — a shutdown path would hang here")
	}
	// Stop is idempotent.
	s.Stop()
}
