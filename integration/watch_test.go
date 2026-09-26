package integration

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestWatchFeed verifies the long-poll change feed: a watcher on a
// collection receives upsert and delete events as NDJSON.
func TestWatchFeed(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")

	// Open the watch stream (3s window; shared client timeout is 5s).
	resp, err := client.Get(a.addr + "/api/collections/wf/watch?since=0&wait=3")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "ndjson") {
		t.Fatalf("expected ndjson content type, got %s", ct)
	}

	// Write + delete while the stream is open.
	go func() {
		time.Sleep(200 * time.Millisecond)
		put(t, a.addr, "wf", "w1", map[string]interface{}{"v": 1})
		time.Sleep(100 * time.Millisecond)
		req, _ := http.NewRequest("DELETE", a.addr+"/api/collections/wf/docs/w1", nil)
		client.Do(req)
	}()

	// Read two events with a deadline.
	events := make(chan map[string]interface{}, 4)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			var e map[string]interface{}
			if json.Unmarshal(sc.Bytes(), &e) == nil {
				events <- e
			}
		}
	}()

	var kinds []string
	timeout := time.After(5 * time.Second)
	for len(kinds) < 2 {
		select {
		case e := <-events:
			kinds = append(kinds, e["kind"].(string))
			if e["collection"] != "wf" || e["id"] != "w1" {
				t.Fatalf("wrong event target: %v", e)
			}
		case <-timeout:
			t.Fatalf("timed out waiting for watch events, got kinds=%v", kinds)
		}
	}
	if kinds[0] != "upsert" || kinds[1] != "delete" {
		t.Fatalf("expected [upsert delete], got %v", kinds)
	}
}
