package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"microdb/internal/storage"
)

// TestRecordAPIFencing: a write carrying an epoch below the node's
// current ring epoch is rejected 409; at-or-above is accepted.
func TestRecordAPIFencing(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	// Node epoch is 1 after bootstrap. A write at epoch 0 must be fenced.
	rec := &storage.Record{Collection: "rec", ID: "x", Ver: 1, TS: 1000, Epoch: 0,
		Fields: map[string]interface{}{"v": 1}}
	b, _ := json.Marshal(rec)
	req, _ := http.NewRequest("PUT", a.addr+"/internal/record/rec/x", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 409 {
		t.Fatalf("stale-epoch write must be 409: %d", resp.StatusCode)
	}
	// Epoch 1 (current) accepted.
	rec.Epoch = 1
	b, _ = json.Marshal(rec)
	req, _ = http.NewRequest("PUT", a.addr+"/internal/record/rec/x", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("current-epoch write must be 200: %d", resp.StatusCode)
	}
	// Readable through the record API.
	r := storage.NewRemote(a.addr)
	got, ok := r.Get("rec", "x")
	if !ok || got.Ver != 1 || got.Fields["v"].(float64) != 1 {
		t.Fatalf("record get wrong: %+v ok=%v", got, ok)
	}
}

// TestRecordRemoteRoundTrip: the Remote client speaks the full
// RecordStore surface against a live node.
func TestRecordRemoteRoundTrip(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	r := storage.NewRemote(a.addr)

	// Epoch discovery.
	ep := r.Epoch()
	if ep < 1 {
		t.Fatalf("epoch should be >=1, got %d", ep)
	}
	// Puts.
	for i := 1; i <= 5; i++ {
		rec := &storage.Record{
			Collection: "rt", ID: "d" + string(rune('0'+i)),
			Ver: 1, TS: int64(1000 + i), Epoch: ep,
			Fields: map[string]interface{}{"i": i},
		}
		if err := r.Put(rec); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Scan pages in id order.
	recs := r.Scan("rt", "", 3)
	if len(recs) != 3 || recs[0].ID != "d1" || recs[2].ID != "d3" {
		t.Fatalf("scan page 1 wrong: %v", idsOf(recs))
	}
	recs2 := r.Scan("rt", "d3", 10)
	if len(recs2) != 2 || recs2[0].ID != "d4" {
		t.Fatalf("scan resume wrong: %v", idsOf(recs2))
	}
	// Tombstone via record put.
	del := &storage.Record{Collection: "rt", ID: "d1", Ver: 2, TS: 9999,
		Deleted: true, Epoch: ep}
	if err := r.Put(del); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Get("rt", "d1"); ok {
		t.Fatal("deleted record still live")
	}
	// Scan skips tombstones.
	for _, rec := range r.Scan("rt", "", 100) {
		if rec.ID == "d1" {
			t.Fatal("scan returned tombstoned doc")
		}
	}
	// Stale epoch through Remote surfaces ErrStaleEpoch.
	stale := &storage.Record{Collection: "rt", ID: "z", Ver: 1, TS: 1, Epoch: ep - 1,
		Fields: map[string]interface{}{}}
	if err := r.Put(stale); err != storage.ErrStaleEpoch {
		t.Fatalf("stale put must surface ErrStaleEpoch, got %v", err)
	}
}

func idsOf(recs []*storage.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.ID
	}
	return out
}
