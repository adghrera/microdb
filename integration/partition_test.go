package integration

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestPartitionedLocality: with partition_field set, all docs of one
// partition route to the SAME shard, and the partition query is
// served from one node with no scatter.
func TestPartitionedLocality(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	b := startNode(t, t.TempDir()+"/b")
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, func() bool {
		return len(a.cl.Peers()) == 1 && len(b.cl.Peers()) == 1
	}, "mesh")

	// Configure partitioning on "events" by "user".
	code, out := gsiDo(t, "PUT", a.addr+"/api/collections/events/config",
		`{"partition_field":"user","sort_field":"ts"}`)
	if code != 200 || out["partition_field"] != "user" {
		t.Fatalf("config: %d %v", code, out)
	}
	// The config must reach BOTH nodes before writes/queries route by
	// the partition key — otherwise the peer still uses the old
	// col/id routing.
	eventually(t, 10*time.Second, func() bool {
		resp, err := client.Get(b.addr + "/api/collections/events/config")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var c map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&c)
		return c["partition_field"] == "user"
	}, "config replicated to B")

	// Write docs for two users, ids "user|ts".
	for _, u := range []string{"alice", "bob"} {
		for _, ts := range []string{"001", "002", "003"} {
			id := u + "|" + ts
			code := put(t, a.addr, "events", id, map[string]interface{}{"user": u, "ts": ts})
			if code != 200 {
				t.Fatalf("put %s: %d", id, code)
			}
		}
	}

	// All alice docs share one owner (partition locality).
	ownerAlice := a.apiSrv.OwnerSet("events", "alice|001")
	ownerAlice2 := a.apiSrv.OwnerSet("events", "alice|003")
	if ownerAlice[0] != ownerAlice2[0] {
		t.Fatalf("alice docs split across shards: %v vs %v", ownerAlice[0], ownerAlice2[0])
	}
	// The ring key is the partition, not the full id: bob may live
	// elsewhere but alice's whole partition is one shard.
	if !a.apiSrv.Owns("events", "alice|002") && !b.apiSrv.Owns("events", "alice|002") {
		t.Fatal("nobody owns alice partition")
	}

	// Partition query with sort range: served from one shard.
	owner := ownerAlice[0]
	resp, err := client.Get(owner + "/api/collections/events/partition/alice?sort_gte=002&sort_lte=003")
	if err != nil {
		t.Fatal(err)
	}
	var qout struct {
		Count       int                      `json:"count"`
		ShardsQuery int                      `json:"shards_queried"`
		Docs        []map[string]interface{} `json:"docs"`
	}
	json.NewDecoder(resp.Body).Decode(&qout)
	resp.Body.Close()
	if qout.Count != 2 || qout.ShardsQuery != 1 {
		t.Fatalf("partition query: count=%d shards=%d", qout.Count, qout.ShardsQuery)
	}
	if qout.Docs[0]["id"] != "alice|002" || qout.Docs[1]["id"] != "alice|003" {
		t.Fatalf("range wrong: %v", qout.Docs)
	}

	// Descending + limit.
	resp, _ = client.Get(owner + "/api/collections/events/partition/alice?desc=true&limit=2")
	json.NewDecoder(resp.Body).Decode(&qout)
	resp.Body.Close()
	if qout.Count != 2 || qout.Docs[0]["id"] != "alice|003" {
		t.Fatalf("desc+limit wrong: %v", qout.Docs)
	}

	// Query forwarded from the NON-owner node still works.
	other := b.addr
	if owner == b.addr {
		other = a.addr
	}
	// Wait until the owner has all 3 alice docs (async replication).
	eventually(t, 10*time.Second, func() bool {
		r, err := client.Get(owner + "/api/collections/events/partition/alice")
		if err != nil {
			return false
		}
		var q struct {
			Count int `json:"count"`
		}
		json.NewDecoder(r.Body).Decode(&q)
		r.Body.Close()
		return q.Count == 3
	}, "owner has all alice docs")
	resp, err = client.Get(other + "/api/collections/events/partition/alice")
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&qout)
	resp.Body.Close()
	if qout.Count != 3 {
		t.Fatalf("forwarded partition query: %d", qout.Count)
	}
}

// TestPartitionValidation: mismatched partition field vs id prefix
// is refused; batch spanning partitions is refused.
func TestPartitionValidation(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	gsiDo(t, "PUT", a.addr+"/api/collections/v/config", `{"partition_field":"user"}`)

	// id prefix != user value -> 400
	code, _ := gsiDo(t, "PUT", a.addr+"/api/collections/v/docs/bob|1", `{"user":"alice"}`)
	if code != 400 {
		t.Fatalf("mismatched partition must be 400: %d", code)
	}
	// missing partition field -> 400
	code, _ = gsiDo(t, "PUT", a.addr+"/api/collections/v/docs/alice|1", `{"other":1}`)
	if code != 400 {
		t.Fatalf("missing partition field must be 400: %d", code)
	}
	// matching -> 200
	code, _ = gsiDo(t, "PUT", a.addr+"/api/collections/v/docs/alice|1", `{"user":"alice"}`)
	if code != 200 {
		t.Fatalf("matching write: %d", code)
	}
	// batch spanning two partitions -> 400
	code, out := gsiDo(t, "POST", a.addr+"/api/collections/v/docs/batch",
		`{"docs":{"alice|2":{"user":"alice"},"bob|1":{"user":"bob"}}}`)
	if code != 400 {
		t.Fatalf("cross-partition batch must be 400: %d %v", code, out)
	}
	// batch within one partition -> 200
	code, _ = gsiDo(t, "POST", a.addr+"/api/collections/v/docs/batch",
		`{"docs":{"alice|2":{"user":"alice"},"alice|3":{"user":"alice"}}}`)
	if code != 200 {
		t.Fatalf("same-partition batch: %d", code)
	}
}

// TestPartitionQueryUnpartitioned: 400 on collections without a
// partition field.
func TestPartitionQueryUnpartitioned(t *testing.T) {
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	resp, _ := client.Get(a.addr + "/api/collections/plain/partition/x")
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("unpartitioned collection: %d want 400", resp.StatusCode)
	}
}

var _ = http.StatusOK
