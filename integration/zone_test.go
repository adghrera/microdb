package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"
)

func startZonedNode(t *testing.T, dir string, rf int, zone string) *node {
	t.Helper()
	n := startNodeRF(t, dir, rf)
	n.cl.SetZone(zone)
	return n
}

func zonesOf(t *testing.T, addr string) map[string]string {
	t.Helper()
	resp, err := client.Get(addr + "/api/cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Zones map[string]string `json:"zones"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("cluster view: %v (%s)", err, raw)
	}
	return out.Zones
}

func ownersOf(t *testing.T, addr, col, id string) []string {
	t.Helper()
	resp, err := client.Get(addr + "/internal/owners/" + col + "/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Owners []string `json:"owners"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("owners: %v (%s)", err, raw)
	}
	return out.Owners
}

// TestZoneAwarePlacement: with RF=2 over two availability zones, the
// two replicas of a key must sit in different zones — otherwise one
// rack outage takes both copies.
func TestZoneAwarePlacement(t *testing.T) {
	const rf = 2
	n0 := startZonedNode(t, t.TempDir()+"/n0", rf, "az-a")
	n1 := startZonedNode(t, t.TempDir()+"/n1", rf, "az-a")
	n2 := startZonedNode(t, t.TempDir()+"/n2", rf, "az-b")
	n3 := startZonedNode(t, t.TempDir()+"/n3", rf, "az-b")

	if err := n1.cl.Join(n0.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := n2.cl.Join(n1.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := n3.cl.Join(n2.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	eventually(t, 15*time.Second, func() bool {
		return len(n0.cl.Peers()) == 3
	}, "four-node cluster formed")

	// Zones travel by gossip, and the ring is rebuilt from them; wait
	// until this node has seen all four before judging placement.
	eventually(t, 15*time.Second, func() bool {
		return len(zonesOf(t, n0.addr)) >= 4
	}, "zones propagated")

	bad := 0
	checked := 0
	eventually(t, 15*time.Second, func() bool {
		zones := zonesOf(t, n0.addr)
		bad, checked = 0, 0
		for i := 0; i < 40; i++ {
			owners := ownersOf(t, n0.addr, "z", fmt.Sprintf("k%02d", i))
			if len(owners) != rf {
				continue
			}
			checked++
			if zones[owners[0]] != "" && zones[owners[0]] == zones[owners[1]] {
				bad++
			}
		}
		return checked == 40 && bad == 0
	}, "every key's replicas span zones")

	if checked != 40 {
		t.Fatalf("only %d keys returned %d owners", checked, rf)
	}
	if bad != 0 {
		t.Errorf("%d of %d keys put both replicas in the same zone", bad, checked)
	}
}

// TestZoneAwarePlacementLateTopologyRebuild is the regression test for
// the ring only rebuilding on membership changes. Here the cluster
// settles with NO zones known, and only then are the zones assigned — so
// no join, eviction, or epoch bump ever fires after the topology is
// complete. Before the fix a node that learned a peer's zone purely by
// gossip stored it but never rebuilt, so its placement kept stacking
// replicas in one zone forever; the fix rebuilds when a failure-domain
// VALUE changes, not only when the member set moves.
func TestZoneAwarePlacementLateTopologyRebuild(t *testing.T) {
	const rf = 2
	// Start un-zoned: the ring forms with no failure-domain data at all.
	n0 := startNodeRF(t, t.TempDir()+"/n0", rf)
	n1 := startNodeRF(t, t.TempDir()+"/n1", rf)
	n2 := startNodeRF(t, t.TempDir()+"/n2", rf)
	n3 := startNodeRF(t, t.TempDir()+"/n3", rf)
	for _, j := range []struct {
		n    *node
		seed string
	}{{n1, n0.addr}, {n2, n1.addr}, {n3, n2.addr}} {
		if err := j.n.cl.Join(j.seed); err != nil {
			t.Fatalf("join: %v", err)
		}
	}
	eventually(t, 15*time.Second, func() bool {
		return len(n0.cl.Peers()) == 3
	}, "four-node cluster formed with no zones")

	// NOW assign the failure domains. Setting a zone rebuilds the local
	// ring; the peers must pick it up by gossip and rebuild theirs with no
	// further membership change.
	n0.cl.SetZone("az-a")
	n1.cl.SetZone("az-a")
	n2.cl.SetZone("az-b")
	n3.cl.SetZone("az-b")

	eventually(t, 15*time.Second, func() bool {
		z := zonesOf(t, n0.addr)
		return z[n0.addr] == "az-a" && z[n1.addr] == "az-a" &&
			z[n2.addr] == "az-b" && z[n3.addr] == "az-b"
	}, "n0 learned every peer's zone by gossip")

	eventually(t, 10*time.Second, func() bool {
		z := zonesOf(t, n0.addr)
		for i := 0; i < 40; i++ {
			owners := ownersOf(t, n0.addr, "z", fmt.Sprintf("k%02d", i))
			if len(owners) != rf {
				return false
			}
			if z[owners[0]] == z[owners[1]] {
				return false // both replicas in one zone: ring is stale
			}
		}
		return true
	}, "ring rebuilt to span zones after late topology")
}

// TestZoneIsGossipedAndReported: an operator must be able to see the
// topology the ring is using.
func TestZoneIsGossipedAndReported(t *testing.T) {
	a := startZonedNode(t, t.TempDir()+"/a", 3, "eu-west-1a")
	b := startNodeRF(t, t.TempDir()+"/b", 3) // no zone configured
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		z := zonesOf(t, b.addr)
		return z[a.addr] == "eu-west-1a" && z[b.addr] == ""
	}, "A's zone learned by B, B's empty zone reported as empty")
}

// TestRegionGossipedAndReported: the region is the domain above a
// zone, and the ring cannot spread replicas across regions it has
// never heard of — so it must arrive by gossip like the zone does.
func TestRegionGossipedAndReported(t *testing.T) {
	a := startNodeRF(t, t.TempDir()+"/a", 3)
	a.cl.SetRegion("eu-west-1")
	a.cl.SetZone("eu-west-1a")
	b := startNodeRF(t, t.TempDir()+"/b", 3) // no region configured
	if err := b.cl.Join(a.addr); err != nil {
		t.Fatalf("join: %v", err)
	}
	eventually(t, 10*time.Second, func() bool {
		zones := zonesOf(t, b.addr)
		regions := regionsOf(t, b.addr)
		return regions[a.addr] == "eu-west-1" && zones[a.addr] == "eu-west-1a" &&
			regions[b.addr] == "" && zones[b.addr] == ""
	}, "A's region and zone learned by B, B's empty ones reported as empty")

	// The node that owns the topology reports it consistently with what
	// it gossips (the ring is built from exactly this map).
	if got := regionsOf(t, a.addr); got[a.addr] != "eu-west-1" {
		t.Errorf("A reports region %q for itself", got[a.addr])
	}
}

func regionsOf(t *testing.T, addr string) map[string]string {
	t.Helper()
	resp, err := client.Get(addr + "/api/cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Regions map[string]string `json:"regions"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("cluster view: %v (%s)", err, raw)
	}
	return out.Regions
}
