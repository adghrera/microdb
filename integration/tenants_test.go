package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"microdb/internal/tenants"
)

func doAs(t *testing.T, method, url, token, body string) *http.Response {
	t.Helper()
	var r *bytes.Reader = bytes.NewReader(nil)
	if body != "" {
		r = bytes.NewReader([]byte(body))
	}
	req, _ := http.NewRequest(method, url, r)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func startTenantedWith(t *testing.T, cfg string) *node {
	t.Helper()
	a := startNode(t, t.TempDir()+"/a")
	a.cl.MarkBootstrapped()
	f := filepath.Join(t.TempDir(), "tenants.json")
	if err := os.WriteFile(f, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := tenants.Load(f)
	if err != nil {
		t.Fatal(err)
	}
	a.apiSrv.SetTenants(reg)
	return a
}

// quota config: generous rate, tight doc quota.
func startQuotaNode(t *testing.T) *node {
	return startTenantedWith(t, `{"tenants":[
		{"name":"acme","token":"tok-acme","rate_rps":1000,"burst":1000,"max_docs":3}]}`)
}

// rate config: tight bucket.
func startRateNode(t *testing.T) *node {
	return startTenantedWith(t, `{"tenants":[
		{"name":"acme","token":"tok-acme","rate_rps":5,"burst":5},
		{"name":"globex","token":"tok-globex","rate_rps":1000,"burst":1000}]}`)
}

// TestTenantNamespaceIsolation: acme can only touch acme.* collections.
func TestTenantNamespaceIsolation(t *testing.T) {
	a := startTenantedWith(t, `{"tenants":[
		{"name":"acme","token":"tok-acme","rate_rps":1000,"burst":1000},
		{"name":"globex","token":"tok-globex","rate_rps":1000,"burst":1000}]}`)
	// Own namespace: OK.
	resp := doAs(t, "PUT", a.addr+"/api/collections/acme.users/docs/1", "tok-acme", `{"v":1}`)
	if resp.StatusCode != 200 {
		t.Fatalf("own-namespace write: %d", resp.StatusCode)
	}
	resp.Body.Close()
	// Someone else's namespace: 403.
	resp = doAs(t, "GET", a.addr+"/api/collections/globex.users/docs/1", "tok-acme", "")
	if resp.StatusCode != 403 {
		t.Fatalf("cross-tenant read must be 403: %d", resp.StatusCode)
	}
	resp.Body.Close()
	// Unprefixed collection: 403 (no tenant owns it).
	resp = doAs(t, "PUT", a.addr+"/api/collections/plain/docs/1", "tok-acme", `{"v":1}`)
	if resp.StatusCode != 403 {
		t.Fatalf("unprefixed collection must be 403: %d", resp.StatusCode)
	}
	resp.Body.Close()
	// Unknown token: 401.
	resp = doAs(t, "GET", a.addr+"/api/collections/acme.users/docs/1", "bogus", "")
	if resp.StatusCode != 401 {
		t.Fatalf("unknown token must be 401: %d", resp.StatusCode)
	}
	resp.Body.Close()
	// Health stays open.
	resp, _ = client.Get(a.addr + "/health")
	if resp.StatusCode != 200 {
		t.Fatalf("/health must stay open: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestTenantQuota: acme is capped at 3 live docs; the 4th write is
// refused with 403 quota exceeded. Overwriting existing docs is free.
func TestTenantQuota(t *testing.T) {
	a := startQuotaNode(t)
	for i := 1; i <= 3; i++ {
		resp := doAs(t, "PUT", a.addr+"/api/collections/acme.docs/docs/x"+string(rune('0'+i)), "tok-acme", `{"i":`+string(rune('0'+i))+`}`)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("write %d: %d", i, resp.StatusCode)
		}
		// Overwrite same id: never counts against quota.
		resp = doAs(t, "PUT", a.addr+"/api/collections/acme.docs/docs/x"+string(rune('0'+i)), "tok-acme", `{"i":0}`)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("overwrite %d: %d", i, resp.StatusCode)
		}
	}
	// 3 distinct docs now live. 4th distinct doc refused.
	resp := doAs(t, "PUT", a.addr+"/api/collections/acme.docs/docs/y", "tok-acme", `{"i":4}`)
	if resp.StatusCode != 403 {
		t.Fatalf("4th doc must hit quota: %d", resp.StatusCode)
	}
	var out map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if out["error"] != "tenant storage quota exceeded" {
		t.Fatalf("wrong quota error: %v", out)
	}
	// Overwriting an existing doc still works at quota.
	resp = doAs(t, "PUT", a.addr+"/api/collections/acme.docs/docs/x1", "tok-acme", `{"i":99}`)
	if resp.StatusCode != 200 {
		t.Fatalf("overwrite at quota must work: %d", resp.StatusCode)
	}
	resp.Body.Close()
	// Delete frees quota.
	resp = doAs(t, "DELETE", a.addr+"/api/collections/acme.docs/docs/x1", "tok-acme", "")
	resp.Body.Close()
	resp = doAs(t, "PUT", a.addr+"/api/collections/acme.docs/docs/y", "tok-acme", `{"i":4}`)
	if resp.StatusCode != 200 {
		t.Fatalf("after delete, quota should free: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestTenantRateLimit: 5 rps burst 5 -> the 6th immediate request
// gets 429 with Retry-After.
func TestTenantRateLimit(t *testing.T) {
	a := startRateNode(t)
	// Fill the bucket with 5 allowed requests.
	for i := 0; i < 5; i++ {
		resp := doAs(t, "GET", a.addr+"/api/collections/acme.users/docs/none", "tok-acme", "")
		resp.Body.Close() // 404 is fine — we're testing the limiter, not the doc
		if resp.StatusCode == 429 {
			t.Fatalf("request %d should be within burst", i+1)
		}
	}
	// 6th: rate limited.
	resp := doAs(t, "GET", a.addr+"/api/collections/acme.users/docs/none", "tok-acme", "")
	resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("6th request must be 429: %d", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("429 must carry Retry-After")
	}
	// Other tenant unaffected.
	resp = doAs(t, "GET", a.addr+"/api/collections/globex.users/docs/none", "tok-globex", "")
	resp.Body.Close()
	if resp.StatusCode == 429 {
		t.Fatal("globex must not be limited by acme's bucket")
	}
	// After waiting, acme recovers (5 rps -> ~200ms per token).
	time.Sleep(300 * time.Millisecond)
	resp = doAs(t, "GET", a.addr+"/api/collections/acme.users/docs/none", "tok-acme", "")
	resp.Body.Close()
	if resp.StatusCode == 429 {
		t.Fatal("acme should recover after refill")
	}
}
