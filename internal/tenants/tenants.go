// Package tenants implements multi-tenant isolation at the API edge:
// per-tenant bearer tokens, collection namespacing (a tenant only
// sees collections under its own prefix), token-bucket rate limiting,
// and a storage quota (max live documents under the tenant prefix).
//
// The design keeps "tiny" honest: tenancy is an admission-layer
// concern, not a storage-engine one. Collections remain the unit of
// data; a tenant named "acme" owns "acme.*" collections. Quota
// counting is O(1) per write via an incrementing counter that is
// recomputed from the store on startup.
package tenants

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// Tenant is one tenant's admission policy.
type Tenant struct {
	Name      string  `json:"name"`
	Token     string  `json:"token"`
	RateRPS   float64 `json:"rate_rps"`  // sustained requests/sec (0 = unlimited)
	Burst     int     `json:"burst"`     // bucket size (default 2x rate)
	MaxDocs   int64   `json:"max_docs"`  // live docs under prefix (0 = unlimited)
}

// Registry holds tenants and their live limiters.
type Registry struct {
	byToken map[string]*Tenant
	states  map[string]*bucket
	mu      sync.Mutex
}

// Load reads a tenants JSON file: {"tenants":[{...},...]}.
func Load(path string) (*Registry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var in struct {
		Tenants []Tenant `json:"tenants"`
	}
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, err
	}
	r := &Registry{
		byToken: map[string]*Tenant{},
		states:  map[string]*bucket{},
	}
	for i := range in.Tenants {
		t := in.Tenants[i]
		if t.Name == "" || t.Token == "" {
			return nil, fmt.Errorf("tenant %d: name and token are required", i)
		}
		if _, dup := r.byToken[t.Token]; dup {
			return nil, fmt.Errorf("duplicate token for tenant %s", t.Name)
		}
		r.byToken[t.Token] = &in.Tenants[i]
	}
	return r, nil
}

// Empty reports whether no tenants are configured (feature off).
func (r *Registry) Empty() bool { return r == nil || len(r.byToken) == 0 }

// Lookup resolves a bearer token to a tenant in constant time over
// the token map (map lookup is O(1) but the compare is constant-time
// per candidate to avoid timing side channels on token bytes).
func (r *Registry) Lookup(token string) (*Tenant, bool) {
	if r == nil {
		return nil, false
	}
	var found *Tenant
	for tok, t := range r.byToken {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(token)) == 1 {
			found = t
			// Don't break early on a match: keep scanning so the
			// loop cost doesn't reveal match position.
		}
	}
	return found, found != nil
}

// AllowRate consumes one token from the tenant's bucket. Returns
// (allowed, retryAfter).
func (r *Registry) AllowRate(t *Tenant) (bool, time.Duration) {
	if t.RateRPS <= 0 {
		return true, 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.states[t.Name]
	if !ok {
		burst := t.Burst
		if burst <= 0 {
			burst = int(2 * t.RateRPS)
			if burst < 1 {
				burst = 1
			}
		}
		b = &bucket{tokens: float64(burst), max: float64(burst), rate: t.RateRPS, last: time.Now()}
		r.states[t.Name] = b
	}
	b.refill(time.Now())
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	need := 1 - b.tokens
	wait := time.Duration(need / b.rate * float64(time.Second))
	if wait < time.Second {
		wait = time.Second
	}
	return false, wait
}

// All returns all configured tenants (for name-based lookup by the
// API layer).
func (r *Registry) All() []*Tenant {
	if r == nil {
		return nil
	}
	out := make([]*Tenant, 0, len(r.byToken))
	for _, t := range r.byToken {
		out = append(out, t)
	}
	return out
}

// CheckQuota returns ok=false if admitting delta more live docs
// would exceed the tenant's MaxDocs. current is the tenant's live
// doc count as counted from the store by the caller.
func (r *Registry) CheckQuota(t *Tenant, current, delta int64) bool {
	if t.MaxDocs <= 0 {
		return true
	}
	return current+delta <= t.MaxDocs
}

// PrefixAllowed checks that a collection is inside the tenant's
// namespace: "acme" owns "acme.*" only.
func PrefixAllowed(t *Tenant, collection string) bool {
	return strings.HasPrefix(collection, t.Name+".")
}

// TenantFromCollection extracts the tenant prefix from a collection
// name ("acme.users" -> "acme"), or "" if there is no prefix.
func TenantFromCollection(collection string) string {
	if i := strings.Index(collection, "."); i > 0 {
		return collection[:i]
	}
	return ""
}

// bucket is a classic token bucket.
type bucket struct {
	tokens float64
	max    float64
	rate   float64
	last   time.Time
}

func (b *bucket) refill(now time.Time) {
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.max {
			b.tokens = b.max
		}
		b.last = now
	}
}
