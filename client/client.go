// Package client is a typed Go client for microdb. It follows
// write-forwarding (any node accepts any write) and exposes the full
// query surface: filters, sort, pagination, watch, batch, cluster.
package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	BaseURL    string
	HTTP       *http.Client
	Token      string // optional bearer token
	mu         sync.Mutex
	ownerCache map[string]ownerEntry // "col/id" -> cached primary + epoch
}

// New creates a client for a node URL (http:// or https://).
// For TLS with a custom CA, use NewTLS.
func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTP: &http.Client{Timeout: 10 * time.Second}, ownerCache: map[string]ownerEntry{}}
}

// NewTLS creates a TLS client trusting the given CA PEM file.
func NewTLS(baseURL, caFile string) (*Client, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("no certs in %s", caFile)
	}
	return &Client{
		BaseURL:    baseURL,
		HTTP:       &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}},
		ownerCache: map[string]ownerEntry{},
	}, nil
}

// Doc mirrors the server's document envelope.
type Doc struct {
	ID         string                 `json:"id"`
	Ver        int64                  `json:"ver"`
	TS         int64                  `json:"ts"`
	Deleted    bool                   `json:"deleted,omitempty"`
	Collection string                 `json:"collection"`
	Fields     map[string]interface{} `json:"fields"`
}

// QueryResult is the paginated query envelope.
type QueryResult struct {
	Count int    `json:"count"`
	Total int    `json:"total"`
	Docs  []*Doc `json:"docs"`
	// TotalExact reports whether Total is the exact match count.
	// With sort/limit pushdown a truncated shard makes Total a lower
	// bound (TotalExact=false); the returned window is always exact.
	TotalExact bool `json:"total_exact"`
	Partial    bool `json:"partial,omitempty"`
}

// Event is a change-feed event.
type Event struct {
	Seq        int64  `json:"seq"`
	TS         int64  `json:"ts"`
	Collection string `json:"collection"`
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Doc        *Doc   `json:"doc,omitempty"`
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, out interface{}) error {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s: %d: %s", method, path, resp.StatusCode, string(b))
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// Put upserts a document (schema-free fields).
func (c *Client) Put(ctx context.Context, col, id string, fields map[string]interface{}) (*Doc, error) {
	b, _ := json.Marshal(fields)
	var d Doc
	err := c.do(ctx, "PUT", "/api/collections/"+col+"/docs/"+id, b, &d)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// --- shard-map aware routing -------------------------------------
//
// A shard-map-aware client caches the primary owner of each key
// (learned from GET /internal/owners) and sends writes straight to
// the primary, skipping the coordinator hop through a non-owner node.
// The cache carries the ring epoch; callers can refresh it wholesale
// when they observe an epoch change.

type ownerEntry struct {
	primary string
	epoch   int64
}

// Owners returns the cached primary owner for col/id, refreshing the
// cache from the client's base node if the entry is missing.
func (c *Client) Owners(ctx context.Context, col, id string) (string, error) {
	c.mu.Lock()
	if e, ok := c.ownerCache[col+"/"+id]; ok {
		c.mu.Unlock()
		return e.primary, nil
	}
	c.mu.Unlock()
	var out struct {
		Owners []string `json:"owners"`
		Epoch  int64    `json:"epoch"`
	}
	if err := c.do(ctx, "GET", "/internal/owners/"+col+"/"+id, nil, &out); err != nil {
		return "", err
	}
	if len(out.Owners) == 0 {
		return "", fmt.Errorf("no owners known for %s/%s", col, id)
	}
	c.mu.Lock()
	c.ownerCache[col+"/"+id] = ownerEntry{primary: out.Owners[0], epoch: out.Epoch}
	c.mu.Unlock()
	return out.Owners[0], nil
}

// PutRouted writes directly to the key's primary owner (learned via
// the shard map), falling back to the base node if routing fails.
// Saves a server-side forward hop for hot keys.
func (c *Client) PutRouted(ctx context.Context, col, id string, fields map[string]interface{}) (*Doc, error) {
	primary, err := c.Owners(ctx, col, id)
	if err != nil {
		return c.Put(ctx, col, id, fields) // fall back to plain forwarding path
	}
	b, _ := json.Marshal(fields)
	var d Doc
	if err := c.doTo(ctx, primary, "PUT", "/api/collections/"+col+"/docs/"+id, b, &d); err != nil {
		// Primary may have moved; drop the cache entry and retry once
		// through the base node (which forwards correctly).
		c.mu.Lock()
		delete(c.ownerCache, col+"/"+id)
		c.mu.Unlock()
		return c.Put(ctx, col, id, fields)
	}
	return &d, nil
}

// doTo issues a request against an arbitrary node URL (same auth and
// error handling as do).
func (c *Client) doTo(ctx context.Context, base, method, path string, body []byte, out interface{}) error {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, base+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s%s: %d: %s", method, base, path, resp.StatusCode, string(b))
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// InvalidateShardMap drops all cached ownership (call on epoch
// mismatch or topology change).
func (c *Client) InvalidateShardMap() {
	c.mu.Lock()
	c.ownerCache = map[string]ownerEntry{}
	c.mu.Unlock()
}

// Get fetches one document.
func (c *Client) Get(ctx context.Context, col, id string) (*Doc, error) {
	var d Doc
	if err := c.do(ctx, "GET", "/api/collections/"+col+"/docs/"+id, nil, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// Delete removes a document.
func (c *Client) Delete(ctx context.Context, col, id string) error {
	return c.do(ctx, "DELETE", "/api/collections/"+col+"/docs/"+id, nil, nil)
}

// Batch upserts many docs in one round trip.
func (c *Client) Batch(ctx context.Context, col string, docs map[string]map[string]interface{}) ([]*Doc, error) {
	body, _ := json.Marshal(map[string]interface{}{"docs": docs})
	var out struct {
		Applied int    `json:"applied"`
		Docs    []*Doc `json:"docs"`
	}
	if err := c.do(ctx, "POST", "/api/collections/"+col+"/docs/batch", body, &out); err != nil {
		return nil, err
	}
	return out.Docs, nil
}

// QueryOpts builds a collection query.
type QueryOpts struct {
	Filter map[string]interface{}
	Sort   string
	Desc   bool
	Limit  int // <0 = unlimited
	Offset int
}

// Query runs a scatter-gather collection query.
func (c *Client) Query(ctx context.Context, col string, opts QueryOpts) (*QueryResult, error) {
	q := url.Values{}
	if opts.Filter != nil {
		fb, _ := json.Marshal(opts.Filter)
		q.Set("filter", string(fb))
	}
	if opts.Sort != "" {
		q.Set("sort", opts.Sort)
		q.Set("desc", strconv.FormatBool(opts.Desc))
	}
	if opts.Limit >= 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Offset > 0 {
		q.Set("offset", strconv.Itoa(opts.Offset))
	}
	var res QueryResult
	path := "/api/collections/" + col + "/docs"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	if err := c.do(ctx, "GET", path, nil, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// WatchStream iterates change-feed events. Call Watch first to get a
// stream; iterate with Next until the context is cancelled or the
// server window closes.
type WatchStream struct {
	col   string
	since int64
	c     *Client
	body  io.ReadCloser
	sc    *bufio.Scanner
}

// Watch opens a long-poll stream on a collection starting at `since`
// (0 = from now on; use Head() to find the current cursor).
func (c *Client) Watch(ctx context.Context, col string, since int64) (*WatchStream, error) {
	path := "/api/collections/" + col + "/watch?since=" + strconv.FormatInt(since, 10) + "&wait=30"
	req, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("watch: %d: %s", resp.StatusCode, string(b))
	}
	return &WatchStream{col: col, since: since, c: c, body: resp.Body, sc: bufio.NewScanner(resp.Body)}, nil
}

// Next returns the next event, or io.EOF when the window closes.
// Reconnect with the last seen Seq to continue.
func (w *WatchStream) Next() (*Event, error) {
	if !w.sc.Scan() {
		return nil, io.EOF
	}
	var e Event
	if err := json.Unmarshal(w.sc.Bytes(), &e); err != nil {
		return nil, err
	}
	w.since = e.Seq
	return &e, nil
}

// Since returns the last consumed sequence number (for reconnecting).
func (w *WatchStream) Since() int64 { return w.since }

// Close ends the stream.
func (w *WatchStream) Close() error { return w.body.Close() }

// ClusterInfo is the /api/cluster response.
type ClusterInfo struct {
	Self  string   `json:"self"`
	Peers []string `json:"peers"`
}

// Cluster returns the node's view of the cluster.
func (c *Client) Cluster(ctx context.Context) (*ClusterInfo, error) {
	var info ClusterInfo
	if err := c.do(ctx, "GET", "/api/cluster", nil, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// Health checks node liveness.
func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, "GET", "/health", nil, nil)
}
