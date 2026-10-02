// Package loadgen is microdb's benchmark harness: a closed-loop HTTP
// load driver plus the latency-percentile math used to judge every
// performance feature in FEATURES.md.
//
// A closed loop means a new request is issued as soon as the previous
// one returns, so the offered load follows the server rather than
// queueing in front of it. That keeps the numbers honest: latency you
// measure is latency the server actually produced, not queueing delay
// you created.
package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Workload selects the operation mix driven by Run.
const (
	WorkloadMixed = "mixed" // ReadPct% reads, rest writes
	WorkloadRead  = "read"  // point reads only
	WorkloadWrite = "write" // point writes only
	WorkloadQuery = "query" // collection queries with a filter + limit
)

// DefaultMaxSamples bounds the latency samples retained per run. Ops
// beyond the cap still execute and count toward Ops/Errors; they just
// stop feeding percentiles, so a long run can't exhaust memory.
const DefaultMaxSamples = 4 << 20

// Config describes one load run.
type Config struct {
	URL        string        // base URL of the node under test
	Token      string        // bearer token ("" = no auth)
	Collection string        // collection to hit (default "bench")
	Duration   time.Duration // how long to run (default 10s)
	Workers    int           // closed-loop workers (default 8)
	Workload   string        // mixed|read|write|query (default mixed)
	ReadPct    int           // reads share inside "mixed" (default 50)
	Keyspace   int           // distinct doc ids (default 1000)
	QueryLimit int           // limit for "query" workload (default 20)
	Burst      bool          // add Workers more at the halfway point
	Seed       bool          // pre-write the keyspace before measuring
	Timeout    time.Duration // per-request timeout (default 5s)
	MaxSamples int           // latency sample cap (default DefaultMaxSamples)
	Client     *http.Client  // optional: reuse a client (e.g. TLS)
}

func (c *Config) normalize() error {
	if c.URL == "" {
		return errors.New("loadgen: URL is required")
	}
	if c.Collection == "" {
		c.Collection = "bench"
	}
	if c.Duration <= 0 {
		c.Duration = 10 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = 8
	}
	if c.Workload == "" {
		c.Workload = WorkloadMixed
	}
	switch c.Workload {
	case WorkloadMixed, WorkloadRead, WorkloadWrite, WorkloadQuery:
	default:
		return fmt.Errorf("loadgen: unknown workload %q (want mixed|read|write|query)", c.Workload)
	}
	if c.Keyspace <= 0 {
		c.Keyspace = 1000
	}
	if c.ReadPct < 0 {
		c.ReadPct = 0
	}
	if c.ReadPct > 100 {
		c.ReadPct = 100
	}
	if c.QueryLimit <= 0 {
		c.QueryLimit = 20
	}
	if c.Timeout <= 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxSamples <= 0 {
		c.MaxSamples = DefaultMaxSamples
	}
	if c.Client == nil {
		c.Client = &http.Client{
			Timeout: c.Timeout,
			Transport: &http.Transport{
				MaxIdleConns:        c.Workers * 4,
				MaxIdleConnsPerHost: c.Workers * 4,
			},
		}
	}
	// A pure read or query run needs data to read, otherwise every
	// op is a 404 and the "benchmark" measures the error path.
	if !c.Seed {
		c.Seed = c.Workload == WorkloadRead || c.Workload == WorkloadQuery
	}
	return nil
}

// Latency is the latency distribution in milliseconds. Milliseconds
// (not nanoseconds) because these numbers go into JSON reports that
// humans and dashboards read.
type Latency struct {
	Mean float64 `json:"mean_ms"`
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
}

// Report is the machine-readable result of a run.
type Report struct {
	Target     string   `json:"target"`
	Workload   string   `json:"workload"`
	Workers    int      `json:"workers"`
	Burst      bool     `json:"burst"`
	DurationMs int64    `json:"duration_ms"`
	Ops        int64    `json:"ops"`
	Errors     int64    `json:"errors"`
	OpsPerSec  float64  `json:"ops_per_sec"`
	Samples    int      `json:"samples"` // latencies retained for percentiles
	Reads      int64    `json:"reads"`
	Writes     int64    `json:"writes"`
	Latency    Latency  `json:"latency"`
	ErrorSamps []string `json:"error_samples,omitempty"`
}

// Percentile returns the p-th percentile (0..100) of an ascending
// sorted slice. p is clamped into range; an empty slice returns 0.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p < 0 {
		p = 0
	}
	if p > 100 {
		p = 100
	}
	// Nearest-rank: index = ceil(p/100 * n) - 1, which is exact and
	// never interpolates between two real samples.
	rank := int(p/100*float64(len(sorted)) + 0.999999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// Summary turns raw samples into a Latency distribution.
func Summary(samples []time.Duration) Latency {
	if len(samples) == 0 {
		return Latency{}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	var total time.Duration
	for _, d := range samples {
		total += d
	}
	return Latency{
		Mean: ms(total) / float64(len(samples)),
		P50:  ms(Percentile(samples, 50)),
		P95:  ms(Percentile(samples, 95)),
		P99:  ms(Percentile(samples, 99)),
		Max:  ms(samples[len(samples)-1]),
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Run drives the target for cfg.Duration and returns the report.
func Run(cfg Config) (*Report, error) {
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	rep := &Report{
		Target:   cfg.URL,
		Workload: cfg.Workload,
		Workers:  cfg.Workers,
		Burst:    cfg.Burst,
	}

	if cfg.Seed {
		if err := seed(cfg); err != nil {
			return nil, fmt.Errorf("loadgen: seed: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var (
		wg       sync.WaitGroup
		ops      atomic.Int64
		errs     atomic.Int64
		reads    atomic.Int64
		writes   atomic.Int64
		sampMu   sync.Mutex
		samples  []time.Duration
		sampN    int
		errMu    sync.Mutex
		errSamps []string
	)

	record := func(d time.Duration, err error) {
		if err != nil {
			errs.Add(1)
			errMu.Lock()
			if len(errSamps) < 8 {
				errSamps = append(errSamps, err.Error())
			}
			errMu.Unlock()
		}
		ops.Add(1)
		sampMu.Lock()
		if sampN < cfg.MaxSamples {
			samples = append(samples, d)
			sampN++
		}
		sampMu.Unlock()
	}

	client := cfg.Client
	rndSeed := time.Now().UnixNano()

	worker := func(seed int64) {
		defer wg.Done()
		rnd := rand.New(rand.NewSource(seed))
		buf := make([]byte, 0, 256)
		for {
			if ctx.Err() != nil {
				return
			}
			start := time.Now()
			var err error
			switch cfg.Workload {
			case WorkloadRead:
				err = doRead(client, cfg, rnd.Intn(cfg.Keyspace))
				reads.Add(1)
			case WorkloadWrite:
				buf = buf[:0]
				buf = append(buf, `{"n":`...)
				buf = append(buf, fmt.Sprintf("%d,", rnd.Intn(cfg.Keyspace))...)
				buf = append(buf, `"pad":"`...)
				buf = append(buf, pad(rnd)...)
				buf = append(buf, `"}`...)
				err = doWrite(client, cfg, rnd.Intn(cfg.Keyspace), buf)
				writes.Add(1)
			case WorkloadQuery:
				err = doQuery(client, cfg, rnd.Intn(cfg.Keyspace))
				reads.Add(1)
			default: // mixed
				if rnd.Intn(100) < cfg.ReadPct {
					err = doRead(client, cfg, rnd.Intn(cfg.Keyspace))
					reads.Add(1)
				} else {
					buf = buf[:0]
					buf = append(buf, `{"n":`...)
					buf = append(buf, fmt.Sprintf("%d,", rnd.Intn(cfg.Keyspace))...)
					buf = append(buf, `"pad":"`...)
					buf = append(buf, pad(rnd)...)
					buf = append(buf, `"}`...)
					err = doWrite(client, cfg, rnd.Intn(cfg.Keyspace), buf)
					writes.Add(1)
				}
			}
			record(time.Since(start), err)
		}
	}

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go worker(rndSeed + int64(i))
	}

	// Burst: double the concurrency for the second half of the run so
	// the report shows how p99 behaves when load steps up.
	if cfg.Burst {
		time.AfterFunc(cfg.Duration/2, func() {
			if ctx.Err() != nil {
				return
			}
			for i := 0; i < cfg.Workers; i++ {
				wg.Add(1)
				go worker(rndSeed + int64(1000+i))
			}
		})
	}

	<-ctx.Done()
	wg.Wait()

	rep.Ops = ops.Load()
	rep.Errors = errs.Load()
	rep.Reads = reads.Load()
	rep.Writes = writes.Load()
	rep.DurationMs = cfg.Duration.Milliseconds()
	if d := cfg.Duration.Seconds(); d > 0 {
		rep.OpsPerSec = float64(rep.Ops) / d
	}
	rep.Samples = len(samples)
	rep.Latency = Summary(samples)
	rep.ErrorSamps = errSamps
	return rep, nil
}

const padChars = "abcdefghijklmnopqrstuvwxyz0123456789"

func pad(rnd *rand.Rand) string {
	b := make([]byte, 64)
	for i := range b {
		b[i] = padChars[rnd.Intn(len(padChars))]
	}
	return string(b)
}

func docURL(cfg Config, id int) string {
	return fmt.Sprintf("%s/api/collections/%s/docs/%s%d", cfg.URL, cfg.Collection, "k", id)
}

func queryURL(cfg Config, n int) string {
	u, _ := url.Parse(cfg.URL + "/api/collections/" + cfg.Collection + "/docs")
	q := url.Values{}
	q.Set("filter", fmt.Sprintf(`{"n":{"$gt":%d}}`, n))
	q.Set("limit", fmt.Sprintf("%d", cfg.QueryLimit))
	u.RawQuery = q.Encode()
	return u.String()
}

func doWrite(c *http.Client, cfg Config, id int, body []byte) error {
	req, err := http.NewRequest(http.MethodPut, docURL(cfg, id), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return statusErr(resp)
}

func doRead(c *http.Client, cfg Config, id int) error {
	req, err := http.NewRequest(http.MethodGet, docURL(cfg, id), nil)
	if err != nil {
		return err
	}
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return statusErr(resp)
}

func doQuery(c *http.Client, cfg Config, n int) error {
	req, err := http.NewRequest(http.MethodGet, queryURL(cfg, n), nil)
	if err != nil {
		return err
	}
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return statusErr(resp)
}

func statusErr(resp *http.Response) error {
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// seed writes every key in the keyspace once so read/query runs have
// something to measure against. Failures here are fatal to the run —
// a benchmark against a half-populated dataset is meaningless.
func seed(cfg Config) error {
	for i := 0; i < cfg.Keyspace; i++ {
		body := fmt.Sprintf(`{"n":%d,"seed":true}`, i)
		if err := doWrite(cfg.Client, cfg, i, []byte(body)); err != nil {
			return fmt.Errorf("key k%d: %w", i, err)
		}
	}
	return nil
}

// Marshal renders a report as indented JSON.
func (r *Report) Marshal() []byte {
	b, _ := json.MarshalIndent(r, "", "  ")
	return b
}
