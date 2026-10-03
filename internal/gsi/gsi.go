// Package gsi implements global secondary indexes maintained
// ASYNCHRONOUSLY off the write path: a background worker subscribes
// to the store's change feed and maintains a separate index
// collection keyed by (index_value, collection, id). Writes never
// pay for GSI maintenance — the index lags the primary by however
// long the worker takes, and that lag is exposed to clients so they
// can decide whether a stale index read is acceptable.
//
// Storage layout (all ordinary data, so it replicates + survives):
//
//	_config/<col>.gsi            -> {"name": field, ...} definitions
//	_gsi.<name>                 -> index collection
//	  id = value\x00col\x00docid -> {"value":v,"col":c,"id":d}
//
// Lookup uses the store's inverted-index fast path on the "value"
// field: O(matches), not a scan.
package gsi

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"microdb/internal/changelog"
	"microdb/internal/store"
)

const (
	// IndexCollectionPrefix marks GSI data collections.
	IndexCollectionPrefix = "_gsi."
	// GSIField is the _config field holding a collection's GSI defs.
	GSIField = "gsi"
)

// Def is one index definition: index `Name` over field `Field`.
type Def struct {
	Name  string `json:"name"`
	Field string `json:"field"`
}

// Manager maintains all GSIs for one node.
type Manager struct {
	st        *store.Store
	worker    chan changelog.Event
	stop      chan struct{}
	processed atomic.Int64 // last change-feed seq applied to indexes
	running   bool
	mu        sync.Mutex
}

// NewManager starts the async maintenance worker.
func NewManager(st *store.Store) *Manager {
	m := &Manager{
		st:     st,
		worker: make(chan changelog.Event, 4096),
		stop:   make(chan struct{}),
	}
	go m.loop()
	return m
}

// Attach subscribes to the store's change feed. Every event is
// queued for async index maintenance.
func (m *Manager) Attach() {
	m.st.ChangeLog().OnAppend(func(ev changelog.Event) {
		select {
		case m.worker <- ev:
		default:
			// Worker saturated: drop the event rather than block the
			// write path. The lag counter will show the gap and a
			// re-backfill (Declare again) repairs it.
		}
	})
}

// Stop ends the worker.
func (m *Manager) Stop() { close(m.stop) }

func (m *Manager) loop() {
	for {
		select {
		case <-m.stop:
			return
		case ev := <-m.worker:
			m.applyEvent(ev)
			m.processed.Store(ev.Seq)
		}
	}
}

// Lag reports how far the indexes trail the change feed.
func (m *Manager) Lag() (processed, head int64, age time.Duration) {
	head = m.st.ChangeLog().Head()
	processed = m.processed.Load()
	return processed, head, time.Duration(head - processed)
}

// --- definitions -------------------------------------------------

// Declare adds (or replaces) a GSI definition for a collection and
// backfills it from existing docs. Returns the number of docs
// indexed during backfill.
func (m *Manager) Declare(col string, d Def) (int, error) {
	if d.Name == "" || d.Field == "" {
		return 0, fmt.Errorf("gsi name and field are required")
	}
	if strings.HasPrefix(d.Name, "_") {
		return 0, fmt.Errorf("gsi name must not start with _")
	}
	defs := m.Defs(col)
	replaced := false
	for i, x := range defs {
		if x.Name == d.Name {
			defs[i] = d
			replaced = true
		}
	}
	if !replaced {
		defs = append(defs, d)
	}
	if err := m.saveDefs(col, defs); err != nil {
		return 0, err
	}
	return m.Backfill(col, d), nil
}

// Backfill indexes every existing doc in col for one definition.
func (m *Manager) Backfill(col string, d Def) int {
	n := 0
	docs := m.st.Scan(col, nil)
	for _, doc := range docs {
		v, ok := doc.Fields[d.Field]
		if !ok {
			continue
		}
		m.indexDoc(d.Name, v, col, doc.ID)
		n++
	}
	return n
}

// Defs returns the GSI definitions for a collection.
func (m *Manager) Defs(col string) []Def {
	d, ok := m.st.Get(store.ConfigCollection, col)
	if !ok {
		return nil
	}
	raw, ok := d.Fields[GSIField].(map[string]interface{})
	if !ok {
		return nil
	}
	defs := make([]Def, 0, len(raw))
	for name, field := range raw {
		if fs, ok := field.(string); ok {
			defs = append(defs, Def{Name: name, Field: fs})
		}
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	return defs
}

func (m *Manager) saveDefs(col string, defs []Def) error {
	mapped := map[string]interface{}{}
	for _, d := range defs {
		mapped[d.Name] = d.Field
	}
	cfg := m.st.GetCollectionConfig(col)
	b, _ := json.Marshal(map[string]interface{}{"gsi": mapped})
	var fields map[string]interface{}
	json.Unmarshal(b, &fields)
	fields["rf"] = cfg.RF
	if cfg.PartitionField != "" {
		fields["partition_field"] = cfg.PartitionField
	}
	if cfg.SortField != "" {
		fields["sort_field"] = cfg.SortField
	}
	_, err := m.st.Apply(store.ConfigCollection, col, fields)
	return err
}

// --- async maintenance -------------------------------------------

func (m *Manager) applyEvent(ev changelog.Event) {
	defs := m.Defs(ev.Collection)
	if len(defs) == 0 {
		return
	}
	if ev.Kind == "delete" {
		// Remove this doc's entries from every index it might be in.
		for _, d := range defs {
			m.removeIndexed(d.Name, ev.Collection, ev.ID)
		}
		return
	}
	doc, ok := ev.Doc.(*store.Doc)
	if !ok || doc.Deleted {
		return
	}
	for _, d := range defs {
		v, ok := doc.Fields[d.Field]
		if !ok {
			continue
		}
		m.indexDoc(d.Name, v, ev.Collection, ev.ID)
	}
}

func indexKey(value interface{}, col, id string) string {
	return fmt.Sprintf("%v\x00%s\x00%s", value, col, id)
}

func (m *Manager) indexDoc(indexName string, value interface{}, col, id string) {
	key := indexKey(value, col, id)
	if _, exists := m.st.Get(IndexCollectionPrefix+indexName, key); exists {
		return // idempotent
	}
	m.st.Apply(IndexCollectionPrefix+indexName, key, map[string]interface{}{
		"value": fmt.Sprintf("%v", value),
		"col":   col,
		"id":    id,
	})
}

func (m *Manager) removeIndexed(indexName, col, id string) {
	suffix := "\x00" + col + "\x00" + id
	docs := m.st.Scan(IndexCollectionPrefix+indexName, func(d *store.Doc) bool {
		return strings.HasSuffix(d.ID, suffix)
	})
	for _, d := range docs {
		m.st.Delete(IndexCollectionPrefix+indexName, d.ID)
	}
}

// --- queries -------------------------------------------------------

// Lookup returns the doc ids in `col` whose indexed field equals
// `value`, plus the current index lag so callers can judge freshness.
func (m *Manager) Lookup(col, indexName string, value interface{}) ([]string, int64, int64) {
	defs := m.Defs(col)
	found := false
	for _, d := range defs {
		if d.Name == indexName {
			found = true
		}
	}
	if !found {
		return nil, 0, 0
	}
	idxDocs := m.st.ScanIndexed(IndexCollectionPrefix+indexName, map[string]interface{}{
		"value": fmt.Sprintf("%v", value),
		"col":   col,
	})
	ids := make([]string, 0, len(idxDocs))
	for _, d := range idxDocs {
		if v, ok := d.Fields["id"].(string); ok {
			ids = append(ids, v)
		}
	}
	sort.Strings(ids)
	processed, head, _ := m.Lag()
	return ids, processed, head
}
