// Package readcache provides a read-through document cache for the
// API layer with changelog-driven invalidation: every local write or
// replicated apply publishes an invalidation through the store's
// change feed, and the cache subscribes to that feed instead of
// TTL-guessing. A cached entry is therefore never staler than the
// moment the mutation was applied on this node — read-your-writes
// holds for cached reads too.
//
// Design: fixed-size sharded LRU (map + list), keyed by
// "collection\x00id". Deletes and upserts both invalidate (we do NOT
// cache-through writes: writes go to the owner, the cache lives on
// the reader; keeping it read-only avoids cross-node coherence).
// Entries also carry a max age as a safety net against missed
// invalidations (there shouldn't be any, but defense in depth).
package readcache

import (
	"container/list"
	"sync"
	"time"
)

type entry struct {
	key    string
	value  interface{}
	stored time.Time
}

type shard struct {
	mu    sync.Mutex
	items map[string]*list.Element
	lru   *list.List // front = most recently used
	cap   int
}

// Cache is a sharded LRU keyed by string.
type Cache struct {
	shards []*shard
	maxAge time.Duration
	hits   int64
	misses int64
	inv    int64
	stats  sync.Mutex
}

// New creates a cache with the given total capacity (rounded to
// 16 shards) and a max entry age (0 = no age limit).
func New(capacity int, maxAge time.Duration) *Cache {
	const nshards = 16
	c := &Cache{maxAge: maxAge}
	per := capacity / nshards
	if per < 1 {
		per = 1
	}
	for i := 0; i < nshards; i++ {
		c.shards = append(c.shards, &shard{
			items: map[string]*list.Element{},
			lru:   list.New(),
			cap:   per,
		})
	}
	return c
}

func (c *Cache) pick(key string) *shard {
	// FNV-ish spread over the key.
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h = (h ^ uint32(key[i])) * 16777619
	}
	return c.shards[h%uint32(len(c.shards))]
}

// Get returns a cached value if present and fresh.
func (c *Cache) Get(key string) (interface{}, bool) {
	s := c.pick(key)
	s.mu.Lock()
	el, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		c.stats.Lock()
		c.misses++
		c.stats.Unlock()
		return nil, false
	}
	e := el.Value.(*entry)
	if c.maxAge > 0 && time.Since(e.stored) > c.maxAge {
		s.lru.Remove(el)
		delete(s.items, key)
		s.mu.Unlock()
		c.stats.Lock()
		c.misses++
		c.stats.Unlock()
		return nil, false
	}
	s.lru.MoveToFront(el)
	v := e.value
	s.mu.Unlock()
	c.stats.Lock()
	c.hits++
	c.stats.Unlock()
	return v, true
}

// Put stores a value, evicting the LRU entry when the shard is full.
func (c *Cache) Put(key string, value interface{}) {
	s := c.pick(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[key]; ok {
		el.Value.(*entry).value = value
		el.Value.(*entry).stored = time.Now()
		s.lru.MoveToFront(el)
		return
	}
	el := s.lru.PushFront(&entry{key: key, value: value, stored: time.Now()})
	s.items[key] = el
	for s.lru.Len() > s.cap {
		back := s.lru.Back()
		if back == nil {
			break
		}
		s.lru.Remove(back)
		delete(s.items, back.Value.(*entry).key)
	}
}

// Invalidate drops one key (called on every mutation of that doc).
func (c *Cache) Invalidate(key string) {
	s := c.pick(key)
	s.mu.Lock()
	if el, ok := s.items[key]; ok {
		s.lru.Remove(el)
		delete(s.items, key)
	}
	s.mu.Unlock()
	c.stats.Lock()
	c.inv++
	c.stats.Unlock()
}

// Stats returns cache counters.
func (c *Cache) Stats() (hits, misses, invalidations int64) {
	c.stats.Lock()
	defer c.stats.Unlock()
	return c.hits, c.misses, c.inv
}

// Len counts live entries across shards (approximate, for metrics).
func (c *Cache) Len() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += len(s.items)
		s.mu.Unlock()
	}
	return n
}
