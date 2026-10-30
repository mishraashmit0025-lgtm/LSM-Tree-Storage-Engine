package db

import (
	"container/list"
	"sync"
)

// blockCache is an LRU cache of decoded data blocks, bounded by total bytes.
type blockCache struct {
	mu       sync.Mutex
	capacity int
	used     int
	ll       *list.List
	items    map[cacheKey]*list.Element
	hits     uint64
	misses   uint64
}

type cacheKey struct {
	file   uint64
	offset uint64
}

type cacheEntry struct {
	key  cacheKey
	data []byte
}

func newBlockCache(capacity int) *blockCache {
	return &blockCache{capacity: capacity, ll: list.New(), items: map[cacheKey]*list.Element{}}
}

func (c *blockCache) get(k cacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[k]; ok {
		c.ll.MoveToFront(e)
		c.hits++
		return e.Value.(*cacheEntry).data, true
	}
	c.misses++
	return nil, false
}

func (c *blockCache) put(k cacheKey, data []byte) {
	if c == nil || len(data) > c.capacity {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[k]; ok {
		c.ll.MoveToFront(e)
		return
	}
	c.items[k] = c.ll.PushFront(&cacheEntry{k, data})
	c.used += len(data)
	for c.used > c.capacity {
		e := c.ll.Back()
		ent := e.Value.(*cacheEntry)
		c.ll.Remove(e)
		delete(c.items, ent.key)
		c.used -= len(ent.data)
	}
}

func (c *blockCache) evictFile(file uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.items {
		if k.file == file {
			c.ll.Remove(e)
			delete(c.items, k)
			c.used -= len(e.Value.(*cacheEntry).data)
		}
	}
}

func (c *blockCache) stats() (hits, misses uint64) {
	if c == nil {
		return 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses
}
