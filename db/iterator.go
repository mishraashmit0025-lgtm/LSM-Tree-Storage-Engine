package db

import "container/heap"

type internalIter interface {
	First()
	SeekGE(k ikey)
	Valid() bool
	Next()
	Key() ikey
	Value() []byte
	Err() error
	Close() error
}

// mergeIter merges sorted child iterators with a min-heap. Children are given
// newest source first; on equal keys (which cannot happen with unique sequence
// numbers, but keeps the order total) the earlier child wins.
type mergeIter struct {
	children []internalIter
	h        iterHeap
}

type iterHeap struct {
	items []int
	its   []internalIter
}

func (h iterHeap) Len() int { return len(h.items) }
func (h iterHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if c := compareIKey(h.its[a].Key(), h.its[b].Key()); c != 0 {
		return c < 0
	}
	return a < b
}
func (h iterHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *iterHeap) Push(x any)   { h.items = append(h.items, x.(int)) }
func (h *iterHeap) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

func newMergeIter(children []internalIter) *mergeIter {
	return &mergeIter{children: children, h: iterHeap{its: children}}
}

func (m *mergeIter) rebuild() {
	m.h.items = m.h.items[:0]
	for i, c := range m.children {
		if c.Valid() {
			m.h.items = append(m.h.items, i)
		}
	}
	heap.Init(&m.h)
}

func (m *mergeIter) First() {
	for _, c := range m.children {
		c.First()
	}
	m.rebuild()
}

func (m *mergeIter) SeekGE(k ikey) {
	for _, c := range m.children {
		c.SeekGE(k)
	}
	m.rebuild()
}

func (m *mergeIter) Valid() bool   { return len(m.h.items) > 0 }
func (m *mergeIter) Key() ikey     { return m.children[m.h.items[0]].Key() }
func (m *mergeIter) Value() []byte { return m.children[m.h.items[0]].Value() }

func (m *mergeIter) Next() {
	top := m.h.items[0]
	m.children[top].Next()
	if m.children[top].Valid() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
}

func (m *mergeIter) Err() error {
	for _, c := range m.children {
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (m *mergeIter) Close() error {
	var first error
	for _, c := range m.children {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Iterator walks live user keys in order as of a fixed sequence number. It
// hides older versions and deleted keys.
type Iterator struct {
	m       *mergeIter
	seq     uint64
	release func()
	key     []byte
	value   []byte
	valid   bool
	end     []byte // exclusive upper bound, nil = none
}

func (it *Iterator) skipTo(start ikey) {
	it.m.SeekGE(start)
	it.findNext(nil)
}

// findNext advances the merge iterator to the newest visible version of the
// next user key strictly greater than prev (or the first, when prev is nil).
func (it *Iterator) findNext(prev []byte) {
	it.valid = false
	for it.m.Valid() {
		if it.m.Err() != nil {
			return // stop at the first read error rather than skip data
		}
		k := it.m.Key()
		if it.end != nil && string(k.user) >= string(it.end) {
			return
		}
		if k.seq > it.seq || (prev != nil && string(k.user) == string(prev)) {
			it.m.Next()
			continue
		}
		// k is the newest visible version of k.user
		if k.kind == kindDelete {
			prev = cloneBytes(k.user)
			it.m.Next()
			continue
		}
		it.key, it.value, it.valid = k.user, it.m.Value(), true
		return
	}
}

// Valid reports whether the iterator is positioned at a key.
func (it *Iterator) Valid() bool { return it.valid }

// Key returns the current key. The slice is only valid until Next.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value. The slice is only valid until Next.
func (it *Iterator) Value() []byte { return it.value }

// Next moves to the next live key.
func (it *Iterator) Next() {
	prev := cloneBytes(it.key)
	it.m.Next()
	it.findNext(prev)
}

// Err returns the first I/O or corruption error seen.
func (it *Iterator) Err() error { return it.m.Err() }

// Close releases the tables pinned by the iterator.
func (it *Iterator) Close() error {
	err := it.m.Close()
	if it.release != nil {
		it.release()
		it.release = nil
	}
	return err
}
