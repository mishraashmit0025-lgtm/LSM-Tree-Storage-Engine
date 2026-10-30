package db

import (
	"math/rand/v2"
	"sync/atomic"
)

// The memtable is an insert-only skiplist. Writers are serialized by the DB
// mutex; readers never lock. Each new node is fully built before it is published
// with an atomic store, so a concurrent reader either sees it complete or not at all.

const maxHeight = 12

type node struct {
	key   ikey
	value []byte
	next  [maxHeight]atomic.Pointer[node]
}

type skiplist struct {
	head   node
	height atomic.Int32
	size   atomic.Int64 // approximate bytes, used to decide when to flush
	count  atomic.Int64
}

func newSkiplist() *skiplist {
	s := &skiplist{}
	s.height.Store(1)
	return s
}

func randomHeight() int {
	h := 1
	for h < maxHeight && rand.IntN(4) == 0 {
		h++
	}
	return h
}

// findGE returns the first node >= k and fills prev with the rightmost node
// before it on every level (when prev is non-nil).
func (s *skiplist) findGE(k ikey, prev *[maxHeight]*node) *node {
	x := &s.head
	for level := int(s.height.Load()) - 1; level >= 0; level-- {
		for {
			n := x.next[level].Load()
			if n == nil || compareIKey(n.key, k) >= 0 {
				break
			}
			x = n
		}
		if prev != nil {
			prev[level] = x
		}
	}
	return x.next[0].Load()
}

// insert must be called with the DB write lock held.
func (s *skiplist) insert(k ikey, value []byte) {
	var prev [maxHeight]*node
	s.findGE(k, &prev)
	h := randomHeight()
	if cur := int(s.height.Load()); h > cur {
		for i := cur; i < h; i++ {
			prev[i] = &s.head
		}
		s.height.Store(int32(h))
	}
	n := &node{key: k, value: value}
	for i := 0; i < h; i++ {
		n.next[i].Store(prev[i].next[i].Load())
	}
	for i := 0; i < h; i++ {
		prev[i].next[i].Store(n) // publish bottom-up
	}
	s.size.Add(int64(len(k.user) + len(value) + 24))
	s.count.Add(1)
}

// get returns the newest version of user visible at seq.
func (s *skiplist) get(user []byte, seq uint64) (value []byte, kind byte, found bool) {
	n := s.findGE(ikey{user: user, seq: seq}, nil)
	if n != nil && string(n.key.user) == string(user) {
		return n.value, n.key.kind, true
	}
	return nil, 0, false
}

type skiplistIter struct {
	s *skiplist
	n *node
}

func (s *skiplist) iter() *skiplistIter { return &skiplistIter{s: s} }

func (it *skiplistIter) First()        { it.n = it.s.head.next[0].Load() }
func (it *skiplistIter) SeekGE(k ikey) { it.n = it.s.findGE(k, nil) }
func (it *skiplistIter) Valid() bool   { return it.n != nil }
func (it *skiplistIter) Next()         { it.n = it.n.next[0].Load() }
func (it *skiplistIter) Key() ikey     { return it.n.key }
func (it *skiplistIter) Value() []byte { return it.n.value }
func (it *skiplistIter) Err() error    { return nil }
func (it *skiplistIter) Close() error  { return nil }
