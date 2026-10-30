package db

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

// SSTable layout:
//
//	data block 0 | data block 1 | ... | index block | bloom block | footer
//
// A data block is a run of entries followed by a crc32c of the block:
//
//	klen uvarint | vlen uvarint | seq u64 | kind u8 | key | value
//
// The index block has one entry per data block: the block's last key (with seq),
// its offset and its length. The footer is fixed size and points at the index
// and bloom blocks.

const (
	footerSize  = 48
	tableMagic  = 0x4c534d54424c3031 // "LSMTBL01"
	blockTarget = 4 << 10
)

var errCorrupt = errors.New("sstable: corrupt")

type indexEntry struct {
	last   ikey
	offset uint64
	length uint64
}

type tableWriter struct {
	f       *os.File
	w       *bufio.Writer
	offset  uint64
	block   []byte
	last    ikey
	index   []indexEntry
	keys    [][]byte
	count   uint64
	first   ikey
	started bool
}

func newTableWriter(path string) (*tableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &tableWriter{f: f, w: bufio.NewWriterSize(f, 256<<10)}, nil
}

// add appends an entry; entries must arrive in ikey order.
func (t *tableWriter) add(k ikey, value []byte) error {
	if !t.started {
		t.first = ikey{cloneBytes(k.user), k.seq, k.kind}
		t.started = true
	}
	t.block = binary.AppendUvarint(t.block, uint64(len(k.user)))
	t.block = binary.AppendUvarint(t.block, uint64(len(value)))
	t.block = binary.LittleEndian.AppendUint64(t.block, k.seq)
	t.block = append(t.block, k.kind)
	t.block = append(t.block, k.user...)
	t.block = append(t.block, value...)
	t.last = ikey{cloneBytes(k.user), k.seq, k.kind}
	if len(t.keys) == 0 || string(t.keys[len(t.keys)-1]) != string(k.user) {
		t.keys = append(t.keys, t.last.user)
	}
	t.count++
	if len(t.block) >= blockTarget {
		return t.finishBlock()
	}
	return nil
}

func (t *tableWriter) estimatedSize() uint64 { return t.offset + uint64(len(t.block)) }

func (t *tableWriter) finishBlock() error {
	if len(t.block) == 0 {
		return nil
	}
	t.block = binary.LittleEndian.AppendUint32(t.block, crc32.Checksum(t.block, castagnoli))
	if _, err := t.w.Write(t.block); err != nil {
		return err
	}
	t.index = append(t.index, indexEntry{t.last, t.offset, uint64(len(t.block))})
	t.offset += uint64(len(t.block))
	t.block = t.block[:0]
	return nil
}

// finish writes index, bloom and footer, syncs and closes the file.
func (t *tableWriter) finish() (smallest, largest ikey, size uint64, err error) {
	defer t.f.Close()
	if err = t.finishBlock(); err != nil {
		return
	}
	var idx []byte
	for _, e := range t.index {
		idx = binary.AppendUvarint(idx, uint64(len(e.last.user)))
		idx = append(idx, e.last.user...)
		idx = binary.LittleEndian.AppendUint64(idx, e.last.seq)
		idx = append(idx, e.last.kind)
		idx = binary.LittleEndian.AppendUint64(idx, e.offset)
		idx = binary.LittleEndian.AppendUint64(idx, e.length)
	}
	idx = binary.LittleEndian.AppendUint32(idx, crc32.Checksum(idx, castagnoli))
	indexOff := t.offset
	if _, err = t.w.Write(idx); err != nil {
		return
	}
	bf := newBloom(len(t.keys), 10)
	for _, k := range t.keys {
		bf.add(k)
	}
	bl := bf.encode()
	bloomOff := indexOff + uint64(len(idx))
	if _, err = t.w.Write(bl); err != nil {
		return
	}
	var foot []byte
	foot = binary.LittleEndian.AppendUint64(foot, indexOff)
	foot = binary.LittleEndian.AppendUint64(foot, uint64(len(idx)))
	foot = binary.LittleEndian.AppendUint64(foot, bloomOff)
	foot = binary.LittleEndian.AppendUint64(foot, uint64(len(bl)))
	foot = binary.LittleEndian.AppendUint64(foot, t.count)
	foot = binary.LittleEndian.AppendUint64(foot, tableMagic)
	if _, err = t.w.Write(foot); err != nil {
		return
	}
	if err = t.w.Flush(); err != nil {
		return
	}
	if err = t.f.Sync(); err != nil {
		return
	}
	return t.first, t.last, bloomOff + uint64(len(bl)) + footerSize, nil
}

func (t *tableWriter) abort() {
	t.f.Close()
	os.Remove(t.f.Name())
}

// table is an open, immutable SSTable. It is reference counted so that a
// compaction can retire it while readers that started earlier finish with it.
type table struct {
	num      uint64
	path     string
	size     uint64
	smallest ikey
	largest  ikey
	f        *os.File
	index    []indexEntry
	bloom    *bloom
	cache    *blockCache

	refs     atomic.Int32
	mu       sync.Mutex
	obsolete bool
}

func openTable(path string, num uint64, cache *blockCache) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := uint64(st.Size())
	if size < footerSize {
		f.Close()
		return nil, fmt.Errorf("%s: %w (too small)", path, errCorrupt)
	}
	foot := make([]byte, footerSize)
	if _, err := f.ReadAt(foot, int64(size-footerSize)); err != nil {
		f.Close()
		return nil, err
	}
	u := func(i int) uint64 { return binary.LittleEndian.Uint64(foot[i*8:]) }
	if u(5) != tableMagic {
		f.Close()
		return nil, fmt.Errorf("%s: %w (bad magic)", path, errCorrupt)
	}
	idx := make([]byte, u(1))
	if _, err := f.ReadAt(idx, int64(u(0))); err != nil {
		f.Close()
		return nil, err
	}
	if len(idx) < 4 || crc32.Checksum(idx[:len(idx)-4], castagnoli) != binary.LittleEndian.Uint32(idx[len(idx)-4:]) {
		f.Close()
		return nil, fmt.Errorf("%s: %w (index checksum)", path, errCorrupt)
	}
	bl := make([]byte, u(3))
	if _, err := f.ReadAt(bl, int64(u(2))); err != nil {
		f.Close()
		return nil, err
	}
	t := &table{num: num, path: path, size: size, f: f, bloom: decodeBloom(bl), cache: cache}
	p := idx[:len(idx)-4]
	for len(p) > 0 {
		kl, n := binary.Uvarint(p)
		if n <= 0 || len(p) < n+int(kl)+25 {
			f.Close()
			return nil, fmt.Errorf("%s: %w (index entry)", path, errCorrupt)
		}
		p = p[n:]
		var e indexEntry
		e.last.user = p[:kl]
		p = p[kl:]
		e.last.seq = binary.LittleEndian.Uint64(p)
		e.last.kind = p[8]
		e.offset = binary.LittleEndian.Uint64(p[9:])
		e.length = binary.LittleEndian.Uint64(p[17:])
		p = p[25:]
		t.index = append(t.index, e)
	}
	if len(t.index) > 0 {
		t.largest = t.index[len(t.index)-1].last
		it := t.iter()
		it.First()
		if it.Valid() {
			k := it.Key()
			t.smallest = ikey{cloneBytes(k.user), k.seq, k.kind}
		}
		if err := it.Err(); err != nil {
			f.Close()
			return nil, err
		}
	}
	t.refs.Store(1)
	return t, nil
}

func (t *table) ref() { t.refs.Add(1) }

// unref drops a reference; the last one closes the file and, if the table was
// retired by a compaction, deletes it.
func (t *table) unref() {
	if t.refs.Add(-1) != 0 {
		return
	}
	t.f.Close()
	t.cache.evictFile(t.num)
	t.mu.Lock()
	obsolete := t.obsolete
	t.mu.Unlock()
	if obsolete {
		os.Remove(t.path)
	}
}

func (t *table) markObsolete() {
	t.mu.Lock()
	t.obsolete = true
	t.mu.Unlock()
}

type blockEntry struct {
	key   ikey
	value []byte
}

func (t *table) readBlock(i int) ([]blockEntry, error) {
	e := t.index[i]
	ck := cacheKey{t.num, e.offset}
	raw, ok := t.cache.get(ck)
	if !ok {
		raw = make([]byte, e.length)
		if _, err := t.f.ReadAt(raw, int64(e.offset)); err != nil {
			return nil, err
		}
		if len(raw) < 4 || crc32.Checksum(raw[:len(raw)-4], castagnoli) != binary.LittleEndian.Uint32(raw[len(raw)-4:]) {
			return nil, fmt.Errorf("%s block %d: %w (checksum)", t.path, i, errCorrupt)
		}
		raw = raw[:len(raw)-4]
		t.cache.put(ck, raw)
	}
	var out []blockEntry
	p := raw
	for len(p) > 0 {
		kl, n1 := binary.Uvarint(p)
		if n1 <= 0 {
			return nil, errCorrupt
		}
		vl, n2 := binary.Uvarint(p[n1:])
		if n2 <= 0 {
			return nil, errCorrupt
		}
		p = p[n1+n2:]
		if uint64(len(p)) < 9+kl+vl {
			return nil, errCorrupt
		}
		seq := binary.LittleEndian.Uint64(p)
		kind := p[8]
		p = p[9:]
		out = append(out, blockEntry{ikey{p[:kl], seq, kind}, p[kl : kl+vl]})
		p = p[kl+vl:]
	}
	return out, nil
}

// get looks up the newest version of user with sequence <= seq.
func (t *table) get(user []byte, seq uint64) (value []byte, kind byte, found bool, err error) {
	if t.bloom != nil && !t.bloom.mayContain(user) {
		return nil, 0, false, nil
	}
	it := t.iter()
	it.SeekGE(ikey{user: user, seq: seq})
	if it.Valid() && string(it.Key().user) == string(user) {
		return it.Value(), it.Key().kind, true, nil
	}
	return nil, 0, false, it.Err()
}

type tableIter struct {
	t       *table
	bi      int
	entries []blockEntry
	pos     int
	err     error
}

func (t *table) iter() *tableIter { return &tableIter{t: t, bi: -1} }

func (it *tableIter) load(bi int) bool {
	for bi < len(it.t.index) {
		es, err := it.t.readBlock(bi)
		if err != nil {
			it.err = err
			it.entries = nil
			return false
		}
		it.bi, it.entries, it.pos = bi, es, 0
		if len(es) > 0 {
			return true
		}
		bi++
	}
	it.bi, it.entries = len(it.t.index), nil
	return false
}

func (it *tableIter) First() { it.load(0) }

func (it *tableIter) SeekGE(k ikey) {
	bi := sort.Search(len(it.t.index), func(i int) bool { return compareIKey(it.t.index[i].last, k) >= 0 })
	if !it.load(bi) {
		return
	}
	it.pos = sort.Search(len(it.entries), func(i int) bool { return compareIKey(it.entries[i].key, k) >= 0 })
	if it.pos == len(it.entries) {
		it.load(it.bi + 1)
	}
}

func (it *tableIter) Valid() bool {
	return it.err == nil && it.entries != nil && it.pos < len(it.entries)
}
func (it *tableIter) Key() ikey     { return it.entries[it.pos].key }
func (it *tableIter) Value() []byte { return it.entries[it.pos].value }
func (it *tableIter) Err() error    { return it.err }
func (it *tableIter) Close() error  { return nil }

func (it *tableIter) Next() {
	it.pos++
	if it.pos >= len(it.entries) {
		it.load(it.bi + 1)
	}
}
