// Package db is a log-structured merge-tree key-value store.
//
// Writes go to a write-ahead log and an in-memory skiplist. Full memtables are
// flushed to immutable sorted tables (SSTables) in level 0, and a background
// goroutine merges tables down through levels 1..6 with leveled compaction.
// Reads check the memtables, then level 0 newest first, then one table per
// deeper level, using per-table Bloom filters to skip most disk reads.
package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const numLevels = 7

// Options tunes the engine. Zero values pick the defaults in brackets.
type Options struct {
	MemtableSize       int   // flush threshold in bytes [4 MiB]
	L0CompactTrigger   int   // L0 table count that starts a compaction [4]
	L0StopWrites       int   // L0 table count that stalls writers [12]
	BaseLevelSize      int64 // target size of L1 [10 MiB]; each level is 10x the previous
	TargetFileSize     int64 // SSTable size produced by compaction [2 MiB]
	BlockCacheSize     int   // [32 MiB]
	SyncWrites         bool  // fsync the WAL on every write
	DisableCompactions bool  // for tests and benchmarks
}

func (o *Options) defaults() {
	if o.MemtableSize == 0 {
		o.MemtableSize = 4 << 20
	}
	if o.L0CompactTrigger == 0 {
		o.L0CompactTrigger = 4
	}
	if o.L0StopWrites == 0 {
		o.L0StopWrites = 12
	}
	if o.BaseLevelSize == 0 {
		o.BaseLevelSize = 10 << 20
	}
	if o.TargetFileSize == 0 {
		o.TargetFileSize = 2 << 20
	}
	if o.BlockCacheSize == 0 {
		o.BlockCacheSize = 32 << 20
	}
}

// ErrNotFound is returned by Get for missing or deleted keys.
var ErrNotFound = errors.New("not found")

// ErrClosed is returned after Close.
var ErrClosed = errors.New("db closed")

// DB is safe for concurrent use.
type DB struct {
	dir   string
	opts  Options
	cache *blockCache

	mu        sync.Mutex
	cond      *sync.Cond
	mem       *skiplist
	imm       *skiplist
	wal       *walWriter
	walNum    uint64
	immLog    uint64 // log backing imm; must survive until imm is flushed
	nextFile  uint64
	lastSeq   uint64
	levels    [numLevels][]*table
	snapshots map[*Snapshot]struct{}
	compactAt [numLevels][]byte // round-robin cursor per level
	bgErr     error
	closed    bool
	bgDone    chan struct{}
	stats     Stats
}

// Stats are cumulative counters since Open.
type Stats struct {
	UserBytes       int64 // bytes of keys+values passed to Put/Delete
	FlushBytes      int64 // bytes written by memtable flushes
	CompactionBytes int64 // bytes written by compactions
	Flushes         int
	Compactions     int
	WriteStalls     int
	CacheHits       uint64
	CacheMisses     uint64
	LevelFiles      [numLevels]int
	LevelBytes      [numLevels]int64
}

// WriteAmplification is bytes written to disk tables per byte written by the user.
func (s Stats) WriteAmplification() float64 {
	if s.UserBytes == 0 {
		return 0
	}
	return float64(s.FlushBytes+s.CompactionBytes) / float64(s.UserBytes)
}

type manifest struct {
	NextFile uint64     `json:"next_file"`
	LastSeq  uint64     `json:"last_seq"`
	LogNum   uint64     `json:"log_num"`
	Levels   [][]uint64 `json:"levels"`
}

func (d *DB) path(num uint64, ext string) string {
	return filepath.Join(d.dir, fmt.Sprintf("%06d.%s", num, ext))
}

// Open opens or creates a database in dir, replaying any write-ahead log left
// by a previous run (including one that crashed).
func Open(dir string, opts Options) (*DB, error) {
	opts.defaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	d := &DB{dir: dir, opts: opts, cache: newBlockCache(opts.BlockCacheSize), mem: newSkiplist(),
		snapshots: map[*Snapshot]struct{}{}, nextFile: 1, bgDone: make(chan struct{})}
	d.cond = sync.NewCond(&d.mu)

	var m manifest
	raw, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	switch {
	case err == nil:
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("manifest: %w", err)
		}
	case !os.IsNotExist(err):
		return nil, err
	}
	d.nextFile = max(m.NextFile, 1)
	d.lastSeq = m.LastSeq
	live := map[uint64]bool{}
	for lvl, nums := range m.Levels {
		for _, n := range nums {
			t, err := openTable(d.path(n, "sst"), n, d.cache)
			if err != nil {
				d.closeTables()
				return nil, err
			}
			d.levels[lvl] = append(d.levels[lvl], t)
			live[n] = true
		}
	}

	// Replay logs newer than the last flush, oldest first, into one memtable.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var logs []uint64
	for _, e := range entries {
		name := e.Name()
		num, ext, ok := parseName(name)
		if !ok {
			continue
		}
		switch {
		case ext == "log" && num >= m.LogNum:
			logs = append(logs, num)
		case ext == "log":
			os.Remove(filepath.Join(dir, name))
		case ext == "sst" && !live[num]:
			os.Remove(filepath.Join(dir, name)) // output of an interrupted compaction
		}
		d.nextFile = max(d.nextFile, num+1)
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i] < logs[j] })
	for _, n := range logs {
		err := replayWAL(d.path(n, "log"), func(b *Batch, seq uint64) error {
			for i, op := range b.ops {
				d.mem.insert(ikey{op.key, seq + uint64(i), op.kind}, op.value)
			}
			d.lastSeq = max(d.lastSeq, seq+uint64(len(b.ops))-1)
			return nil
		})
		if err != nil {
			d.closeTables()
			return nil, err
		}
	}

	// Persist whatever the logs held as a level-0 table, then start a fresh log.
	if d.mem.count.Load() > 0 {
		num := d.nextFile
		d.nextFile++
		t, n, err := d.writeMemtable(d.mem, num)
		if err != nil {
			d.closeTables()
			return nil, err
		}
		d.levels[0] = append([]*table{t}, d.levels[0]...)
		d.stats.FlushBytes += int64(n)
		d.mem = newSkiplist()
	}
	d.walNum = d.nextFile
	d.nextFile++
	if d.wal, err = createWAL(d.path(d.walNum, "log"), opts.SyncWrites); err != nil {
		d.closeTables()
		return nil, err
	}
	if err := d.writeManifest(); err != nil {
		d.closeTables()
		return nil, err
	}
	for _, n := range logs {
		os.Remove(d.path(n, "log"))
	}
	go d.background()
	return d, nil
}

func parseName(name string) (uint64, string, bool) {
	base, ext, ok := strings.Cut(name, ".")
	if !ok {
		return 0, "", false
	}
	n, err := strconv.ParseUint(base, 10, 64)
	return n, ext, err == nil
}

// writeManifest atomically replaces MANIFEST. Callers hold d.mu (or are in Open).
func (d *DB) writeManifest() error {
	logNum := d.walNum
	if d.imm != nil {
		logNum = d.immLog
	}
	m := manifest{NextFile: d.nextFile, LastSeq: d.lastSeq, LogNum: logNum, Levels: make([][]uint64, numLevels)}
	for i, lvl := range d.levels {
		m.Levels[i] = []uint64{}
		for _, t := range lvl {
			m.Levels[i] = append(m.Levels[i], t.num)
		}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(d.dir, "MANIFEST.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(d.dir, "MANIFEST"))
}

func (d *DB) closeTables() {
	for _, lvl := range d.levels {
		for _, t := range lvl {
			t.unref()
		}
	}
}

// Put sets key to value.
func (d *DB) Put(key, value []byte) error {
	var b Batch
	b.Put(key, value)
	return d.Write(&b)
}

// Delete removes key. Deleting a missing key is not an error.
func (d *DB) Delete(key []byte) error {
	var b Batch
	b.Delete(key)
	return d.Write(&b)
}

// Write applies a batch atomically: after a crash either all of it or none of it is visible.
func (d *DB) Write(b *Batch) error {
	if b.Len() == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.makeRoomForWrite(false); err != nil {
		return err
	}
	seq := d.lastSeq + 1
	if err := d.wal.append(b.encode(seq)); err != nil {
		return err
	}
	for i, op := range b.ops {
		d.mem.insert(ikey{op.key, seq + uint64(i), op.kind}, op.value)
		d.stats.UserBytes += int64(len(op.key) + len(op.value))
	}
	d.lastSeq = seq + uint64(len(b.ops)) - 1
	return nil
}

// makeRoomForWrite switches to a new memtable when the current one is full (or
// force is set and it is non-empty), stalling while the previous one is still
// being flushed or level 0 is backed up.
func (d *DB) makeRoomForWrite(force bool) error {
	stalled := false
	for {
		switch {
		case d.closed:
			return ErrClosed
		case d.bgErr != nil:
			return d.bgErr
		case len(d.levels[0]) >= d.opts.L0StopWrites && !d.opts.DisableCompactions:
			if !stalled {
				d.stats.WriteStalls++
				stalled = true
			}
			d.cond.Wait()
		case force && d.mem.count.Load() == 0:
			return nil
		case !force && d.mem.size.Load() < int64(d.opts.MemtableSize):
			return nil
		case d.imm != nil:
			if !stalled {
				d.stats.WriteStalls++
				stalled = true
			}
			d.cond.Wait()
		default:
			num := d.nextFile
			d.nextFile++
			w, err := createWAL(d.path(num, "log"), d.opts.SyncWrites)
			if err != nil {
				return err
			}
			if err := d.wal.close(); err != nil {
				w.close()
				return err
			}
			d.immLog = d.walNum
			d.wal, d.walNum = w, num
			d.imm, d.mem = d.mem, newSkiplist()
			force = false
			d.cond.Broadcast()
		}
	}
}

// readState pins everything a read needs so it can proceed without the lock.
type readState struct {
	mem, imm *skiplist
	levels   [numLevels][]*table
	seq      uint64
}

func (d *DB) pin(seq uint64) (*readState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrClosed
	}
	rs := &readState{mem: d.mem, imm: d.imm, levels: d.levels, seq: seq}
	if seq == 0 {
		rs.seq = d.lastSeq
	}
	for _, lvl := range d.levels {
		for _, t := range lvl {
			t.ref()
		}
	}
	return rs, nil
}

func (rs *readState) release() {
	for _, lvl := range rs.levels {
		for _, t := range lvl {
			t.unref()
		}
	}
}

// Get returns the value of key, or ErrNotFound.
func (d *DB) Get(key []byte) ([]byte, error) { return d.get(key, 0) }

func (d *DB) get(key []byte, seq uint64) ([]byte, error) {
	rs, err := d.pin(seq)
	if err != nil {
		return nil, err
	}
	defer rs.release()
	result := func(v []byte, kind byte) ([]byte, error) {
		if kind == kindDelete {
			return nil, ErrNotFound
		}
		return cloneBytes(v), nil
	}
	for _, m := range []*skiplist{rs.mem, rs.imm} {
		if m == nil {
			continue
		}
		if v, kind, ok := m.get(key, rs.seq); ok {
			return result(v, kind)
		}
	}
	// Level 0 tables may overlap, so check all that cover key, newest first.
	for _, t := range rs.levels[0] {
		if string(key) < string(t.smallest.user) || string(key) > string(t.largest.user) {
			continue
		}
		v, kind, ok, err := t.get(key, rs.seq)
		if err != nil {
			return nil, err
		}
		if ok {
			return result(v, kind)
		}
	}
	// Deeper levels are sorted and non-overlapping: at most one candidate each.
	for lvl := 1; lvl < numLevels; lvl++ {
		ts := rs.levels[lvl]
		i := sort.Search(len(ts), func(i int) bool { return string(ts[i].largest.user) >= string(key) })
		if i == len(ts) || string(key) < string(ts[i].smallest.user) {
			continue
		}
		v, kind, ok, err := ts[i].get(key, rs.seq)
		if err != nil {
			return nil, err
		}
		if ok {
			return result(v, kind)
		}
	}
	return nil, ErrNotFound
}

// Scan returns an iterator over live keys in [start, end). A nil end means no
// upper bound. The caller must Close it.
func (d *DB) Scan(start, end []byte) (*Iterator, error) { return d.scan(start, end, 0) }

func (d *DB) scan(start, end []byte, seq uint64) (*Iterator, error) {
	rs, err := d.pin(seq)
	if err != nil {
		return nil, err
	}
	var its []internalIter
	for _, m := range []*skiplist{rs.mem, rs.imm} {
		if m != nil {
			its = append(its, m.iter())
		}
	}
	for _, t := range rs.levels[0] {
		its = append(its, t.iter())
	}
	for lvl := 1; lvl < numLevels; lvl++ {
		if len(rs.levels[lvl]) > 0 {
			its = append(its, newLevelIter(rs.levels[lvl]))
		}
	}
	it := &Iterator{m: newMergeIter(its), seq: rs.seq, release: rs.release, end: cloneBytes(end)}
	it.skipTo(ikey{user: start, seq: maxSeq})
	return it, nil
}

// Snapshot is a consistent, read-only view of the database at a point in time.
type Snapshot struct {
	d   *DB
	seq uint64
}

// NewSnapshot captures the current state. Old versions it can see are kept
// through compaction until it is released.
func (d *DB) NewSnapshot() *Snapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := &Snapshot{d: d, seq: d.lastSeq}
	d.snapshots[s] = struct{}{}
	return s
}

func (s *Snapshot) Get(key []byte) ([]byte, error) { return s.d.get(key, s.seq) }

func (s *Snapshot) Scan(start, end []byte) (*Iterator, error) { return s.d.scan(start, end, s.seq) }

func (s *Snapshot) Release() {
	s.d.mu.Lock()
	delete(s.d.snapshots, s)
	s.d.mu.Unlock()
}

func (d *DB) smallestSnapshot() uint64 {
	seq := d.lastSeq
	for s := range d.snapshots {
		seq = min(seq, s.seq)
	}
	return seq
}

// Stats returns a copy of the counters and the current shape of the tree.
func (d *DB) Stats() Stats {
	d.mu.Lock()
	s := d.stats
	for i, lvl := range d.levels {
		s.LevelFiles[i] = len(lvl)
		for _, t := range lvl {
			s.LevelBytes[i] += int64(t.size)
		}
	}
	d.mu.Unlock()
	s.CacheHits, s.CacheMisses = d.cache.stats()
	return s
}

// Flush forces the current memtable to disk and waits for it.
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.imm != nil && d.bgErr == nil {
		d.cond.Wait()
	}
	if d.mem.count.Load() == 0 || d.bgErr != nil {
		return d.bgErr
	}
	if err := d.makeRoomForWrite(true); err != nil {
		return err
	}
	for d.imm != nil && d.bgErr == nil {
		d.cond.Wait()
	}
	return d.bgErr
}

// WaitForCompactions blocks until no compaction is pending.
func (d *DB) WaitForCompactions() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for d.bgErr == nil && (d.imm != nil || (!d.opts.DisableCompactions && d.pickCompaction() != nil)) {
		d.cond.Wait()
	}
	return d.bgErr
}

// Close waits for background work to stop and releases all files. Data in the
// memtable is safe in the write-ahead log and is replayed on the next Open.
func (d *DB) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.cond.Broadcast()
	d.mu.Unlock()
	<-d.bgDone
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.wal.close()
	d.closeTables()
	return err
}
