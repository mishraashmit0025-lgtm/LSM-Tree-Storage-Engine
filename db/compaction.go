package db

import (
	"fmt"
	"os"
	"sort"
)

// background runs flushes and compactions one at a time. Doing everything on a
// single goroutine keeps version changes simple: only this goroutine removes
// tables, so the inputs it chose are still present when it installs the result.
func (d *DB) background() {
	defer close(d.bgDone)
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		for !d.closed && d.bgErr == nil && d.imm == nil &&
			(d.opts.DisableCompactions || d.pickCompaction() == nil) {
			d.cond.Wait()
		}
		if d.closed || d.bgErr != nil {
			d.cond.Broadcast()
			return
		}
		var err error
		if d.imm != nil {
			err = d.flushImm()
		} else if c := d.pickCompaction(); c != nil {
			err = d.compact(c)
		}
		if err != nil {
			d.bgErr = fmt.Errorf("background: %w", err)
		}
		d.cond.Broadcast()
	}
}

// writeMemtable writes every entry of m into table number num.
func (d *DB) writeMemtable(m *skiplist, num uint64) (*table, uint64, error) {
	path := d.path(num, "sst")
	w, err := newTableWriter(path)
	if err != nil {
		return nil, 0, err
	}
	it := m.iter()
	for it.First(); it.Valid(); it.Next() {
		if err := w.add(it.Key(), it.Value()); err != nil {
			w.abort()
			return nil, 0, err
		}
	}
	_, _, size, err := w.finish()
	if err != nil {
		os.Remove(path)
		return nil, 0, err
	}
	t, err := openTable(path, num, d.cache)
	return t, size, err
}

func (d *DB) flushImm() error {
	imm := d.imm
	num := d.nextFile
	d.nextFile++
	d.mu.Unlock()
	t, size, err := d.writeMemtable(imm, num)
	d.mu.Lock()
	if err != nil {
		return err
	}
	d.levels[0] = append([]*table{t}, d.levels[0]...)
	oldLog := d.immLog
	d.imm = nil
	if err := d.writeManifest(); err != nil {
		return err
	}
	os.Remove(d.path(oldLog, "log"))
	d.stats.FlushBytes += int64(size)
	d.stats.Flushes++
	return nil
}

type compaction struct {
	level  int // inputs come from level and level+1
	inputs [2][]*table
}

func (d *DB) maxBytes(level int) int64 {
	b := d.opts.BaseLevelSize
	for i := 1; i < level; i++ {
		b *= 10
	}
	return b
}

func levelSize(ts []*table) int64 {
	var n int64
	for _, t := range ts {
		n += int64(t.size)
	}
	return n
}

// pickCompaction chooses the level whose size (or L0 file count) most exceeds
// its target, or nil if every level is within bounds.
func (d *DB) pickCompaction() *compaction {
	best, bestScore := -1, 1.0
	if s := float64(len(d.levels[0])) / float64(d.opts.L0CompactTrigger); s >= bestScore {
		best, bestScore = 0, s
	}
	for lvl := 1; lvl < numLevels-1; lvl++ {
		if s := float64(levelSize(d.levels[lvl])) / float64(d.maxBytes(lvl)); s > bestScore {
			best, bestScore = lvl, s
		}
	}
	if best < 0 {
		return nil
	}
	c := &compaction{level: best}
	if best == 0 {
		c.inputs[0] = append([]*table(nil), d.levels[0]...)
	} else {
		// round-robin through the level so every key range gets compacted in turn
		ts := d.levels[best]
		i := sort.Search(len(ts), func(i int) bool { return string(ts[i].smallest.user) > string(d.compactAt[best]) })
		if i == len(ts) {
			i = 0
		}
		c.inputs[0] = []*table{ts[i]}
	}
	lo, hi := keyRange(c.inputs[0])
	c.inputs[1] = overlapping(d.levels[best+1], lo, hi)
	return c
}

func keyRange(ts []*table) (lo, hi []byte) {
	for i, t := range ts {
		if i == 0 || string(t.smallest.user) < string(lo) {
			lo = t.smallest.user
		}
		if i == 0 || string(t.largest.user) > string(hi) {
			hi = t.largest.user
		}
	}
	return lo, hi
}

func overlapping(ts []*table, lo, hi []byte) []*table {
	var out []*table
	for _, t := range ts {
		if string(t.largest.user) >= string(lo) && string(t.smallest.user) <= string(hi) {
			out = append(out, t)
		}
	}
	return out
}

// compact merges the inputs into new tables in level+1. Called with d.mu held;
// the lock is released while tables are read and written.
func (d *DB) compact(c *compaction) error {
	out := c.level + 1
	lo, hi := keyRange(append(append([]*table(nil), c.inputs[0]...), c.inputs[1]...))
	bottom := true
	for lvl := out + 1; lvl < numLevels; lvl++ {
		if len(overlapping(d.levels[lvl], lo, hi)) > 0 {
			bottom = false
		}
	}
	smallestSnap := d.smallestSnapshot()
	var its []internalIter
	for _, ts := range c.inputs {
		for _, t := range ts {
			t.ref()
			its = append(its, t.iter())
		}
	}
	d.mu.Unlock()
	outputs, written, err := d.mergeTables(newMergeIter(its), smallestSnap, bottom)
	for _, ts := range c.inputs {
		for _, t := range ts {
			t.unref()
		}
	}
	d.mu.Lock()
	if err != nil {
		for _, t := range outputs {
			t.markObsolete()
			t.unref()
		}
		return err
	}

	gone := map[*table]bool{}
	for _, ts := range c.inputs {
		for _, t := range ts {
			gone[t] = true
		}
	}
	for _, lvl := range []int{c.level, out} {
		kept := d.levels[lvl][:0:0]
		for _, t := range d.levels[lvl] {
			if !gone[t] {
				kept = append(kept, t)
			}
		}
		d.levels[lvl] = kept
	}
	d.levels[out] = append(d.levels[out], outputs...)
	sort.Slice(d.levels[out], func(i, j int) bool {
		return string(d.levels[out][i].smallest.user) < string(d.levels[out][j].smallest.user)
	})
	if c.level > 0 {
		d.compactAt[c.level] = cloneBytes(c.inputs[0][len(c.inputs[0])-1].largest.user)
	}
	if err := d.writeManifest(); err != nil {
		return err
	}
	for t := range gone {
		t.markObsolete()
		t.unref()
	}
	d.stats.CompactionBytes += written
	d.stats.Compactions++
	return nil
}

// mergeTables writes the merged stream into tables of about TargetFileSize,
// dropping versions no snapshot can see. A user key's versions are never split
// across two output tables, so the output level stays non-overlapping.
func (d *DB) mergeTables(it *mergeIter, smallestSnap uint64, bottom bool) ([]*table, int64, error) {
	defer it.Close()
	var (
		outputs []*table
		written int64
		w       *tableWriter
		num     uint64
		lastKey []byte
		haveKey bool
		lastSeq uint64
	)
	finish := func() error {
		if w == nil {
			return nil
		}
		_, _, size, err := w.finish()
		w = nil
		if err != nil {
			return err
		}
		t, err := openTable(d.path(num, "sst"), num, d.cache)
		if err != nil {
			return err
		}
		outputs = append(outputs, t)
		written += int64(size)
		return nil
	}
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		newKey := !haveKey || string(k.user) != string(lastKey)
		if newKey {
			lastKey, haveKey, lastSeq = cloneBytes(k.user), true, maxSeq
		}
		drop := false
		switch {
		case lastSeq <= smallestSnap:
			drop = true // shadowed by a newer version that every reader can already see
		case k.kind == kindDelete && k.seq <= smallestSnap && bottom:
			drop = true // nothing older exists below, so the tombstone has done its job
		}
		lastSeq = k.seq
		if drop {
			continue
		}
		if w != nil && newKey && int64(w.estimatedSize()) >= d.opts.TargetFileSize {
			if err := finish(); err != nil {
				return outputs, written, err
			}
		}
		if w == nil {
			d.mu.Lock()
			num = d.nextFile
			d.nextFile++
			d.mu.Unlock()
			var err error
			if w, err = newTableWriter(d.path(num, "sst")); err != nil {
				return outputs, written, err
			}
		}
		if err := w.add(k, it.Value()); err != nil {
			w.abort()
			return outputs, written, err
		}
	}
	if err := it.Err(); err != nil {
		if w != nil {
			w.abort()
		}
		return outputs, written, err
	}
	return outputs, written, finish()
}

// levelIter concatenates the sorted, non-overlapping tables of one level,
// opening each table's iterator only when it is reached.
type levelIter struct {
	ts  []*table
	i   int
	cur *tableIter
}

func newLevelIter(ts []*table) *levelIter { return &levelIter{ts: ts} }

func (l *levelIter) open(i int) {
	l.i, l.cur = i, nil
	if i < len(l.ts) {
		l.cur = l.ts[i].iter()
	}
}

func (l *levelIter) skipEmpty() {
	for l.cur != nil && !l.cur.Valid() && l.cur.Err() == nil {
		l.open(l.i + 1)
		if l.cur != nil {
			l.cur.First()
		}
	}
}

func (l *levelIter) First() {
	l.open(0)
	if l.cur != nil {
		l.cur.First()
	}
	l.skipEmpty()
}

func (l *levelIter) SeekGE(k ikey) {
	i := sort.Search(len(l.ts), func(i int) bool { return compareIKey(l.ts[i].largest, k) >= 0 })
	l.open(i)
	if l.cur != nil {
		l.cur.SeekGE(k)
	}
	l.skipEmpty()
}

func (l *levelIter) Valid() bool   { return l.cur != nil && l.cur.Valid() }
func (l *levelIter) Key() ikey     { return l.cur.Key() }
func (l *levelIter) Value() []byte { return l.cur.Value() }
func (l *levelIter) Close() error  { return nil }

func (l *levelIter) Next() {
	l.cur.Next()
	l.skipEmpty()
}

func (l *levelIter) Err() error {
	if l.cur != nil {
		return l.cur.Err()
	}
	return nil
}
