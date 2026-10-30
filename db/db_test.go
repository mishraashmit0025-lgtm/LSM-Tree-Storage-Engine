package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// small settings so a few thousand writes exercise flushes and several levels
func smallOpts() Options {
	return Options{MemtableSize: 16 << 10, L0CompactTrigger: 2, L0StopWrites: 8,
		BaseLevelSize: 64 << 10, TargetFileSize: 16 << 10, BlockCacheSize: 1 << 20}
}

func mustOpen(t *testing.T, dir string, o Options) *DB {
	t.Helper()
	d, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func key(i int) []byte { return []byte(fmt.Sprintf("key%06d", i)) }

func TestPutGetDelete(t *testing.T) {
	d := mustOpen(t, t.TempDir(), Options{})
	defer d.Close()
	if err := d.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if v, err := d.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("get a = %q, %v", v, err)
	}
	d.Put([]byte("a"), []byte("2"))
	if v, _ := d.Get([]byte("a")); string(v) != "2" {
		t.Fatalf("overwrite: got %q", v)
	}
	d.Delete([]byte("a"))
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if _, err := d.Get([]byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func scanAll(t *testing.T, it *Iterator) map[string]string {
	t.Helper()
	defer it.Close()
	out := map[string]string{}
	var prev []byte
	for ; it.Valid(); it.Next() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("scan out of order: %q then %q", prev, it.Key())
		}
		prev = cloneBytes(it.Key())
		out[string(it.Key())] = string(it.Value())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func check(t *testing.T, d *DB, model map[string]string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := key(i)
		v, err := d.Get(k)
		want, ok := model[string(k)]
		switch {
		case ok && (err != nil || string(v) != want):
			t.Fatalf("get %s = %q, %v; want %q", k, v, err, want)
		case !ok && !errors.Is(err, ErrNotFound):
			t.Fatalf("get %s = %q, %v; want not found", k, v, err)
		}
	}
	it, err := d.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := scanAll(t, it)
	if len(got) != len(model) {
		t.Fatalf("scan has %d keys, model %d", len(got), len(model))
	}
	for k, v := range model {
		if got[k] != v {
			t.Fatalf("scan %s = %q want %q", k, got[k], v)
		}
	}
}

// TestRandomAgainstModel drives random puts, deletes and batches through
// flushes, compactions and reopens, comparing against a plain map.
func TestRandomAgainstModel(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, smallOpts())
	model := map[string]string{}
	r := rand.New(rand.NewPCG(1, 2))
	const keys = 2000
	for round := 0; round < 6; round++ {
		for i := 0; i < 4000; i++ {
			k := key(r.IntN(keys))
			switch r.IntN(10) {
			case 0, 1:
				if err := d.Delete(k); err != nil {
					t.Fatal(err)
				}
				delete(model, string(k))
			case 2:
				var b Batch
				for j := 0; j < 5; j++ {
					bk := key(r.IntN(keys))
					v := fmt.Sprintf("b%d-%d", round, i)
					b.Put(bk, []byte(v))
					model[string(bk)] = v
				}
				if err := d.Write(&b); err != nil {
					t.Fatal(err)
				}
			default:
				v := fmt.Sprintf("v%d-%d-%s", round, i, bytes.Repeat([]byte("x"), r.IntN(64)))
				if err := d.Put(k, []byte(v)); err != nil {
					t.Fatal(err)
				}
				model[string(k)] = v
			}
		}
		check(t, d, model, keys)
		if err := d.WaitForCompactions(); err != nil {
			t.Fatal(err)
		}
		check(t, d, model, keys)
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		d = mustOpen(t, dir, smallOpts())
		check(t, d, model, keys)
	}
	st := d.Stats()
	deep := 0
	for lvl := 1; lvl < numLevels; lvl++ {
		deep += st.LevelFiles[lvl]
	}
	if st.Compactions == 0 && deep == 0 {
		t.Fatalf("expected compactions to run, stats %+v", st)
	}
	d.Close()
}

func TestLevelsStayNonOverlapping(t *testing.T) {
	d := mustOpen(t, t.TempDir(), smallOpts())
	defer d.Close()
	r := rand.New(rand.NewPCG(3, 4))
	for i := 0; i < 30000; i++ {
		d.Put(key(r.IntN(5000)), bytes.Repeat([]byte("v"), 40))
	}
	if err := d.WaitForCompactions(); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for lvl := 1; lvl < numLevels; lvl++ {
		ts := d.levels[lvl]
		for i := 1; i < len(ts); i++ {
			if string(ts[i-1].largest.user) >= string(ts[i].smallest.user) {
				t.Fatalf("level %d tables %d and %d overlap: %q >= %q", lvl, i-1, i, ts[i-1].largest.user, ts[i].smallest.user)
			}
		}
	}
}

func TestSnapshotIsolation(t *testing.T) {
	d := mustOpen(t, t.TempDir(), smallOpts())
	defer d.Close()
	for i := 0; i < 500; i++ {
		d.Put(key(i), []byte("old"))
	}
	snap := d.NewSnapshot()
	for i := 0; i < 500; i++ {
		if i%2 == 0 {
			d.Delete(key(i))
		} else {
			d.Put(key(i), []byte("new"))
		}
	}
	d.Put(key(9999), []byte("new"))
	// push everything through flushes and compactions; the snapshot must survive it
	for i := 0; i < 20000; i++ {
		d.Put(key(1000+i%3000), bytes.Repeat([]byte("f"), 30))
	}
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.WaitForCompactions(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		v, err := snap.Get(key(i))
		if err != nil || string(v) != "old" {
			t.Fatalf("snapshot get %d = %q, %v", i, v, err)
		}
	}
	if _, err := snap.Get(key(9999)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot sees a later write: %v", err)
	}
	it, _ := snap.Scan(key(0), key(500))
	if got := scanAll(t, it); len(got) != 500 {
		t.Fatalf("snapshot scan saw %d keys, want 500", len(got))
	}
	snap.Release()
	if _, err := d.Get(key(0)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("live view: key 0 should be deleted, got %v", err)
	}
	if v, _ := d.Get(key(1)); string(v) != "new" {
		t.Fatalf("live view: key 1 = %q", v)
	}
}

func TestScanRange(t *testing.T) {
	d := mustOpen(t, t.TempDir(), smallOpts())
	defer d.Close()
	for i := 0; i < 3000; i++ {
		d.Put(key(i), key(i))
	}
	for i := 0; i < 3000; i += 3 {
		d.Delete(key(i))
	}
	d.Flush()
	it, err := d.Scan(key(100), key(200))
	if err != nil {
		t.Fatal(err)
	}
	got := scanAll(t, it)
	var keys []string
	for k := range got {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	want := 0
	for i := 100; i < 200; i++ {
		if i%3 != 0 {
			want++
		}
	}
	if len(keys) != want || keys[0] != "key000100" || keys[len(keys)-1] != "key000199" {
		t.Fatalf("range scan got %d keys (%v..%v), want %d", len(keys), keys[0], keys[len(keys)-1], want)
	}
}

// TestTornWALTail simulates a crash in the middle of a write: the last log
// record is cut short. Everything before it must come back; the torn batch must not.
func TestTornWALTail(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{})
	for i := 0; i < 100; i++ {
		d.Put(key(i), []byte("ok"))
	}
	var b Batch
	b.Put(key(500), []byte("torn"))
	b.Put(key(501), []byte("torn"))
	d.Write(&b)
	walPath := d.path(d.walNum, "log")
	d.Close()

	// Close leaves the memtable in the log; chop a few bytes off its last record.
	st, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(walPath, st.Size()-3); err != nil {
		t.Fatal(err)
	}
	d = mustOpen(t, dir, Options{})
	defer d.Close()
	for i := 0; i < 100; i++ {
		if v, err := d.Get(key(i)); err != nil || string(v) != "ok" {
			t.Fatalf("key %d lost after crash: %q %v", i, v, err)
		}
	}
	for _, k := range []int{500, 501} {
		if _, err := d.Get(key(k)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("torn batch partially applied: key %d %v", k, err)
		}
	}
}

func TestCorruptBlockIsReported(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, Options{DisableCompactions: true})
	for i := 0; i < 2000; i++ {
		d.Put(key(i), bytes.Repeat([]byte("z"), 50))
	}
	d.Flush()
	d.Close()
	ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(ssts) == 0 {
		t.Fatal("no sstable written")
	}
	raw, _ := os.ReadFile(ssts[0])
	raw[len(raw)/2] ^= 0xff // flip bits in a data block in the middle of the table
	os.WriteFile(ssts[0], raw, 0o644)
	d = mustOpen(t, dir, Options{DisableCompactions: true, BlockCacheSize: 1})
	defer d.Close()
	it, err := d.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for ; it.Valid(); it.Next() {
		n++
	}
	err = it.Err()
	it.Close()
	if !errors.Is(err, errCorrupt) || n >= 2000 {
		t.Fatalf("expected the scan to stop with a corruption error, read %d keys, err %v", n, err)
	}
}

func TestConcurrentReadersDuringCompaction(t *testing.T) {
	d := mustOpen(t, t.TempDir(), smallOpts())
	defer d.Close()
	for i := 0; i < 2000; i++ {
		d.Put(key(i), []byte(fmt.Sprintf("v%d", i)))
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	errs := make(chan error, 8)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := r.IntN(2000)
				v, err := d.Get(key(i))
				if err != nil || string(v) != fmt.Sprintf("v%d", i) {
					errs <- fmt.Errorf("reader got %q, %v for key %d", v, err, i)
					return
				}
			}
		}(uint64(w))
	}
	// overwrite a disjoint key range hard enough to force many compactions
	for i := 0; i < 40000; i++ {
		d.Put(key(10000+i%5000), bytes.Repeat([]byte("w"), 40))
	}
	d.WaitForCompactions()
	close(stop)
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	b := newBloom(10000, 10)
	for i := 0; i < 10000; i++ {
		b.add(key(i))
	}
	for i := 0; i < 10000; i++ {
		if !b.mayContain(key(i)) {
			t.Fatalf("false negative for %d", i)
		}
	}
	fp := 0
	for i := 10000; i < 110000; i++ {
		if b.mayContain(key(i)) {
			fp++
		}
	}
	if rate := float64(fp) / 100000; rate > 0.02 {
		t.Fatalf("false positive rate %.4f, expected about 0.01 at 10 bits/key", rate)
	}
}
