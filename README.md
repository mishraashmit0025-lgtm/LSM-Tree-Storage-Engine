# LSM-Tree Storage Engine

A log-structured merge-tree key-value store written from scratch in Go, in the same family as LevelDB and RocksDB. It has no dependencies outside the standard library. A small server speaks the Redis protocol, so `redis-cli` and existing Redis client libraries work against it.

## Design

```
 Put/Delete ──> WAL (crc per record) ──> memtable (lock-free skiplist)
                                              │ full (4 MiB)
                                              v
                                     immutable memtable ──flush──> L0 SSTables (may overlap)
                                                                       │ 4 files
                                                                       v  leveled compaction
                                                                  L1 (10 MiB) ──> L2 (100 MiB) ──> ... L6
```

- **Write-ahead log.** Every write batch is one checksummed record. On restart the log is replayed; a torn record at the tail (crash in the middle of a write) is discarded, so a batch is either fully applied or not at all.
- **Memtable.** An insert-only skiplist. Writers are serialized, and readers never take a lock: each node is built fully and then published with an atomic pointer store.
- **SSTables.** 4 KiB data blocks, each with a CRC-32C, followed by a block index, a Bloom filter (10 bits per key, about 1% false positives) and a fixed footer. Blocks go through an LRU block cache.
- **MVCC.** Every record carries a sequence number. Snapshots read the database as of a fixed sequence, and compaction keeps any old version a live snapshot can still see.
- **Leveled compaction.** A background goroutine scores each level (L0 by file count, deeper levels by size against a 10x size ratio), picks the worst one, merges a table with the overlapping tables below it, and drops shadowed versions and tombstones that are no longer needed. A user key's versions are never split across two output files, which keeps every level from L1 down non-overlapping. A test checks that invariant.
- **Crash-safe metadata.** The set of live tables is written to a MANIFEST through write-to-temp plus rename. Tables are reference counted, so a compaction can retire a file while an older iterator is still reading it.
- **Backpressure.** Writers stall when level 0 piles up or a flush is still running, and the stall count shows up in the stats.

## Benchmarks

`lsmkv bench -n 1000000` on a laptop (Windows 11, NVMe SSD), 16-byte keys and 100-byte values, WAL without fsync per write:

| Workload | Result |
|---|---|
| 1M random-order writes | 110k ops/s (9.1 s; 12.2 s until flushes and compactions settle) |
| 200k random reads, all hits | 62k ops/s, about 16 µs average |
| 200k random reads, all misses | 767k ops/s (Bloom filters skip the disk) |
| Full scan of 1M keys | 1.6M keys/s |
| Write amplification | 5.6x |

After the run the tree was L0: 2 files, L1: 4 files (8 MiB), L2: 49 files (98 MiB), L3: 6 files.

## Use it as a library

```go
d, err := db.Open("./data", db.Options{})
defer d.Close()

d.Put([]byte("user:1"), []byte("alice"))
v, err := d.Get([]byte("user:1"))   // db.ErrNotFound if missing

var b db.Batch                       // atomic multi-key write
b.Put([]byte("a"), []byte("1"))
b.Delete([]byte("b"))
d.Write(&b)

snap := d.NewSnapshot()              // consistent point-in-time view
defer snap.Release()

it, _ := d.Scan([]byte("user:"), []byte("user;"))
for ; it.Valid(); it.Next() {
    fmt.Printf("%s=%s\n", it.Key(), it.Value())
}
it.Close()
```

## Use it as a server

```bash
go build -o lsmkv ./cmd/lsmkv
./lsmkv serve -dir ./data -addr 127.0.0.1:6380

redis-cli -p 6380 SET user:1 alice
redis-cli -p 6380 GET user:1
redis-cli -p 6380 MSET a 1 b 2
redis-cli -p 6380 SCAN user: "" 10     # keys in [start, end), limit
redis-cli -p 6380 INFO                 # levels, compactions, write amplification
```

Supported commands: PING, GET, SET, MSET (atomic), DEL, SCAN, INFO. Pipelined requests are answered with one flush.

## Tests

```bash
go test ./...
```

- A randomized model test runs puts, deletes and batches through many flushes, compactions and reopens and compares every key against a Go map.
- An invariant test checks that levels 1 and below never contain overlapping tables.
- A snapshot test checks that a snapshot keeps seeing old values and deleted keys after heavy compaction.
- A crash test cuts the last WAL record short and checks that earlier writes survive and the torn batch is not half applied.
- A corruption test flips bits in a data block and checks that reads fail loudly instead of returning wrong data.
- A concurrency test runs readers while compactions rewrite the tree underneath them.
- A Bloom filter test checks the false positive rate.

## Layout

```
db/key.go         internal key ordering (user key asc, sequence desc)
db/wal.go         write-ahead log and write batches
db/skiplist.go    memtable
db/sstable.go     table writer, reader, iterator
db/bloom.go       Bloom filter
db/cache.go       LRU block cache
db/iterator.go    merging iterator and the public Iterator
db/compaction.go  flush, compaction picking, merging
db/db.go          Open, Get/Put/Delete/Write, Scan, snapshots, recovery
cmd/lsmkv         RESP server and benchmark
```

## Not done yet

No group commit (each write takes the lock while it appends to the WAL), no block compression, no range tombstones, and a single background thread for both flushes and compactions.
