// Command lsmkv serves the storage engine over the Redis protocol (RESP), so
// redis-cli and any Redis client library can talk to it, and runs benchmarks.
//
//	lsmkv serve -dir ./data -addr 127.0.0.1:6380
//	lsmkv bench -dir ./bench -n 1000000
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mishraashmit0025-lgtm/LSM-Tree-Storage-Engine/db"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: lsmkv {serve|bench} [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "bench":
		err = bench(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "usage: lsmkv {serve|bench} [flags]")
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dir := fs.String("dir", "./data", "data directory")
	addr := fs.String("addr", "127.0.0.1:6380", "listen address")
	syncWrites := fs.Bool("sync", false, "fsync the log on every write")
	_ = fs.Parse(args)
	d, err := db.Open(*dir, db.Options{SyncWrites: *syncWrites})
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		d.Close()
		return err
	}
	log.Printf("lsmkv listening on %s, data in %s", *addr, *dir)
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
		<-ch
		lis.Close()
	}()
	var wg sync.WaitGroup
	for {
		conn, err := lis.Accept()
		if err != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle(conn, d)
		}()
	}
	wg.Wait()
	return d.Close()
}

// readCommand parses one RESP array of bulk strings (or an inline command).
func readCommand(r *bufio.Reader) ([][]byte, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		var out [][]byte
		for _, f := range strings.Fields(line) {
			out = append(out, []byte(f))
		}
		return out, nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 || n > 1<<20 {
		return nil, errors.New("bad array header")
	}
	out := make([][]byte, n)
	for i := range out {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if !strings.HasPrefix(hdr, "$") {
			return nil, errors.New("expected bulk string")
		}
		l, err := strconv.Atoi(hdr[1:])
		if err != nil || l < 0 || l > 512<<20 {
			return nil, errors.New("bad bulk length")
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		out[i] = buf[:l]
	}
	return out, nil
}

func handle(conn net.Conn, d *db.DB) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	bulk := func(b []byte) { fmt.Fprintf(w, "$%d\r\n%s\r\n", len(b), b) }
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		cmd := strings.ToUpper(string(args[0]))
		switch {
		case cmd == "PING":
			w.WriteString("+PONG\r\n")
		case cmd == "SET" && len(args) == 3:
			if err := d.Put(args[1], args[2]); err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else {
				w.WriteString("+OK\r\n")
			}
		case cmd == "GET" && len(args) == 2:
			v, err := d.Get(args[1])
			switch {
			case errors.Is(err, db.ErrNotFound):
				w.WriteString("$-1\r\n")
			case err != nil:
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			default:
				bulk(v)
			}
		case cmd == "DEL" && len(args) >= 2:
			var b db.Batch
			for _, k := range args[1:] {
				b.Delete(k)
			}
			if err := d.Write(&b); err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else {
				fmt.Fprintf(w, ":%d\r\n", len(args)-1)
			}
		case cmd == "MSET" && len(args) >= 3 && len(args)%2 == 1:
			var b db.Batch
			for i := 1; i < len(args); i += 2 {
				b.Put(args[i], args[i+1])
			}
			if err := d.Write(&b); err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
			} else {
				w.WriteString("+OK\r\n")
			}
		case cmd == "SCAN" && (len(args) == 3 || len(args) == 4):
			// SCAN start end [limit]: keys in [start, end), "" end means unbounded
			limit := 100
			if len(args) == 4 {
				limit, _ = strconv.Atoi(string(args[3]))
			}
			var end []byte
			if len(args[2]) > 0 {
				end = args[2]
			}
			it, err := d.Scan(args[1], end)
			if err != nil {
				fmt.Fprintf(w, "-ERR %v\r\n", err)
				break
			}
			var kv [][]byte
			for ; it.Valid() && len(kv) < 2*limit; it.Next() {
				kv = append(kv, append([]byte(nil), it.Key()...), append([]byte(nil), it.Value()...))
			}
			it.Close()
			fmt.Fprintf(w, "*%d\r\n", len(kv))
			for _, x := range kv {
				bulk(x)
			}
		case cmd == "INFO":
			s := d.Stats()
			var sb strings.Builder
			fmt.Fprintf(&sb, "flushes:%d\r\ncompactions:%d\r\nwrite_amplification:%.2f\r\nwrite_stalls:%d\r\ncache_hits:%d\r\ncache_misses:%d\r\n",
				s.Flushes, s.Compactions, s.WriteAmplification(), s.WriteStalls, s.CacheHits, s.CacheMisses)
			for i := range s.LevelFiles {
				fmt.Fprintf(&sb, "level%d:files=%d,bytes=%d\r\n", i, s.LevelFiles[i], s.LevelBytes[i])
			}
			bulk([]byte(sb.String()))
		default:
			fmt.Fprintf(w, "-ERR unknown command or wrong number of arguments for '%s'\r\n", cmd)
		}
		if r.Buffered() == 0 { // flush once per pipeline
			if w.Flush() != nil {
				return
			}
		}
	}
}

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[int(p*float64(len(ds)-1))]
}

func bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	dir := fs.String("dir", "./bench-data", "data directory (wiped first)")
	n := fs.Int("n", 1_000_000, "number of keys")
	valueSize := fs.Int("value", 100, "value size in bytes")
	reads := fs.Int("reads", 200_000, "random reads")
	_ = fs.Parse(args)
	os.RemoveAll(*dir)
	d, err := db.Open(*dir, db.Options{})
	if err != nil {
		return err
	}
	defer os.RemoveAll(*dir)
	key := func(i int) []byte { return []byte(fmt.Sprintf("user%012d", i)) }
	val := make([]byte, *valueSize)
	r := rand.New(rand.NewPCG(7, 7))

	perm := r.Perm(*n)
	t0 := time.Now()
	for _, i := range perm {
		for j := range val {
			val[j] = byte('a' + r.IntN(26))
		}
		if err := d.Put(key(i), val); err != nil {
			return err
		}
	}
	writeDur := time.Since(t0)
	if err := d.Flush(); err != nil {
		return err
	}
	if err := d.WaitForCompactions(); err != nil {
		return err
	}
	settled := time.Since(t0)
	fmt.Printf("random writes: %d keys x %d B in %v (%.0f ops/s); including flush+compaction %v\n",
		*n, *valueSize, writeDur.Round(time.Millisecond), float64(*n)/writeDur.Seconds(), settled.Round(time.Millisecond))

	lat := make([]time.Duration, 0, *reads)
	t0 = time.Now()
	for i := 0; i < *reads; i++ {
		s := time.Now()
		if _, err := d.Get(key(r.IntN(*n))); err != nil {
			return err
		}
		lat = append(lat, time.Since(s))
	}
	rd := time.Since(t0)
	fmt.Printf("random reads (hits): %d in %v (%.0f ops/s), avg %v, p99 %v\n",
		*reads, rd.Round(time.Millisecond), float64(*reads)/rd.Seconds(), rd/time.Duration(*reads), percentile(lat, 0.99))

	lat = lat[:0]
	t0 = time.Now()
	for i := 0; i < *reads; i++ {
		s := time.Now()
		if _, err := d.Get(key(*n + r.IntN(*n))); !errors.Is(err, db.ErrNotFound) {
			return fmt.Errorf("expected miss, got %v", err)
		}
		lat = append(lat, time.Since(s))
	}
	rd = time.Since(t0)
	fmt.Printf("random reads (misses, Bloom filtered): %d in %v (%.0f ops/s), avg %v, p99 %v\n",
		*reads, rd.Round(time.Millisecond), float64(*reads)/rd.Seconds(), rd/time.Duration(*reads), percentile(lat, 0.99))

	t0 = time.Now()
	it, err := d.Scan(nil, nil)
	if err != nil {
		return err
	}
	cnt := 0
	for ; it.Valid(); it.Next() {
		cnt++
	}
	if err := it.Err(); err != nil {
		return err
	}
	it.Close()
	sd := time.Since(t0)
	fmt.Printf("full scan: %d keys in %v (%.0f keys/s)\n", cnt, sd.Round(time.Millisecond), float64(cnt)/sd.Seconds())

	s := d.Stats()
	fmt.Printf("write amplification %.2f, flushes %d, compactions %d, write stalls %d\n",
		s.WriteAmplification(), s.Flushes, s.Compactions, s.WriteStalls)
	for i := range s.LevelFiles {
		if s.LevelFiles[i] > 0 {
			fmt.Printf("  L%d: %d files, %.1f MiB\n", i, s.LevelFiles[i], float64(s.LevelBytes[i])/(1<<20))
		}
	}
	return d.Close()
}
