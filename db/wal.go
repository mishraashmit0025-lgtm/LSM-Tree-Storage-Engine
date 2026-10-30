package db

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// WAL record layout:
//
//	crc32c(payload) u32 | len(payload) u32 | payload
//
// payload is one write batch: first seq u64, count uvarint, then per entry
// kind u8, key (uvarint length + bytes), value (uvarint length + bytes).
// A batch is applied all-or-nothing on replay: a torn or corrupt tail record
// ends replay and everything before it is kept.

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type walWriter struct {
	f    *os.File
	w    *bufio.Writer
	sync bool
}

func createWAL(path string, sync bool) (*walWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &walWriter{f: f, w: bufio.NewWriterSize(f, 64<<10), sync: sync}, nil
}

func (l *walWriter) append(payload []byte) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], crc32.Checksum(payload, castagnoli))
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(len(payload)))
	if _, err := l.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := l.w.Write(payload); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if l.sync {
		return l.f.Sync()
	}
	return nil
}

func (l *walWriter) close() error {
	if err := l.w.Flush(); err != nil {
		l.f.Close()
		return err
	}
	if err := l.f.Sync(); err != nil {
		l.f.Close()
		return err
	}
	return l.f.Close()
}

// replayWAL calls fn for every intact batch in the log.
func replayWAL(path string, fn func(b *Batch, firstSeq uint64) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64<<10)
	var hdr [8]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil // clean end or torn header
		}
		sum := binary.LittleEndian.Uint32(hdr[0:4])
		n := binary.LittleEndian.Uint32(hdr[4:8])
		if n > 1<<30 {
			return nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil // torn write at the tail
		}
		if crc32.Checksum(payload, castagnoli) != sum {
			return nil
		}
		b, seq, err := decodeBatch(payload)
		if err != nil {
			return nil
		}
		if err := fn(b, seq); err != nil {
			return err
		}
	}
}

// Batch groups writes that become visible atomically.
type Batch struct {
	ops []batchOp
}

type batchOp struct {
	kind  byte
	key   []byte
	value []byte
}

func (b *Batch) Put(key, value []byte) {
	b.ops = append(b.ops, batchOp{kindSet, cloneBytes(key), cloneBytes(value)})
}

func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{kindDelete, cloneBytes(key), nil})
}

func (b *Batch) Len() int { return len(b.ops) }

func (b *Batch) encode(firstSeq uint64) []byte {
	buf := binary.LittleEndian.AppendUint64(nil, firstSeq)
	buf = binary.AppendUvarint(buf, uint64(len(b.ops)))
	for _, op := range b.ops {
		buf = append(buf, op.kind)
		buf = binary.AppendUvarint(buf, uint64(len(op.key)))
		buf = append(buf, op.key...)
		buf = binary.AppendUvarint(buf, uint64(len(op.value)))
		buf = append(buf, op.value...)
	}
	return buf
}

var errBadBatch = errors.New("corrupt batch")

func decodeBatch(p []byte) (*Batch, uint64, error) {
	if len(p) < 8 {
		return nil, 0, errBadBatch
	}
	seq := binary.LittleEndian.Uint64(p)
	p = p[8:]
	count, n := binary.Uvarint(p)
	if n <= 0 {
		return nil, 0, errBadBatch
	}
	p = p[n:]
	b := &Batch{}
	readBytes := func() ([]byte, bool) {
		l, n := binary.Uvarint(p)
		if n <= 0 || uint64(len(p)-n) < l {
			return nil, false
		}
		out := p[n : n+int(l)]
		p = p[n+int(l):]
		return out, true
	}
	for i := uint64(0); i < count; i++ {
		if len(p) < 1 {
			return nil, 0, errBadBatch
		}
		kind := p[0]
		p = p[1:]
		k, ok1 := readBytes()
		v, ok2 := readBytes()
		if !ok1 || !ok2 {
			return nil, 0, errBadBatch
		}
		b.ops = append(b.ops, batchOp{kind, k, v})
	}
	return b, seq, nil
}
