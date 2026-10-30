package db

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// bloom is a standard Bloom filter using double hashing (Kirsch & Mitzenmacher):
// probe i is h1 + i*h2, which behaves like k independent hashes.
type bloom struct {
	bits []byte
	k    uint32
}

func newBloom(n int, bitsPerKey int) *bloom {
	if n < 1 {
		n = 1
	}
	m := n * bitsPerKey
	if m < 64 {
		m = 64
	}
	k := uint32(math.Round(float64(bitsPerKey) * math.Ln2))
	k = max(1, min(k, 30))
	return &bloom{bits: make([]byte, (m+7)/8), k: k}
}

func hashes(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	s := h.Sum64()
	return uint32(s), uint32(s>>32) | 1
}

func (b *bloom) add(key []byte) {
	h1, h2 := hashes(key)
	m := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		p := (h1 + i*h2) % m
		b.bits[p/8] |= 1 << (p % 8)
	}
}

func (b *bloom) mayContain(key []byte) bool {
	h1, h2 := hashes(key)
	m := uint32(len(b.bits) * 8)
	for i := uint32(0); i < b.k; i++ {
		p := (h1 + i*h2) % m
		if b.bits[p/8]&(1<<(p%8)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloom) encode() []byte {
	return binary.LittleEndian.AppendUint32(append([]byte(nil), b.bits...), b.k)
}

func decodeBloom(p []byte) *bloom {
	if len(p) < 5 {
		return nil
	}
	return &bloom{bits: p[:len(p)-4], k: binary.LittleEndian.Uint32(p[len(p)-4:])}
}
