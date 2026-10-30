package db

import "bytes"

// Every record carries the user key, a sequence number assigned at write time,
// and a kind. Ordering is by user key ascending, then sequence descending, so the
// newest version of a key is always met first while iterating.

const (
	kindDelete byte = 0
	kindSet    byte = 1
)

// maxSeq is used as the sequence of a seek key to land on the newest version.
const maxSeq = 1<<56 - 1

type ikey struct {
	user []byte
	seq  uint64
	kind byte
}

func compareIKey(a, b ikey) int {
	if c := bytes.Compare(a.user, b.user); c != 0 {
		return c
	}
	switch {
	case a.seq > b.seq:
		return -1
	case a.seq < b.seq:
		return 1
	}
	return 0
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
