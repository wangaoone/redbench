package benchclient

import (
	"bytes"
	"io"
)

type ByteReader struct {
	io.Reader

	buf []byte
}

func NewByteReader(b []byte) *ByteReader {
	return &ByteReader{
		Reader: bytes.NewReader(b),
		buf: b,
	}
}

func (r *ByteReader) Len() int { return len(r.buf) }

func (r *ByteReader) ReadAll() ([]byte, error) { return r.buf, nil }

func (r *ByteReader) Close() error { return nil }
