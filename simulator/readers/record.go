package readers

import (
	"errors"
	"sync"
)

var (
	ErrNoData = errors.New("nothing to read")
)

type Record struct {
	// Timestamp Timestamp can be relative.
	Timestamp int64

	// Method Http method in lower case.
	Method string

	// Key Object identifier
	Key string

	// Size Object size
	Size uint64

	// Start Start position of fragment object if supported
	Start uint64

	// End End position of fragment object if supported
	End uint64

	// TTL Lifetime of object
	TTL int64

	// Error Error on reading the record
	Error error
}

type RecordReader interface {
	Read() (*Record, error)
	Done(*Record)
	Report() []string
}

type BaseReader struct {
	pool *sync.Pool
}

func NewBaseReader() *BaseReader {
	return &BaseReader{
		pool: &sync.Pool{
			New: func() interface{} {
				return &Record{}
			},
		},
	}
}

func (r *BaseReader) Read() (*Record, error) {
	rec := r.pool.Get().(*Record)
	rec.Error = nil
	rec.Size = 0
	rec.Start = 0
	rec.End = 0
	rec.TTL = 0
	return rec, nil
}

func (r *BaseReader) Done(rec *Record) {
	r.pool.Put(rec)
}
