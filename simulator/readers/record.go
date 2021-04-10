package readers

import "errors"

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
	Report() []string
}
