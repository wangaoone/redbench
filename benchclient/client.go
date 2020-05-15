package benchclient

import (
	"io"
)

type Client interface {
	EcSet(string, []byte, ...interface{}) (string, bool)
	EcGet(string, int, ...interface{}) (string, io.ReadCloser, bool)
	Close()
}
