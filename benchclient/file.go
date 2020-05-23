package benchclient

import (
	"io/ioutil"
	"os"
	"path"
	infinicache "github.com/mason-leap-lab/infinicache/client"
)

type File struct {
	*defaultClient
	basePath   string
}

func NewFile(provider string, path string) *File {
	client := &File{
		defaultClient: newDefaultClient(provider + ": "),
		basePath: path,
	}
	client.setter = client.set
	client.getter = client.get
	return client
}

func (c *File) set(key string, val []byte) (err error) {
	var file *os.File
	file, err = os.OpenFile(path.Join(c.basePath, key), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	_, err = file.Write(val)
	return
}

func (c *File) get(key string) (reader infinicache.ReadAllCloser, err error) {
	var file *os.File
	if file, err = os.OpenFile(path.Join(c.basePath, key), os.O_RDONLY, 0); err != nil {
		return
	}
	defer file.Close()

	var data []byte
	if data, err = ioutil.ReadAll(file); err != nil {
		return
	}

	return NewByteReader(data), nil
}
