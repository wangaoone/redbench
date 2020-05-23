package benchclient

import (
	"time"

	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
	infinicache "github.com/mason-leap-lab/infinicache/client"
)

type Client interface {
	EcSet(string, []byte, ...interface{}) (string, bool)
	EcGet(string, ...interface{}) (string, infinicache.ReadAllCloser, bool)
	Close()
}

type clientSetter func(string, []byte) error
type clientGetter func(string) (infinicache.ReadAllCloser, error)

type defaultClient struct {
	log     logger.ILogger
	setter  clientSetter
	getter  clientGetter
}

func newDefaultClient(logPrefix string) *defaultClient {
	return newDefaultClientWithAccessor(logPrefix, nil, nil)
}

func newDefaultClientWithAccessor(logPrefix string, setter clientSetter, getter clientGetter) *defaultClient {
	return &defaultClient{
		log: &logger.ColorLogger{
			Verbose: true,
			Level:   logger.LOG_LEVEL_ALL,
			Color:   true,
			Prefix:  logPrefix,
		},
		setter: setter,
		getter: getter,
	}
}

func (c *defaultClient) EcSet(key string, val []byte, args ...interface{}) (string, bool) {
	reqId := uuid.New().String()

	// Debuging options
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, true
	}

	if c.setter == nil {
		return reqId, false
	}

	// Timing
	start := time.Now()
	if err := c.setter(key, val); err != nil {
		c.log.Error("failed to upload: %v", err)
		return reqId, false
	}
	c.log.Info("Set %s %d", key, int64(time.Since(start)))
	return reqId, true
}

func (c *defaultClient) EcGet(key string, args ...interface{}) (string, infinicache.ReadAllCloser, bool) {
	reqId := uuid.New().String()

	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil, true
	}

	if c.getter == nil {
		return reqId, nil, false
	}

	// Timing
	start := time.Now()
	reader, err := c.getter(key);
	if err != nil {
		c.log.Error("failed to download: %v", err)
		return reqId, nil, false
	}
	c.log.Info("Get %s %d", key, int64(time.Since(start)))

	return reqId, reader, true
}

func (c *defaultClient) Close() {
	// Nothing
}
