package benchclient

import (
	"bytes"
	"io"
	"io/ioutil"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/mason-leap-lab/infinicache/common/logger"
)

var (
	AWSElasticCacheCluster = func() ([]redis.ClusterSlot, error) {
		slots := []redis.ClusterSlot{
			// First node with 1 master and 1 slave.
			{
				Start: 0,
				End:   3276,
				Nodes: []redis.ClusterNode{{
					Addr: "trace1.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379",
				}},
			},
			// Second node with 1 master and 1 slave.
			{
				Start: 3277,
				End:   6553,
				Nodes: []redis.ClusterNode{{
					Addr: "trace2.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379",
				}},
			},
			{
				Start: 6554,
				End:   9830,
				Nodes: []redis.ClusterNode{{
					Addr: "trace3.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379", // master
				}},
			},
			{
				Start: 9831,
				End:   13107,
				Nodes: []redis.ClusterNode{{
					Addr: "trace4.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379", // master
				}},
			},
			{
				Start: 13108,
				End:   16383,
				Nodes: []redis.ClusterNode{{
					Addr: "trace5.lqm2mp.ng.0001.use1.cache.amazonaws.com:6379", // master
				}},
			},
		}
		return slots, nil
	}
)

type Redis struct {
	backend redis.UniversalClient
	log     logger.ILogger
}

func NewRedis(addr string) *Redis {
	backend := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
	})
	return NewRedisWithBackend(backend)
}

func NewRedisWithBackend(backend redis.UniversalClient) *Redis {
	//client := newSession(addr)
	return &Redis{
		backend: backend,
		log: &logger.ColorLogger{
			Verbose: true,
			Level:   logger.LOG_LEVEL_ALL,
			Color:   true,
			Prefix:  "Redis: ",
		},
	}
}

func NewElasticCache() *Redis {
	return NewRedisWithBackend(redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots:  AWSElasticCacheCluster,
		RouteRandomly: true,
	}))
}

func (r *Redis) EcSet(key string, val []byte, args ...interface{}) (string, bool) {
	reqId := uuid.New().String()
	// Debuging options
	var dryrun int
	var mark string
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
		mark, _ = args[2].(string)

	}
	if dryrun > 0 {
		return reqId, true
	}

	// set to redis
	start := time.Now()
	err := r.backend.Set(key, val, 0).Err()
	if err != nil {
		r.log.Error("failed to SET file: %v", err)
		return reqId, false
	}
	r.log.Info("%sSet %s %d", mark, key, int64(time.Since(start)))
	return reqId, true
}

func (r *Redis) EcGet(key string, size int, args ...interface{}) (string, io.ReadCloser, bool) {
	reqId := uuid.New().String()
	// Debuging options
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, nil, true
	}

	// GET from Redis
	start := time.Now()
	val, err := r.backend.Get(key).Bytes()
	if err != nil {
		r.log.Error("failed to GET file: %v", err)
		return reqId, nil, false
	}
	r.log.Info("Get %s %d", key, int64(time.Since(start)))
	return reqId, ioutil.NopCloser(bytes.NewReader(val)), true
}

func (r *Redis) Close() {
	if r.backend != nil {
		r.backend.Close()
		r.backend = nil
	}
}
