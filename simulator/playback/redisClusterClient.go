package main

import (
	"bytes"
	"github.com/go-redis/redis"
	"github.com/google/uuid"
	"github.com/wangaoone/LambdaObjectstore/lib/logger"
	"io"
	"io/ioutil"
	"time"
)

type RedisClusterClient struct {
	clusterClient *redis.ClusterClient
	log           logger.ILogger
}

func newClusterSession() *redis.ClusterClient {
	clusterSlots := func() ([]redis.ClusterSlot, error) {
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
	client := redis.NewClusterClient(&redis.ClusterOptions{
		ClusterSlots:  clusterSlots,
		RouteRandomly: true,
		//Addrs: []string{
		//	"trace-0001-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
		//	"trace-0002-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
		//	"trace-0003-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
		//	"trace-0004-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
		//	"trace-0005-001.lqm2mp.0001.use1.cache.amazonaws.com:6379"},
	})
	//client.Ping()
	return client
}

func NewClusterRedisClient() *RedisClusterClient {
	clusterClient := newClusterSession()
	return &RedisClusterClient{
		clusterClient: clusterClient,
		log: &logger.ColorLogger{
			Verbose: true,
			Level:   logger.LOG_LEVEL_ALL,
			Color:   true,
			Prefix:  "RedisClusterClient ",
		},
	}
}

func (r *RedisClusterClient) EcSet(key string, val []byte, args ...interface{}) (string, bool) {
	reqId := uuid.New().String()
	// Debuging options
	var dryrun int
	if len(args) > 0 {
		dryrun, _ = args[0].(int)
	}
	if dryrun > 0 {
		return reqId, true
	}

	// set to redis
	start := time.Now()
	err := r.clusterClient.Set(key, val, 0).Err()
	if err != nil {
		r.log.Error("failed to SET file: %v", err)
		return reqId, false
	}
	r.log.Info("Set %s %d", key, int64(time.Since(start)))
	return reqId, true
}

func (r *RedisClusterClient) EcGet(key string, size int, args ...interface{}) (string, io.ReadCloser, bool) {
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
	val, err := r.clusterClient.Get(key).Bytes()
	if err != nil {
		r.log.Error("failed to GET file: %v", err)
		return reqId, nil, false
	}
	r.log.Info("Get %s %d", key, int64(time.Since(start)))
	return reqId, ioutil.NopCloser(bytes.NewReader(val)), true
}
