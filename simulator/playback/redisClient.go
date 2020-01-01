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

type RedisClient struct {
	client *redis.Client
	addr   string
	log    logger.ILogger
}

func newSession(addr string) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
	})
	return client
}

func NewRedisClient(addr string) *RedisClient {
	//client := newSession(addr)
	return &RedisClient{
		addr: addr,
		log: &logger.ColorLogger{
			Verbose: true,
			Level:   logger.LOG_LEVEL_ALL,
			Color:   true,
			Prefix:  "RedisClient ",
		},
	}
}

func (r *RedisClient) EcSet(key string, val []byte, args ...interface{}) (string, bool) {
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
	// create new session
	r.client = newSession(r.addr)
	defer r.client.Close()
	err := r.client.Set(key, val, 0).Err()
	if err != nil {
		r.log.Error("failed to SET file: %v", err)
		return reqId, false
	}
	r.log.Info("%sSet %s %d", mark, key, int64(time.Since(start)))
	return reqId, true
}

func (r *RedisClient) EcGet(key string, size int, args ...interface{}) (string, io.ReadCloser, bool) {
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
	// create new session
	r.client = newSession(r.addr)
	val, err := r.client.Get(key).Bytes()
	if err != nil {
		r.log.Error("failed to GET file: %v", err)
		return reqId, nil, false
	}
	r.log.Info("Get %s %d", key, int64(time.Since(start)))
	return reqId, ioutil.NopCloser(bytes.NewReader(val)), true
}
