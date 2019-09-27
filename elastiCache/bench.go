package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/wangaoone/LambdaObjectstore/lib/logger"
	"log"
	"math/rand"
	"sync"
	"time"
)

var (
	op      = flag.Int("op", 0, "operation type")
	objSize = flag.Int("size", 1048576, "size of object")
	//clientNum = flag.Int("c", 1, "client number")
	objNum = flag.Int("n", 10, "client number")
)

type RedisClusterClient struct {
	id            int
	clusterClient *redis.ClusterClient
	log           logger.ILogger
}

type Object struct {
	key   string
	value []byte
}

func newClusterSession() *redis.ClusterClient {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"trace-0001-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0002-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0003-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0004-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0005-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0006-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0007-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0008-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0009-001.lqm2mp.0001.use1.cache.amazonaws.com:6379",
			"trace-0010-001.lqm2mp.0001.use1.cache.amazonaws.com:6379"},
	})
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
		id: 1,
	}
}

//func getRandomRange(min int, max int) int {
//	var rn int
//	rand.Seed(time.Now().UnixNano())
//	rn = rand.Intn(max-min) + min
//	return rn
//}
//
//func genKey(keymin int, keymax int, op int, i int) string {
//	var ret string
//	if op == 0 { // SET
//		keyIdx := keymin + i%(keymax-keymin+1)
//		ret = strings.Join([]string{"key_", strconv.Itoa(keyIdx)}, "")
//	} else { // GET
//		rn := getRandomRange(keymin, keymax)
//		ret = strings.Join([]string{"key_", strconv.Itoa(rn)}, "")
//	}
//	log.Println("generated key: ", ret, "len: ", len(ret))
//	return ret
//}

func main() {
	flag.Parse()

	// create all clients
	//clients := make([]*RedisClusterClient, *clientNum)
	//for i := 0; i < *clientNum; i++ {
	//	clients[i] = NewClusterRedisClient()
	//	clients[i].id = i
	//}
	client := NewClusterRedisClient()

	var wg sync.WaitGroup
	tstart := time.Now()
	val := make([]byte, *objSize)
	rand.Read(val)
	switch *op {
	case 0:
		for i := 0; i < *objNum; i++ {
			wg.Add(1)
			key := fmt.Sprintf("%s_%d", "key", i)
			//log.Println("SET", key, len(val))
			go Set(client, key, val, &wg)
		}
		wg.Wait()
		//tend := time.Since(tstart)
		//log.Println("set finished, duration is ", tend)
	case 1:
		for i := 0; i < *objNum; i++ {
			wg.Add(1)
			key := fmt.Sprintf("%s_%d", "key", i)
			//log.Println("GET", key)
			go Get(client, key, &wg)
			time.Sleep(1 * time.Second)
		}
		wg.Wait()
		//tend := time.Since(tstart)
		//log.Println("Get finished, duration is ", tend)
	}
	log.Println("FINISHED", time.Since(tstart))
	err := client.clusterClient.Close()
	if err != nil {
		client.log.Error("close client err %v", err)
	}

}

func Set(client *RedisClusterClient, key string, val []byte, wg *sync.WaitGroup) {
	t := time.Now()
	res, err := client.clusterClient.Set(key, val, 0).Result()
	if err != nil {
		client.log.Error("set error %v", client.id, err)
	}
	e := time.Since(t)
	log.Println(res, client.id, "Set time duration: ", e)
	wg.Done()
}

func Get(client *RedisClusterClient, key string, wg *sync.WaitGroup) {
	t := time.Now()
	val, err := client.clusterClient.Get(key).Bytes()
	if err != nil {
		client.log.Error("set error %v", client.id, err)
	}
	e := time.Since(t)
	log.Println("Get time duration: ", e, "len of val is", len(val))
	wg.Done()
}
