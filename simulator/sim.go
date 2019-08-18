package main

import (
	"bufio"
	"encoding/csv"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Object struct {
	Key  string
	Sz   uint64
	Freq uint64
}

type Lambda struct {
	Kvs     map[string]*Object
	MemUsed uint64
}

type Proxy struct {
	Id         string
	LambdaPool []Lambda
	Map        map[string][]int
}

type Record struct {
	Key       string
	Sz        uint64
	Timestamp string
}

type Member string

func (m Member) String() string {
	//return strconv.Atoi(m)
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// random will generate random sequence within the lambda stores
// index and get top n id
func random(numLambdas int, numChunks int) []int {
	rand.Seed(time.Now().UnixNano())
	return rand.Perm(300)[:numChunks]
}

func perform(p Proxy, rec Record) {
	log.Println("Key:", rec.Key, "mapped to Proxy:", p.Id)
	if val, ok := p.Map[rec.Key]; ok {
		// if key exists
		for _, idx := range val {
			obj := p.LambdaPool[idx].Kvs[rec.Key]
			obj.Sz = rec.Sz / 10
			obj.Freq++
		}
		log.Println("Key:", rec.Key, "existing...")
		log.Println("Hitted lambda indexes:", val)
	} else {
		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		index := random(300, 12)
		p.Map[rec.Key] = index
		for _, idx := range index {
			p.LambdaPool[idx].Kvs[rec.Key] = &Object{
				Key:  rec.Key,
				Sz:   rec.Sz / 10,
				Freq: 0,
			}
			p.LambdaPool[idx].MemUsed += rec.Sz / 10
		}
		log.Println("Key:", rec.Key, "not existing...")
		log.Println("Generating lambda indexes:", index)
	}
}

func initProxies(nProxies int, nLambdasPerProxy int) ([]Proxy, *consistent.Consistent) {
	proxies := make([]Proxy, nProxies)
	members := []consistent.Member{}
	for i, _ := range proxies {
		proxies[i].Id = strconv.Itoa(i)
		proxies[i].LambdaPool = make([]Lambda, nLambdasPerProxy)
		proxies[i].Map = make(map[string][]int)
		for j, _ := range proxies[i].LambdaPool {
			proxies[i].LambdaPool[j].Kvs = make(map[string]*Object)
			proxies[i].LambdaPool[j].MemUsed = 0
		}
		member := Member(proxies[i].Id)
		log.Println("id:", proxies[i].Id, "id:", i)
		members = append(members, member)
	}
	log.Println("members size:", len(members))

	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	ring := consistent.New(members, cfg)

	return proxies, ring
}

func main() {
	traceFile, err := os.Open("/home/ubuntu/lambda_store/docker_traces/data_centers/csv/lon02_get_10mb.csv")
	if err != nil {
		log.Fatal("Failed to open file")
		os.Exit(1)
	}
	defer traceFile.Close()

	reader := csv.NewReader(bufio.NewReader(traceFile))
	proxies, ring := initProxies(10, 300)

	var rec Record
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}
		//sz, _ := strconv.ParseUint(line[9], 10, 64)
		sz, _ := strconv.ParseFloat(line[9], 64)
		rec = Record{
			Key:       line[6],
			Sz:        uint64(sz),
			Timestamp: line[11],
		}
		//log.Println(line)
		//log.Println("parsed record:", rec)

		member := ring.LocateKey([]byte(rec.Key))
		hostId := member.String()
		id, _ := strconv.Atoi(hostId)
		//log.Println("Key:", rec.Key, "mapped to Proxy id:", id)
		perform(proxies[id], rec)
	}
}
