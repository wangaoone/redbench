package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/wangaoone/LambdaObjectstore/lib/logger"
	"github.com/wangaoone/ecRedis"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	log           = &logger.ColorLogger{
		Level: logger.LOG_LEVEL_ALL,
	}
)

type Options struct {
	AddrList       string
	Cluster        int
	Datashard      int
	Parityshard    int
	ECmaxgoroutine int
	CSV            bool
	Stdout         io.Writer
	Stderr         io.Writer
	Printlog       bool
	File           string
	Compact        bool
	Interval       int64
	Dryrun         bool
}

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
	Placements map[string][]int
}

type Record struct {
	Key       string
	Sz        uint64
	Time      time.Time
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

func perform(opts *Options, client *ecRedis.Client, p *Proxy, rec *Record) {
	// log.Debug("Key:", rec.Key, "mapped to Proxy:", p.Id)
	if placements, ok := p.Placements[rec.Key]; ok {
		// if key exists
		log.Debug("Get %s.", rec.Key)
		if !opts.Dryrun {
			reader, success := client.EcGet(rec.Key, int(rec.Sz))
			if !success {
				return
			}
			reader.Close()
		}

		for _, idx := range placements {
			obj := p.LambdaPool[idx].Kvs[rec.Key]
			obj.Freq++
		}
	} else {
		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		val := make([]byte, rec.Sz)
		rand.Read(val)
		placements := make([]int, opts.Datashard + opts.Parityshard)
		dryrun := 0
		if opts.Dryrun {
			dryrun = opts.Cluster
		}
		success := client.EcSet(rec.Key, val, dryrun, placements)
		if !success {
			return
		}

		p.Placements[rec.Key] = placements
		for _, idx := range placements {
			p.LambdaPool[idx].Kvs[rec.Key] = &Object{
				Key:  rec.Key,
				Sz:   rec.Sz / 10,
				Freq: 0,
			}
			p.LambdaPool[idx].MemUsed += rec.Sz / 10
		}
		log.Debug("Set %s, placements: %v.", rec.Key, placements)
	}
}

func initProxies(nProxies int, nLambdasPerProxy int) ([]Proxy, *consistent.Consistent) {
	proxies := make([]Proxy, nProxies)
	members := []consistent.Member{}
	for i, _ := range proxies {
		proxies[i].Id = strconv.Itoa(i)
		proxies[i].LambdaPool = make([]Lambda, nLambdasPerProxy)
		proxies[i].Placements = make(map[string][]int)
		for j, _ := range proxies[i].LambdaPool {
			proxies[i].LambdaPool[j].Kvs = make(map[string]*Object)
			proxies[i].LambdaPool[j].MemUsed = 0
		}
		member := Member(proxies[i].Id)
		members = append(members, member)
	}

	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	ring := consistent.New(members, cfg)

	return proxies, ring
}

func helpInfo() {
	fmt.Fprintf(os.Stderr, "Usage: ./playback [options] tracefile")
	flag.PrintDefaults()
}

func main() {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	options := &Options{
	}
	flag.StringVar(&options.AddrList, "addrlist", "127.0.0.1:6378", "proxy address:port")
	flag.IntVar(&options.Cluster, "cluster", 300, "number of instance per proxy")
	flag.IntVar(&options.Datashard, "d", 4, "number of data shards for RS erasure coding")
	flag.IntVar(&options.Parityshard, "p", 2, "number of parity shards for RS erasure coding")
	flag.IntVar(&options.ECmaxgoroutine, "g", 32, "max number of goroutines for RS erasure coding")
	flag.BoolVar(&options.Printlog, "log", true, "print debugging log?")
	flag.StringVar(&options.File, "file", "playback", "print result to file")
	flag.BoolVar(&options.Compact, "compact", true, "playback in compact mode")
	flag.Int64Var(&options.Interval, "i", 2000, "interval for every req (ms), valid only if compact=true")
	flag.BoolVar(&options.Dryrun, "dryrun", false, "no actual invocation")

	flag.Parse()

	if printInfo || flag.NArg() < 3 {
		helpInfo()
		os.Exit(0)
	}

	if !options.Printlog {
		log.Level = logger.LOG_LEVEL_INFO
	}

	traceFile, err := os.Open(flag.Arg(1))
	if err != nil {
		log.Error("Failed to open trace file")
		os.Exit(1)
	}
	defer traceFile.Close()

	addrArr := strings.Split(options.AddrList, ",")
	proxies, ring := initProxies(len(addrArr), options.Cluster)
	client := ecRedis.NewClient(options.Datashard, options.Parityshard, options.ECmaxgoroutine)
	client.Dial(addrArr)

	reader := csv.NewReader(bufio.NewReader(traceFile))
	timer := time.NewTimer(0)
	start := time.Now()
	var startRecord *Record
	var lastRecord *Record
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		sz, szErr := strconv.ParseFloat(line[9], 64)
		t, tErr := time.Parse("2006-01-02 03:04:05.000", line[11])
		if szErr != nil || tErr != nil {
			log.Warn("Error on parse record, skip %v.", line)
			continue
		}
		rec := &Record{
			Key:       line[6],
			Sz:        uint64(sz),
			Time:      t,
		}

		if lastRecord != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timeout := options.Interval * int64(time.Millisecond)
			if options.Compact {
				next := int64(rec.Time.Sub(lastRecord.Time))
				if next < timeout {
					timeout = next
				}
			} else {
				// Use absolute time span for accuracy
				timeout = int64(rec.Time.Sub(startRecord.Time)) - int64(time.Since(start))
			}
			if timeout <= 0{
				timeout = 0
			} else {
				log.Debug("Playback in %v", time.Duration(timeout))
			}
			timer.Reset(time.Duration(timeout))
		} else {
			startRecord = rec
		}

		<-timer.C
		log.Debug("Playbacking %v(exp %v, act %v)...", rec, rec.Time.Sub(startRecord.Time), time.Since(start))
		member := ring.LocateKey([]byte(rec.Key))
		hostId := member.String()
		id, _ := strconv.Atoi(hostId)
		perform(options, client, &proxies[id], rec)

		lastRecord = rec
	}

	maxMem := float64(0)
	for i := 0; i < len(proxies); i++ {
		proxy := &proxies[i]
		for j := 0; j < len(proxy.LambdaPool); j++ {
			lambda := &proxy.LambdaPool[j]
			maxMem = math.Max(maxMem, float64(lambda.MemUsed))
		}
	}
	log.Debug("Max memory consumed per lambda: %d", maxMem)
}
