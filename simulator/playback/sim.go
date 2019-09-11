package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	humanize "github.com/dustin/go-humanize"
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

const (
	TIME_PATTERN = "2006-01-02 15:04:05.000"
	TIME_PATTERN2 = "2006-01-02 15:04:05"
)

var (
	log           = &logger.ColorLogger{
		Verbose: true,
		Level: logger.LOG_LEVEL_ALL,
		Color: true,
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
	Lean           bool
	MaxSz          uint64
	ScaleSz        float64
}

type Object struct {
	Key  string
	Sz   uint64
	Freq uint64
	Reset uint64
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

func perform(opts *Options, client *ecRedis.Client, p *Proxy, rec *Record) string {
	dryrun := 0
	if opts.Dryrun {
		dryrun = opts.Cluster
	}
	// log.Debug("Key:", rec.Key, "mapped to Proxy:", p.Id)
	if placements, ok := p.Placements[rec.Key]; ok {
		reqId, reader, success := client.EcGet(rec.Key, int(rec.Sz), dryrun)
		if !success {
			val := make([]byte, rec.Sz)
			rand.Read(val)
			resetPlacements := make([]int, opts.Datashard + opts.Parityshard)
			_, reset := client.EcSet(rec.Key, val, 0, resetPlacements)
			if reset {
				log.Trace("Reset %s.", rec.Key)
				displaced := false
				for i, idx := range resetPlacements {
					obj, exists := p.LambdaPool[idx].Kvs[rec.Key]
					if !exists {
						displaced = true
						log.Warn("Placement changed on reset %s, %d -> %d", rec.Key, placements[i], idx)
						obj = p.LambdaPool[placements[i]].Kvs[rec.Key]
						delete(p.LambdaPool[placements[i]].Kvs, rec.Key)
						p.LambdaPool[idx].Kvs[rec.Key] = obj
						p.LambdaPool[idx].MemUsed += obj.Sz
					}
					obj.Reset++
				}
				if displaced {
					p.Placements[rec.Key] = resetPlacements
				}
			}
			return reqId
		} else if reader != nil {
			reader.Close()
		}
		log.Trace("Get %s.", rec.Key)

		for _, idx := range placements {
			obj := p.LambdaPool[idx].Kvs[rec.Key]
			obj.Freq++
		}
		return reqId
	} else {
		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		var val []byte
		if !opts.Lean {
			val = make([]byte, rec.Sz)
			rand.Read(val)
		}
		placements := make([]int, opts.Datashard + opts.Parityshard)
		dryrun := 0
		if opts.Dryrun {
			dryrun = opts.Cluster
		}
		reqId, success := client.EcSet(rec.Key, val, dryrun, placements)
		if !success {
			return reqId
		}

		p.Placements[rec.Key] = placements
		for _, idx := range placements {
			obj := &Object{
				Key:  rec.Key,
				Sz:   rec.Sz / uint64(opts.Datashard),
				Freq: 0,
			}
			p.LambdaPool[idx].Kvs[rec.Key] = obj
			p.LambdaPool[idx].MemUsed += obj.Sz
		}
		log.Trace("Set %s, placements: %v.", rec.Key, placements)
		return reqId
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
	fmt.Fprintf(os.Stderr, "Usage: ./playback [options] tracefile\n")
	fmt.Fprintf(os.Stderr, "Available options:\n")
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
	flag.BoolVar(&options.Lean, "lean", false, "run with minimum memory consumtion, valid only if dryrun=true")
	flag.Uint64Var(&options.MaxSz, "maxsz", 2147483648, "max object size")
	flag.Float64Var(&options.ScaleSz, "scalesz", 1, "scale object size")

	flag.Parse()

	if printInfo || flag.NArg() < 1 {
		helpInfo()
		os.Exit(0)
	}

	if !options.Printlog {
		log.Verbose = false
		log.Level = logger.LOG_LEVEL_INFO
	}

	traceFile, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Error("Failed to open trace file: %s", flag.Arg(0))
		os.Exit(1)
	}
	defer traceFile.Close()

	addrArr := strings.Split(options.AddrList, ",")
	proxies, ring := initProxies(len(addrArr), options.Cluster)
	client := ecRedis.NewClient(options.Datashard, options.Parityshard, options.ECmaxgoroutine)
	if !options.Dryrun {
		client.Dial(addrArr)
	}

	reader := csv.NewReader(bufio.NewReader(traceFile))
	// Skip first line
	_, err = reader.Read()
	if err == io.EOF {
		panic(errors.New(fmt.Sprintf("Empty file: %s", flag.Arg(0))))
	} else if err != nil {
		panic(err)
	}

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
		t, tErr := time.Parse(TIME_PATTERN, line[11][:len(TIME_PATTERN)])
		if tErr != nil {
			t, tErr = time.Parse(TIME_PATTERN2, line[11][:len(TIME_PATTERN2)])
		}
		if szErr != nil || tErr != nil {
			log.Warn("Error on parse record, skip %v: %v, %v", line, szErr, tErr)
			continue
		}
		rec := &Record{
			Key:       line[6],
			Sz:        uint64(sz),
			Time:      t,
		}
		if rec.Sz > options.MaxSz {
			rec.Sz = options.MaxSz
		}
		rec.Sz = uint64(float64(rec.Sz) * options.ScaleSz)

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
			if timeout <= 0 || options.Dryrun {
				timeout = 0
			} else {
				log.Info("Playback in %v", time.Duration(timeout))
			}
			timer.Reset(time.Duration(timeout))
		} else {
			startRecord = rec
		}

		<-timer.C
		log.Info("Playbacking %v(exp %v, act %v)...", rec.Key, rec.Time.Sub(startRecord.Time), time.Since(start))
		member := ring.LocateKey([]byte(rec.Key))
		hostId := member.String()
		id, _ := strconv.Atoi(hostId)
		reqId := perform(options, client, &proxies[id], rec)
		log.Debug("csv,%s,%s,%d,%d", reqId, rec.Key, int64(rec.Time.Sub(startRecord.Time)), int64(time.Since(start)))

		lastRecord = rec
	}

	maxMem := float64(0)
	minMem := float64(options.MaxSz)
	maxChunks := float64(0)
	minChunks := float64(1000)
	set := 0
	got := uint64(0)
	reset := uint64(0)
	for i := 0; i < len(proxies); i++ {
		proxy := &proxies[i]
		for j := 0; j < len(proxy.LambdaPool); j++ {
			lambda := &proxy.LambdaPool[j]
			minMem = math.Min(minMem, float64(lambda.MemUsed))
			maxMem = math.Max(maxMem, float64(lambda.MemUsed))
			minChunks = math.Min(minChunks, float64(len(lambda.Kvs)))
			maxChunks = math.Max(maxChunks, float64(len(lambda.Kvs)))
			set += len(lambda.Kvs)
			for _, obj := range lambda.Kvs {
				got += obj.Freq
				reset += obj.Reset
			}
		}
	}
	log.Info("Memory consumed per lambda: %s - %s", humanize.Bytes(uint64(minMem)), humanize.Bytes(uint64(maxMem)))
	log.Info("Chunks per lambda: %d - %d", int(minChunks), int(maxChunks))
	log.Info("Set %d, Got %d, Reset %d", set, got, reset)
}
