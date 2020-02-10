package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/dustin/go-humanize"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/client"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"io"
	syslog "log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wangaoone/redbench/simulator/playback/proxy"
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

func init() {
	global.Log = log
}

type Options struct {
	AddrList       string
	Cluster        int
	Datashard      int
	Parityshard    int
	ECmaxgoroutine int
	CSV            bool
	Stdout         io.Writer
	Stderr         io.Writer
	NoDebug        bool
	SummaryOnly    bool
	File           string
	Compact        bool
	Interval       int64
	Dryrun         bool
	Lean           bool
	MaxSz          uint64
	ScaleFrom      uint64
	ScaleSz        float64
	Skip           int64
	S3             string
	Redis          string
	RedisCluster	 bool
	Balance        bool
}

type Client interface {
	EcSet(string, []byte, ...interface{}) (string, bool)
	EcGet(string, int, ...interface{}) (string, io.ReadCloser, bool)
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

func perform(opts *Options, cli Client, p *proxy.Proxy, obj *proxy.Object) (string, string) {
	dryrun := 0
	if opts.Dryrun {
		dryrun = opts.Cluster
	}
	// log.Debug("Key:", obj.Key, "mapped to Proxy:", p.Id)
	log.Trace("find placement,%v,%v", p.Placements[obj.Key], obj.Key)

	if placements, ok := p.Placements[obj.Key]; ok {
		reqId, reader, success := cli.EcGet(obj.Key, int(obj.Sz), dryrun)
		if opts.Dryrun && opts.Balance {
			success = p.Validate(obj)
		}

		if !success {
			val := make([]byte, obj.Sz)
			rand.Read(val)
			resetPlacements := make([]int, opts.Datashard+opts.Parityshard)
			_, reset := cli.EcSet(obj.Key, val, dryrun, resetPlacements, "Reset")
			if reset {
				log.Trace("Reset %s.", obj.Key)

				displaced := false
				resetPlacements = p.Remap(resetPlacements, obj)
				for i, idx := range resetPlacements {
					chk := p.LambdaPool[placements[i]].Kvs[fmt.Sprintf("%d@%s", i, obj.Key)]
					if chk == nil {
						// Eviction tracked by simulator. Try find chunk from evicts.
						chk = p.Evicts[fmt.Sprintf("%d@%s", i, obj.Key)]

						displaced = true
						p.LambdaPool[idx].Kvs[chk.Key] = chk
						p.LambdaPool[idx].MemUsed += chk.Sz
					} else if idx != placements[i] {
						// Placement changed?
						displaced = true
						log.Warn("Placement changed on reset %s, %d -> %d", chk.Key, placements[i], idx)
						p.LambdaPool[placements[i]].MemUsed -= chk.Sz
						delete(p.LambdaPool[placements[i]].Kvs, chk.Key)
						p.LambdaPool[idx].Kvs[chk.Key] = chk
						p.LambdaPool[idx].MemUsed += chk.Sz
					}
					chk.Reset++
					(&p.LambdaPool[idx]).Activate(obj.Time)
				}
				if displaced {
					p.Placements[obj.Key] = resetPlacements
				}
			}
			return "get", reqId
		} else if reader != nil {
			reader.Close()
		}
		log.Trace("Get %s.", obj.Key)

		for i, idx := range placements {
			chk, ok := p.LambdaPool[idx].Kvs[fmt.Sprintf("%d@%s", i, obj.Key)]
			if !ok {
				log.Error("Unexpected key %s not found in %d", chk.Key, idx)
			}
			chk.Freq++
			(&p.LambdaPool[idx]).Activate(obj.Time)
		}
		return "get", reqId
	} else {
		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		var val []byte
		if !opts.Lean {
			val = make([]byte, obj.Sz)
			rand.Read(val)
		}
		placements := make([]int, opts.Datashard+opts.Parityshard)
		dryrun := 0
		if opts.Dryrun {
			dryrun = opts.Cluster
		}
		reqId, success := cli.EcSet(obj.Key, val, dryrun, placements, "Normal")
		if !success {
			return "set", reqId
		}

		placements = p.Remap(placements, obj)
		for i, idx := range placements {
			chkKey := fmt.Sprintf("%d@%s", i, obj.Key)
			chk := p.Evicts[chkKey]
			if chk == nil {
				chk = &proxy.Chunk{
					Key:  chkKey,
					Sz:   obj.ChunkSz,
					Freq: 0,
				}
			}
			p.LambdaPool[idx].Kvs[chk.Key] = chk
			p.LambdaPool[idx].MemUsed += chk.Sz
			if opts.Dryrun && opts.Balance {
				p.Adapt(idx, chk)
			}
			(&p.LambdaPool[idx]).Activate(obj.Time)
		}
		log.Trace("Set %s, placements: %v.", obj.Key, placements)
		p.Placements[obj.Key] = placements
		return "set", reqId
	}
}

func initProxies(nProxies int, opts *Options) ([]proxy.Proxy, *consistent.Consistent) {
	proxies := make([]proxy.Proxy, nProxies)
	members := []consistent.Member{}
	for i, _ := range proxies {
		proxies[i].Id = strconv.Itoa(i)
		proxies[i].LambdaPool = make([]proxy.Lambda, opts.Cluster)
		proxies[i].Placements = make(map[string][]int)
		for j, _ := range proxies[i].LambdaPool {
			proxies[i].LambdaPool[j].Id = j
			proxies[i].LambdaPool[j].Kvs = make(map[string]*proxy.Chunk)
			proxies[i].LambdaPool[j].MemUsed = 100 * 1000000     // MB
			proxies[i].LambdaPool[j].Capacity = 1024 * 1000000    // MB
		}
		proxies[i].Evicts = make(map[string]*proxy.Chunk)
		if opts.Balance {
			proxies[i].Balancer = &proxy.LRUPlacer{}
			proxies[i].Init()
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
	flag.BoolVar(&options.NoDebug, "disable-debug", false, "disable printing debugging log?")
	flag.BoolVar(&options.SummaryOnly, "summary-only", false, "show summary only")
	flag.StringVar(&options.File, "file", "playback", "print result to file")
	flag.BoolVar(&options.Compact, "compact", false, "playback in compact mode")
	flag.Int64Var(&options.Interval, "i", 2000, "interval for every req (ms), valid only if compact=true")
	flag.BoolVar(&options.Dryrun, "dryrun", false, "no actual invocation")
	flag.BoolVar(&options.Lean, "lean", false, "run with minimum memory consumtion, valid only if dryrun=true")
	flag.Uint64Var(&options.MaxSz, "maxsz", 2147483648, "max object size")
	flag.Uint64Var(&options.ScaleFrom, "scalefrom", 104857600, "objects larger than this size will be scaled")
	flag.Float64Var(&options.ScaleSz, "scalesz", 1, "scale object size")
	flag.Int64Var(&options.Skip, "skip", 0, "skip N records")
	flag.StringVar(&options.S3, "s3", "", "s3 bucket for enable s3 simulation")
	flag.StringVar(&options.Redis, "redis", "", "Redis for enable Redis simulation")
	flag.BoolVar(&options.RedisCluster, "redisCluster", false, "redisCluster for enable Redis simulation")
	flag.BoolVar(&options.Balance, "balance", false, "enable balancer on dryrun")

	flag.Parse()

	if printInfo || flag.NArg() < 1 {
		helpInfo()
		os.Exit(0)
	}

	if options.NoDebug {
		log.Verbose = false
		log.Level = logger.LOG_LEVEL_INFO
	}
	if options.SummaryOnly {
		log.Verbose = false
		log.Level = logger.LOG_LEVEL_WARN
	}

	traceFile, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Error("Failed to open trace file: %s", flag.Arg(0))
		os.Exit(1)
	}
	defer traceFile.Close()

	addrArr := strings.Split(options.AddrList, ",")
	proxies, ring := initProxies(len(addrArr), options)
	var cli Client
	if options.S3 != "" {
		cli = NewS3Client(options.S3)
	} else if options.Redis != "" {
		cli = NewRedisClient(options.Redis)
	} else if options.RedisCluster == true{
		cli = NewClusterRedisClient()
	} else {
		cli = client.NewClient(options.Datashard, options.Parityshard, options.ECmaxgoroutine)
		if !options.Dryrun {
			cli.(*client.Client).Dial(addrArr)
		}
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
	read := int64(0)
	var skipedDuration time.Duration
	var startObject *proxy.Object
	var lastObject *proxy.Object
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
		obj := &proxy.Object{
			Key:       line[6],
			Sz:        uint64(sz),
			ChunkSz:   uint64(sz) / uint64(options.Datashard),
			Time:      t,
		}
		if obj.Sz > options.MaxSz {
			obj.Sz = options.MaxSz
		}
		if obj.Sz > options.ScaleFrom {
			obj.Sz = uint64(float64(obj.Sz) * options.ScaleSz)
		}

		if lastObject != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timeout := time.Duration(options.Interval) * time.Millisecond
			if options.Compact {
				next := obj.Time.Sub(lastObject.Time)
				if next < timeout {
					timeout = next
				}
			} else {
				// Use absolute time span for accuracy
				timeout = obj.Time.Sub(startObject.Time) - skipedDuration - time.Since(start)
			}
			if timeout <= 0 || (options.Dryrun && options.Compact) {
				timeout = 0
			}

			// On skiping, use elapsed to record time.
			if read >= options.Skip {
				if timeout > 0 {
					log.Info("Playback %d in %v", read + 1, timeout)
				}
				timer.Reset(timeout)
			} else {
				skipedDuration += timeout
				if timeout > 0 {
					log.Info("Skip %d: %v", read + 1, timeout)
				}
			}
		} else {
			startObject = obj
		}

		var reqId string
		if read >= options.Skip {
			<-timer.C
			log.Info("%d Playbacking %v(exp %v, act %v)...", read + 1, obj.Key, obj.Time.Sub(startObject.Time), skipedDuration + time.Since(start))
			member := ring.LocateKey([]byte(obj.Key))
			hostId := member.String()
			id, _ := strconv.Atoi(hostId)
			_, reqId = perform(options, cli, &proxies[id], obj)
			log.Debug("csv,%s,%s,%d,%d", reqId, obj.Key, int64(obj.Time.Sub(startObject.Time)), int64(skipedDuration + time.Since(start)))
		}

		lastObject = obj
		read++
	}

	totalMem := float64(0)
	maxMem := float64(0)
	minMem := float64(options.MaxSz)
	maxChunks := float64(0)
	minChunks := float64(1000)
	set := 0
	got := uint64(0)
	reset := uint64(0)
	activated := 0
	var balancerCost time.Duration
	for i := 0; i < len(proxies); i++ {
		prxy := &proxies[i]
		for j := 0; j < len(prxy.LambdaPool); j++ {
			lambda := &prxy.LambdaPool[j]
			totalMem += float64(lambda.MemUsed)
			minMem = math.Min(minMem, float64(lambda.MemUsed))
			maxMem = math.Max(maxMem, float64(lambda.MemUsed))
			minChunks = math.Min(minChunks, float64(len(lambda.Kvs)))
			maxChunks = math.Max(maxChunks, float64(len(lambda.Kvs)))
			set += len(lambda.Kvs)
			for _, chk := range lambda.Kvs {
				got += chk.Freq
				reset += chk.Reset
			}
			activated += lambda.ActiveMinites
		}
		for _, chk := range prxy.Evicts {
			got += chk.Freq
			reset += chk.Reset
		}
		balancerCost += prxy.BalancerCost
		prxy.Close()
	}
	syslog.Printf("Total records: %d\n", read - options.Skip)
	syslog.Printf("Total memory consumed: %s\n", humanize.Bytes(uint64(totalMem)))
	syslog.Printf("Memory consumed per lambda: %s - %s\n", humanize.Bytes(uint64(minMem)), humanize.Bytes(uint64(maxMem)))
	syslog.Printf("Chunks per lambda: %d - %d\n", int(minChunks), int(maxChunks))
	syslog.Printf("Set %d, Got %d, Reset %d\n", set, got, reset)
	syslog.Printf("Active Minutes %d\n", activated)
	syslog.Printf("BalancerCost: %s(%s per request)", balancerCost, balancerCost / time.Duration(read - options.Skip))
}
