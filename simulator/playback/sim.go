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
	"github.com/wangaoone/LambdaObjectstore/lib/logger"
	"github.com/wangaoone/ecRedis"
	"io"
	syslog "log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	TIME_PATTERN      = "2006-01-02 15:04:05.000"
	TIME_PATTERN2     = "2006-01-02 15:04:05"
	INIT_LRU_CAPACITY = 10000000
)

var (
	log = &logger.ColorLogger{
		Verbose: true,
		Level:   logger.LOG_LEVEL_ALL,
		Color:   true,
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
	RedisCluster   bool
	Balance        bool
	LRU            bool
}

type Client interface {
	EcSet(string, []byte, ...interface{}) (string, bool)
	EcGet(string, int, ...interface{}) (string, io.ReadCloser, bool)
}

type Object struct {
	Key        string
	Sz         uint64
	Freq       uint64
	Reset      uint64
	ArriveTime int64
	Tier       string
}

type Lambda struct {
	Id             int
	Kvs            map[string]*Object
	MemUsed        uint64
	ActiveMinites  int
	LastActive     time.Time
	Capacity       uint64
	UsedPercentile int

	block  int
	blocks []int
}

func (l *Lambda) Activate(recTime time.Time) {
	if l.ActiveMinites == 0 {
		l.ActiveMinites++
	} else if recTime.Sub(l.LastActive) >= time.Minute {
		l.ActiveMinites++
	}
	l.LastActive = recTime
}

type Record struct {
	Key  string
	Sz   uint64
	Time time.Time
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

func perform(opts *Options, client Client, p *Proxy, rec *Record, placer *Placer) (string, string) {
	dryrun := 0
	if opts.Dryrun {
		dryrun = opts.Cluster
	}
	// log.Debug("Key:", rec.Key, "mapped to Proxy:", p.Id)
	if placements, ok := p.Placements[rec.Key]; ok {
		log.Trace("ok is %v, %v", ok, placements)
		reqId, reader, success := client.EcGet(rec.Key, int(rec.Sz), dryrun)
		log.Trace("success is %v", success)
		if !success {
			val := make([]byte, rec.Sz)
			rand.Read(val)
			resetPlacements := make([]int, opts.Datashard+opts.Parityshard)
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
			return "get", reqId
		} else if reader != nil {
			reader.Close()
		}
		log.Trace("Get %s.", rec.Key)

		for _, idx := range placements {
			obj := p.LambdaPool[idx].Kvs[rec.Key]
			log.Trace("key is, %v, obj is %v", rec.Key, obj)
			obj.Freq++
			(&p.LambdaPool[idx]).Activate(rec.Time)
		}
		return "get", reqId
	} else {
		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		var val []byte
		if !opts.Lean {
			val = make([]byte, rec.Sz)
			rand.Read(val)
		}
		placements := make([]int, opts.Datashard+opts.Parityshard)
		dryrun := 0
		if opts.Dryrun {
			dryrun = opts.Cluster
		}
		reqId, success := client.EcSet(rec.Key, val, dryrun, placements)
		if !success {
			return "set", reqId
		}

		//p.Placements[rec.Key] = p.Remap(placements)
		p.Placements[rec.Key] = placements
		log.Trace("placements is ", placements)
		// obj
		obj := &Object{
			Key:        rec.Key,
			Sz:         rec.Sz,
			Freq:       0,
			ArriveTime: time.Now().Unix(),
		}
		// placer append whole obj instead of obj chunk
		// assign object size tier
		placer.Append(obj)

		del := false
		// determine whether current placements is enough for object
		for _, idx := range placements {
			if p.LambdaPool[idx].MemUsed+(obj.Sz/uint64(opts.Datashard)) >= p.LambdaPool[idx].Capacity {
				del = true
				break
			}
		}
		if del == true {
			log.Trace("del is true")
			newPlacement := placer.FindPlacement(obj, p)
			// new placement for incoming key
			p.Placements[obj.Key] = newPlacement
			// perform set after delete with new placement
			for _, idx := range newPlacement {
				p.LambdaPool[idx].Kvs[rec.Key] = obj
				p.LambdaPool[idx].MemUsed += obj.Sz / uint64(opts.Datashard)
				//if opts.Dryrun && opts.Balance {
				//	p.Adapt(idx)
				//}
				//(&p.LambdaPool[idx]).Activate(rec.Time)
			}
			log.Trace("del and set is finish")
		} else {
			for _, idx := range placements {
				p.LambdaPool[idx].Kvs[rec.Key] = obj
				p.LambdaPool[idx].MemUsed += obj.Sz / uint64(opts.Datashard)
				//if opts.Dryrun && opts.Balance {
				//	p.Adapt(idx)
				//}
				//(&p.LambdaPool[idx]).Activate(rec.Time)
			}
		}
		log.Trace("Set %s, placements: %v.", rec.Key, placements)
		return "set", reqId
	}
}

func performNoLRU(opts *Options, client Client, p *Proxy, rec *Record) (string, string) {
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
			resetPlacements := make([]int, opts.Datashard+opts.Parityshard)
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
			return "get", reqId
		} else if reader != nil {
			reader.Close()
		}
		log.Trace("Get %s.", rec.Key)

		for _, idx := range placements {
			obj := p.LambdaPool[idx].Kvs[rec.Key]
			obj.Freq++
			(&p.LambdaPool[idx]).Activate(rec.Time)
		}
		return "get", reqId
	} else {
		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		var val []byte
		if !opts.Lean {
			val = make([]byte, rec.Sz)
			rand.Read(val)
		}
		placements := make([]int, opts.Datashard+opts.Parityshard)
		dryrun := 0
		if opts.Dryrun {
			dryrun = opts.Cluster
		}
		reqId, success := client.EcSet(rec.Key, val, dryrun, placements)
		if !success {
			return "set", reqId
		}

		p.Placements[rec.Key] = p.Remap(placements)
		for _, idx := range placements {
			obj := &Object{
				Key:  rec.Key,
				Sz:   rec.Sz / uint64(opts.Datashard),
				Freq: 0,
			}
			p.LambdaPool[idx].Kvs[rec.Key] = obj
			p.LambdaPool[idx].MemUsed += obj.Sz
			if opts.Dryrun && opts.Balance {
				p.Adapt(idx)
			}
			(&p.LambdaPool[idx]).Activate(rec.Time)
		}
		log.Trace("Set %s, placements: %v.", rec.Key, placements)
		return "set", reqId
	}
}

func initProxies(nProxies int, opts *Options) ([]Proxy, *consistent.Consistent) {
	proxies := make([]Proxy, nProxies)
	members := []consistent.Member{}
	for i, _ := range proxies {
		proxies[i].Id = strconv.Itoa(i)
		proxies[i].LambdaPool = make([]Lambda, opts.Cluster)
		proxies[i].Placements = make(map[string][]int)
		for j, _ := range proxies[i].LambdaPool {
			proxies[i].LambdaPool[j].Id = j
			proxies[i].LambdaPool[j].Kvs = make(map[string]*Object)
			proxies[i].LambdaPool[j].MemUsed = 0
			proxies[i].LambdaPool[j].Capacity = 1073741824
			//proxies[i].LambdaPool[j].Capacity = 1048576000 / 2
		}
		if opts.Balance {
			proxies[i].Balancer = &PriorityBalancer{}
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
	//flag.BoolVar(&options.LRU, "LRU", false, "enable LRU evict on dryrun")

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
	var client Client
	if options.S3 != "" {
		client = NewS3Client(options.S3)
	} else if options.Redis != "" {
		client = NewRedisClient(options.Redis)
	} else if options.RedisCluster == true {
		client = NewClusterRedisClient()
	} else {
		client = ecRedis.NewClient(options.Datashard, options.Parityshard, options.ECmaxgoroutine)
		if !options.Dryrun {
			client.(*ecRedis.Client).Dial(addrArr)
		}
	}
	var placer *Placer
	//if options.LRU != false {
	placer = NewPlacer()
	//}
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
			Key:  line[6],
			Sz:   uint64(sz),
			Time: t,
		}
		if rec.Sz > options.MaxSz {
			rec.Sz = options.MaxSz
		}
		if rec.Sz > options.ScaleFrom {
			rec.Sz = uint64(float64(rec.Sz) * options.ScaleSz)
		}

		if lastRecord != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timeout := time.Duration(options.Interval) * time.Millisecond
			if options.Compact {
				next := rec.Time.Sub(lastRecord.Time)
				if next < timeout {
					timeout = next
				}
			} else {
				// Use absolute time span for accuracy
				timeout = rec.Time.Sub(startRecord.Time) - skipedDuration - time.Since(start)
			}
			if timeout <= 0 || (options.Dryrun && options.Compact) {
				timeout = 0
			}

			// On skiping, use elapsed to record time.
			if read >= options.Skip {
				if timeout > 0 {
					log.Info("Playback %d in %v", read+1, timeout)
				}
				timer.Reset(timeout)
			} else {
				skipedDuration += timeout
				if timeout > 0 {
					log.Info("Skip %d: %v", read+1, timeout)
				}
			}
		} else {
			startRecord = rec
		}

		var reqId string
		if read >= options.Skip {
			<-timer.C
			log.Info("%d Playbacking %v(exp %v, act %v)...", read+1, rec.Key, rec.Time.Sub(startRecord.Time), skipedDuration+time.Since(start))
			member := ring.LocateKey([]byte(rec.Key))
			hostId := member.String()
			id, _ := strconv.Atoi(hostId)
			_, reqId = perform(options, client, &proxies[id], rec, placer)
			log.Debug("csv,%s,%s,%d,%d", reqId, rec.Key, int64(rec.Time.Sub(startRecord.Time)), int64(skipedDuration+time.Since(start)))
		}

		lastRecord = rec
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
		proxy := &proxies[i]
		for j := 0; j < len(proxy.LambdaPool); j++ {
			lambda := &proxy.LambdaPool[j]
			totalMem += float64(lambda.MemUsed)
			minMem = math.Min(minMem, float64(lambda.MemUsed))
			maxMem = math.Max(maxMem, float64(lambda.MemUsed))
			minChunks = math.Min(minChunks, float64(len(lambda.Kvs)))
			maxChunks = math.Max(maxChunks, float64(len(lambda.Kvs)))
			set += len(lambda.Kvs)
			for _, obj := range lambda.Kvs {
				got += obj.Freq
				reset += obj.Reset
			}
			activated += lambda.ActiveMinites
		}
		balancerCost += proxy.BalancerCost
	}
	syslog.Printf("Total records: %d\n", read-options.Skip)
	syslog.Printf("Total memory consumed: %s\n", humanize.Bytes(uint64(totalMem)))
	syslog.Printf("Memory consumed per lambda: %s - %s\n", humanize.Bytes(uint64(minMem)), humanize.Bytes(uint64(maxMem)))
	syslog.Printf("Chunks per lambda: %d - %d\n", int(minChunks), int(maxChunks))
	syslog.Printf("Set %d, Got %d, Reset %d\n", set, got, reset)
	syslog.Printf("Active Minutes %d\n", activated)
	syslog.Printf("BalancerCost: %s(%s per request)", balancerCost, balancerCost/time.Duration(read-options.Skip))
}
