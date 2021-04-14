/*
Copyright (c) 2020 LeapLab @ CS_GMU

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package main

import (
	sysflag "flag"
	"fmt"
	"io"
	syslog "log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ScottMansfield/nanolog"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/dustin/go-humanize"
	"github.com/mason-leap-lab/infinicache/client"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/wangaoone/redbench/benchclient"

	"github.com/wangaoone/redbench/simulator/playback/helpers"
	"github.com/wangaoone/redbench/simulator/playback/proxy"
	"github.com/wangaoone/redbench/simulator/readers"
)

const (
	TIME_PATTERN  = "2006-01-02 15:04:05.000"
	TIME_PATTERN2 = "2006-01-02 15:04:05"
)

var (
	log = &logger.ColorLogger{
		Verbose: true,
		Level:   logger.LOG_LEVEL_ALL,
		Color:   true,
	}
	clients    *proxy.Pool
	numClients int32
)

func init() {
	global.Log = log
}

type Options struct {
	AddrList        string
	Cluster         int
	Datashard       int
	Parityshard     int
	ECmaxgoroutine  int
	CSV             bool
	Stdout          io.Writer
	Stderr          io.Writer
	NoDebug         bool
	SummaryOnly     bool
	File            string
	Compact         bool
	Dryrun          bool
	Lean            bool
	MaxSz           uint64
	ScaleFrom       uint64
	ScaleSz         float64
	Limit           int64
	LimitHour       int64
	Skip            int64
	S3              string
	Redis           string
	RedisCluster    bool
	Balance         bool
	Concurrency     int
	Bandwidth       int64
	TraceName       string
	SampleFractions uint64
	SampleKey       uint64
}

type FinalizeOptions struct {
	once         sync.Once
	closeNanolog bool
	traceFile    *os.File
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

func perform(opts *Options, cli benchclient.Client, p *proxy.Proxy, obj *proxy.Object) (string, string) {
	dryrun := 0
	if opts.Dryrun {
		dryrun = opts.Cluster
		if !opts.Compact && obj.Estimation > time.Duration(0) {
			log.Debug("Sleep %v to simulate processing %s: ", obj.Estimation, obj.Key)
			time.Sleep(obj.Estimation)
		}
	}

	// log.Debug("Key:", obj.Key, "mapped to Proxy:", p.Id)
	if placements := p.Placements(obj.Key); placements != nil {
		log.Trace("Found placements of %v: %v", obj.Key, placements)

		reqId, reader, success := cli.EcGet(obj.Key, dryrun)
		if opts.Dryrun && opts.Balance {
			success = p.Validate(obj)
		}

		if !success {
			val := make([]byte, obj.Size)
			rand.Read(val)
			resetPlacements32 := make([]int, opts.Datashard+opts.Parityshard)
			_, reset := cli.EcSet(obj.Key, val, dryrun, resetPlacements32, "Reset")
			if reset {
				log.Trace("Reset %s.", obj.Key)

				displaced := false
				resetPlacements := make([]uint64, len(resetPlacements32))
				for i := 0; i < len(resetPlacements32); i++ {
					resetPlacements[i] = uint64(resetPlacements32[i])
				}
				resetPlacements = p.Remap(resetPlacements, obj)
				for i, idx := range resetPlacements {
					p.ValidateLambda(idx)
					chk, _ := p.LambdaPool[placements[i]].GetChunk(fmt.Sprintf("%d@%s", i, obj.Key))
					if chk == nil {
						// Eviction tracked by simulator. Try find chunk from evicts.
						chk = p.GetEvicted(fmt.Sprintf("%d@%s", i, obj.Key))

						displaced = true
						p.LambdaPool[idx].AddChunk(chk)
						p.LambdaPool[idx].MemUsed += chk.Sz
					} else if idx != placements[i] {
						// Placement changed?
						displaced = true
						log.Warn("Placement changed on reset %s, %d -> %d", chk.Key, placements[i], idx)
						p.LambdaPool[placements[i]].MemUsed -= chk.Sz
						p.LambdaPool[placements[i]].DelChunk(chk.Key)
						p.LambdaPool[idx].AddChunk(chk)
						p.LambdaPool[idx].MemUsed += chk.Sz
					}
					chk.Reset++
					p.LambdaPool[idx].Activate(obj.Timestamp)
				}
				if displaced {
					p.SetPlacements(obj.Key, resetPlacements)
				}
			}
			return "get", reqId
		} else if reader != nil {
			reader.Close()
		}
		log.Trace("Get %s.", obj.Key)

		for i, idx := range placements {
			chk, ok := p.LambdaPool[idx].GetChunk(fmt.Sprintf("%d@%s", i, obj.Key))
			if !ok {
				log.Error("Unexpected key %d@%s not found in %d", i, obj.Key, idx)
				continue
			}
			chk.Freq++
			p.LambdaPool[idx].Activate(obj.Timestamp)
		}
		return "get", reqId
	} else {
		log.Trace("No placements found: %v", obj.Key)

		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		var val []byte
		if !opts.Lean {
			val = make([]byte, obj.Size)
			rand.Read(val)
		}
		placements32 := make([]int, opts.Datashard+opts.Parityshard)
		placements := make([]uint64, len(placements32))
		dryrun := 0
		if opts.Dryrun {
			dryrun = opts.Cluster
		}
		reqId, success := cli.EcSet(obj.Key, val, dryrun, placements32, "Normal")
		if !success {
			return "set", reqId
		}
		for i := 0; i < len(placements32); i++ {
			placements[i] = uint64(placements32[i])
		}

		placements = p.Remap(placements, obj)
		for i, idx := range placements {
			chkKey := fmt.Sprintf("%d@%s", i, obj.Key)
			chk := p.GetEvicted(chkKey)
			if chk == nil {
				chk = &proxy.Chunk{
					Key:  chkKey,
					Sz:   obj.ChunkSz,
					Freq: 0,
				}
			}
			p.ValidateLambda(idx)
			p.LambdaPool[idx].AddChunk(chk)
			p.LambdaPool[idx].MemUsed += chk.Sz
			if opts.Dryrun && opts.Balance {
				p.Adapt(idx, chk)
			}
			p.LambdaPool[idx].Activate(obj.Timestamp)
		}
		log.Trace("Set %s, placements: %v.", obj.Key, placements)
		p.SetPlacements(obj.Key, placements)
		return "set", reqId
	}
}

func initProxies(nProxies int, opts *Options) ([]*proxy.Proxy, *consistent.Consistent) {
	proxies := make([]*proxy.Proxy, nProxies)
	members := []consistent.Member{}
	for i := range proxies {
		var balancer proxy.ProxyBalancer
		if opts.Balance {
			// Balancer optiosn:
			// balancer = &proxy.LRUPlacer{}
			balancer = &proxy.PriorityBalancer{}
			// balancer = &proxy.WeightedBalancer{}
		}
		proxies[i] = proxy.NewProxy(strconv.Itoa(i), opts.Cluster, balancer)

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

func helpInfo(flag *sysflag.FlagSet) {
	fmt.Fprintf(os.Stderr, "Usage: ./playback [options] tracefile\n")
	fmt.Fprintf(os.Stderr, "Available options:\n")
	flag.PrintDefaults()
}

func main() {
	flag := &sysflag.FlagSet{}

	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	finalizeOptions := &FinalizeOptions{}
	defer finalize(finalizeOptions)

	options := &Options{}
	flag.StringVar(&options.AddrList, "addrlist", "127.0.0.1:6378", "proxy address:port")
	flag.IntVar(&options.Cluster, "cluster", 300, "number of instance per proxy")
	flag.IntVar(&options.Datashard, "d", 10, "number of data shards for RS erasure coding")
	flag.IntVar(&options.Parityshard, "p", 2, "number of parity shards for RS erasure coding")
	flag.IntVar(&options.ECmaxgoroutine, "g", 32, "max number of goroutines for RS erasure coding")
	flag.BoolVar(&options.NoDebug, "disable-debug", false, "disable printing debugging log?")
	flag.BoolVar(&options.SummaryOnly, "summary-only", false, "show summary only")
	flag.StringVar(&options.File, "file", "", "print result to file")
	flag.BoolVar(&options.Dryrun, "dryrun", false, "no actual invocation, with -lean and -compact set to true by default.")
	// flag.BoolVar(&options.Lean, "lean", false, "run with minimum memory consumtion, valid only if dryrun=true")
	// flag.BoolVar(&options.Compact, "compact", false, "playback in compact mode")
	flag.Uint64Var(&options.MaxSz, "maxsz", 2147483648, "max object size")
	flag.Uint64Var(&options.ScaleFrom, "scalefrom", 104857600, "objects larger than this size will be scaled")
	flag.Float64Var(&options.ScaleSz, "scalesz", 1, "scale object size")
	flag.Int64Var(&options.Limit, "limit", 0, "limit to play N records only")
	flag.Int64Var(&options.LimitHour, "limitHour", 0, "limit to play N hours only")
	flag.Int64Var(&options.Skip, "skip", 0, "skip N records")
	flag.StringVar(&options.S3, "s3", "", "s3 bucket for enable s3 simulation")
	flag.StringVar(&options.Redis, "redis", "", "Redis for enable Redis simulation")
	flag.BoolVar(&options.RedisCluster, "redisCluster", false, "redisCluster for enable Redis simulation")
	flag.BoolVar(&options.Balance, "balance", false, "enable balancer on dryrun")
	flag.IntVar(&options.Concurrency, "c", 100, "max concurrency allowed, minimum 1.")
	flag.Int64Var(&options.Bandwidth, "w", 0, "unit bandwidth per shard in MiB/s. 0 for unlimited bandwidth")
	flag.StringVar(&options.TraceName, "trace", "IBMDockerRegistry", "type of trace: IBMDockerRegistry, IBMObjectStore")
	flag.Uint64Var(&options.SampleFractions, "sf", 1, "enable sampling by raising fraction's denominator.")
	flag.Uint64Var(&options.SampleKey, "sk", 0, "the key of sample")

	flag.Parse(os.Args[1:])

	if options.Dryrun {
		options.Lean = true
		options.Compact = true
	}

	flag.BoolVar(&options.Lean, "lean", options.Lean, "run with minimum memory consumtion, valid only if dryrun=true")
	flag.BoolVar(&options.Compact, "compact", options.Compact, "playback in compact mode")

	flagErr := flag.Parse(os.Args[1:])
	if flagErr != nil {
		syslog.Fatalln(flagErr)
		printInfo = true
	}
	if printInfo || flag.NArg() < 1 {
		helpInfo(flag)
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
	if options.File != "" {
		if err := logCreate(options); err != nil {
			panic(err)
		}
		finalizeOptions.closeNanolog = true
	}
	if options.Concurrency <= 0 {
		options.Concurrency = 1
	}
	options.Bandwidth *= 1024 * 1024 * int64(options.Datashard+options.Parityshard)

	traceFile, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Error("Failed to open trace file: %s", flag.Arg(0))
		os.Exit(1)
	}
	finalizeOptions.traceFile = traceFile

	addrArr := strings.Split(options.AddrList, ",")
	proxies, ring := initProxies(len(addrArr), options)
	clients = proxy.InitPool(&proxy.Pool{
		New: func() interface{} {
			atomic.AddInt32(&numClients, 1)
			var cli benchclient.Client
			if options.S3 != "" {
				cli = benchclient.NewS3(options.S3)
			} else if options.Redis != "" {
				cli = benchclient.NewRedis(options.Redis)
			} else if options.RedisCluster {
				cli = benchclient.NewElasticCache()
			} else {
				cli = client.NewClient(options.Datashard, options.Parityshard, options.ECmaxgoroutine)
				if !options.Dryrun {
					cli.(*client.Client).Dial(addrArr)
				}
			}
			return cli
		},
		Finalize: func(c interface{}) {
			c.(benchclient.Client).Close()
		},
	}, options.Concurrency, proxy.PoolForStrictConcurrency)

	var reader readers.RecordReader
	switch strings.ToLower(options.TraceName) {
	case "ibmobjectstore":
		reader = readers.NewIBMObjectStoreReader(traceFile)
	default:
		reader = readers.NewIBMDockerRegistryReader(traceFile)
	}

	timer := time.NewTimer(0)
	requestsCleared := make(chan time.Time, 1) // To be notified that all invoked requests were responded.
	read := int64(0)
	var skippedDuration time.Duration
	var firstTs int64
	var startTs int64
	var concurrency int32
	var maxConcurrency int32
	// cond := sync.NewCond(&sync.Mutex{})
	var close bool
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT)
	go func() {
		<-sig
		log.Info("Receive signal, stop server...")
		close = true
	}()
	var skipper *helpers.TimeSkipper
	if options.Dryrun && options.Compact && options.Bandwidth > 0 {
		skipper = helpers.NewTimeSkipper(options.Concurrency)
	}

	// Start replaying.
	start := time.Now()
	stop := int64(0)
	if options.Limit > 0 {
		stop = options.Skip + options.Limit
	}
	stopAt := time.Duration(0)
	if options.LimitHour > 0 {
		stopAt = time.Duration(options.LimitHour) * time.Hour
	}
	for {
		if close {
			// Close check
			break
		} else if stop > 0 && read >= stop {
			// Limit check
			log.Info("Limit(%d) reached.", options.Limit)
			break
		}

		// Read next request.
		rec, err := reader.Read()
		read++
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		} else if rec.Error == readers.ErrIgnoreIBMObjectStoreFragment {
			reader.Done(rec)
			log.Debug("Skip %d: %v", read, rec.Error)
			continue
		} else if rec.Error != nil {
			reader.Done(rec)
			log.Warn("Skip %d: %v", read, rec.Error)
			continue
		} else if rec.Method != "" && rec.Method != "GET" && rec.Method != "PUT" {
			reader.Done(rec)
			log.Debug("Skip %d: unsupported method %v", read, rec.Method)
			continue
		} else if options.SampleFractions > 1 && (xxhash.Sum64([]byte(rec.Key))%options.SampleFractions) != options.SampleKey {
			// Sampleing
			reader.Done(rec)
			continue
		}

		obj := &proxy.Object{Record: rec}
		if obj.Size > options.MaxSz {
			obj.Size = options.MaxSz
		}
		if obj.Size > options.ScaleFrom {
			obj.Size = uint64(float64(obj.Size) * options.ScaleSz)
		}
		obj.DChunks = options.Datashard
		obj.PChunks = options.Parityshard
		obj.ChunkSz = obj.Size / uint64(options.Datashard)
		if options.Bandwidth > 0 {
			obj.Estimation = time.Duration(float64(obj.Size)/float64(options.Bandwidth)*float64(time.Second)) + 10*time.Millisecond
		}

		// Calculate the time to invoke the request.
		var timeToStart time.Duration
		planned := time.Now()
		if firstTs == 0 {
			firstTs = obj.Timestamp
		} else {
			// Stop timer to be safe.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			// Use absolute time span for accuracy: time difference in trace - skipped - time replayed
			timeToStart = time.Duration(obj.Timestamp-firstTs) - skippedDuration - planned.Sub(start)
			if timeToStart <= 0 {
				timeToStart = 0
			}

			// On skiping, use elapsed to record time.
			if read <= options.Skip {
				skippedDuration += timeToStart
				if timeToStart > 0 {
					log.Info("Skip %d: %v", read, timeToStart)
				}
			} else {
				if timeToStart > 0 {
					timer.Reset(timeToStart)
					planned = planned.Add(timeToStart)
					log.Info("Playback %d in %v", read, timeToStart)
				}
			}
		}

		// Playback
		if read > options.Skip {
			if startTs == 0 {
				startTs = obj.Timestamp
			}

			if stopAt > time.Duration(0) && stopAt < time.Duration(obj.Timestamp-startTs) {
				log.Info("Time limit(%v) reached.", stopAt)
				break
			}

			now := time.Now()
			if timeToStart > 0 {
				if skipper != nil {
					// Use skipper in dryrun and compact mode.
					skipper.SkipTo(planned)
					skippedDuration += planned.Sub(now) // Update total duration skipped.
					log.Info("Simulating skipped and forwarded %v in compact mode", timeToStart)
				} else if !options.Compact {
					// In normal mode, wait for the next request.
					now = <-timer.C
				} else {
					select {
					// In compact mode, cleared event will be the time to can skip to the next request.
					case clearedAt := <-requestsCleared:
						skipped := planned.Sub(clearedAt) // Calculate duration to skip.
						if skipped < 0 {
							skipped = 0
						}
						skippedDuration += skipped // Update total duration skipped.
						now = clearedAt
						log.Info("Forwarded %v in compact mode", skipped)
						// No need to stop timer. It is stopped on processing each object.
					case now = <-timer.C:
						// Do nothing
					}
				}
			}

			member := ring.LocateKey([]byte(obj.Key))
			hostId := member.String()
			id, _ := strconv.Atoi(hostId)

			// Concurrency control
			// cond.L.Lock()
			// for options.Concurrency > 0 && atomic.LoadInt32(&concurrency) >= int32(options.Concurrency) {
			// 	cond.Wait()
			// }
			cli := clients.Get().(benchclient.Client)

			// Start perform
			var notifier *helpers.TimeSkipNotification
			if skipper != nil {
				log.Debug("Mark to skip %v for simulating processing %d:%s", obj.Estimation, read, obj.Key)
				notifier = skipper.MarkDuration(read, obj.Estimation)
			}
			go func(sn int64, cli benchclient.Client, p *proxy.Proxy, obj *proxy.Object, expected time.Duration, scheduled time.Duration, notifier *helpers.TimeSkipNotification) {
				// defer func() {
				// 	finalize(finalizeOptions)
				// 	// if err := recover(); err != nil {
				// 	// 	log.Error("Abort due to fatal err: %v", err)
				// 	// 	close = true
				// 	// }
				// }()

				c := atomic.AddInt32(&concurrency, 1)
				max := maxConcurrency
				for !atomic.CompareAndSwapInt32(&maxConcurrency, max, MaxInt32(max, c)) {
					max = maxConcurrency
				}

				actural := skippedDuration + time.Since(start)
				log.Info("%d(c:%d) Playbacking %v %s (expc %v, schd %v, actc %v)...", sn, c, obj.Key, humanize.Bytes(obj.Size), expected, scheduled, actural)

				_, reqId := perform(options, cli, p, obj)
				clients.Put(cli)
				if notifier != nil {
					notifier.Wait()
					// log.Debug("Skipped %d:%s", sn, obj.Key)
				}

				// Requests cleared
				if atomic.AddInt32(&concurrency, -1) == 0 {
					select {
					case requestsCleared <- time.Now():
					default:
						// update
						<-requestsCleared
						requestsCleared <- time.Now()
					}
				}
				log.Debug("csv,%s,%s,%d,%d,%d", reqId, obj.Key, expected, actural, obj.Size)
				reader.Done(obj.Record)
				obj.Record = nil
				// cond.Signal()
			}(read, cli, proxies[id], obj, time.Duration(obj.Timestamp-firstTs), skippedDuration+now.Sub(start), notifier)

			// cond.L.Unlock()
		}
	}

	// Wait for all current requests to be cleared.
	if skipper != nil {
		skippedDuration += skipper.SkipAll()
	}
	<-requestsCleared

	totalMem := float64(0)
	maxMem := float64(0)
	minMem := float64(10000000000000)
	maxChunks := float64(0)
	minChunks := float64(1000)
	set := 0
	got := uint64(0)
	reset := uint64(0)
	activated := 0
	var balancerCost time.Duration
	for i := 0; i < len(proxies); i++ {
		prxy := proxies[i]
		for j := 0; j < len(prxy.LambdaPool); j++ {
			lambda := prxy.LambdaPool[j]
			totalMem += float64(lambda.MemUsed)
			minMem = math.Min(minMem, float64(lambda.MemUsed))
			maxMem = math.Max(maxMem, float64(lambda.MemUsed))
			minChunks = math.Min(minChunks, float64(lambda.NumChunks()))
			maxChunks = math.Max(maxChunks, float64(lambda.NumChunks()))
			set += lambda.NumChunks()
			for chk := range lambda.AllChunks() {
				got += chk.Value.(*proxy.Chunk).Freq
				reset += chk.Value.(*proxy.Chunk).Reset
			}
			activated += lambda.ActiveMinutes
		}
		for chk := range prxy.AllEvicts() {
			got += chk.Value.(*proxy.Chunk).Freq
			reset += chk.Value.(*proxy.Chunk).Reset
		}
		balancerCost += prxy.BalancerCost
		prxy.Close()
	}
	syslog.Printf("Time elpased: %v\n", time.Since(start))
	syslog.Printf("Total records: %d\n", read-options.Skip)
	syslog.Printf("Total memory consumed: %s\n", humanize.Bytes(uint64(totalMem)))
	syslog.Printf("Memory consumed per lambda: %s - %s\n", humanize.Bytes(uint64(minMem)), humanize.Bytes(uint64(maxMem)))
	syslog.Printf("Chunks per lambda: %d - %d\n", int(minChunks), int(maxChunks))
	syslog.Printf("Set %d, Got %d, Reset %d\n", set, got, reset)
	syslog.Printf("Active Minutes %d\n", activated)
	syslog.Printf("BalancerCost: %s(%s per request)", balancerCost, balancerCost/time.Duration(read-options.Skip))
	syslog.Printf("Max concurrency: %d, clients initialized: %d\n", maxConcurrency, atomic.LoadInt32(&numClients))
	for _, msg := range reader.Report() {
		syslog.Println(msg)
	}

	clients.Close()
}

func finalize(opts *FinalizeOptions) {
	opts.once.Do(func() {
		if opts.closeNanolog {
			nanolog.Flush()
			opts.closeNanolog = false
		}

		if opts.traceFile != nil {
			opts.traceFile.Close()
			opts.traceFile = nil
		}
	})
}

//logCreate create the nanoLog
func logCreate(opts *Options) error {
	// Set up nanoLog writer
	path := opts.File + "_playback.clog"
	nanoLogout, err := os.Create(path)
	if err != nil {
		return err
	}
	err = nanolog.SetWriter(nanoLogout)
	if err != nil {
		return err
	}

	client.SetLogger(nanolog.Log)

	return nil
}

func MaxInt32(a int32, b int32) int32 {
	if a < b {
		return b
	} else {
		return a
	}
}
