package proxy

import (
	"fmt"
	syslog "log"
	"sync"
	"sync/atomic"

	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"github.com/mason-leap-lab/infinicache/proxy/server/cluster"
	"github.com/mason-leap-lab/infinicache/proxy/server/metastore"
	"github.com/mason-leap-lab/infinicache/proxy/types"
)

type LRUPlacer struct {
	proxy   *Proxy
	backend *metastore.LRUPlacer
	queues  []chan interface{}

	served    sync.WaitGroup
	instances []*lambdastore.Instance
	sliceBase uint64
}

func (lru *LRUPlacer) SetProxy(p *Proxy) {
	lru.proxy = p
}

func (lru *LRUPlacer) Init() {
	numCluster := len(lru.proxy.LambdaPool)

	group := cluster.NewGroup(numCluster)
	for i := group.StartIndex(); i < group.EndIndex(); i = i.Next() {
		ins := lambdastore.NewInstance("SimInstance", uint64(i))
		// Update capacity, overhead should be consistent between infinicache configuration and simulator.
		ins.Meta.ResetCapacity(lru.proxy.LambdaPool[i].Capacity, lru.proxy.LambdaPool[i].MemUsed)
		group.Set(group.Reserve(i, ins))
	}
	lru.instances = group.All()
	lru.backend = metastore.NewLRUPlacer(metastore.New(), lru)

	lru.queues = make([]chan interface{}, numCluster)
	for i := 0; i < numCluster; i++ {
		lru.queues[i] = make(chan interface{})
		lru.served.Add(1)
		go lru.simServe(lru.queues[i], &lru.served)
	}
}

func (lru *LRUPlacer) Remap(placements []uint64, obj *Object) []uint64 {
	var remapped sync.WaitGroup
	for i, lambdaId := range placements {
		remapped.Add(1)
		lru.queues[lambdaId] <- func(m *metastore.Meta) func() {
			return func() {
				// Uncomment this to simulate proxy without eviction tracks.
				// _, _, _ = lru.backend.GetOrInsert(m.Key, m)

				// Uncomment this to simulate proxy with eviction tracks.
				_, postProcess, _ := lru.backend.Insert(m.Key, m)
				if postProcess != nil {
					postProcess(lru.dropEvicted)
				}

				remapped.Done()
			}
		}(lru.backend.NewMeta(obj.Key, int64(obj.Size), obj.DChunks, obj.PChunks, i, int64(obj.ChunkSz), uint64(placements[i]), SLICE_SIZE))
	}

	remapped.Wait()
	meta, _ := lru.backend.Get(obj.Key, 0)
	copy(placements, meta.Placement)
	return placements
}

func (lru *LRUPlacer) Adapt(_ uint64, _ *Chunk) {

}

func (lru *LRUPlacer) Validate(obj *Object) bool {
	meta, ok := lru.backend.Get(obj.Key, 0)
	return !ok || !meta.Deleted
}

func (lru *LRUPlacer) Close() {
	numCluster := len(lru.proxy.LambdaPool)
	for i := 0; i < numCluster; i++ {
		close(lru.queues[i])
	}
	lru.served.Wait()
}

func (lru *LRUPlacer) simServe(incomes chan interface{}, done *sync.WaitGroup) {
	for income := range incomes {
		switch m := income.(type) {
		case func():
			m()
		default:
			syslog.Fatal("LRUPlacer.simServe: Unsupported type")
		}
	}
	done.Done()
}

func (lru *LRUPlacer) dropEvicted(meta *metastore.Meta) {
	// delete(lru.proxy.Placements, meta.Key)
	for i, lambdaId := range meta.Placement {
		chk, ok := lru.proxy.LambdaPool[lambdaId].DelChunk(fmt.Sprintf("%d@%s", i, meta.Key))
		if ok {
			lru.proxy.Evict(chk.Key, chk)
			lru.proxy.ClearPlacements(meta.Key)
		}
	}
}

// metastore.ClusterManager implementation
func (lru *LRUPlacer) GetActiveInstances(int) []*lambdastore.Instance {
	return lru.instances
}

func (lru *LRUPlacer) GetSlice(size int) metastore.Slice {
	return cluster.NewSlice(size, lru.nextSlice)
}

func (lru *LRUPlacer) Trigger(int, ...interface{}) {
	// do nothing
}

func (lru *LRUPlacer) Instance(id uint64) *lambdastore.Instance {
	return lru.instances[id]
}

func (lru *LRUPlacer) Recycle(ins types.LambdaDeployment) error {
	return cluster.ErrUnsupported
}

func (lru *LRUPlacer) nextSlice(sliceSize int) (int, int) {
	return int((atomic.AddUint64(&lru.sliceBase, uint64(sliceSize)) - uint64(sliceSize)) % uint64(len(lru.instances))), len(lru.instances)
}
