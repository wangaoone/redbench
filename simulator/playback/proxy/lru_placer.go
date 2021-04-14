package proxy

import (
	"fmt"
	syslog "log"
	"sync"

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
}

func (lru *LRUPlacer) SetProxy(p *Proxy) {
	lru.proxy = p
}

func (lru *LRUPlacer) Init() {
	numCluster := len(lru.proxy.LambdaPool)

	group := cluster.NewGroup(numCluster)
	for i := group.StartIndex(); i < group.EndIndex(); i = i.Next() {
		ins := lambdastore.NewInstance("SimInstance", uint64(i))
		ins.Meta.Capacity = lru.proxy.LambdaPool[i].Capacity
		ins.Meta.IncreaseSize(int64(lru.proxy.LambdaPool[i].MemUsed))
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
	numCluster := len(lru.proxy.LambdaPool)

	var remapped sync.WaitGroup
	for i, lambdaId := range placements {
		remapped.Add(1)
		lru.queues[lambdaId] <- func(m *metastore.Meta) func() {
			return func() {
				// Uncomment this to simulate proxy without eviction tracks.
				// _, _, _ = lru.backend.GetOrInsert(m.Key, m)

				// Uncomment this to simulate proxy with eviction tracks.
				_, _, postProcess := lru.backend.GetOrInsert(m.Key, m)
				if postProcess != nil {
					postProcess(lru.dropEvicted)
				}

				remapped.Done()
			}
		}(lru.backend.NewMeta(obj.Key, int64(numCluster), obj.DChunks, obj.PChunks, i, int64(obj.ChunkSz), uint64(placements[i]), numCluster))
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
		chk, _ := lru.proxy.LambdaPool[lambdaId].GetChunk(fmt.Sprintf("%d@%s", i, meta.Key))

		lru.proxy.LambdaPool[lambdaId].MemUsed -= chk.Sz
		lru.proxy.LambdaPool[lambdaId].DelChunk(chk.Key)

		lru.proxy.Evict(chk.Key, chk)
	}
}

// metastore.ClusterManager implementation
func (lru *LRUPlacer) GetActiveInstances(int) []*lambdastore.Instance {
	return lru.instances
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
