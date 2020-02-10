package proxy

import (
	syslog "log"
	"fmt"
	"github.com/mason-leap-lab/infinicache/proxy/server"
	"github.com/mason-leap-lab/infinicache/proxy/lambdastore"
	"sync"
)

type LRUPlacer struct {
	proxy        *Proxy
	backend      *server.Placer
	queues       []chan interface{}

	served       sync.WaitGroup
}

func (lru *LRUPlacer) SetProxy(p *Proxy) {
	lru.proxy = p
}

func (lru *LRUPlacer) Init() {
	numCluster := len(lru.proxy.LambdaPool)

	group := server.NewGroup(numCluster)
	for i := 0; i < numCluster; i++ {
		ins := lambdastore.NewInstance("SimInstance", uint64(i), false)
		ins.Meta.Capacity = lru.proxy.LambdaPool[i].Capacity
		ins.Meta.IncreaseSize(int64(lru.proxy.LambdaPool[i].MemUsed))
		group.Set(group.Reserve(i, ins))
	}
	lru.backend = server.NewPlacer(server.NewMataStore(), group)

	lru.queues = make([]chan interface{}, numCluster)
	for i := 0; i < numCluster; i++ {
		lru.queues[i] = make(chan interface{})
		lru.served.Add(1)
		go lru.simServe(lru.queues[i], &lru.served)
	}
}

func (lru *LRUPlacer) Remap(placements []int, obj *Object) []int {
	numCluster := len(lru.proxy.LambdaPool)


	var remapped sync.WaitGroup
	for i, lambdaId := range placements {
		remapped.Add(1)
		lru.queues[lambdaId] <- func(m *server.Meta) func() {
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
		}(lru.backend.NewMeta(obj.Key, numCluster, len(placements), i, lambdaId, int64(obj.ChunkSz)))
	}

	remapped.Wait()
	meta, _ := lru.backend.Get(obj.Key, 0)
	copy(placements, meta.Placement)
	return placements
}

func (lru *LRUPlacer) Adapt(_ int, _ *Chunk) {

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

func (lru *LRUPlacer) dropEvicted(meta *server.Meta) {
	// delete(lru.proxy.Placements, meta.Key)
	for i, lambdaId := range meta.Placement {
		chk := lru.proxy.LambdaPool[lambdaId].Kvs[fmt.Sprintf("%d@%s", i, meta.Key)]

		lru.proxy.LambdaPool[lambdaId].MemUsed -= chk.Sz
		delete(lru.proxy.LambdaPool[lambdaId].Kvs, chk.Key)

		lru.proxy.Evicts[chk.Key] = chk
	}
}
