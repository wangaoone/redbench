package proxy

import (
	"container/heap"
	syslog "log"
	"fmt"
)

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Lambda

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].MemUsed < pq[j].MemUsed
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].block = i
	pq[j].block = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	lambda := x.(*Lambda)
	lambda.block = n
	*pq = append(*pq, lambda)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	lambda := old[n-1]
	old[n-1] = nil  // avoid memory leak
	lambda.block = -1
	*pq = old[0 : n-1]
	return lambda
}

type PriorityBalancer struct {
	proxy        *Proxy
	minority     PriorityQueue
}

func (b *PriorityBalancer) SetProxy(p *Proxy) {
	b.proxy = p
}

func (b *PriorityBalancer) Init() {
	b.minority = make(PriorityQueue, len(b.proxy.LambdaPool))
	for j := 0; j < len(b.proxy.LambdaPool); j++ {
		b.minority[j] = &b.proxy.LambdaPool[j]
		b.minority[j].block = j
	}
}

func (b *PriorityBalancer) Remap(placements []int, _ *Object) []int {
	for i, placement := range placements {
		// Mapping to lambda in nextGroup
		if b.minority[i].MemUsed < b.minority[placement].MemUsed {
			placements[i] = b.minority[i].Id
		}
	}
	return placements
}

func (b *PriorityBalancer) Adapt(lambdaId int, _ *Chunk) {
	heap.Fix(&b.minority, b.proxy.LambdaPool[lambdaId].block)
	// b.dump()
}

func (b *PriorityBalancer) dump() {
	msg := "[%d:%d]%s"
	for _, lambda := range b.minority {
		msg = fmt.Sprintf(msg, lambda.Id, lambda.MemUsed, ",[%d:%d]%s")
	}
	syslog.Printf(msg, 0, 0, "\n")
}
