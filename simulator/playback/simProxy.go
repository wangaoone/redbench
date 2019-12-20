package main

import (
	"container/heap"
	"math"
	syslog "log"
	"fmt"
	"time"
)

type Proxy struct {
	Id         string
	LambdaPool []Lambda
	Placements map[string][]int
	Balancer   ProxyBalancer

	BalancerCost time.Duration
}

func (p *Proxy) Init() {
	if p.Balancer == nil {
		return
	}

	p.Balancer.SetProxy(p)
	p.Balancer.Init()
}

func (p *Proxy) Remap(placements []int) []int {
	if p.Balancer == nil {
		return placements
	}

	p.Balancer.SetProxy(p)
	return p.Balancer.Remap(placements)
}

func (p *Proxy) Adapt(j int) {
	if p.Balancer == nil {
		return
	}

	p.Balancer.SetProxy(p)
	start := time.Now()
	p.Balancer.Adapt(j)
	p.BalancerCost += time.Since(start)
}

type ProxyBalancer interface {
	SetProxy(*Proxy)
	Init()
	Remap([]int) []int
	Adapt(int)
}

type WeightedBalancer struct {
	proxy        *Proxy
	lambdaBlocks []int
	nextGroup    int
	nextLambda   int
}

func (b *WeightedBalancer) SetProxy(p *Proxy) {
	b.proxy = p
}

func (b *WeightedBalancer) Init() {
	b.lambdaBlocks = make([]int, 100 * len(b.proxy.LambdaPool))
	for j := 0; j < len(b.proxy.LambdaPool); j++ {
		b.proxy.LambdaPool[j].blocks = make([]int, 100)
	}
	idx := 0
	for i := 0; i < 100; i++ {
		for j := 0; j < len(b.proxy.LambdaPool); j++ {
			b.lambdaBlocks[idx] = j
			b.proxy.LambdaPool[j].blocks[i] = idx
			idx++
		}
	}
}

func (b *WeightedBalancer) Remap(placements []int) []int {
	for i, placement := range placements {
		// Mapping to lambda in nextGroup
		placements[i] = b.lambdaBlocks[b.nextGroup * 100 + placement]
	}
	b.nextGroup = int(math.Mod(float64(b.nextGroup + 1), 100))
	return placements
}

func (b *WeightedBalancer) Adapt(j int) {
	// Remove a block from lambda, and allocated to nextLambda
	l := &b.proxy.LambdaPool[j]
	for int(math.Floor(float64(l.MemUsed) / float64(l.Capacity) * 100)) > l.UsedPercentile {
//		syslog.Printf("Left blocks on lambda %d: %d", j, len(l.blocks))
		if len(l.blocks) == 0 {
			break
		}

		// Skip current lambda
		if b.nextLambda == j {
			b.nextLambda = int(math.Mod(float64(b.nextLambda + 1), float64(len(b.proxy.LambdaPool))))
		}

		// Get block idx to be reallocated
		reallocIdx := l.blocks[0]

		// Remove block from lambda
		l.blocks = l.blocks[1:]

		// Add blcok to next lambda
		nextL := &b.proxy.LambdaPool[b.nextLambda]
		nextL.blocks = append(nextL.blocks, reallocIdx)

		// Reset lambda at reallocIdx
		b.lambdaBlocks[reallocIdx] = b.nextLambda

		// Move on
		b.nextLambda = int(math.Mod(float64(b.nextLambda + 1), float64(len(b.proxy.LambdaPool))))

		l.UsedPercentile++
	}
}

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

func (b *PriorityBalancer) Remap(placements []int) []int {
	for i, placement := range placements {
		// Mapping to lambda in nextGroup
		if b.minority[i].MemUsed < b.minority[placement].MemUsed {
			placements[i] = b.minority[i].Id
		}
	}
	return placements
}

func (b *PriorityBalancer) Adapt(j int) {
	heap.Fix(&b.minority, b.proxy.LambdaPool[j].block)
	// b.dump()
}

func (b *PriorityBalancer) dump() {
	msg := "[%d:%d]%s"
	for _, lambda := range b.minority {
		msg = fmt.Sprintf(msg, lambda.Id, lambda.MemUsed, ",[%d:%d]%s")
	}
	syslog.Printf(msg, 0, 0, "\n")
}
