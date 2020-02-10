package proxy

import (
	// syslog "log"
	"math"
)

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

func (b *WeightedBalancer) Remap(placements []int, _ *Object) []int {
	for i, placement := range placements {
		// Mapping to lambda in nextGroup
		placements[i] = b.lambdaBlocks[b.nextGroup * 100 + placement]
	}
	b.nextGroup = int(math.Mod(float64(b.nextGroup + 1), 100))
	return placements
}

func (b *WeightedBalancer) Adapt(j int, _ *Chunk) {
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
