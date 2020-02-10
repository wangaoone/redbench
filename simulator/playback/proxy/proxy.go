package proxy

import (
	"time"
)

type Chunk struct {
	Key  string
	Sz   uint64
	Freq uint64
	Reset uint64
}

type Object struct {
	Key       string
	Sz        uint64
	ChunkSz   uint64
	Time      time.Time
}

type Lambda struct {
	Id      int
	Kvs     map[string]*Chunk
	MemUsed uint64
	ActiveMinites int
	LastActive time.Time
	Capacity uint64
	UsedPercentile int

	block    int
	blocks   []int
}

func (l *Lambda) Activate(recTime time.Time) {
	if l.ActiveMinites == 0  {
		l.ActiveMinites++
	} else if recTime.Sub(l.LastActive) >= time.Minute {
		l.ActiveMinites++
	}
	l.LastActive = recTime
}

type Proxy struct {
	Id         string
	LambdaPool []Lambda
	Evicts     map[string]*Chunk
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

func (p *Proxy) Remap(placements []int, obj *Object) []int {
	if p.Balancer == nil {
		return placements
	}

	p.Balancer.SetProxy(p)
	return p.Balancer.Remap(placements, obj)
}

func (p *Proxy) Adapt(lambdaId int, chk *Chunk) {
	if p.Balancer == nil {
		return
	}

	p.Balancer.SetProxy(p)
	start := time.Now()
	p.Balancer.Adapt(lambdaId, chk)
	p.BalancerCost += time.Since(start)
}

func (p *Proxy) Validate(obj *Object) bool {
	if p.Balancer == nil {
		return true
	}

	p.Balancer.SetProxy(p)
	return p.Balancer.Validate(obj)
}

func (p *Proxy) Close() {
	if p.Balancer == nil {
		return
	}

	p.Balancer.SetProxy(p)
	p.Balancer.Close()
}
