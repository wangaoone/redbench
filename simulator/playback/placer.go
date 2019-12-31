package main

import (
	"github.com/wangaoone/LambdaObjectstore/lib/logger"
	"math"
)

type Placer struct {
	smallArr []*Object
	medArr   []*Object
	largeArr []*Object
	sTier    uint64
	lTier    uint64
	log      logger.ILogger
}

func NewPlacer() *Placer {
	placer := &Placer{
		sTier: 1024 * 1024,       // 1MB
		lTier: 1024 * 1024 * 200, // 200 MB
		log: &logger.ColorLogger{
			Prefix: "Placer ",
			Level:  logger.LOG_LEVEL_ALL,
			Color:  true,
		},
	}
	placer.smallArr = make([]*Object, 0, INIT_LRU_CAPACITY)
	placer.medArr = make([]*Object, 0, INIT_LRU_CAPACITY)
	placer.largeArr = make([]*Object, 0, INIT_LRU_CAPACITY)
	return placer
}

func (p *Placer) FindDelObj(tier string) *Object {
	//min := time.Now().Unix()
	var delObj *Object

	switch tier {
	case "s":
		delObj, p.smallArr = p.findOldestObj(p.smallArr)
		break
	case "m":
		delObj, p.medArr = p.findOldestObj(p.medArr)
		break
	case "l":
		delObj, p.largeArr = p.findOldestObj(p.largeArr)
		break
	}

	return delObj
}

func (p *Placer) FindPlacement(obj *Object, proxy *Proxy) []int {
	var placement []int
	enough := false
	// find obj based on tier and time
	//p.log.Debug("find del obj is %v,%v", delObj.Key, delPlacement)
	count := 0
	for !enough {
		count += 1

		oldestObj := p.FindDelObj(obj.Tier)
		delPlacement := proxy.Placements[oldestObj.Key]
		chunkSize := oldestObj.Sz / uint64(10)

		p.log.Debug("del placement is %v", delPlacement)

		for i, idx := range delPlacement {
			p.log.Debug("%v,%v, %v, %v", i, idx, proxy.LambdaPool[idx].MemUsed, delPlacement)
			//proxy.LambdaPool[idx].Kvs[oldestObj.Key] = nil
			//if proxy.LambdaPool[idx].MemUsed > 0 {
			proxy.LambdaPool[idx].MemUsed = proxy.LambdaPool[idx].MemUsed - chunkSize
			//}before delete
		}
		//time.Sleep(500 * time.Millisecond)
		for i, idx := range delPlacement {
			p.log.Debug("%v,%v,after delete %v, %v", i, idx, proxy.LambdaPool[idx].MemUsed, delPlacement)
			if proxy.LambdaPool[idx].MemUsed+chunkSize >= proxy.LambdaPool[idx].Capacity {
				enough = false
				p.log.Debug("not enough, break to next oldest obj")
				break
			}
			enough = true
		}
		delete(proxy.Placements, oldestObj.Key)
		//fmt.Print("11")
		placement = delPlacement
	}

	//p.log.Debug("Count is %v", count)
	return placement

}

//func (p *Placer) findTier(obj *Object) {
//	if obj.Sz < p.sTier {
//		obj.Tier = "s"
//	} else if obj.Sz < p.lTier {
//		obj.Tier = "m"
//	} else {
//		obj.Tier = "l"
//	}
//}

func (p *Placer) Append(obj *Object) {

	if obj.Sz < p.sTier {
		obj.Tier = "s"
	} else if obj.Sz < p.lTier {
		obj.Tier = "m"
	} else {
		obj.Tier = "l"
	}

	switch obj.Tier {
	case "s":
		p.smallArr = append(p.smallArr, obj)
		break
	case "m":
		p.medArr = append(p.medArr, obj)
		break
	case "l":
		p.largeArr = append(p.largeArr, obj)
		break
	}
}

func (p *Placer) findOldestObj(arr []*Object) (*Object, []*Object) {
	var oldestObj *Object
	var oldestIdx int

	p.log.Debug("arr length is %v", len(arr))
	min := int64(math.MaxInt64)

	for i, obj := range arr {
		if obj.ArriveTime < min {
			min = obj.ArriveTime
			oldestObj = obj
			oldestIdx = i
		}
	}
	p.log.Debug("oldest obj is %v,%v,%v", oldestObj.Key, oldestObj.Tier, oldestIdx)

	p.log.Debug("before remove length is %v", len(arr))
	arr = append(arr[:oldestIdx], arr[oldestIdx+1:]...)
	p.log.Debug("after remove length is %v", len(arr))
	return oldestObj, arr
}

//func remove(s []*Object, i int) []*Object {
//	s[len(s)-1], s[i] = s[i], s[len(s)-1]
//	return s[:len(s)-1]
//}
