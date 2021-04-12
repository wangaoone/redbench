package helpers

import (
	"container/heap"
	"log"
	"runtime"
	"sync"
	"time"
)

type TimeSkipNotification struct {
	id       int64
	timeout  int64
	notifier sync.WaitGroup
	skipped  sync.WaitGroup
}

func (n *TimeSkipNotification) mark(id int64, ts int64) {
	n.id = id
	n.timeout = ts
	n.notifier.Add(1)
}

func (n *TimeSkipNotification) skip() {
	n.skipped.Add(1)
	n.notifier.Done()
}

func (n *TimeSkipNotification) Wait() {
	n.notifier.Wait()
	n.skipped.Done()
}

func (n *TimeSkipNotification) waitSkipped() {
	n.skipped.Wait()
}

// A PriorityQueue implements heap.Interface and holds Items.
type Heap []*TimeSkipNotification

func (h Heap) Len() int {
	return len(h)
}

func (h Heap) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return h[i].timeout < h[j].timeout
}

func (h Heap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *Heap) Push(x interface{}) {
	*h = append(*h, x.(*TimeSkipNotification))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	ret := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return ret
}

func (h Heap) Peak() *TimeSkipNotification {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

type TimeSkipper struct {
	pool        *sync.Pool
	heap        Heap
	mu          sync.Mutex
	batchBuffer []*TimeSkipNotification
}

func NewTimeSkipper(cap int) *TimeSkipper {
	return &TimeSkipper{
		pool: &sync.Pool{
			New: func() interface{} {
				return &TimeSkipNotification{}
			},
		},
		heap:        make(Heap, 0, cap),
		batchBuffer: make([]*TimeSkipNotification, 0, cap),
	}
}

func (s *TimeSkipper) MarkTimestamp(id int64, ts int64) *TimeSkipNotification {
	n := s.pool.Get().(*TimeSkipNotification)
	n.mark(id, ts)
	// log.Printf("Wait add %d:%d\n", n.id, n.timeout)

	s.mu.Lock()
	heap.Push(&s.heap, n)
	s.mu.Unlock()

	return n
}

func (s *TimeSkipper) Mark(id int64, to time.Time) *TimeSkipNotification {
	return s.MarkTimestamp(id, to.UnixNano())
}

func (s *TimeSkipper) MarkDuration(id int64, d time.Duration) *TimeSkipNotification {
	return s.Mark(id, time.Now().Add(d))
}

func (s *TimeSkipper) Skip(skip time.Duration) {
	s.SkipTo(time.Now().Add(skip))
}

func (s *TimeSkipper) SkipTo(planned time.Time) {
	s.mu.Lock()
	s.batchBuffer = s.batchBuffer[:0]
	for n := s.heap.Peak(); n != nil && n.timeout <= planned.UnixNano(); n = s.heap.Peak() {
		heap.Pop(&s.heap)
		// log.Printf("Wait done %d:%d\n", n.id, n.timeout)
		n.skip()
		s.batchBuffer = append(s.batchBuffer, n)
	}
	s.mu.Unlock()

	// Give WaitGroup.Wait() chance to run, so we can reuse the TimeSkipNotification
	runtime.Gosched()
	for _, n := range s.batchBuffer {
		n.waitSkipped()
		s.pool.Put(n)
	}
}

func (s *TimeSkipper) SkipAll() time.Duration {
	now := time.Now().UnixNano()
	skippedTo := now

	s.mu.Lock()
	for _, n := range s.heap {
		if skippedTo < n.timeout {
			skippedTo = n.timeout
		}
		log.Printf("Wait done all %d\n", n.timeout)
		n.skip()
	}
	s.mu.Unlock()

	// Give WaitGroup.Wait() chance to run, so we can reuse the TimeSkipNotification
	runtime.Gosched()
	for _, n := range s.heap {
		n.waitSkipped()
		s.pool.Put(n)
	}
	s.heap = s.heap[:0]

	return time.Duration(skippedTo - now)
}
