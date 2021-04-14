package proxy

type ProxyBalancer interface {
	// Set proxy
	SetProxy(*Proxy)

	// Init balancer
	Init()

	// Remap random placement to actual placement
	Remap([]uint64, *Object) []uint64

	// Call on apply placement
	Adapt(uint64, *Chunk)

	// Validate if the object is evicted.
	Validate(*Object) bool

	// Clean up
	Close()
}
