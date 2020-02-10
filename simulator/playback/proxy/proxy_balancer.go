package proxy

type ProxyBalancer interface {
	// Set proxy
	SetProxy(*Proxy)

	// Init balancer
	Init()

	// Remap random placement to actual placement
	Remap([]int, *Object) []int

	// Call on apply placement
	Adapt(int, *Chunk)

	// Validate if the object is evicted.
	Validate(*Object) bool

	// Clean up
	Close()
}
