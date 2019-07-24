package main

import (
	"flag"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"log"
	"time"
)

func main() {
	sz := flag.Int("sz", 128, "object data size")
	d := flag.Int("d", 4, "number of data shards for RS erasure coding")
	p := flag.Int("p", 2, "number of parity shards for RS erasure coding")
	g := flag.Int("g", 32, "max number of goroutines for RS erasure coding")
	flag.Parse()

	log.Println("chunk sz:", *sz, "# data shards:", *d, "# parity shards:", *p, "# max goroutines:", *g)

	enc, err := reedsolomon.New(*d, *p, reedsolomon.WithMaxGoroutines(*g))

	arrRange := *d + *p
	data := make([][]byte, arrRange)
	// Create all shards, size them at 50000 each
	for i := 0; i < arrRange; i++ {
		data[i] = make([]byte, *sz)
	}

	// Fill some data into the data shards
	for i, in := range data[:*d] {
		for j := range in {
			in[j] = byte((i + j) & 0xff)
		}
	}

	// Encoding phase
	err = enc.Encode(data)
	if err != nil {
		fmt.Println("encoding encode err", err)
		return
	}
	ok, err := enc.Verify(data)
	if err != nil {
		fmt.Println("encoding verify failed", err)
		return
	}

	data[0] = nil
	data[1] = nil
	//data[2] = nil
	//data[2][0] = 54321

	// Decoding phase
	t0 := time.Now()
	ok, err = enc.Verify(data)
	if ok {
		fmt.Println("No reconstruction needed")
	} else {
		log.Println("First Verify() takes:", time.Since(t0))
		fmt.Println("Verification failed. Reconstructing data")
		t1 := time.Now()
		err = enc.Reconstruct(data)
		if err != nil {
			fmt.Println("Reconstruct failed -", err)
		}
		log.Println("Reconstruct() takes:", time.Since(t1))
		t2 := time.Now()
		ok, err = enc.Verify(data)
		if !ok {
			fmt.Println("Verification failed after reconstruction, data likely corrupted.")
		}
		if err != nil {
			fmt.Println(err)
		}
		log.Println("Verify() after Reconstruct() takes:", time.Since(t2))
		log.Println("The whole decoding phase takes:", time.Since(t1))
	}
	//fmt.Println(ok)
	//}
}
