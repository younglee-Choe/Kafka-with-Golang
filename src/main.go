package main

import (
	"main/src/producer"
	"main/src/producer2"
	"main/src/producer3"
	"main/src/producer4"
	"sync"
)

func callProducers() {
	// generate WaitGroup
	// Used to synchronize goroutines
	var wg sync.WaitGroup

	wg.Add(4)

	go func() {
		defer wg.Done() // signal the termination of a goroutine
		producer.Producer()
	}()

	go func() {
		defer wg.Done()
		producer2.Producer()
	}()

	go func() {
		defer wg.Done()
		producer3.Producer()
	}()

	go func() {
		defer wg.Done()
		producer4.Producer()
	}()

	wg.Wait()
}

func main() {
	callProducers()
}
