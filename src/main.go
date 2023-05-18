package main

import (
	"sync"
	"main/producer"
	"main/producer2"
	"main/producer3"
)

func main() {
	// generate WaitGroup
	// Used to synchronize goroutines
	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()		// signal the termination of a goroutine
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

	wg.Wait()
}