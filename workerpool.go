package main

import (
	"fmt"
	"time"
) 

// the purpose of this file:
//  1. create 5 different work in a channel
//  2. Create 3 different goroutines
//  3. Result --> 3 goroutines does 5 task

func work(id int, jobs <-chan int, result <-chan int) {
	for c : range jobs { 
		fmt.Println("worker ",id,"is executing job: ", c)
		time.Sleep(time.Second)
		fmt.Println("worker", id, "finished job", c)
		result <- c + 100 
		
	}
}

func main() {

	const numJobs = 5 
	jobs 	:= make(chan int, numJobs)
	result  := make(chan int, numJobs)

	for w = 0; w < 3; w++;{ 
		go work(w,jobs,result)
	}

	for i = 1; i <= numJobs; i++;{ 
		jobs <- i
	}
	close(jobs)
	

	// this loop is interesting, if the main dies then the main process will end, other processed also collaspes with main so we have to be careful with main
	// therefore, result waits for all the goroutine's work to complete and safely exits. 
	for a := 1; a <= numJobs; a++ {
        <-results
    }
}
