package main

import (
	"github.com/bhaskarsaraogi/executor"
	"log"
	"time"
)

type dummyJobType struct {

}

// implement Job interface
// the argument is variadic to provide flexibility to add external params needed for execution of job
func (d dummyJobType) Execute(...struct{}) error {
	log.Println("Task executed")
	return nil
}

func main()  {
	ex := executor.NewExecutor(8)
	ex.Run()

	log.Println("Creating payloads")

	// Go through each job and queue the individually for the job to be executed
	go spawnJobs(ex)

	time.Sleep(time.Millisecond * 150)
	ex.Abort()

	log.Println("Sent quit signal")

	time.Sleep(time.Millisecond * 250)

}

func spawnJobs(ex *executor.Executor) {
	for {
		// Push the job onto the queue.
		ex.QueueJob(new(dummyJobType))
	}
}
