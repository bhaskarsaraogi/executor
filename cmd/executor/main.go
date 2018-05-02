package main

import (
	"github.com/bhaskarsaraogi/executor"
	"log"
	"runtime"
	"time"
)

type dummyJobType struct {

}

// implement Job interface
// the argument is variadic to provide flexibility to add external params needed for execution of job
func (d dummyJobType) Execute(...struct{}) (*struct{}, error) {
	//log.Println("Task executed")
	return nil, nil
}

func main()  {
	ex := executor.NewExecutor(1)
	ex.Run()
	quit := make(chan struct{})

	// Go through each job and queue the individually for the job to be executed
	go spawnJobs(ex, quit)

	info()

	ex.ReScale(2)
	info()

	ex.ReScale(4)
	info()

	ex.ReScale(8)
	info()

	ex.ReScale(6)
	info()

	ex.ReScale(3)
	info()

	ex.ReScale(1)
	info()

	quit <- struct{}{}
	ex.Abort()

	log.Println("Sent quit signal")
}
func info() {
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second * 4)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
}

func spawnJobs(ex *executor.Executor, quit <-chan struct{}) {
	for {
		// Push the job onto the queue.
		ex.QueueJob(new(dummyJobType))
		//time.Sleep(10*time.Millisecond)

		select {
			// encountered quit signal to stop spawning jobs
			case <-quit:
				return
			// continue spawning jobs
			case <-time.After(10 * time.Millisecond):
		}

	}
}
