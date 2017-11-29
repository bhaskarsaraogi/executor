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
func (d dummyJobType) Execute(...struct{}) error {
	//log.Println("Task executed")
	return nil
}

func main()  {
	ex := executor.NewExecutor(1)
	ex.Run()

	// Go through each job and queue the individually for the job to be executed
	go spawnJobs(ex)

	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second*10)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())

	ex.ReScale(2)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second*10)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())


	ex.ReScale(4)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second*10)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())

	ex.ReScale(8)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second*10)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())

	ex.ReScale(6)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second*10)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())

	ex.ReScale(3)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second*10)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())


	ex.ReScale(1)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second*10)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())


	ex.Abort()

	log.Println("Sent quit signal")
}

func spawnJobs(ex *executor.Executor) {
	for {
		// Push the job onto the queue.
		ex.QueueJob(new(dummyJobType))
	}
}
