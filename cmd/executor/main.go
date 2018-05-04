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
	time.Sleep(time.Millisecond)
	return nil, nil
}

func main()  {
	ex := executor.NewExecutor(1)
	ex.Run()
	quit := make(chan struct{})

	jobCount := 0
	jobExecutedCount := 0

	// Go through each job and queue the individually for the job to be executed
	go spawnJobs(ex, &quit, &jobCount)
	go consumeExecutedJobs(ex, &jobExecutedCount)

	info(&jobCount, &jobExecutedCount)

	ex.ReScale(2)
	info(&jobCount, &jobExecutedCount)

	ex.ReScale(4)
	info(&jobCount, &jobExecutedCount)

	ex.ReScale(8)
	info(&jobCount, &jobExecutedCount)

	ex.ReScale(6)
	info(&jobCount, &jobExecutedCount)

	ex.ReScale(3)
	info(&jobCount, &jobExecutedCount)

	ex.ReScale(1)
	info(&jobCount, &jobExecutedCount)

	close(quit)
	log.Println("Quit spawning jobs")
	//ex.Stop()
	ex.Abort()

	log.Println("Sent quit signal")

	time.Sleep(100*time.Millisecond)

	log.Printf("Total jobs queued %d\n", jobCount)

	log.Printf("Total jobs executed %d\n", jobExecutedCount)
}
func info(jobCount *int, jobExecutedCount *int) {
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	time.Sleep(time.Second * 3)
	log.Println("GOROUTINES: ", runtime.NumGoroutine())
	log.Printf("Total jobs queued %d\n", *jobCount)
	log.Printf("Total jobs executed %d\n", *jobExecutedCount)
}

func spawnJobs(ex *executor.Executor, quit *chan struct{}, jobCount *int) {
	for {
		// Push the job onto the queue.
		err := ex.QueueJob(new(dummyJobType))

		if err != nil {
			//log.Println("Error queuing jobs")
		} else {
			*jobCount++
		}


		select {
			// encountered quit signal to stop spawning jobs
			case <-*quit:
				return
			// continue spawning jobs
			case <-time.After(time.Nanosecond):
		}

	}
}

func consumeExecutedJobs(ex *executor.Executor, jobExecutedCount *int) {
	for range ex.JobWrapperChannel {
		*jobExecutedCount++
	}
}