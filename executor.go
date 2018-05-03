package executor

import (
	"log"
	"github.com/pkg/errors"
)

// Core executor struct, has a WorkerPool channel of all currently available workers as all active workers would have published there JobChannel into the WorkerPool to get next job
type Executor struct {
	// A pool of workers channels that are registered with the dispatcher
	JobQueue JobChannel // A channel that we can send work requests on
	JobWrapperChannel JobWrapperChannel // Channel on which all executed jobs are sent by all workers
	WorkerCount int
	Workers     []*Worker
}

// Create new executor instance
// maxWorkers specify the number of workers/agents(underneath goroutines) one would like to spawn, general idea to have as many as the number of cpu available
func NewExecutor(workers int) *Executor {
	return &Executor{JobQueue: make(JobChannel), JobWrapperChannel: make(JobWrapperChannel), WorkerCount: workers}
}

// Start all workers on executor
func (e *Executor) Run() {
	// starting n number of workers
	for i := 1; i <= e.WorkerCount; i++ {
		worker := NewWorker(&e.JobQueue, &e.JobWrapperChannel) // Get pointer to a worker instance
		e.Workers = append(e.Workers, worker) // Append worker instance pointer to slick of pointers to worker instances
		worker.Start()
	}
}


// Stop all workers on execute
// Gracefully stop all operation
func (e *Executor) Stop() {

	// Stop all workers
	for _, worker := range e.Workers {
		log.Println("Stopping worker")
		worker.Stop()
	}
}


// Stop all workers on execute
// Hard instant shutdown
// fixme
func (e *Executor) Abort() {

	// Stop all workers
	for _, worker := range e.Workers {
		log.Println("Stopping worker")
		worker.Stop()
	}
}

// push job to the global JobQueue or bus
func (e *Executor) QueueJob(job Job)  {
	e.JobQueue <- job
}

// rescale the executor worker pool
func (e *Executor) ReScale(workers int) error {
	// fixme add any given point of time not more than one call to this should be allowed, make it atomic

	if workers <= 0 {
		return errors.New("Inavlid value for workers")
	} else if e.WorkerCount != workers {

		if e.WorkerCount < workers {
			// add workers as current workers is less than max workers

			log.Println("Scaling up workers")

			for i := e.WorkerCount; i < e.WorkerCount+ (workers- e.WorkerCount); i++ {
				worker := NewWorker(&e.JobQueue, &e.JobWrapperChannel)
				e.Workers = append(e.Workers, worker)
				worker.Start()
			}

		} else {
			// reduce worker from here

			log.Println("Scaling down workers")

			for i := 0; i < e.WorkerCount - workers; i++ {
				e.Workers[i].Stop()
			}

			var newWorkers []*Worker
			for i:= e.WorkerCount-workers; i < e.WorkerCount; i++  {
				newWorkers = append(newWorkers, e.Workers[i])
			}
			e.Workers = newWorkers

		}

		// set the new worker count after resize
		// fixme should be atomic with above for loop isnt it ?
		e.WorkerCount = workers

	}
	return nil
}

