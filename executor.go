package executor

import (
	"log"
	"github.com/pkg/errors"
)


// Job represents the job to be run
type Job interface {
	Execute(params ...struct{}) error
}

// A buffered channel that we can send work requests on.
var JobQueue = make(chan Job)

// Worker represents the worker that executes the job
type Worker struct {
	Id int
	WorkerPool  chan chan Job
	JobChannel  chan Job
	quit    	chan bool
}

func NewWorker(id int, workerPool chan chan Job) *Worker {
	return &Worker{
		Id: id,
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				log.Println("recvd job to execute")
				// we have received a work request.
				if err := job.Execute(); err != nil {
					log.Printf("Error executing job: %s\n", err.Error())
				}

			case <-w.quit:
				// we have received a signal to stop
				log.Println("recvd quit signal on worker")
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

// Core executor struct, has a WorkerPool channel of all currently available workers as all active workers would have published there JobChannel into the WorkerPool to get next job
type Executor struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool  chan chan Job
	WorkerCount int
	Workers     []*Worker
}

// Create new executor instance
// maxWorkers specify the number of workers/agents(underneath goroutines) one would like to spawn, general idea to have as many as the number of cpu available
func NewExecutor(workers int) *Executor {
	pool := make(chan chan Job, workers)
	return &Executor{WorkerPool: pool, WorkerCount: workers}
}

// Start all workers on executor
func (e *Executor) Run() {
	// starting n number of workers
	for i := 1; i <= e.WorkerCount; i++ {
		worker := NewWorker(i, e.WorkerPool)
		e.Workers = append(e.Workers, worker)
		worker.Start()
	}

	go e.dispatchJob()
}


// Stop all workers on execute
func (e *Executor) Abort() {

	// Stop all workers
	for _, worker := range e.Workers {
		log.Println("Stopping worker")
		worker.Stop()
	}
}

// background job to listen for new incoming jobs on JobQueue and dispatch them to a available worker from WorkerPool
func (e *Executor) dispatchJob() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-e.WorkerPool

				// dispatchJob the job to the worker job channel
				jobChannel <- job
				log.Println("pushed to job channel")
			}(job)
		}
	}
}

// push job to the global JobQueue or bus
func (e *Executor) QueueJob(job Job)  {
	JobQueue <- job
}

// rescale the executor worker pool
func (e *Executor) ReScale(workers int) error {
	// TODO add any given point of time not more than one call to this should be allowed

	if workers <= 0 {
		return errors.New("Inavlid value for workers")
	} else if e.WorkerCount != workers {

		if e.WorkerCount < workers {
			// add workers as current workers is less than max workers
			for i := e.WorkerCount; i <= e.WorkerCount+ (workers- e.WorkerCount); i++ {
				worker := NewWorker(i, e.WorkerPool)
				e.Workers = append(e.Workers, worker)
				worker.Start()
			}

		} else {
			// reduce worker from here
			for i := 0; i < workers - e.WorkerCount; i++ {
				e.Workers[0].Stop()
				e.Workers = append(e.Workers[:i], e.Workers[i+1:]...)
			}

		}

		// set the new worker count after resize
		e.WorkerCount = workers

	}
	return nil
}

