package executor

import (
	"log"
)


// Job represents the job to be run
type Job interface {
	Execute(params ...struct{}) error
}

// A buffered channel that we can send work requests on.
var JobQueue = make(chan Job)

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool  chan chan Job
	JobChannel  chan Job
	quit    	chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
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
			log.Println("adding job channel to pool")
			w.WorkerPool <- w.JobChannel
			log.Println("added job channel to pool")

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
	WorkerPool chan chan Job
	MaxWorkers int
	Workers []Worker
}

// Create new executor instance
// maxWorkers specify the number of workers/agents(underneath goroutines) one would like to spawn, general idea to have as many as the number of cpu available
func NewExecutor(maxWorkers int) *Executor {
	pool := make(chan chan Job, maxWorkers)
	return &Executor{WorkerPool: pool, MaxWorkers: maxWorkers}
}

// Start all workers on executor
func (e *Executor) Run() {
	// starting n number of workers
	for i := 0; i < e.MaxWorkers; i++ {
		worker := NewWorker(e.WorkerPool)
		e.Workers = append(e.Workers, worker)
		worker.Start()
	}

	go e.dispatchJob()
}


// Stop all workers on execute
func (e *Executor) Abort() {

	// Stop all workers
	for _, Worker := range e.Workers {
		log.Println("Stopping worker")
		Worker.Stop()
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
				log.Println("recvd job waiting for job channel")
				jobChannel := <-e.WorkerPool
				log.Println("got job channel")

				// dispatchJob the job to the worker job channel
				log.Println("pushing to job channel")
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



