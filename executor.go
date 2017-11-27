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
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				if err := job.Execute(); err != nil {
					log.Printf("Error executing job: %s\n", err.Error())
				}

			case <-w.quit:
				// we have received a signal to stop
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


type Executor struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	MaxWorkers int
	Workers []Worker
}

func NewExecutor(maxWorkers int) *Executor {
	pool := make(chan chan Job, maxWorkers)
	return &Executor{WorkerPool: pool, MaxWorkers: maxWorkers}
}

func (e *Executor) Run() {
	// starting n number of workers
	for i := 0; i < e.MaxWorkers; i++ {
		worker := NewWorker(e.WorkerPool)
		e.Workers = append(e.Workers, worker)
		worker.Start()
	}

	go e.dispatchJob()
}

func (e *Executor) Abort() {

	// Stop all workers
	for _, Worker := range e.Workers {
		Worker.Stop()
	}
}

func (e *Executor) dispatchJob() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			log.Println("recvd job to dispatch")
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-e.WorkerPool

				// dispatchJob the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

func (e *Executor) QueueJob(job Job)  {
	JobQueue <- job
}



