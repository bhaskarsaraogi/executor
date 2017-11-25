package main

import (
	"log"
)

type Payload struct {
}


func (p *Payload) Execute() error {
	return nil
}

// Job represents the job to be run
type Job struct {
	Payload Payload
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

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
				if err := job.Payload.Execute(); err != nil {
					log.Println("Error executing job: %s", err.Error())
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

func (d *Executor) Run() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		d.Workers = append(d.Workers, worker)
		worker.Start()
	}

	go d.dispatchJob()
}

func (d *Executor) Abort() {

	// Stop all workers
	for _, Worker := range d.Workers {
		Worker.Stop()
	}
}

func (d *Executor) dispatchJob() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatchJob the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}

func main()  {
	executor := NewExecutor(8)
	executor.Run()

	var Payloads []Payload

	// Go through each payload and queue items individually for the jon to be executed
	for _, payload := range Payloads {

		// let's create a job with the payload
		work := Job{Payload: payload}

		// Push the work onto the queue.
		JobQueue <- work
	}

}

