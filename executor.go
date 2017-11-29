package executor

import (
	"log"
	"github.com/pkg/errors"
	"io"
	"fmt"
	"crypto/rand"
)


// Job represents the job to be run
type Job interface {
	Execute(params ...struct{}) error
}

// A buffered channel that we can send work requests on.
var JobQueue = make(chan Job)

// Worker represents the worker that executes the job
type Worker struct {
	Id string
	WorkerPool  chan chan Job
	JobChannel  chan Job
	quit    	chan bool
}

func NewWorker(id string, workerPool chan chan Job) *Worker {
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
				// we have received a work request.
				if err := job.Execute(); err != nil {
					log.Printf("Error executing job: %s\n", err.Error())
				}

			case <-w.quit:
				// we have received a signal to stop
				close(w.JobChannel)
				return
			}
		}
	}()
}

func (w *Worker) String() string {
	return w.Id
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		log.Println("Stopping: ", w.Id)
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
		worker := NewWorker(newUUID(), e.WorkerPool)
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
			go e.pushToWorker(job)
		}
	}
}

func (e *Executor) pushToWorker(job Job) {

	// recover from panic if job channel is closed while pushing the job
	defer func() {
		if err := recover(); err!=nil {
			log.Println("Error occured pushing to job channel, retry!")
			e.pushToWorker(job)
		}
	}()

	// try to obtain a worker job channel that is available.
	// this will block until a worker is idle
	jobChannel := <-e.WorkerPool

	// dispatchJob the job to the worker job channel
	jobChannel <- job
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

			log.Println("Scaling up workers")

			for i := e.WorkerCount; i < e.WorkerCount+ (workers- e.WorkerCount); i++ {
				worker := NewWorker(newUUID(), e.WorkerPool)
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
		e.WorkerCount = workers

	}
	return nil
}


// newUUID generates a random UUID according to RFC 4122
func newUUID() string {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return newUUID()
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
}
