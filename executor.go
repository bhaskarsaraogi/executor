package executor

import (
	"log"
	"github.com/pkg/errors"
	"github.com/google/uuid"
)


// Job represents the job to be run
type Job interface {
	Execute(params ...struct{}) error
}

// A buffered channel that we can send work requests on.
var JobQueue = make(chan Job)

// Worker represents the worker that executes the job
type Worker struct {
	Id uuid.UUID // unique Id of every worker
	WorkerPool  chan chan Job // fixme comment
	JobChannel  chan Job // fixme comment
	quit    	chan struct{} // channel to notify worker to stop
}

func NewWorker(id uuid.UUID, workerPool chan chan Job) *Worker {
	return &Worker{
		Id: id,
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan struct{})}
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
	return w.Id.String()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		log.Println("Stopping: ", w.Id)
		close(w.quit)
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
		uuid, _ := uuid.NewUUID() // fixme error case in UUID generation
		worker := NewWorker(uuid, e.WorkerPool)
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
	// fixme its a bad construct but reason why we need this
	defer func() {
		if recover() != nil {
			log.Println("Error occured pushing to job channel, retry!")
			e.pushToWorker(job)
		}
	}()

	// try to obtain a worker job channel that is available.
	// this will block until a worker is idle
	// fixme what happens when all workers are blocked for a long time, maybe we need to add timeout on job/executor level ?
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
	// fixme add any given point of time not more than one call to this should be allowed

	if workers <= 0 {
		return errors.New("Inavlid value for workers")
	} else if e.WorkerCount != workers {

		if e.WorkerCount < workers {
			// add workers as current workers is less than max workers

			log.Println("Scaling up workers")

			for i := e.WorkerCount; i < e.WorkerCount+ (workers- e.WorkerCount); i++ {
				uuid, _ := uuid.NewUUID() // fixme error case in UUID generation
				worker := NewWorker(uuid, e.WorkerPool)
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

