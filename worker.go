package executor

import (
	"github.com/google/uuid"
	"log"
)

// Worker represents the worker that executes the job
type Worker struct {
	Id uuid.UUID // unique Id of every worker
	WorkerPool  WorkerPool // represents the worker pool this worker belongs to
	JobChannel  JobChannel // jobchannel of worker on which it receives job to execute
	quit    	chan struct{} // channel to notify worker to stop
}

// Create new worker using provided UUID, and the worker pool it wants to be part of
func NewWorker(workerPool WorkerPool) *Worker {
	uuid, _ := uuid.NewUUID()
	return &Worker{
		Id: uuid,
		WorkerPool: workerPool,
		JobChannel: make(JobChannel),
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
				if _, err := job.Execute(); err != nil {
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

// Worker to string, essentially worker Id to string
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