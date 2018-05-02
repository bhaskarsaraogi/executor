package executor

import (
	"github.com/google/uuid"
	"log"
)

// Worker represents the worker that executes the job
type Worker struct {
	Id uuid.UUID // unique Id of every worker
	WorkerPool  WorkerPool // fixme comment
	JobChannel  JobChannel // fixme comment
	quit    	chan struct{} // channel to notify worker to stop
}


func NewWorker(id uuid.UUID, workerPool WorkerPool) *Worker {
	return &Worker{
		Id: id,
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