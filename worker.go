package executor

import (
	"github.com/google/uuid"
	"log"
	"sync"
)

// Worker represents the worker that executes the job
type Worker struct {
	Id uuid.UUID // unique Id of every worker
	JobChannel  *JobChannel // jobchannel of worker on which it receives job to execute
	JobWrapperChannel *JobWrapperChannel
	quit    	chan struct{} // channel to notify worker to stop
}

// Create new worker using provided UUID, and the worker pool it wants to be part of
func NewWorker(jobChannel *JobChannel, jobWrapperChannel *JobWrapperChannel) *Worker {
	uuid, _ := uuid.NewUUID()
	return &Worker{
		Id: uuid,
		JobChannel: jobChannel,
		JobWrapperChannel: jobWrapperChannel,
		quit:       make(chan struct{})}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w *Worker) Start(wg *sync.WaitGroup) {
	go func() {

		wg.Add(1)

		for {
			select {
			case job, close := <-*w.JobChannel:
				//process
				if !close {
					log.Println("Worker quit")
					wg.Done()
					return
				} else {
					jobOutput, err := job.Execute()
					*w.JobWrapperChannel <- &JobWrapper{Job:job, JobOutput:jobOutput, err:err}
				}

			case <-w.quit:
				log.Println("Worker quit")
				wg.Done()
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
func (w *Worker) Stop() {
	close(w.quit)
}