package executor

import (
	"github.com/google/uuid"
	"log"
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
func (w Worker) Start() {
	go func() {

		// Go over jobs being published and excute, this is fanout, as this jobChannel is Executor jobQueue only
		for {
			select {
				case <-w.quit:
					return
				case job := <-*w.JobChannel:
					jobOutput, err := job.Execute()
					*w.JobWrapperChannel <- &JobWrapper{Job:job, JobOutput:jobOutput, err:err}
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