package executor

import (
	"github.com/google/uuid"
	"log"
	"sync/atomic"
	"sync"
)

// Worker represents the worker that executes the job
type Worker struct {
	Id uuid.UUID // unique Id of every worker
	JobChannel  *JobChannel // jobchannel of worker on which it receives job to execute
	JobWrapperChannel *JobWrapperChannel
	quit    	chan struct{} // channel to notify worker to stop
	active int32
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
func (w Worker) Start(wg sync.WaitGroup) {
	go func() {

		wg.Add(1)
		atomic.StoreInt32(&w.active, 1)


		for job := range *w.JobChannel{
			jobOutput, err := job.Execute()
			*w.JobWrapperChannel <- &JobWrapper{Job:job, JobOutput:jobOutput, err:err}

			// there might be due to concurrency a job gets consumed, so we need to finish that then only stop this worker
			if atomic.LoadInt32(&w.active) == 0 {
				log.Println("Worker quit")
				wg.Done()
				return
			}
		}

		wg.Done()
		log.Println("Closed job channel")

	}()
}

// Worker to string, essentially worker Id to string
func (w *Worker) String() string {
	return w.Id.String()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	// Go routine because dont want to keep other guys waiting while stopping this
	atomic.StoreInt32(&w.active, 0)
}