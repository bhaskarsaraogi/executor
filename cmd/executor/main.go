package main

import (
	"github.com/bhaskarsaraogi/executor"
	"log"
)

type dummyJobType struct {

}

func (d dummyJobType) Execute(...struct{}) error {
	log.Println("Task executed")
	return nil
}

func main()  {
	ex := executor.NewExecutor(8)
	ex.Run()

	log.Println("Creating payloads")
	jobs := []executor.Job{new(dummyJobType), new(dummyJobType)}

	// Go through each job and queue the individually for the job to be executed
	for _, job := range jobs {

		log.Println("Pushing job payload")

		// Push the job onto the queue.
		ex.QueueJob(job)
		log.Println("Job pushed")
	}

}
