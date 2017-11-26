package main

import (
	"github.com/bhaskarsaraogi/executor"
	"log"
)

func main()  {
	ex := executor.NewExecutor(8)
	ex.Run()

	log.Println("Creating payloads")
	payloads := []executor.Payload{{}, {}}

	// Go through each payload and queue items individually for the jon to be executed
	for _, payload := range payloads {

		log.Println("Pushing job payload")
		// let's create a job with the payload
		job := executor.Job{Payload: payload}

		// Push the job onto the queue.
		ex.QueueJob(job)
		log.Println("Job pushed")
	}

}
