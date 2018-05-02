package executor

type JobWrapper struct {
	Job Job
	JobOutput *struct{}
	err error
}

