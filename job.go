package executor

// Job represents the job to be run
type Job interface {
	Execute(params ...struct{}) error // fixme should have provision to return output as well
}

