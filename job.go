package executor

// Job interface represents the job to be run
// Any implementation of job needs to have an Execute method defined
type Job interface {
	Execute(params ...struct{}) error // fixme should have provision to return output as well
}

