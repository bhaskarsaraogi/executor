package executor

// Job interface represents the job to be run
// Any implementation of job needs to have an Execute method defined
type Job interface {
	// variadic method Execute, returns anything and instance of error if any
	Execute(params ...struct{}) (*struct{}, error)
}

