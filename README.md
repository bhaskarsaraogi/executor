A golang library to execute tasks(io calls/processing) in parallel

Uses a two tier channel system for queuing jobs and a worker pool(set of goroutines) to operate on jobs queued via channel as soon as the worker becomes available to process next job