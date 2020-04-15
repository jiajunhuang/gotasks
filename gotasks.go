package gotasks

import (
	"log"
)

// gotasks is a job/task framework for Golang.
// Usage:
// * First, register your function in your code:
// gotasks.Register("a_unique_job_name", job_handle_function1, job_handle_function2)
// * Then, start worker by:
// gotasks.Run()
// * Last, enqueue a job:
// gotasks.Enqueue("a_unique_job_name", gotasks.ArgsMap{"a": "b"})
//
// Note that job will be executed in register order, and every job handle function
// must have a signature which match gotasks.JobHandler, which receives a ArgsMap and
// return a ArgsMap which will be arguments input for next handler.
var (
	jobMap = map[string][]JobHandler{}
)

func Register(jobName string, handlers ...JobHandler) {
	if _, ok := jobMap[jobName]; ok {
		log.Panicf("job name %s already exist, check your code", jobName)
		return // never executed here
	}

	jobMap[jobName] = handlers
}

func Run(queues ...string) {

}

func Enqueue(queueName, jobName string, argsMap ArgsMap) string {
	taskID := broker.Enqueue(NewTask(queueName, jobName, argsMap))
	log.Printf("job %s enqueued to %s, taskID is %s", jobName, queueName, taskID)
	return taskID
}
