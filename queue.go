package gotasks

import (
	"log"
)

type Queue struct {
	Name     string
	MaxLimit int
	Async    bool

	// monitor
	MonitorInterval int
}

type QueueOption func(*Queue)

func WithMaxLimit(max int) QueueOption {
	return func(q *Queue) {
		q.MaxLimit = max
	}
}

func WithMonitorInterval(seconds int) QueueOption {
	return func(q *Queue) {
		q.MonitorInterval = seconds
	}
}

func WithAsyncHandleTask(async bool) QueueOption {
	return func(q *Queue) {
		q.Async = async
	}
}

func NewQueue(name string, options ...QueueOption) *Queue {
	queue := &Queue{name, 10, false, 5}

	for _, o := range options {
		o(queue)
	}

	return queue
}

func (q *Queue) Enqueue(jobName string, argsMap ArgsMap) string {
	return enqueue(q.Name, jobName, argsMap)
}

// enqueue a job(which will be wrapped in task) into queue
func enqueue(queueName, jobName string, argsMap ArgsMap) string {
	taskID := broker.Enqueue(NewTask(queueName, jobName, argsMap))
	log.Printf("job %s enqueued to %s, taskID is %s", jobName, queueName, taskID)
	return taskID
}
