package main

import (
	"github.com/jiajunhuang/gotasks"
	//"github.com/jiajunhuang/gotasks/metrics"
)

const (
	uniqueJobName = "a-unique-job-name"
	redisURL      = "redis://127.0.0.1:6379/0"
	queueName     = "job-queue-name"
)

func main() {
	// set broker
	gotasks.UseRedisBroker(redisURL, gotasks.WithRedisTaskTTL(1000))

	// enqueue
	// or you can use a queue:
	queue := gotasks.NewQueue(queueName, gotasks.WithMaxLimit(10))
	queue.Enqueue(uniqueJobName, gotasks.MapToArgsMap(map[string]interface{}{})) // or gotasks.StructToArgsMap
}
