package main

import (
	"context"
	"time"

	"github.com/jiajunhuang/gotasks"
	//"github.com/jiajunhuang/gotasks/metrics"
)

const (
	uniqueJobName = "a-unique-job-name"
	redisURL      = "redis://127.0.0.1:6379/0"
	queueName     = "job-queue-name"
)

func Handler1(args gotasks.ArgsMap) (gotasks.ArgsMap, error) {
	time.Sleep(time.Duration(1) * time.Second)
	return args, nil
}
func Handler2(args gotasks.ArgsMap) (gotasks.ArgsMap, error) {
	time.Sleep(time.Duration(1) * time.Second)
	return args, nil
}

func register() {
	// if handler1 failed, the task will stop, but if handler2 failed(return a non-nil error)
	// handler2 will be retry 3 times, and sleep 100 ms each time
	gotasks.Register(uniqueJobName, Handler1, gotasks.Reentrant(Handler2, gotasks.WithMaxTimes(3), gotasks.WithSleepyMS(10)))
}

func worker() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gotasks.Run(ctx, queueName)
}

func main() {
	// set broker
	gotasks.UseRedisBroker(redisURL, gotasks.WithRedisTaskTTL(1000))

	register()
	worker()
}
