package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jiajunhuang/gotasks"
	//"github.com/jiajunhuang/gotasks/metrics"
)

const (
	uniqueJobName = "a-unique-job-name"
	redisURL      = "redis://127.0.0.1:6379/0"
	rabbitMQURL   = "amqp://celery:celerypassword@127.0.0.1:5672/"
	queueName     = "celery"
)

var (
	asWorker = flag.Bool("asWorker", false, "run as worker?")
)

func worker() {
	// setup signal handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		log.Printf("gonna listen on SIGINT...")
		s := <-sigChan
		switch s {
		case syscall.SIGINT:
			cancel()
		default:
		}
	}()

	gotasks.Run(ctx, queueName)
}

func main() {
	flag.Parse()

	// register tasks
	handler1 := func(args gotasks.ArgsMap) (gotasks.ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		return args, nil
	}
	handler2 := func(args gotasks.ArgsMap) (gotasks.ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		return args, errors.New("some error")
	}
	// if handler1 failed, the task will stop, but if handler2 failed(return a non-nil error)
	// handler2 will be retry 3 times, and sleep 100 ms each time
	gotasks.Register(uniqueJobName, handler1, gotasks.Reentrant(handler2, gotasks.WithMaxTimes(3), gotasks.WithSleepyMS(10)))

	// set broker
	gotasks.UseRabbitMQBroker("amqp://celery:celerypassword@192.168.250.4:5672/")

	// enqueue
	// or you can use a queue:
	if !*asWorker {
		queue := gotasks.NewQueue(queueName, gotasks.WithMaxLimit(10))
		queue.Enqueue(uniqueJobName, gotasks.MapToArgsMap(map[string]interface{}{})) // or gotasks.StructToArgsMap
	} else {
		worker()
	}

	// or you can integrate metrics handler yourself in your own web app
	// go metrics.RunServer(":2121")
}
