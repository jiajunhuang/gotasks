package gotasks

import (
	"context"
	"log"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
type AckWhenStatus int

const (
	AckWhenAcquired AckWhenStatus = iota
	AckWhenSucceed
)

var (
	jobMap  = map[string][]JobHandler{}
	ackWhen = AckWhenSucceed

	// gotasks builtin queue
	FatalQueue = "fatal"

	// prometheus
	taskHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "task_execution_stats",
		Help: "task execution duration and status(success/fail)",
	}, []string{"queue_name", "job_name", "status"})
	taskGuage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "task_queue_stats",
		Help: "task stats in queue",
	}, []string{"queue_name"})
)

func init() {
	prometheus.MustRegister(taskHistogram)
	prometheus.MustRegister(taskGuage)
}

func AckWhen(i AckWhenStatus) {
	ackWhen = i
}

func Register(jobName string, handlers ...JobHandler) {
	if _, ok := jobMap[jobName]; ok {
		log.Panicf("job name %s already exist, check your code", jobName)
		return // never executed here
	}

	jobMap[jobName] = handlers
}

func handleTask(task *Task, queueName string) {
	defer func() {
		r := recover()
		status := "success"

		if r != nil {
			status = "fail"

			task.ResultLog = string(debug.Stack())
			broker.Update(task)
			log.Printf("recovered from queue %s and task %+v with recover info %+v", queueName, task, r)
		}

		taskHistogram.WithLabelValues(task.QueueName, task.JobName, status).Observe(task.UpdatedAt.Sub(task.CreatedAt).Seconds())

		if r != nil {
			// save to fatal queue
			task.QueueName = FatalQueue
			broker.Enqueue(task)
		}
	}()

	handlers, ok := jobMap[task.JobName]
	if !ok {
		log.Panicf("can't find job handlers of %s", task.JobName)
		return
	}

	var (
		err  error
		args = task.ArgsMap
	)
	for i, handler := range handlers {
		if task.CurrentHandlerIndex > i {
			log.Printf("skip step %d of task %s because it was executed successfully", i, task.ID)
			continue
		}

		task.CurrentHandlerIndex = i
		handlerName := getHandlerName(handler)
		log.Printf("task %s is executing step %d with handler %+v", task.ID, task.CurrentHandlerIndex, handlerName)
		reentrantOptions, ok := reentrantMap[handlerName]
		if ok { // check if the handler can retry
			for j := 0; j < reentrantOptions.MaxTimes; j++ {
				args, err = handler(args)
				if err == nil {
					break
				}
				time.Sleep(time.Microsecond * time.Duration(reentrantOptions.SleepyMS))
				log.Printf("retry step %d of task %s the %d rd time", task.CurrentHandlerIndex, task.ID, j)
			}
		} else {
			args, err = handler(args)
		}

		// error occurred
		if err != nil {
			log.Panicf("failed to execute handler %+v: %s", handler, err)
		}
		task.ArgsMap = args
		broker.Update(task)
	}
}

func run(ctx context.Context, wg *sync.WaitGroup, queueName string) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Printf("ctx.Done() received, quit (for queue %s) now", queueName)
			return
		default:
			log.Printf("gonna acquire a task from queue %s", queueName)
		}

		task := broker.Acquire(queueName)

		if ackWhen == AckWhenAcquired {
			ok := broker.Ack(task)
			log.Printf("ack broker of task %+v with status %t", task.ID, ok)
		}

		handleTask(task, queueName)

		if ackWhen == AckWhenSucceed {
			ok := broker.Ack(task)
			log.Printf("ack broker of task %+v with status %t", task.ID, ok)
		}
	}
}

func monitorQueue(ctx context.Context, wg *sync.WaitGroup, queueName string, interval int) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Printf("ctx.Done() received, quit (for queue %s) now", queueName)
			return
		default:
			log.Printf("gonna collect metrics from queue %s", queueName)
		}

		taskGuage.WithLabelValues(queueName).Set(float64(broker.QueueLen(queueName)))
		time.Sleep(time.Second * time.Duration(interval))
	}
}

func Run(ctx context.Context, queues ...string) {
	wg := sync.WaitGroup{}

	go monitorQueue(ctx, &wg, FatalQueue, 5)
	for _, queue := range queues {
		go run(ctx, &wg, queue)
		go monitorQueue(ctx, &wg, queue, 5)
	}

	wg.Wait()
}

func Enqueue(queueName, jobName string, argsMap ArgsMap) string {
	taskID := broker.Enqueue(NewTask(queueName, jobName, argsMap))
	log.Printf("job %s enqueued to %s, taskID is %s", jobName, queueName, taskID)
	return taskID
}

func MetricsServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(addr, nil)
}
