package gotasks

import (
	"context"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jiajunhuang/gotasks/loop"
	"github.com/jiajunhuang/gotasks/pool"
	"github.com/prometheus/client_golang/prometheus"
)

// gotasks is a job/task framework for Golang.
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
	jobMap     = map[string][]JobHandler{}
	jobMapLock sync.RWMutex

	ackWhen     = AckWhenSucceed
	ackWhenLock sync.Mutex

	// gotasks builtin queue
	FatalQueueName = "fatal"

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

// AckWhen set when will the ack be sent to broker
func AckWhen(i AckWhenStatus) {
	ackWhenLock.Lock()
	defer ackWhenLock.Unlock()

	ackWhen = i
}

func Register(jobName string, handlers ...JobHandler) {
	jobMapLock.Lock()
	defer jobMapLock.Unlock()

	if _, ok := jobMap[jobName]; ok {
		log.Panicf("job name %s already exist, check your code", jobName)
		return // never executed here
	}

	jobMap[jobName] = handlers
}

func runHandlers(task *Task) {
	jobMapLock.RLock()
	defer jobMapLock.RUnlock()

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
		log.Printf("task %s is executing step %d with handler %s", task.ID, task.CurrentHandlerIndex, handlerName)

		reentrantMapLock.RLock()
		reentrantOptions, ok := reentrantMap[handlerName]
		reentrantMapLock.RUnlock()

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
			log.Panicf("failed to execute handler %s: %s", handlerName, err)
		}
		task.ArgsMap = args
		broker.Update(task)
	}
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
			task.QueueName = FatalQueueName
			broker.Enqueue(task)
		}
	}()

	runHandlers(task)
}

func run(ctx context.Context, wg *sync.WaitGroup, queue *Queue) {
	defer wg.Done()

	gopool := pool.NewGoPool(pool.WithMaxLimit(queue.MaxLimit))
	defer gopool.Wait()

	loop.Execute(ctx, func() {
		fn := func() {
			task := broker.Acquire(queue.Name)

			if ackWhen == AckWhenAcquired {
				ok := broker.Ack(task)
				log.Printf("ack broker of task %+v with status %t", task.ID, ok)
			}
			handleTask(task, queue.Name)

			if ackWhen == AckWhenSucceed {
				ok := broker.Ack(task)
				log.Printf("ack broker of task %+v with status %t", task.ID, ok)
			}
		}

		if queue.Async {
			gopool.Submit(fn)
		} else {
			fn()
		}
	})
}

func monitorQueue(ctx context.Context, wg *sync.WaitGroup, queue *Queue) {
	defer wg.Done()

	loop.Execute(ctx, func() {
		taskGuage.WithLabelValues(queue.Name).Set(float64(broker.QueueLen(queue.Name)))
		time.Sleep(time.Second * time.Duration(queue.MonitorInterval))
	})
}

// Run a worker that listen on queues
func Run(ctx context.Context, queueNames ...string) {
	wg := sync.WaitGroup{}

	wg.Add(1)
	go monitorQueue(ctx, &wg, NewQueue(FatalQueueName))
	for _, queueName := range queueNames {
		wg.Add(2)
		queue := NewQueue(queueName)
		go run(ctx, &wg, queue)
		go monitorQueue(ctx, &wg, queue)
	}

	wg.Wait()
}
