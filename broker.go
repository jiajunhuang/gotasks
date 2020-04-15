package gotasks

import (
	"log"
	"time"

	redis "github.com/go-redis/redis/v7"
)

var (
	broker Broker
	_      Broker = &RedisBroker{}

	// rc: RedisClient
	rc *redis.Client
)

func genTaskName(taskID string) string {
	return "gt:task:" + taskID
}

func genQueueName(queueName string) string {
	return "gt:queue:" + queueName
}

type Broker interface {
	Acquire(string) *Task
	Ack(*Task) bool
	Update(*Task)
	Enqueue(*Task) string
}

type RedisBroker struct {
	TaskTTL int
}

func UseRedisBroker(redisURL string, taskTTL int) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Panicf("failed to parse redis URL %s: %s", redisURL, err)
	}

	rc = redis.NewClient(options)
	broker = &RedisBroker{taskTTL}
}

func (r *RedisBroker) Acquire(queueName string) *Task {
	task := Task{}
	vs := rc.BRPop(time.Duration(0), queueName).Val()
	v := []byte(vs[0])

	if err := json.Unmarshal(v, &task); err != nil {
		log.Panicf("failed to get task from redis: %s", err)
		return nil
	}

	return &task
}

func (r *RedisBroker) Ack(task *Task) bool {
	// redis doesn't support ACK
	return true
}

func (r *RedisBroker) Update(task *Task) {
	task.UpdatedAt = time.Now()
	taskBytes, err := json.Marshal(task)
	if err != nil {
		log.Panicf("failed to enquue task %+v: %s", task, err)
		return // never executed here
	}
	rc.Set(genTaskName(task.ID), taskBytes, time.Duration(r.TaskTTL)*time.Second)
}

func (r *RedisBroker) Enqueue(task *Task) string {
	taskBytes, err := json.Marshal(task)
	if err != nil {
		log.Panicf("failed to enquue task %+v: %s", task, err)
		return "" // never executed here
	}

	rc.Set(genTaskName(task.ID), taskBytes, time.Duration(r.TaskTTL)*time.Second)
	rc.LPush(genQueueName(task.QueueName), taskBytes)
	return task.ID
}
