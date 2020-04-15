package gotasks

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testJobName         = "test_job"
	testPanicJobName    = "test_panic_job"
	testArgsPassJobName = "test_args_pass_job"
	testQueueName       = "test_queue"
	testRedisURL        = "redis://127.0.0.1:6379/0"
)

func TestGenFunctions(t *testing.T) {
	assert.Equal(t, "gt:task:abcd", genTaskName("abcd"))
	assert.Equal(t, "gt:queue:abcd", genQueueName("abcd"))
}

func TestRedisBroker(t *testing.T) {
	// register tasks
	handler1 := func(args ArgsMap) (ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		return args, nil
	}
	handler2 := func(args ArgsMap) (ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		return args, nil
	}
	Register(testJobName, handler1, handler2)

	// set broker
	UseRedisBroker(testRedisURL, 100)

	// enqueue
	Enqueue(testQueueName, testJobName, MapToArgsMap(map[string]interface{}{}))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second * time.Duration(1))
		cancel()
	}()
	run(ctx, testQueueName) // it will blocking until the first job is executed
}

func TestPanicHandler(t *testing.T) {
	// register tasks
	handler1 := func(args ArgsMap) (ArgsMap, error) {
		time.Sleep(time.Duration(2) * time.Second)
		return args, nil
	}
	handler2 := func(args ArgsMap) (ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		panic("whoops")
		//return args, nil
	}
	handler3 := func(args ArgsMap) (ArgsMap, error) {
		time.Sleep(time.Duration(0) * time.Second)
		return args, nil
	}
	Register(testPanicJobName, handler1, handler2, handler3)

	// set broker
	UseRedisBroker(testRedisURL, 100)

	// enqueue
	taskID := Enqueue(testQueueName, testPanicJobName, MapToArgsMap(map[string]interface{}{}))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second * time.Duration(1))
		cancel()
	}()
	run(ctx, testQueueName) // it will blocking until the first job is executed

	// check result
	taskBytes := []byte{}
	if err := rc.Get(genTaskName(taskID)).Scan(&taskBytes); err != nil {
		t.Logf("failed to get task %s: %s", taskID, err)
		t.FailNow()
	}
	task := Task{}
	if err := json.Unmarshal(taskBytes, &task); err != nil {
		t.Logf("failed to get task %s: %s", taskID, err)
		t.FailNow()
	}
	assert.Equal(t, 1, task.CurrentHandlerIndex)

	// check result ttl
	duration, err := rc.TTL(genTaskName(taskID)).Result()
	if err != nil {
		t.Logf("task %s should have ttl with err %s", taskID, err)
		t.FailNow()
	}
	if duration.Seconds() == 0 {
		t.Logf("task %s should have ttl but not", taskID)
		t.FailNow()
	}
}

func TestArgsPass(t *testing.T) {
	// register tasks
	handler1 := func(args ArgsMap) (ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		args["hello"] = "world"
		return args, nil
	}
	handler2 := func(args ArgsMap) (ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		assert.Equal(t, "world", args["hello"])
		return args, nil
	}
	Register(testArgsPassJobName, handler1, handler2)

	// set broker
	UseRedisBroker(testRedisURL, 100)

	// enqueue
	Enqueue(testQueueName, testArgsPassJobName, MapToArgsMap(map[string]interface{}{}))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(time.Second * time.Duration(1))
		cancel()
	}()
	run(ctx, testQueueName) // it will blocking until the first job is executed
}
