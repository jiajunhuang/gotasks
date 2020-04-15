# gotasks

gotasks is a task/job queue framework for Golang. Currently we support use Redis as broker, but you can replace it
easily by implement `Broker` interface in `broker.go`.

In gotasks, we encourage developer to split tasks into smaller pieces(see the demo bellow) so we can:

- maintain tasks easily
- split code into reentrant and un-reentrant pieces, so when reentrant part failed, framework will retry it automatically

# Usage

```go
package main

import (
	"time"

	"github.com/jiajunhuang/gotasks"
)

const (
	uniqueJobName = "a-unique-job-name"
	redisURL      = "redis://127.0.0.1:6379/0"
	queueName     = "job-queue-name"
)

func worker() {
	gotasks.Run(queueName)
}

func main() {
	go worker()

	// register tasks
	handler1 := func(args gotasks.ArgsMap) (gotasks.ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		return args, nil
	}
	handler2 := func(args gotasks.ArgsMap) (gotasks.ArgsMap, error) {
		time.Sleep(time.Duration(1) * time.Second)
		return args, nil
	}
	gotasks.Register(uniqueJobName, handler1, handler2)

	// set broker
	gotasks.UseRedisBroker(redisURL, 100)

	// enqueue
	gotasks.Enqueue(queueName, uniqueJobName, gotasks.MapToArgsMap(map[string]interface{}{})) // or gotasks.StructToArgsMap
}
```

## License

NOTE that from the first commit, we use a GPL-v3 open source license.
