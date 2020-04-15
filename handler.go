package gotasks

import (
	"reflect"
	"runtime"
)

type JobHandler func(ArgsMap) (ArgsMap, error)

type ReentrantOptions struct {
	MaxTimes      int
	SleepySeconds int
}

var (
	reentrantMap = map[string]ReentrantOptions{}
)

func getHandlerName(handler JobHandler) string {
	return runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
}

func Reentrant(handler JobHandler, options ReentrantOptions) JobHandler {
	handlerName := getHandlerName(handler)
	reentrantMap[handlerName] = options

	return handler
}
