package gotasks

import (
	"log"
	"reflect"
	"runtime"
	"sync"
)

type JobHandler func(ArgsMap) (ArgsMap, error)

type ReentrantOptions struct {
	MaxTimes int
	SleepyMS int
}

type ReentrantOption func(*ReentrantOptions)

func WithMaxTimes(max int) ReentrantOption {
	return func(ro *ReentrantOptions) {
		ro.MaxTimes = max
	}
}

func WithSleepyMS(ms int) ReentrantOption {
	return func(ro *ReentrantOptions) {
		ro.SleepyMS = ms
	}
}

var (
	reentrantMap     = map[string]ReentrantOptions{}
	reentrantMapLock sync.Mutex
)

func getHandlerName(handler JobHandler) string {
	return runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name()
}

func Reentrant(handler JobHandler, options ...ReentrantOption) JobHandler {
	handlerName := getHandlerName(handler)
	reentrantMapLock.Lock()
	defer reentrantMapLock.Unlock()

	if _, ok := reentrantMap[handlerName]; ok {
		log.Panicf("reentrant options of %s already exists!", handlerName)
		return nil // never executed here
	}

	reentrantOptions := &ReentrantOptions{0, 0}
	for _, o := range options {
		o(reentrantOptions)
	}
	reentrantMap[handlerName] = *reentrantOptions

	return handler
}
