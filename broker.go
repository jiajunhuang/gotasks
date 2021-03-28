package gotasks

// NOTE: remember that functions in this file is not thread-safe(in Go, goroutine-safe), because we don't add a lock
// to prevent functions call to UseRedisBroker.
// But it is *safe* if you just call it once, in your initial code, it's unsafe if you change broker in serveral
// goroutines.

var (
	broker Broker
)

type Broker interface {
	Acquire(string) *Task
	Ack(*Task) bool
	Update(*Task)
	Enqueue(*Task) string
	QueueLen(string) int64
	Stop()
}
