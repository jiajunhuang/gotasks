package gotasks

var (
	broker Broker
	_      Broker = &RedisBroker{}
)

type Broker interface {
	Acquire()
	Ack()
	Update()
	Enqueue(*Task) string
}

type RedisBroker struct {
	ResultTTL int
}

func NewRedisBroker() *RedisBroker {
	return nil
}

func (r *RedisBroker) Acquire() {

}

func (r *RedisBroker) Ack() {

}

func (r *RedisBroker) Update() {

}

func (r *RedisBroker) Enqueue(task *Task) string {
	return task.ID
}
