package gotasks

type Queue struct {
	Name string
}

func NewQueue(name string) *Queue {
	return &Queue{name}
}

func (q *Queue) Enqueue(jobName string, argsMap ArgsMap) string {
	return Enqueue(q.Name, jobName, argsMap)
}
