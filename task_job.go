package gotasks

import (
	"log"

	"github.com/google/uuid"
)

type ArgsMap map[string]interface{}

func StructToArgsMap(v interface{}) ArgsMap {
	v_bytes, err := json.Marshal(v)
	if err != nil {
		log.Panicf("failed to convert %+v to ArgsMap: %s", v, err)
	}
	argsMap := ArgsMap{}
	err = json.Unmarshal(v_bytes, &argsMap)
	if err != nil {
		log.Panicf("failed to convert %+v to ArgsMap: %s", v, err)
	}

	return argsMap
}

func MapToArgsMap(v interface{}) ArgsMap {
	return StructToArgsMap(v)
}

type Task struct {
	ID                  string  `json:"task_id"`
	QueueName           string  `json:"queue_name"`
	JobName             string  `json:"job_name"`
	ArgsMap             ArgsMap `json:"args_map"`
	CurrentHandlerIndex int     `json:"current_handler_index"`
}

func NewTask(queueName, jobName string, argsMap ArgsMap) *Task {
	u, _ := uuid.NewUUID()
	task := &Task{u.String(), queueName, jobName, argsMap, 0}

	return task
}
