package gotasks

import (
	"log"
	"time"

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
	ID                  string    `json:"task_id"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	QueueName           string    `json:"queue_name"`
	JobName             string    `json:"job_name"`
	ArgsMap             ArgsMap   `json:"args_map"`
	CurrentHandlerIndex int       `json:"current_handler_index"`
	OriginalArgsMap     ArgsMap   `json:"original_args_map"`
	ResultLog           string    `json:"result_log"`
}

func NewTask(queueName, jobName string, argsMap ArgsMap) *Task {
	u, _ := uuid.NewUUID()
	now := time.Now()

	task := &Task{u.String(), now, now, queueName, jobName, argsMap, 0, argsMap, ""}

	return task
}
