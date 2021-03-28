package gotasks

import (
	"log"

	"github.com/streadway/amqp"
)

var (
	_ Broker = &RabbitMQBroker{}

	// rm: RabbitMQ
	rm *RabbitMQBroker
)

type RabbitMQBroker struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	queueMapper map[string]amqp.Queue
}

func UseRabbitMQBroker(rabbitMQURL string) {
	var err error
	rm = &RabbitMQBroker{}

	rm.conn, err = amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Panicf("failed to dial with rabbitmq: %s", err)
	}

	rm.ch, err = rm.conn.Channel()
	if err != nil {
		log.Panicf("failed to declare channel: %s", err)
	}

	err = rm.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Printf("failed to set QoS: %s", err)
	}
}

func (r *RabbitMQBroker) declareQueue(queueName string) error {
	if _, ok := r.queueMapper[queueName]; ok {
		return nil
	}

	q, err := rm.ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Printf("failed to declare queue: %s", err)
		return err
	}
	r.queueMapper[queueName] = q

	return nil
}

func (r *RabbitMQBroker) Acquire(queueName string) *Task {
	r.declareQueue(queueName)

	msgs, err := r.ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Panicf("failed to consume %s: %s", queueName, err)
	}
	if len(msgs) != 1 {
		log.Panicf("queue declared received 1 msg per time, but it did not")
	}

	deliver := <-msgs

	task := Task{}
	if err := json.Unmarshal(deliver.Body, &task); err != nil {
		log.Panicf("failed to get task from redis: %s", err)
		return nil // never executed
	}
	return &task
}

func (r *RabbitMQBroker) Ack(task *Task) bool {
	deliver, ok := task.internal.(amqp.Delivery)
	if !ok {
		log.Printf("failed to get delivery: %v", task)
		return false
	}

	if err := deliver.Ack(true); err != nil {
		log.Printf("failed to ack: %s", err)
		return false
	}

	return true
}

func (r *RabbitMQBroker) Update(task *Task) {
	log.Printf("rabbitmq does not support update payload of message!(task: %v)", task)
}

func (r *RabbitMQBroker) Enqueue(task *Task) string {
	taskBytes, err := json.Marshal(task)
	if err != nil {
		log.Panicf("failed to enquue task %+v: %s", task, err)
		return "" // never executed here
	}

	err = r.ch.Publish(
		"",             // exchange
		task.QueueName, // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        taskBytes,
		},
	)

	return task.ID
}

func (r *RabbitMQBroker) QueueLen(queueName string) int64 {
	return 0
}

func (r *RabbitMQBroker) Stop() {
	r.ch.Close()
	r.conn.Close()
}
