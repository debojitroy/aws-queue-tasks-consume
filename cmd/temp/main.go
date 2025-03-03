package main

import (
	sqs "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/sqs"
	worker "github.com/debojitroy/aws-queue-tasks-consume/internal/services/worker"
)

func main() {
	_entity_queue_url := "https://sqs.us-west-2.amazonaws.com/381491940830/EntityMessagesQueue"

	println("Hello, World!")

	cfg := &sqs.Config{
		QueueURL:        _entity_queue_url,
		NumWorkers:      3,
		BatchSize:       10,
		WaitTimeSeconds: 20,
	}

	sqs.StartSqsConsumer(worker.EntityMessageConsumer, cfg)
}
