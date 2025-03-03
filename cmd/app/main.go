package main

import (
	"log"
	"sync"

	kinesis "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/kinesis"
	sqs "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/sqs"
	worker "github.com/debojitroy/aws-queue-tasks-consume/internal/services/worker"
)

// Color console
// github.com/fatih/color

func main() {
	_entity_count := 2
	_region := "us-west-2"
	_ddb_table := "entity_messages"
	_entity_queue_url := "https://sqs.us-west-2.amazonaws.com/381491940830/EntityMessagesQueue"
	_kinesis_stream_name := "entity-messages-stream"

	// Start Producer
	log.Println("Starting Producer")

	entityProducerConfig := &worker.EntityProducerConfig{
		Region:    _region,
		QueueUrl:  _entity_queue_url,
		TableName: _ddb_table,
	}

	worker.GenerateRandomEntities(_entity_count, entityProducerConfig)

	log.Println("Completing Producer")

	var wg sync.WaitGroup

	// Start the Kinesis Stream Consumer
	log.Println("Starting Kinesis Stream Consumer")
	consumer, err := kinesis.NewKinesisConsumer(_kinesis_stream_name, _region)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		consumer.StartKinesisStreamProcessor(worker.MigrationTrackerHandler)
	}()

	log.Println("Starting SQS Consumer")

	sqsConfig := &sqs.Config{
		TableName:       _ddb_table,
		QueueURL:        _entity_queue_url,
		NumWorkers:      3,
		BatchSize:       10,
		WaitTimeSeconds: 20,
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		sqs.StartSqsConsumer(worker.EntityMessageConsumer, sqsConfig)
	}()

	wg.Wait()
}
