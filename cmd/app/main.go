package main

import (
	"flag"
	"log"
	"sync"

	kinesis "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/kinesis"
	sqs "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/sqs"
	worker "github.com/debojitroy/aws-queue-tasks-consume/internal/services/worker"
	color "github.com/fatih/color"
)

func main() {
	_entity_count_ptr := flag.Int("entity_count", 2, "Number of entities to generate")
	_region_ptr := flag.String("region", "", "AWS Region")
	_ddb_table_ptr := flag.String("ddb_table", "", "DynamoDB Table Name")
	_entity_queue_url_ptr := flag.String("queue_url", "", "SQS Queue URL")
	_kinesis_stream_name_ptr := flag.String("kinesis_stream_name", "", "Kinesis Stream Name")

	flag.Parse()

	_entity_count := *_entity_count_ptr
	_region := *_region_ptr
	_ddb_table := *_ddb_table_ptr
	_entity_queue_url := *_entity_queue_url_ptr
	_kinesis_stream_name := *_kinesis_stream_name_ptr

	if _region == "" {
		log.Fatal("Region is required")
	}

	if _ddb_table == "" {
		log.Fatal("DynamoDB Table Name is required")
	}

	if _entity_queue_url == "" {
		log.Fatal("SQS Queue URL is required")
	}

	if _kinesis_stream_name == "" {
		log.Fatal("Kinesis Stream Name is required")
	}

	// _entity_count := 2
	// _region := "us-west-2"
	// _ddb_table := "entity_messages"
	// _entity_queue_url := "https://sqs.us-west-2.amazonaws.com/381491940830/EntityMessagesQueue"
	// _kinesis_stream_name := "entity-messages-stream"

	c := color.New(color.BgHiYellow)
	cErr := color.New(color.FgRed).Add(color.Bold)

	// Start Producer
	c.Println("Starting Producer")

	entityProducerConfig := &worker.EntityProducerConfig{
		Region:    _region,
		QueueUrl:  _entity_queue_url,
		TableName: _ddb_table,
	}

	worker.GenerateRandomEntities(_entity_count, entityProducerConfig)

	c.Println("Completing Producer")

	var wg sync.WaitGroup

	// Start the Kinesis Stream Consumer
	c.Println("Starting Kinesis Stream Consumer")
	consumer, err := kinesis.NewKinesisConsumer(_kinesis_stream_name, _region)
	if err != nil {
		cErr.Printf("Error creating consumer: %+v \n", err)
		log.Fatalf("Error creating consumer: %v", err)
	}

	wg.Add(1)

	go func() {
		defer wg.Done()
		consumer.StartKinesisStreamProcessor(worker.MigrationTrackerHandler)
	}()

	c.Println("Starting SQS Consumer")

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
