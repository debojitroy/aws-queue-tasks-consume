package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	kinesis "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/kinesis"
	worker "github.com/debojitroy/aws-queue-tasks-consume/internal/services/worker"
)

func main() {
	streamName := "entity-messages-stream"

	consumer, err := kinesis.NewKinesisConsumer(streamName, "us-west-2")
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumer in a goroutine
	go func() {
		log.Printf("Starting to consume from stream: %s", streamName)
		if err := consumer.Start(worker.MigrationTrackerHandler); err != nil {
			log.Fatalf("Error starting consumer: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down...")
	consumer.Stop()
}
