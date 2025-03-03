package sqs

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Handler func(ctx context.Context, msg *types.Message) error

type Config struct {
	QueueURL        string
	Region          string
	NumWorkers      int
	BatchSize       int32
	WaitTimeSeconds int32
}

type Worker struct {
	Config *Config
	Input  *sqs.ReceiveMessageInput
	Sqs    *sqs.Client
	Events map[string]interface{}
}

func NewWorker(cfg *Config) (*Worker, error) {
	// Load AWS configuration
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config: %v", err)
	}

	// Create SQS client
	sqsClient := sqs.NewFromConfig(awsCfg)

	// Configure receive message input
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &cfg.QueueURL,
		MaxNumberOfMessages: cfg.BatchSize,
		WaitTimeSeconds:     cfg.WaitTimeSeconds,
		MessageAttributeNames: []string{"All"},
	}

	return &Worker{
		Config: cfg,
		Input:  input,
		Sqs:    sqsClient,
		Events: make(map[string]interface{}),
	}, nil
}

func (w *Worker) start(ctx context.Context, handler Handler, wg *sync.WaitGroup) {
	for i := 0; i < w.Config.NumWorkers; i++ {
		wg.Add(1)
		go w.consume(ctx, i, handler, wg)
	}
}

func (w *Worker) consume(ctx context.Context, workerID int, handler Handler, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Starting worker %d", workerID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d shutting down", workerID)
			return
		default:
			// Receive messages
			output, err := w.Sqs.ReceiveMessage(ctx, w.Input)
			if err != nil {
				log.Printf("Error receiving message: %v", err)
				continue
			}

			// Process messages
			for _, message := range output.Messages {
				if err := handler(ctx, &message); err != nil {
					log.Printf("Error processing message: %v", err)
					continue
				}

				// Delete message after successful processing
				_, err := w.Sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      &w.Config.QueueURL,
					ReceiptHandle: message.ReceiptHandle,
				})
				if err != nil {
					log.Printf("Error deleting message: %v", err)
				}
			}
		}
	}
}

func StartSqsConsumer(messageHandler Handler, cfg *Config) {
	worker, err := NewWorker(cfg)
	if err != nil {
		log.Fatalf("Error creating worker: %v", err)
	}

	// Create context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup

	// Start the workers
	worker.start(ctx, messageHandler, &wg)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for interrupt signal
	<-sigChan
	log.Println("Shutting down gracefully...")

	// Cancel context to stop workers
	cancel()

	// Wait for all workers to finish
	wg.Wait()
	log.Println("Shutdown complete")
}

// https://stackoverflow.com/questions/40498371/how-to-send-an-interrupt-signal
