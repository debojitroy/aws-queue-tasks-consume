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
	color "github.com/fatih/color"
)

type Handler func(ctx context.Context, msg *types.Message, region string, tableName string) error

type Config struct {
	TableName       string
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

var c = color.New(color.FgHiGreen)
var cErr = color.New(color.FgRed).Add(color.Bold)

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
		QueueUrl:              &cfg.QueueURL,
		MaxNumberOfMessages:   cfg.BatchSize,
		WaitTimeSeconds:       cfg.WaitTimeSeconds,
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
	c.Printf("Starting worker %d \n", workerID)

	for {
		select {
		case <-ctx.Done():
			c.Printf("Worker %d shutting down \n", workerID)
			return
		default:
			// Receive messages
			output, err := w.Sqs.ReceiveMessage(ctx, w.Input)
			if err != nil {
				cErr.Printf("Error receiving message: %v \n", err)
				continue
			}

			// Process messages
			for _, message := range output.Messages {
				if err := handler(ctx, &message, w.Config.Region, w.Config.TableName); err != nil {
					cErr.Printf("Error processing message: %v \n", err)
					continue
				}

				// Delete message after successful processing
				_, err := w.Sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      &w.Config.QueueURL,
					ReceiptHandle: message.ReceiptHandle,
				})
				if err != nil {
					cErr.Printf("Error deleting message: %v \n", err)
				}
			}
		}
	}
}

func StartSqsConsumer(messageHandler Handler, cfg *Config) {
	worker, err := NewWorker(cfg)
	if err != nil {
		log.Fatalf("Error creating worker: %v \n", err)
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
	c.Println("Shutting down gracefully...")

	// Cancel context to stop workers
	cancel()

	// Wait for all workers to finish
	wg.Wait()
	c.Println("Shutdown complete")
}
