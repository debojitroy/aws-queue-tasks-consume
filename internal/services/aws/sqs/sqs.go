package sqs

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws"
	entity "github.com/debojitroy/aws-queue-tasks-consume/internal/services/entity"
)

type SqsClient struct {
	client   *sqs.Client
	queueUrl string
}

// NewSQSClient creates a new SQS client
func NewSQSClient(region string, queueUrl string) (*SqsClient, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	// Create SQS client
	client := sqs.NewFromConfig(cfg)

	return &SqsClient{
		client:   client,
		queueUrl: queueUrl,
	}, nil
}

func (sqsClient *SqsClient) SendEntityMessages(entity *entity.Entity) error {
	// Process messages in batches of 10 (SQS maximum batch size)
	const batchSize = 10
	messages := entity.GetMessages()

	counter := 1

	for i := 0; i < entity.GetMessageCount(); i += batchSize {
		end := i + batchSize
		if end > entity.GetMessageCount() {
			end = entity.GetMessageCount()
		}

		// Create batch entries for this chunk
		var entries []types.SendMessageBatchRequestEntry
		for j, msg := range messages[i:end] {
			entries = append(entries, types.SendMessageBatchRequestEntry{
				Id: aws.String(fmt.Sprintf("msg%d", i+j)),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"entity_id":  {StringValue: aws.String(entity.GetId()), DataType: aws.String("String")},
					"message_id": {StringValue: aws.String(strconv.Itoa(counter)), DataType: aws.String("String")},
				},
				MessageBody: aws.String(msg),
			})

			counter++
		}

		// Send the batch
		input := &sqs.SendMessageBatchInput{
			QueueUrl: &sqsClient.queueUrl,
			Entries:  entries,
		}

		result, err := sqsClient.client.SendMessageBatch(context.TODO(), input)
		if err != nil {
			log.Fatalf("failed to send batch due to error: %s", err.Error())
			log.Fatalf("failed to send batch: %v", err)
			return err
		}

		// Handle any failed messages
		if len(result.Failed) > 0 {
			for _, failure := range result.Failed {
				log.Printf("Failed to send message ID: %s, Code: %s, Message: %s\n",
					*failure.Id, *failure.Code, *failure.Message)
			}
		}
	}
	return nil
}
