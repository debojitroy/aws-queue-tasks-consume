package worker

import (
	"context"
	"log"

	dynamodb "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/dynamodb"
	sqs "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/sqs"
	entity "github.com/debojitroy/aws-queue-tasks-consume/internal/services/entity"
	color "github.com/fatih/color"
)

type EntityProducerConfig struct {
	Region    string
	QueueUrl  string
	TableName string
}

var c = color.New(color.FgHiBlue)

func GenerateRandomEntities(num int, config *EntityProducerConfig) error {
	var entities []*entity.Entity

	for range num {
		newEntity := entity.NewEntity(100)

		entities = append(entities, newEntity)
	}

	ddb, err := dynamodb.NewDynamoDBClient(config.Region, config.TableName)
	if err != nil {
		log.Fatalf("Failed to create DynamoDB client: %v", err)
		return err
	}

	sqs, err := sqs.NewSQSClient(config.Region, config.QueueUrl)
	if err != nil {
		log.Fatalf("Failed to create SQS client: %v", err)
		return err
	}

	// Publish all entities to the queue
	for _, record := range entities {
		c.Printf("Processing Entity Id: %s \n", record.GetId())
		// Put Item to DynamoDB
		// Create a new DynamoDB client
		entityMessageItem := &dynamodb.EntityMessages{
			EntityId:     record.GetId(),
			MessageCount: record.GetMessageCount(),
		}

		c.Printf("Publishing Message count for Entity Id: %s \n", record.GetId())

		err = ddb.PutMessageCount(context.TODO(), *entityMessageItem)
		if err != nil {
			log.Fatalf("Failed to put item: %v", err)
			return err
		}

		c.Println("Successfully added item to DynamoDB")

		// Publish the messages to SQS
		c.Printf("Publishing messages to SQS for Entity Id: %s \n", record.GetId())

		sqsError := sqs.SendEntityMessages(record)

		if sqsError != nil {
			log.Fatalf("Failed to publish messages to SQS: %v", sqsError)
			return sqsError
		} else {
			c.Println("Successfully published messages to SQS")
		}

		// Add the entity to tracking
		c.Printf("Adding entity to tracking for Entity Id: %s \n", record.GetId())
		entity.AddEntity(record.GetId())
	}

	return nil
}
