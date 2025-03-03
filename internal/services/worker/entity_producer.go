package worker

import (
	"context"
	"log"

	dynamodb "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/dynamodb"
	sqs "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/sqs"
	entity "github.com/debojitroy/aws-queue-tasks-consume/internal/services/entity"
)

type EntityProducerConfig struct {
	Region    string
	QueueUrl  string
	TableName string
}

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
		log.Printf("Processing Entity Id: %s \n", record.GetId())
		// Put Item to DynamoDB
		// Create a new DynamoDB client
		entityMessageItem := &dynamodb.EntityMessages{
			EntityId:     record.GetId(),
			MessageCount: record.GetMessageCount(),
		}

		log.Printf("Publishing Message count for Entity Id: %s \n", record.GetId())

		err = ddb.PutMessageCount(context.TODO(), *entityMessageItem)
		if err != nil {
			log.Fatalf("Failed to put item: %v", err)
			return err
		}

		log.Println("Successfully added item to DynamoDB")

		// Publish the messages to SQS
		log.Printf("Publishing messages to SQS for Entity Id: %s \n", record.GetId())

		sqsError := sqs.SendEntityMessages(record)

		if sqsError != nil {
			log.Fatalf("Failed to publish messages to SQS: %v", sqsError)
			return sqsError
		} else {
			log.Println("Successfully published messages to SQS")
		}

		// Add the entity to tracking
		log.Printf("Adding entity to tracking for Entity Id: %s \n", record.GetId())
		entity.AddEntity(record.GetId())
	}

	return nil
}
