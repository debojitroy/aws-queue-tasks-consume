package main

import (
	"context"
	"fmt"
	"log"

	dynamodb "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/dynamodb"
	entity "github.com/debojitroy/aws-queue-tasks-consume/internal/services/entity"
)

// Color console
// github.com/fatih/color

func main() {
	_region := "us-west-2"
	_ddb_table := "entity_messages"
	_entity := entity.NewEntity(100)

	fmt.Printf("EntityId: %s \n", _entity.GetId())
	fmt.Printf("Message Count: %d \n", _entity.GetMessageCount())

	// for loop to iterate over the messages
	for _, message := range _entity.GetMessages() {
		fmt.Printf("Message:: %s \n", message)
	}

	// Put Item to DynamoDB
	// Create a new DynamoDB client
	ddb, err := dynamodb.NewDynamoDBClient(_region, _ddb_table)
	if err != nil {
		log.Fatalf("Failed to create DynamoDB client: %v", err)
	}

	entityMessageItem := &dynamodb.EntityMessages{
		EntityId:     _entity.GetId(),
		MessageCount: _entity.GetMessageCount(),
	}

	err = ddb.PutMessageCount(context.TODO(), *entityMessageItem)
	if err != nil {
		log.Fatalf("Failed to put item: %v", err)
	}

	log.Println("Successfully added item to DynamoDB")

	log.Println("Decrementing count by 2")
	decrement_err := ddb.DecrementMessageCount(context.TODO(), _entity.GetId(), 2)

	if decrement_err != nil {
		log.Fatalf("Failed to decrement item by 2 :: %v", err)
	}

	log.Println("Successfully decremented count by 2")
}
