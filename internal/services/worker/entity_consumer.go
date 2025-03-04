package worker

import (
	"context"
	"log"

	dynamodb "github.com/debojitroy/aws-queue-tasks-consume/internal/services/aws/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	color "github.com/fatih/color"
)

var cConsumer = color.New(color.FgHiGreen)

func EntityMessageConsumer(ctx context.Context, msg *types.Message, region string, tableName string) error {
	cConsumer.Println("##########################################################")
	cConsumer.Printf("Message: %s \n", *msg.Body)
	entity_id, ok := msg.MessageAttributes["entity_id"]

	if !ok {
		cConsumer.Println("Entity ID is nil")
		return nil
	} else {
		cConsumer.Printf("Entity ID: %s \n", *entity_id.StringValue)
	}

	message_id, ok := msg.MessageAttributes["message_id"]

	if !ok {
		cConsumer.Println("Message ID is nil")
		return nil
	} else {
		cConsumer.Printf("Message ID: %s \n", *message_id.StringValue)
	}
	cConsumer.Println("###########################################################")

	ddb, err := dynamodb.NewDynamoDBClient(region, tableName)
	if err != nil {
		log.Fatalf("Failed to create DynamoDB client: %v \n", err)
		return err
	}

	ddb.DecrementMessageCount(ctx, *entity_id.StringValue, 1)

	return nil
}
