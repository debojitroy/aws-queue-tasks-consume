package worker

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func EntityMessageConsumer(ctx context.Context, msg *types.Message) error {
	log.Println("-----------------------------------------------------------")
	log.Printf("Message: %s", *msg.Body)
	entity_id, ok := msg.MessageAttributes["entity_id"]

	if !ok {
		log.Println("Entity ID is nil")
		return nil
	} else {
		log.Printf("Entity ID: %s", *entity_id.StringValue)
	}

	message_id, ok := msg.MessageAttributes["message_id"]

	if !ok {
		log.Println("Message ID is nil")
		return nil
	} else {
		log.Printf("Message ID: %s", *message_id.StringValue)
	}
	log.Println("###########################################################")

	return nil
}
