package worker

import (
	"encoding/json"
	"log"
	"strconv"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	entity "github.com/debojitroy/aws-queue-tasks-consume/internal/services/entity"
)

type DynamoDBRecord struct {
	AwsRegion    string `json:"awsRegion"`
	EventID      string `json:"eventID"`
	EventName    string `json:"eventName"`
	UserIdentity any    `json:"userIdentity"`
	RecordFormat string `json:"recordFormat"`
	TableName    string `json:"tableName"`
	Dynamodb     struct {
		ApproximateCreationDateTime int64 `json:"ApproximateCreationDateTime"`
		Keys                        struct {
			EntityID struct {
				S string `json:"S"`
			} `json:"entity_id"`
		} `json:"Keys"`
		NewImage struct {
			MessageCount struct {
				N string `json:"N"`
			} `json:"message_count"`
			EntityID struct {
				S string `json:"S"`
			} `json:"entity_id"`
		} `json:"NewImage"`
		OldImage struct {
			MessageCount struct {
				N string `json:"N"`
			} `json:"message_count"`
			EntityID struct {
				S string `json:"S"`
			} `json:"entity_id"`
		} `json:"OldImage"`
		SizeBytes                            int    `json:"SizeBytes"`
		ApproximateCreationDateTimePrecision string `json:"ApproximateCreationDateTimePrecision"`
	} `json:"dynamodb"`
	EventSource string `json:"eventSource"`
}

func MigrationTrackerHandler(record types.Record) error {
	log.Println("---------------------------")

	// value,ok := entity.EntityTracker["value"]

	dynamoRecord := new(DynamoDBRecord)
	err := json.Unmarshal(record.Data, &dynamoRecord)

	if err != nil {
		log.Fatalf("Error unmarshalling record: %v", err)
		return err
	} else {
		log.Printf("New Record: %+v", dynamoRecord.Dynamodb.NewImage)
	}

	messageCount, err := strconv.Atoi(dynamoRecord.Dynamodb.NewImage.MessageCount.N)

	if err != nil {
		log.Fatalf("Error unmarshalling record: %v", err)
		return err
	}

	// If no messages are left, migration is complete
	if messageCount == 0 {
		log.Printf("EntityID: %s, MessageCount: %d completed !!!", dynamoRecord.Dynamodb.NewImage.EntityID.S, messageCount)
		entity.RemoveEntity(dynamoRecord.Dynamodb.NewImage.EntityID.S)
	}

	// Check if Entities are left
	if entity.GetEntityCount() == 0 {
		log.Printf("All Entities are migrated !!!")

		// Exit the program
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	}

	log.Println("---------------------------")
	return nil
}
