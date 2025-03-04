package worker

import (
	"encoding/json"
	"log"
	"strconv"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	entity "github.com/debojitroy/aws-queue-tasks-consume/internal/services/entity"
	color "github.com/fatih/color"
)

var cTrack = color.New(color.FgHiCyan)
var cTrackErr = color.New(color.FgRed).Add(color.Bold)

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
	cTrack.Println("---------------------------")

	dynamoRecord := new(DynamoDBRecord)
	err := json.Unmarshal(record.Data, &dynamoRecord)

	if err != nil {
		log.Fatalf("Error unmarshalling record: %v", err)
		return err
	} else {
		cTrack.Printf("New Record: %+v \n", dynamoRecord.Dynamodb.NewImage)
	}

	messageCount, err := strconv.Atoi(dynamoRecord.Dynamodb.NewImage.MessageCount.N)

	if err != nil {
		cTrackErr.Printf("Error unmarshalling record: %+v. Ignoring record \n", err)
		return nil
	}

	// If no messages are left, migration is complete
	if messageCount == 0 {
		cTrack.Printf("EntityID: %s, MessageCount: %d completed !!!  \n", dynamoRecord.Dynamodb.NewImage.EntityID.S, messageCount)
		entity.RemoveEntity(dynamoRecord.Dynamodb.NewImage.EntityID.S)
	}

	// Check if Entities are left
	if entity.GetEntityCount() == 0 {
		cTrack.Printf("All Entities are migrated !!! \n")

		// Exit the program
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	}

	cTrack.Println("---------------------------")
	return nil
}
