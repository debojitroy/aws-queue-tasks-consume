package dynamodb

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type EntityMessages struct {
	EntityId     string `dynamodbav:"entity_id"`
	MessageCount int    `dynamodbav:"message_count"`
}

// DynamoDBClient wraps the DynamoDB client and table name
type DynamoDBClient struct {
	client    *dynamodb.Client
	tableName string
}

// NewDynamoDBClient creates a new DynamoDB client
func NewDynamoDBClient(region string, tableName string) (*DynamoDBClient, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	// Create DynamoDB client
	client := dynamodb.NewFromConfig(cfg)

	return &DynamoDBClient{
		client:    client,
		tableName: tableName,
	}, nil
}

// PutMessageCount adds the number of messages to DynamoDB
func (d *DynamoDBClient) PutMessageCount(ctx context.Context, item EntityMessages) error {
	// Marshal the Go struct into a DynamoDB attribute value map
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return err
	}

	// Create the PutItem input
	input := &dynamodb.PutItemInput{
		TableName: &d.tableName,
		Item:      av,
	}

	// Execute the PutItem operation
	_, err = d.client.PutItem(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

// DecrementMessageCount decrements the number of messages in DynamoDB
func (d *DynamoDBClient) DecrementMessageCount(ctx context.Context, entityId string, decrementCount int) error {
	// Create the UpdateItem input
	input := &dynamodb.UpdateItemInput{
		TableName: &d.tableName,
		Key: map[string]types.AttributeValue{
			"entity_id": &types.AttributeValueMemberS{Value: entityId},
		},
		UpdateExpression: aws.String("SET #count = #count - :decrement"),
		ExpressionAttributeNames: map[string]string{
			"#count": "message_count",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":decrement": &types.AttributeValueMemberN{Value: strconv.Itoa(decrementCount)},
		},
	}

	// Execute the UpdateItem operation
	_, err := d.client.UpdateItem(ctx, input)
	if err != nil {
		fmt.Printf("Update Error Reason: %s", err.Error())
		fmt.Printf("Update Error: %v", err)
		return err
	}

	return nil
}
