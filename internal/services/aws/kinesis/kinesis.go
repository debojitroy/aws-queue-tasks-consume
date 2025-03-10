package kinesis

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	color "github.com/fatih/color"
)

type KinesisConsumer struct {
	client     *kinesis.Client
	streamName string
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

type KinesisRecordHandler func(record types.Record) error

var c = color.New(color.FgHiGreen)
var cErr = color.New(color.FgRed).Add(color.Bold)

func NewKinesisConsumer(streamName string, region string) (*KinesisConsumer, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	return &KinesisConsumer{
		client:     kinesis.NewFromConfig(cfg),
		streamName: streamName,
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

func (kc *KinesisConsumer) getShardIds() ([]string, error) {
	var shardIds []string
	var nextToken *string

	for {
		input := &kinesis.DescribeStreamInput{
			StreamName: &kc.streamName,
		}
		if nextToken != nil {
			input.ExclusiveStartShardId = nextToken
		}

		output, err := kc.client.DescribeStream(kc.ctx, input)
		if err != nil {
			return nil, err
		}

		for _, shard := range output.StreamDescription.Shards {
			shardIds = append(shardIds, *shard.ShardId)
		}

		if !*output.StreamDescription.HasMoreShards {
			break
		}
		nextToken = output.StreamDescription.Shards[len(output.StreamDescription.Shards)-1].ShardId
	}

	return shardIds, nil
}

func (kc *KinesisConsumer) processRecords(records []types.Record, handler KinesisRecordHandler) error {
	for _, record := range records {
		c.Printf("Shard ID: %s, Sequence Number: %s \n", *record.PartitionKey, *record.SequenceNumber)
		c.Printf("Data: %s \n", string(record.Data))

		handler(record)
	}

	return nil
}

func (kc *KinesisConsumer) processShard(shardId string, handler KinesisRecordHandler) {
	defer kc.wg.Done()

	c.Printf("Starting processing for shard: %s \n", shardId)

	// Get initial shard iterator
	iteratorOutput, err := kc.client.GetShardIterator(kc.ctx, &kinesis.GetShardIteratorInput{
		StreamName:        &kc.streamName,
		ShardId:           &shardId,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		cErr.Printf("Error getting shard iterator for shard %s: %+v  \n", shardId, err)
		return
	}

	shardIterator := iteratorOutput.ShardIterator

	// Process records until context is cancelled or shard is closed
	for {
		select {
		case <-kc.ctx.Done():
			cErr.Printf("Stopping processing for shard: %s \n", shardId)
			return
		default:
			if shardIterator == nil {
				cErr.Printf("Shard iterator is nil for shard %s, stopping \n", shardId)
				return
			}

			// Get records using the shard iterator
			output, err := kc.client.GetRecords(kc.ctx, &kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
				Limit:         aws.Int32(1000), // Adjust based on your needs
			})

			if err != nil {
				cErr.Printf("Error getting records from shard %s: %+v \n", shardId, err)
				time.Sleep(time.Second) // Basic retry mechanism
				continue
			}

			// Process the records
			if len(output.Records) > 0 {
				if err := kc.processRecords(output.Records, handler); err != nil {
					cErr.Printf("Error processing records from shard %s: %+v \n", shardId, err)
				}
			}

			// Update shard iterator for next read
			shardIterator = output.NextShardIterator

			// Handle closed shard
			if shardIterator == nil {
				cErr.Printf("Shard %s has been closed \n", shardId)
				return
			}

			// Add a small delay to avoid hitting API limits
			time.Sleep(time.Second)
		}
	}
}

func (kc *KinesisConsumer) Start(handler KinesisRecordHandler) error {
	// Get all shard IDs
	shardIds, err := kc.getShardIds()
	if err != nil {
		return err
	}

	c.Printf("Found %d shards \n", len(shardIds))

	// Start a goroutine for each shard
	for _, shardId := range shardIds {
		kc.wg.Add(1)
		go kc.processShard(shardId, handler)
	}

	// Wait for all goroutines to complete
	kc.wg.Wait()
	return nil
}

func (kc *KinesisConsumer) Stop() {
	kc.cancel()
	kc.wg.Wait()
	log.Println("Consumer stopped")
}

func (consumer *KinesisConsumer) StartKinesisStreamProcessor(handler KinesisRecordHandler) {
	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumer in a goroutine
	go func() {
		c.Printf("Starting to consume from stream: %s \n", consumer.streamName)
		if err := consumer.Start(handler); err != nil {
			log.Fatalf("Error starting consumer: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	c.Println("Shutting down...")
	consumer.Stop()
}
