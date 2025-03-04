# AWS Queue Consume Tasks

Sample App to demonstrate distributed, event driven producer/consumer model using AWS Services

## Solution Logic

1. Read from command line
   1. Number of entities to generate
   2. Region
   3. SQS Url
   4. Dynamo table
   5. Kinesis for Dynamo Streams
2. Randomly generate Entity Ids
3. Randomly generate messages for each Entity
4. Enter the values in DynamoDB (entity_id + number of records)
5. Create Producer to publish the messages to SQS (one message for each row)
6. Create Consumer to consume messages from queue
   1. Print incoming message
   2. Decrement Atomic counter in DynamoDB
7. Publish results from DynamoDB to streams
   1. Subscribe to Stream
   2. Once value becomes zero, print message
   3. Once all combos are done, exit program

## Solution Design

![Solution Design Diagram](./docs/architecture.png)
