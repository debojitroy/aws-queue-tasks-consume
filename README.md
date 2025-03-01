# aws-queue-tasks-consume
Sample App to demonstrate distributed, event driven producer/consumer model using AWS Services

## Logic

1. Read from command line
   1. Region
   2. SQS Url
   3. Dynamo table
   4. Number of entities to generate
   5. Max number of messages per combination
2. Randomly generate Entity Ids
3. Create a function which calls "entity_id" and tells how many rows
4. Enter the values in DynamoDB (entity_id + number of records)
5. Create Producer to publish the messages to SQS (one message for each row)
6. Create Consumer to consume messages from queue
   1. Print incoming message
   2. Decrement Atomic counter in DynamoDB
7. Publish results from DynamoDB to streams
   1. Subscribe to Stream
   2. Once value becomes zero, print message
   3. Once all combos are done, exit program
