package main

import "fmt"
import entity "github.com/debojitroy/aws-queue-tasks-consume/internal/services/entity"

func main() {
    _entity := entity.NewEntity(100)

		fmt.Printf("EntityId: %s \n", _entity.GetId())
		fmt.Printf("Message Count: %d \n", _entity.GetMessageCount())

		// for loop to iterate over the messages
		for _, message := range _entity.GetMessages() {
				fmt.Printf("Message:: %s \n", message)
		}
}
