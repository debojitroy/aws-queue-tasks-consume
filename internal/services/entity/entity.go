package entity

import (
	"fmt"
	"math/rand"
	"time"
)

var _entityTracker = make(map[string]struct{}, 0)

var source = rand.NewSource(time.Now().UnixNano())
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(r *rand.Rand, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

type Entity struct {
	id           string
	messageCount int
	messages     []string
}

func AddEntity(entityId string) {
	_entityTracker[entityId] = struct{}{}
}

func RemoveEntity(entityId string) {
	delete(_entityTracker, entityId)
}

func GetEntityCount() int {
	return len(_entityTracker)
}

func NewEntity(maxMessages int) *Entity {
	r := rand.New(source)
	entity_id := randSeq(r, 30)
	messageCount := r.Intn(maxMessages)

	var messages []string
	for i := 0; i < messageCount; i++ {
		messages = append(messages, fmt.Sprintf("Entity: %s | Message # %d", entity_id, i))
	}

	return &Entity{
		id:           entity_id,
		messageCount: messageCount,
		messages:     messages,
	}
}

func (e *Entity) GetId() string {
	return e.id
}

func (e *Entity) GetMessageCount() int {
	return e.messageCount
}

func (e *Entity) GetMessages() []string {
	return e.messages
}
