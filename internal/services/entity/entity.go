package entity

import (
	"fmt"
	"math/rand"
	"time"
)

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
}

func NewEntity(maxMessages int) *Entity {
	r := rand.New(source)
	return &Entity{
		id:           randSeq(r, 30),
		messageCount: r.Intn(maxMessages),
	}
}

func (e *Entity) GetId() string {
	return e.id
}

func (e *Entity) GetMessageCount() int {
	return e.messageCount
}

func (e *Entity) GetMessages() []string {
	var result []string
	for i := 0; i < e.messageCount; i++ {
		result = append(result, fmt.Sprintf("Entity: %s | Message # %d", e.id, i))
	}
	return result
}
