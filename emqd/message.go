package emqd

import (
	"time"

	"github.com/ericluj/emq/internal/protocol"
)

type Message struct {
	ClientID int64
	*protocol.Message
}

func NewMessage(id protocol.MessageID, body []byte) *Message {
	return &Message{
		Message: &protocol.Message{
			ID:        id,
			Body:      body,
			Timestamp: time.Now().UnixNano(),
		},
	}
}
