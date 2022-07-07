package emqd

import (
	"io"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts
)

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var total int64
	return total, nil
}
