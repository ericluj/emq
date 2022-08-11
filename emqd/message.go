package emqd

import (
	"encoding/binary"
	"time"

	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/util"
)

type MessageID [common.MsgIDLength]byte

type Message struct {
	ID        MessageID
	Timestamp int64
	Attempts  uint16
	Body      []byte
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// 构造message数据流
func (m *Message) Bytes() ([]byte, error) {
	buf := util.BufferPoolGet()
	defer util.BufferPoolPut(buf)

	err := binary.Write(buf, binary.BigEndian, uint64(m.Timestamp))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buf, binary.BigEndian, m.Attempts)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(m.ID[:])
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(m.Body)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
