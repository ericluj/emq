package emqcli

import (
	"encoding/binary"
	"fmt"

	"github.com/ericluj/emq/internal/common"
)

type MessageID [common.MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	EMQDAddress string
}

func (m *Message) Finish() {

}

// 数据格式：timestamp(8byte) + attempts(2byte) + messageID(16byte) + body
func DecodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < 10+common.MsgIDLength {
		return nil, fmt.Errorf("not enough data to decode valid message")
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+common.MsgIDLength])
	msg.Body = b[10+common.MsgIDLength:]

	return &msg, nil
}
