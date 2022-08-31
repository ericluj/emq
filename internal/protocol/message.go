package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"

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

// 解析字节流为Message结构体
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

func GetMessageID(p []byte) (MessageID, error) {
	var m MessageID

	if len(p) != common.MsgIDLength {
		return m, errors.New("invalid Message ID")
	}

	copy(m[:], p)
	return m, nil
}
