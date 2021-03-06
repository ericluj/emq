package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

type Client interface {
	Close() error
}

type Protocol interface {
	NewClient(net.Conn) Client
	IOLoop(Client) error
}

func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	// 一点思考：为什么只有size和type需要转大端字节序而data不用呢，因为它俩是数字，而data是字符串（data中的数字是字符串形式的）
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4 // 长度包含 type+data

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}

func SendResponse(w io.Writer, data []byte) (int, error) {
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, nil
	}

	n, err := w.Write(data)
	if err != nil {
		return 4, err
	}

	return (n + 4), err
}
