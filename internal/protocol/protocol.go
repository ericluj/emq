package protocol

import (
	"encoding/binary"
	"fmt"
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

// 大端字节序将其转换为数字
func ReadDataSize(r io.Reader) (int32, error) {
	var dataSize int32
	err := binary.Read(r, binary.BigEndian, &dataSize)
	if err != nil {
		return 0, err
	}
	if dataSize < 0 {
		return 0, fmt.Errorf("data size is error: %d", dataSize)
	}

	return dataSize, nil
}

func ReadData(r io.Reader) ([]byte, error) {
	// 读出数据长度
	dataSize, err := ReadDataSize(r)
	if err != nil {
		return nil, err
	}

	// 根据长度读出data内容
	buf := make([]byte, dataSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func SendData(w io.Writer, data []byte) error {
	// 大端字节序写入数据长度
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return err
	}

	// 写入data内容
	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return err
}

// 发送包含frameType的data
func SendFrameData(w io.Writer, frameType int32, data []byte) error {
	// 大端字节序写入数据长度（type+data)
	err := binary.Write(w, binary.BigEndian, int32(4+len(data)))
	if err != nil {
		return err
	}

	// 大端字节序写入frameType
	err = binary.Write(w, binary.BigEndian, frameType)
	if err != nil {
		return err
	}

	// 写入data内容
	_, err = w.Write(data)
	if err != nil {
		return err
	}

	return err
}
