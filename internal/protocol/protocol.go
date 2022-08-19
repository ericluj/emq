package protocol

import (
	"encoding/binary"
	"errors"
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
func readDataSize(r io.Reader) (int32, error) {
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

func readData(r io.Reader) ([]byte, error) {
	// 读出数据长度
	dataSize, err := readDataSize(r)
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

// 读取包含frameType的data
func ReadFrameData(r io.Reader) (int32, []byte, error) {
	data, err := readData(r)
	if err != nil {
		return 0, nil, err
	}

	if len(data) < 4 {
		return 0, nil, errors.New("length of data is too small < 4")
	}

	frameType := int32(binary.BigEndian.Uint32(data[:4]))
	resp := data[4:]

	return frameType, resp, nil
}
