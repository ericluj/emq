package emqcli

import (
	"encoding/binary"
	"fmt"
	"io"
)

var (
	MagicV1 = []byte("  V1")
)

func ReadUnpackedResponse(r io.Reader) (int32, []byte, error) {
	resp, err := ReadResponse(r)
	if err != nil {
		return -1, nil, err
	}
	return UnpackResponse(resp)
}

func ReadResponse(r io.Reader) ([]byte, error) {
	// 读出data长度（4个字节）
	var msgSize int32
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}
	if msgSize < 0 {
		return nil, fmt.Errorf("response msg size is negative: %v", msgSize)
	}

	// 根据长度读出data内容
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// 解析data数据 type + content
func UnpackResponse(response []byte) (int32, []byte, error) {
	if len(response) < 4 {
		return -1, nil, fmt.Errorf("length of response is too small")
	}

	return int32(binary.BigEndian.Uint32(response)), response[4:], nil
}
