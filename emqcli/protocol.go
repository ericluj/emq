package emqcli

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ericluj/emq/internal/protocol"
)

func ReadUnpackedResponse(r io.Reader) (int32, []byte, error) {
	resp, err := protocol.ReadData(r)
	if err != nil {
		return -1, nil, err
	}
	return UnpackResponse(resp)
}

// 解析data数据 type + content
func UnpackResponse(response []byte) (int32, []byte, error) {
	if len(response) < 4 {
		return -1, nil, fmt.Errorf("length of response is too small")
	}

	return int32(binary.BigEndian.Uint32(response)), response[4:], nil
}
