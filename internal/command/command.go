package command

import (
	"encoding/binary"
	"io"

	"github.com/ericluj/emq/internal/common"
)

const (
	SUB      = "SUB"
	PUB      = "PUB"
	NOP      = "NOP"
	IDENTIFY = "IDENTIFY"
)

type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
}

func SubscribeCmd(topic string, channel string) *Command {
	var params = [][]byte{[]byte(topic), []byte(channel)}
	return &Command{Name: []byte(SUB), Params: params, Body: nil}
}

func PublishCmd(topic string, body []byte) *Command {
	var params = [][]byte{[]byte(topic)}
	return &Command{Name: []byte(PUB), Params: params, Body: body}
}

func NopCmd() *Command {
	return &Command{[]byte(NOP), nil, nil}
}

// emqcli to emqd
func (cmd *Command) WriteTo(w io.Writer) (int64, error) {
	var (
		total int64
		err   error
		buf   [4]byte
	)

	// 发送命令名
	n, err := w.Write(cmd.Name)
	total += int64(n)
	if err != nil {
		return total, err
	}

	// 发送参数
	for _, param := range cmd.Params {
		n, err := w.Write(common.SeparatorBytes) // 空格分开
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write(param)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	// 发送消息内容前换行
	n, err = w.Write(common.NewLineBytes)
	total += int64(n)
	if err != nil {
		return total, err
	}

	// 发送消息内容
	if cmd.Body != nil {
		bufs := buf[:]
		// 大端字节序发送内容长度
		binary.BigEndian.PutUint32(bufs, uint32(len(cmd.Body)))
		n, err = w.Write(bufs)
		total += int64(n)
		if err != nil {
			return total, err
		}
		// 发送消息内容
		n, err = w.Write(cmd.Body)
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
