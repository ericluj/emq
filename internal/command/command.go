package command

import (
	"io"

	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

const (
	PING       = "PING"
	SUB        = "SUB"
	PUB        = "PUB"
	NOP        = "NOP"
	IDENTIFY   = "IDENTIFY"
	REGISTER   = "REGISTER"
	UNREGISTER = "UNREGISTER"
)

type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
}

func PingCmd() *Command {
	return &Command{Name: []byte(PING), Params: nil, Body: nil}
}

func RegisterCmd(topic, channel string) *Command {
	params := [][]byte{}
	params = append(params, []byte(topic))
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{Name: []byte(REGISTER), Params: params, Body: nil}
}

func UnRegisterCmd(topic, channel string) *Command {
	params := [][]byte{}
	params = append(params, []byte(topic))
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{Name: []byte(UNREGISTER), Params: params, Body: nil}
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
func (cmd *Command) Write(w io.Writer) error {
	// 发送命令名
	_, err := w.Write(cmd.Name)
	if err != nil {
		return err
	}

	// 发送参数
	for _, param := range cmd.Params {
		_, err = w.Write(common.SeparatorBytes) // 空格分开
		if err != nil {
			return err
		}
		_, err = w.Write(param)
		if err != nil {
			return err
		}
	}

	// 发送消息内容前换行
	_, err = w.Write(common.NewLineBytes)
	if err != nil {
		return err
	}

	// 发送消息内容
	if cmd.Body != nil {
		err = protocol.SendData(w, cmd.Body)
		if err != nil {
			return err
		}
	}

	return nil
}
