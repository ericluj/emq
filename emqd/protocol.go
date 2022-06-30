package emqd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	log "github.com/ericluj/elog"
)

const (
	ProtoMagic        = "  V1"
	defaultBufferSize = 16 * 1024
)

var separatorBytes = []byte(" ")

// 协议结构体 用来组织emqd和client的关联处理
type Protocol struct {
	emqd *EMQD
}

func (p *Protocol) NewClient(conn net.Conn, emqd *EMQD) *Client {
	clientID := atomic.AddInt64(&p.emqd.clientIDSequence, 1)
	c := &Client{
		ID:       clientID,
		emqd:     emqd,
		Conn:     conn,
		Reader:   bufio.NewReaderSize(conn, defaultBufferSize),
		Writer:   bufio.NewWriterSize(conn, defaultBufferSize),
		ExitChan: make(chan int),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

func (p *Protocol) IOLoop(c *Client) error {
	var err error

	// client的一个goruntine，用来执行消息处理分发等相关操作
	messagePumpStartedChan := make(chan bool)
	go p.MessagePump(c, messagePumpStartedChan)
	//保证在messagePump的初始化完成后才往下执行
	<-messagePumpStartedChan

	// 接受cmd并执行操作
	for {
		line, err := c.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF { // 如果结尾了，那么继续循环
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// 去掉'\n'
		line = line[:len(line)-1]
		// 去掉'\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes)
		log.Infof("PROTOCOL: [%s] %s", c.RemoteAddr(), params)

		var response []byte
		response, err = p.Exec(c, params)
		if err != nil {

		}
		if response != nil {

		}
	}

	close(c.ExitChan)

	return err
}

func (p *Protocol) Exec(client *Client, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	}
	return nil, fmt.Errorf("invalid command %s", params[0])
}

func (p *Protocol) PUB(client *Client, params [][]byte) ([]byte, error) {
	if len(params) < 2 {
		return nil, fmt.Errorf("PUB insufficient number of parameters")
	}
	topicName := string(params[1])

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, fmt.Errorf("PUB failed to read message body size")
	}
	if bodyLen <= 0 {
		return nil, fmt.Errorf("PUB invalid message body size %d", bodyLen)
	}
	if int64(bodyLen) > p.emqd.getOpts().MaxMsgSize {
		return nil, fmt.Errorf("PUB message too big %d > %d", bodyLen, p.emqd.getOpts().MaxMsgSize)
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, fmt.Errorf("PUB failed to read message body")
	}

	topic := p.emqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("PUB failed " + err.Error())
	}

	return []byte("OK"), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp) // 读取len(tmp)长度的数据
	if err != nil {
		return 0, err
	}
	// 大端字节序将其转换为数字
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func (p *Protocol) MessagePump(client *Client, startedChan chan bool) {
	var (
		err           error
		memoryMsgChan chan *Message // 消息队列
	)

	memoryMsgChan = client.Channel.memoryMsgChan

	close(startedChan)

	for {
		select {
		case msg := <-memoryMsgChan:
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
		case <-client.ExitChan:
			return
		}
	}

exit:
	log.Infof("PROTOCOL: [%s] exiting messagePump", client)
	// TODO: 一些结束操作
	if err != nil {
		log.Infof("PROTOCOL: [%s] messagePump error - %s", client, err)
	}
}

func (p *Protocol) SendMessage(client *Client, msg *Message) error {
	return nil
}
