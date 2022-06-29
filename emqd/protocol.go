package emqd

import (
	"bufio"
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

		log.Infof(string(line))
	}

	close(c.ExitChan)

	return err
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
