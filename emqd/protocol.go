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

	// client的一个goruntine，用来执行一些操作
	messagePumpStartedChan := make(chan bool)
	go p.MessagePump(c, messagePumpStartedChan)
	//保证在messagePump的初始化完成后才往下执行
	<-messagePumpStartedChan

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
	close(startedChan)

	for {
		select {
		case <-client.ExitChan:
			return
		}
	}
}
