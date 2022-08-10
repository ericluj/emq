package emqd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/command"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
	"github.com/ericluj/emq/internal/util"
)

// 协议结构体 用来组织emqd和client的关联处理
type Protocol struct {
	emqd *EMQD
}

func (p *Protocol) NewClient(conn net.Conn) protocol.Client {
	id := atomic.AddInt64(&p.emqd.idGenerator, 1)
	c := NewClient(id, conn)
	return c
}

func (p *Protocol) IOLoop(c protocol.Client) error {
	var (
		err  error
		line []byte
	)

	client := c.(*Client)

	// client的一个goruntine，用来执行消息处理分发等相关操作
	messagePumpStartedChan := make(chan bool)
	go p.MessagePump(client, messagePumpStartedChan)
	//保证在messagePump的初始化完成后才往下执行
	<-messagePumpStartedChan

	// 接受cmd并执行操作
	for {
		err = client.conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
		if err != nil {
			log.Infof("error: %v", err)
			break
		}

		reader := bufio.NewReaderSize(client.conn, common.DefaultBufferSize)
		line, err = reader.ReadSlice('\n')
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
		params := bytes.Split(line, common.SeparatorBytes)
		log.Infof("PROTOCOL: [%v] %s", client.conn.RemoteAddr(), params)

		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			// TODO: 处理内部error
			log.Infof("error: %v", err)
		}
		if response != nil {
			err = p.Send(client, common.FrameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	log.Infof("PROTOCOL: [%s] exiting ioloop", client.conn.RemoteAddr())
	close(client.ExitChan)
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	return err
}

func (p *Protocol) Exec(client *Client, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte(command.IDENTIFY)) {
		return p.IDENTITY(client, params)
	}

	switch {
	case bytes.Equal(params[0], []byte(command.PUB)):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte(command.SUB)):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte(command.NOP)):
		return p.NOP(client, params)
	}
	return nil, fmt.Errorf("invalid command %s", params[0])
}

func (p *Protocol) MessagePump(client *Client, startedChan chan bool) {
	var (
		err           error
		subChannel    *Channel      // 订阅的channel
		memoryMsgChan chan *Message // 消息队列
	)
	heartbeatTicker := time.NewTicker(common.HeartbeatTimeout)
	heartbeatChan := heartbeatTicker.C
	subEventChan := client.SubEventChan

	close(startedChan)

	for {
		if subChannel != nil {
			memoryMsgChan = subChannel.memoryMsgChan
		}
		select {
		case subChannel = <-subEventChan: // 订阅事件发生，不能再次订阅了
			subEventChan = nil
		case msg := <-memoryMsgChan:
			err = p.SendMessage(client, msg)
			if err != nil {
				goto exit
			}
		case <-heartbeatChan:
			log.Infof("send heartbeat")
			err = p.Send(client, common.FrameTypeResponse, common.HeartbeatBytes)
			if err != nil {
				goto exit
			}
		case <-client.ExitChan:
			return
		}
	}

exit:
	log.Infof("PROTOCOL: [%v] exiting messagePump", client)
	// TODO: 一些结束操作
	if err != nil {
		log.Infof("PROTOCOL: [%v] messagePump error - %s", client, err)
	}
}

func (p *Protocol) SendMessage(client *Client, msg *Message) error {
	log.Infof("PROTOCOL: writing msg(%s) to client(%d) - %s", msg.ID, client.ID, msg.Body)

	buf := util.BufferPoolGet()
	defer util.BufferPoolPut(buf)

	// 构造数据流
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	// 发送数据
	err = p.Send(client, common.FrameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *Protocol) Send(client *Client, frameType int32, data []byte) error {
	client.writeLock.Lock()
	defer client.writeLock.Unlock()

	err := protocol.SendFrameData(client.conn, frameType, data)
	if err != nil {
		return err
	}

	return nil
}
