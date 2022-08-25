package emqd

import (
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
	// 保证在messagePump的初始化完成后才往下执行
	<-messagePumpStartedChan

	// 接受cmd并执行操作
	for {
		// 设置deadline
		err = client.conn.SetReadDeadline(time.Now().Add(common.ReadTimeout))
		if err != nil {
			log.Infof("SetReadDeadline error: %v", err)
			return err
		}

		line, err = client.reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				log.Infof("io.EOF")
				err = nil
			} else {
				err = fmt.Errorf("read command error: %v", err)
			}
			break
		}

		// 去掉'\n'（返回的line包含'\n'）
		line = line[:len(line)-1]
		// 去掉'\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		// 空格拆分
		params := bytes.Split(line, common.SeparatorBytes)
		log.Infof("PROTOCOL: %s, %s", client.conn.RemoteAddr(), params)

		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			log.Infof("Exec error: %v", err)
			sendErr := p.Send(client, common.FrameTypeError, []byte(err.Error()))
			if sendErr != nil {
				err = fmt.Errorf("send FrameTypeError error: %v", err)
				break
			}

			// 如果是严重错误，那么断开连接，普通错误则继续执行循环
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}
		if response != nil {
			err = p.Send(client, common.FrameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("send FrameTypeResponse error: %v", err)
				break
			}
		}
	}

	log.Infof("PROTOCOL: %s exiting IOLoop", client.conn.RemoteAddr())

	// client关闭
	close(client.exitChan)
	// 如果client有订阅channel，那么把关联关系删除
	if client.channel != nil {
		client.channel.RemoveClient(client.ID)
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
	case bytes.Equal(params[0], []byte(command.REQ)):
		return p.REQ(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, protocol.ErrTypeInvalid, fmt.Sprintf("invalid command %s", params[0]))
}

func (p *Protocol) MessagePump(client *Client, startedChan chan bool) {
	var (
		memoryMsgChan chan *Message // 消息队列（初始为nil，不会被执行）
		subChannel    *Channel      // 订阅的channel
	)
	heartbeatTicker := time.NewTicker(common.HeartbeatTimeout)

	close(startedChan)

	for {
		select {
		// 订阅
		case subChannel = <-client.subEventChan:
			client.subEventChan = nil                // 订阅事件发生，置为nil让它不能再次订阅
			memoryMsgChan = subChannel.memoryMsgChan // 用memoryMsgChan是为了防止空指针
		// 获取消息
		case msg := <-memoryMsgChan:
			msg.Attempts++
			subChannel.StartInFlight(msg, client.ID)
			err := p.SendMessage(client, msg)
			if err != nil {
				log.Infof("PROTOCOL: SendMessage %s error: %v", client.conn.RemoteAddr(), err)
				goto exit
			}
		// 心跳
		case <-heartbeatTicker.C:
			log.Infof("send heartbeat to %s", client.conn.RemoteAddr())
			err := p.Send(client, common.FrameTypeResponse, common.HeartbeatBytes)
			if err != nil {
				log.Infof("PROTOCOL: heartbeat %s error: %v", client.conn.RemoteAddr(), err)
				goto exit
			}
		// 退出
		case <-client.exitChan:
			goto exit
		}
	}

exit:
	log.Infof("PROTOCOL: %s exiting messagePump", client.conn.RemoteAddr())
	heartbeatTicker.Stop()
}

func (p *Protocol) SendMessage(client *Client, msg *Message) error {
	log.Infof("PROTOCOL: SendMessage to client %s, id %s, timestamp %d, attempts %d, body %s", client.conn.RemoteAddr(), msg.ID, msg.Timestamp, msg.Attempts, msg.Body)

	// 发送数据
	buf, err := msg.Bytes()
	if err != nil {
		return err
	}
	err = p.Send(client, common.FrameTypeMessage, buf)
	if err != nil {
		return err
	}

	return nil
}

func (p *Protocol) Send(client *Client, frameType int32, data []byte) error {
	client.mtx.Lock()
	defer client.mtx.Unlock()

	err := client.conn.SetWriteDeadline(time.Now().Add(common.WriteTimeout))
	if err != nil {
		return err
	}

	err = protocol.SendFrameData(client.conn, frameType, data)
	if err != nil {
		return err
	}

	return nil
}
