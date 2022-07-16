package emqd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"emq/internal/common"

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
		ID:           clientID,
		emqd:         emqd,
		Conn:         conn,
		Reader:       bufio.NewReaderSize(conn, defaultBufferSize),
		Writer:       bufio.NewWriterSize(conn, defaultBufferSize),
		ExitChan:     make(chan int),
		SubEventChan: make(chan *Channel, 1),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

func (p *Protocol) IOLoop(client *Client) error {
	var (
		err  error
		line []byte
	)

	// client的一个goruntine，用来执行消息处理分发等相关操作
	messagePumpStartedChan := make(chan bool)
	go p.MessagePump(client, messagePumpStartedChan)
	//保证在messagePump的初始化完成后才往下执行
	<-messagePumpStartedChan

	// 接受cmd并执行操作
	for {
		client.SetReadDeadline(time.Now().Add(common.ReadTimeout))

		line, err = client.Reader.ReadSlice('\n')
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
		log.Infof("PROTOCOL: [%s] %s", client.RemoteAddr(), params)

		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			// TODO: 处理内部error
		}
		if response != nil {
			err = p.Send(client, common.FrameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	log.Infof("PROTOCOL: [%s] exiting ioloop", client.RemoteAddr())
	close(client.ExitChan)
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	return err
}

func (p *Protocol) Exec(client *Client, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTITY(client, params)
	}

	switch {
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	}
	return nil, fmt.Errorf("invalid command %s", params[0])
}

func (p *Protocol) IDENTITY(client *Client, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, fmt.Errorf("cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.emqd.getOpts().MaxMsgSize {
		return nil, fmt.Errorf("IDENTIFY body too big %d > %d", bodyLen, p.emqd.getOpts().MaxMsgSize)
	}

	if bodyLen <= 0 {
		return nil, fmt.Errorf("IDENTIFY invalid body size %d", bodyLen)
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body")
	}

	var identifyData identifyData
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to decode JSON body")
	}

	log.Infof("PROTOCOL: [%s] %+v", client, identifyData)

	// TODO: 这里是需要有一些identify的数据处理返回的

	// tls协议
	log.Infof("PROTOCOL: [%s] upgrading connection to TLS", client)
	err = client.UpgradeTLS()
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed " + err.Error())
	}

	err = p.Send(client, common.FrameTypeResponse, common.OKBytes)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed " + err.Error())
	}

	return nil, nil
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

	return common.OKBytes, nil
}

func (p *Protocol) SUB(client *Client, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, fmt.Errorf("cannot SUB in current state")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	channelName := string(params[2])

	// TODO:特殊情况待处理
	topic := p.emqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	err := channel.AddClient(client)
	if err != nil {
		return nil, fmt.Errorf("SUB failed " + err.Error())
	}

	atomic.StoreInt32(&client.State, stateSubscribed)
	client.Channel = channel
	client.SubEventChan <- channel

	return common.OKBytes, nil
}

func (p *Protocol) NOP(client *Client, params [][]byte) ([]byte, error) {
	log.Infof("NOP")
	return nil, nil
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
	log.Infof("PROTOCOL: [%s] exiting messagePump", client)
	// TODO: 一些结束操作
	if err != nil {
		log.Infof("PROTOCOL: [%s] messagePump error - %s", client, err)
	}
}

func (p *Protocol) SendMessage(client *Client, msg *Message) error {
	log.Infof("PROTOCOL: writing msg(%s) to client(%d) - %s", msg.ID, client.ID, msg.Body)

	buf := bufferPoolGet()
	defer bufferPoolPut(buf)

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

	_, err := p.SendFramedResponse(client, frameType, data)
	if err != nil {
		return err
	}

	return nil
}

func (p *Protocol) SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	// 一点思考：为什么只有size和type需要转大端字节序而data不用呢，因为它俩是数字，而data是字符串（data中的数字是字符串形式的）
	beBuf := make([]byte, 4)
	size := uint32(len(data)) + 4 // 长度包含 type+data

	binary.BigEndian.PutUint32(beBuf, size)
	n, err := w.Write(beBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(beBuf, uint32(frameType))
	n, err = w.Write(beBuf)
	if err != nil {
		return n + 4, err
	}

	n, err = w.Write(data)
	return n + 8, err
}
