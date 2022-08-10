package emqd

import (
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

func (p *Protocol) IDENTITY(client *Client, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != common.ClientInit {
		return nil, fmt.Errorf("cannot IDENTIFY in current state")
	}

	bodyLen, err := protocol.ReadDataSize(client.conn)
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
	_, err = io.ReadFull(client.conn, body)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to read body")
	}

	var identifyData identifyData
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, fmt.Errorf("IDENTIFY failed to decode JSON body")
	}

	log.Infof("PROTOCOL: [%v] %+v", client, identifyData)

	// TODO: 这里是需要有一些identify的数据处理返回的

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

	bodyLen, err := protocol.ReadDataSize(client.conn)
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
	_, err = io.ReadFull(client.conn, messageBody)
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
	if atomic.LoadInt32(&client.State) != common.ClientInit {
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

	atomic.StoreInt32(&client.State, common.ClientSubscribed)
	client.Channel = channel
	client.SubEventChan <- channel

	return common.OKBytes, nil
}

func (p *Protocol) NOP(client *Client, params [][]byte) ([]byte, error) {
	log.Infof("NOP")
	return nil, nil
}
