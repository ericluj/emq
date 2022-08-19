package emqd

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	log "github.com/ericluj/elog"
	"github.com/ericluj/emq/internal/common"
	"github.com/ericluj/emq/internal/protocol"
)

func (p *Protocol) IDENTITY(client *Client, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.state) != common.ClientInit {
		return nil, fmt.Errorf("IDENTITY: cannot in current state")
	}

	frameType, body, err := protocol.ReadFrameData(client.reader)
	if err != nil {
		return nil, fmt.Errorf("IDENTITY: failed to read body")
	}
	if frameType == common.FrameTypeError {
		return nil, fmt.Errorf("IDENTITY: failed to read body: %s", body)
	}

	var identifyData IdentifyData
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, fmt.Errorf("IDENTITY: failed to decode JSON body")
	}

	log.Infof("PROTOCOL: %s, %v", client.conn.RemoteAddr(), identifyData)

	// TODO: 这里是需要有一些identify的数据处理返回的

	err = p.Send(client, common.FrameTypeResponse, common.OKBytes)
	if err != nil {
		return nil, fmt.Errorf("IDENTITY: send FrameTypeResponse error: %v", err)
	}

	return nil, nil
}

func (p *Protocol) PUB(client *Client, params [][]byte) ([]byte, error) {
	if len(params) < 2 {
		return nil, fmt.Errorf("PUB: insufficient number of parameters")
	}
	topicName := string(params[1])

	frameType, body, err := protocol.ReadFrameData(client.reader)
	if err != nil {
		return nil, fmt.Errorf("PUB: failed to read body")
	}
	if frameType == common.FrameTypeError {
		return nil, fmt.Errorf("IDENTITY: failed to read body: %s", body)
	}

	topic := p.emqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), body)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, fmt.Errorf("PUB: PutMessage error: %v", err)
	}

	return common.OKBytes, nil
}

func (p *Protocol) SUB(client *Client, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.state) != common.ClientInit {
		return nil, fmt.Errorf("SUB: cannot in current state")
	}

	if len(params) < 2 {
		return nil, fmt.Errorf("SUB: insufficient number of parameters")
	}

	topicName := string(params[1])
	channelName := string(params[2])

	// TODO:特殊情况待处理
	topic := p.emqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	err := channel.AddClient(client)
	if err != nil {
		return nil, fmt.Errorf("SUB: AddClient error: %v", err)
	}

	// 修改client的状态
	atomic.StoreInt32(&client.state, common.ClientSubscribed)
	// 订阅的channel
	client.channel = channel
	// 事件通知
	client.subEventChan <- channel

	return common.OKBytes, nil
}

func (p *Protocol) NOP(client *Client, params [][]byte) ([]byte, error) {
	log.Infof("NOP")
	return common.OKBytes, nil
}
